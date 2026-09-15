import time
import uuid

import pytest
import redis.asyncio as aioredis

from config import settings
from processor.stream_processor import StreamProcessor
from producer.schemas import ServiceHealthEvent
from storage.database import Database

pytestmark = pytest.mark.asyncio

TEST_STREAM = "pulsegrid:test:events"
TEST_DLQ = "pulsegrid:test:events:dlq"
TEST_GROUP = "pulsegrid-test-group"


def make_valid_event(instance="svc-a-00") -> dict:
    return ServiceHealthEvent(
        event_id=str(uuid.uuid4()),
        timestamp=time.time(),
        source="svc-a",
        instance_id=instance,
        cpu_percent=40.0,
        memory_percent=30.0,
        latency_ms=50.0,
        error_rate=0.01,
        requests_per_sec=100.0,
        status="ok",
    ).to_redis_fields()


@pytest.fixture
async def redis_client():
    r = aioredis.Redis(host=settings.redis_host, port=settings.redis_port, db=settings.redis_db, decode_responses=True)
    await r.delete(TEST_STREAM)
    await r.delete(TEST_DLQ)
    try:
        await r.xgroup_destroy(TEST_STREAM, TEST_GROUP)
    except Exception:
        pass
    yield r
    await r.delete(TEST_STREAM)
    await r.delete(TEST_DLQ)
    await r.aclose()


@pytest.fixture
def processor(temp_db_path, redis_client, monkeypatch):
    monkeypatch.setattr(settings, "stream_name", TEST_STREAM, raising=False)
    monkeypatch.setattr(settings, "dead_letter_stream", TEST_DLQ, raising=False)
    monkeypatch.setattr(settings, "consumer_group", TEST_GROUP, raising=False)
    db = Database(db_path=temp_db_path)
    return StreamProcessor(db=db, redis_client=redis_client)


async def test_valid_event_is_processed_and_stored(processor, redis_client):
    await processor.ensure_group()
    await redis_client.xadd(TEST_STREAM, make_valid_event())
    await processor.run(max_messages=1)
    events = processor.db.get_recent_events()
    assert len(events) == 1
    assert processor._processed == 1


async def test_malformed_event_goes_to_dlq_not_crash(processor, redis_client):
    await processor.ensure_group()
    await redis_client.xadd(TEST_STREAM, {"event_id": "bad", "timestamp": "oops", "source": "x"})
    await redis_client.xadd(TEST_STREAM, make_valid_event())
    await processor.run(max_messages=1)
    assert processor._validation_errors == 1
    dlq_len = await redis_client.xlen(TEST_DLQ)
    assert dlq_len == 1
    # the valid event after the bad one should still be processed
    assert processor._processed == 1


async def test_duplicate_event_id_is_deduplicated(processor, redis_client):
    await processor.ensure_group()
    fields = make_valid_event()
    await redis_client.xadd(TEST_STREAM, fields)
    await redis_client.xadd(TEST_STREAM, fields)  # exact duplicate event_id
    await processor.run(max_messages=1)
    events = processor.db.get_recent_events()
    assert len(events) == 1
    assert processor._duplicates == 1


async def test_aggregator_flushes_and_persists(processor, redis_client, monkeypatch):
    monkeypatch.setattr(settings, "aggregation_window_sec", 1, raising=False)
    processor.aggregator.window_sec = 1
    processor.aggregator._window_start -= 2  # force the window to already be due
    await processor.ensure_group()
    for _ in range(3):
        await redis_client.xadd(TEST_STREAM, make_valid_event())
    await processor.run(max_messages=3)
    aggs = processor.db.get_aggregates()
    assert len(aggs) >= 1
    assert aggs[0]["event_count"] >= 1

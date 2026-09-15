"""
StreamProcessor: the heart of PulseGrid's real-time pipeline.

Responsibilities
-----------------
1. Consume events from the Redis Stream via a consumer group (so multiple
   processor instances could share the load and survive individual crashes).
2. Validate + normalize each event; malformed events are routed to a dead
   letter stream instead of crashing the pipeline.
3. Deduplicate using idempotent SQLite inserts keyed by event_id (protects
   against Redis at-least-once delivery producing duplicate processing).
4. Run rolling z-score / EWMA anomaly detection per (instance, metric).
5. Roll events into tumbling-window aggregates for historical analytics.
6. Persist everything to SQLite.
7. Publish a compact JSON update onto a Redis pub/sub channel so any number
   of API/dashboard processes can broadcast live updates over WebSocket
   without being coupled to the processor's process.
8. Periodically reclaim pending (unacked) messages left behind by a crashed
   consumer via XAUTOCLAIM, and detect sources that have gone silent
   (missed heartbeats).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Optional

import redis.asyncio as aioredis

from config import settings
from processor.aggregator import TumblingAggregator
from processor.anomaly_detector import AnomalyDetector
from producer.schemas import EventValidationError, validate_raw_event
from storage.database import Database

logger = logging.getLogger("pulsegrid.processor")

UPDATE_CHANNEL = "pulsegrid:updates"


class StreamProcessor:
    def __init__(self, db: Optional[Database] = None, redis_client: Optional[aioredis.Redis] = None):
        self.redis = redis_client or aioredis.Redis(
            host=settings.redis_host, port=settings.redis_port, db=settings.redis_db, decode_responses=True
        )
        self.db = db or Database()
        self.detector = AnomalyDetector()
        self.aggregator = TumblingAggregator(settings.aggregation_window_sec)

        self._processed = 0
        self._duplicates = 0
        self._validation_errors = 0
        self._latencies: list[float] = []
        self._retry_counts: dict[str, int] = {}
        self._running = False

    # ------------------------------------------------------------- lifecycle
    async def ensure_group(self) -> None:
        try:
            await self.redis.xgroup_create(settings.stream_name, settings.consumer_group, id="0", mkstream=True)
            logger.info("Created consumer group '%s' on stream '%s'", settings.consumer_group, settings.stream_name)
        except aioredis.ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                logger.debug("Consumer group already exists")
            else:
                raise

    async def _move_to_dlq(self, msg_id: str, fields: dict, reason: str) -> None:
        fields = dict(fields)
        fields["_dlq_reason"] = reason
        fields["_original_id"] = msg_id
        await self.redis.xadd(settings.dead_letter_stream, fields, maxlen=50_000, approximate=True)
        logger.warning("Moved malformed/failed event %s to DLQ: %s", msg_id, reason)

    # --------------------------------------------------------------- process
    async def _process_one(self, msg_id: str, fields: dict) -> None:
        start = time.time()
        try:
            event = validate_raw_event(fields)
        except EventValidationError as exc:
            self._validation_errors += 1
            await self._move_to_dlq(msg_id, fields, f"validation_error: {exc}")
            await self.redis.xack(settings.stream_name, settings.consumer_group, msg_id)
            return

        metrics = {
            "cpu": event.cpu_percent,
            "memory": event.memory_percent,
            "latency": event.latency_ms,
            "error": event.error_rate,
            "throughput": event.requests_per_sec,
        }

        anomalies = self.detector.evaluate(event.instance_id, metrics)

        inserted = self.db.insert_event(
            {
                "event_id": event.event_id,
                "timestamp": event.timestamp,
                "source": event.source,
                "instance_id": event.instance_id,
                "event_type": event.event_type,
                "cpu_percent": event.cpu_percent,
                "memory_percent": event.memory_percent,
                "latency_ms": event.latency_ms,
                "error_rate": event.error_rate,
                "requests_per_sec": event.requests_per_sec,
                "status": event.status,
                "processed_at": time.time(),
            }
        )

        if not inserted:
            self._duplicates += 1
            await self.redis.xack(settings.stream_name, settings.consumer_group, msg_id)
            return

        self.db.upsert_source_health(
            event.instance_id, event.source, event.timestamp, event.status, bool(anomalies)
        )

        anomaly_payloads = []
        for a in anomalies:
            self.db.insert_anomaly(
                {
                    "event_id": event.event_id,
                    "timestamp": event.timestamp,
                    "source": event.source,
                    "instance_id": event.instance_id,
                    "metric": a.metric,
                    "observed_value": a.observed_value,
                    "baseline_mean": a.baseline_mean,
                    "baseline_std": a.baseline_std,
                    "expected_low": a.expected_low,
                    "expected_high": a.expected_high,
                    "severity": a.severity,
                    "reason": a.reason,
                    "detection_method": a.detection_method,
                }
            )
            anomaly_payloads.append(
                {
                    "source": event.source,
                    "instance_id": event.instance_id,
                    "metric": a.metric,
                    "observed_value": a.observed_value,
                    "severity": a.severity,
                    "reason": a.reason,
                    "timestamp": event.timestamp,
                }
            )

        self.aggregator.add_event(event.source, metrics, anomaly_count=len(anomalies))

        self._processed += 1
        self._latencies.append((time.time() - start) * 1000)

        await self._publish_update(
            {
                "type": "event",
                "event": {
                    "event_id": event.event_id,
                    "timestamp": event.timestamp,
                    "source": event.source,
                    "instance_id": event.instance_id,
                    "status": event.status,
                    "cpu_percent": event.cpu_percent,
                    "memory_percent": event.memory_percent,
                    "latency_ms": event.latency_ms,
                    "error_rate": event.error_rate,
                    "requests_per_sec": event.requests_per_sec,
                },
                "anomalies": anomaly_payloads,
            }
        )

        await self.redis.xack(settings.stream_name, settings.consumer_group, msg_id)

    async def _publish_update(self, payload: dict) -> None:
        try:
            await self.redis.publish(UPDATE_CHANNEL, json.dumps(payload))
        except Exception:  # noqa: BLE001 - pub/sub failures must never break processing
            logger.exception("Failed to publish live update")

    async def _flush_aggregates_if_due(self) -> None:
        if self.aggregator.should_flush():
            rows = self.aggregator.flush()
            for row in rows:
                self.db.upsert_aggregate(row)
            if rows:
                await self._publish_update({"type": "aggregate", "windows": rows})
                logger.info("Flushed %d aggregate window(s)", len(rows))

    async def _record_stats_if_due(self, last_stat_time: float) -> float:
        if time.time() - last_stat_time >= 10:
            avg_latency = sum(self._latencies) / len(self._latencies) if self._latencies else 0.0
            self.db.record_processing_stats(self._processed, self._duplicates, self._validation_errors, avg_latency)
            logger.info(
                "Stats: processed=%d duplicates=%d errors=%d avg_latency=%.2fms",
                self._processed,
                self._duplicates,
                self._validation_errors,
                avg_latency,
            )
            self._latencies.clear()
            return time.time()
        return last_stat_time

    async def _reclaim_pending(self) -> None:
        """Fault tolerance: reclaim messages left pending by a crashed/slow consumer."""
        try:
            cursor = "0-0"
            next_cursor, claimed, _deleted = await self.redis.xautoclaim(
                settings.stream_name,
                settings.consumer_group,
                settings.consumer_name,
                min_idle_time=settings.pending_claim_idle_ms,
                start_id=cursor,
                count=20,
            )
            for msg_id, fields in claimed:
                attempts = self._retry_counts.get(msg_id, 0) + 1
                self._retry_counts[msg_id] = attempts
                if attempts > settings.max_retries:
                    await self._move_to_dlq(msg_id, fields, "max_retries_exceeded")
                    await self.redis.xack(settings.stream_name, settings.consumer_group, msg_id)
                    self._retry_counts.pop(msg_id, None)
                    continue
                logger.warning("Reclaimed pending message %s (attempt %d)", msg_id, attempts)
                await self._process_one(msg_id, fields)
        except Exception:  # noqa: BLE001
            logger.exception("Error while reclaiming pending messages")

    async def _detect_silent_sources(self) -> None:
        """Detect sources that have stopped sending heartbeats (missing/irregular events)."""
        now = time.time()
        for row in self.db.get_sources():
            elapsed = now - row["last_seen"]
            if elapsed > settings.heartbeat_timeout_sec:
                severity = "high" if elapsed > settings.heartbeat_timeout_sec * 3 else "medium"
                self.db.insert_anomaly(
                    {
                        "event_id": f"silence-{row['instance_id']}-{int(now)}",
                        "timestamp": now,
                        "source": row["source"],
                        "instance_id": row["instance_id"],
                        "metric": "heartbeat",
                        "observed_value": elapsed,
                        "baseline_mean": settings.heartbeat_timeout_sec,
                        "baseline_std": None,
                        "expected_low": None,
                        "expected_high": settings.heartbeat_timeout_sec,
                        "severity": severity,
                        "reason": f"No events received from {row['instance_id']} for {elapsed:.1f}s "
                        f"(timeout={settings.heartbeat_timeout_sec}s)",
                        "detection_method": "heartbeat_timeout",
                    }
                )
                await self._publish_update(
                    {
                        "type": "silence",
                        "source": row["source"],
                        "instance_id": row["instance_id"],
                        "elapsed": elapsed,
                        "severity": severity,
                    }
                )

    # ------------------------------------------------------------------ run
    async def run(self, max_messages: Optional[int] = None) -> None:
        await self.ensure_group()
        self._running = True
        last_stat_time = time.time()
        last_reclaim = time.time()
        last_silence_check = time.time()

        logger.info("StreamProcessor '%s' starting", settings.consumer_name)
        try:
            while self._running:
                try:
                    resp = await self.redis.xreadgroup(
                        settings.consumer_group,
                        settings.consumer_name,
                        {settings.stream_name: ">"},
                        count=settings.batch_size,
                        block=settings.block_ms,
                    )
                except aioredis.ConnectionError:
                    logger.warning("Redis connection lost, retrying in 2s...")
                    await asyncio.sleep(2)
                    continue

                if resp:
                    for _stream, messages in resp:
                        for msg_id, fields in messages:
                            try:
                                await self._process_one(msg_id, fields)
                            except Exception:  # noqa: BLE001 - one bad event must not kill the loop
                                logger.exception("Unhandled error processing %s; will retry via pending list", msg_id)

                await self._flush_aggregates_if_due()
                last_stat_time = await self._record_stats_if_due(last_stat_time)

                if time.time() - last_reclaim > 5:
                    await self._reclaim_pending()
                    last_reclaim = time.time()

                if time.time() - last_silence_check > settings.heartbeat_timeout_sec:
                    await self._detect_silent_sources()
                    last_silence_check = time.time()

                if max_messages is not None and self._processed >= max_messages:
                    break
        finally:
            # flush any partial window on shutdown
            rows = self.aggregator.flush()
            for row in rows:
                self.db.upsert_aggregate(row)
            logger.info(
                "StreamProcessor stopped. processed=%d duplicates=%d errors=%d",
                self._processed,
                self._duplicates,
                self._validation_errors,
            )

    def stop(self) -> None:
        self._running = False


async def _main() -> None:
    processor = StreamProcessor()
    await processor.run()


if __name__ == "__main__":
    logging.basicConfig(level=settings.log_level, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        logger.info("Shutdown requested (Ctrl+C)")

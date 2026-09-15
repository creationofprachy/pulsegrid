#!/usr/bin/env python3
"""
Runs the producer and processor concurrently for a fixed duration at an
elevated event rate and reports real measured throughput/latency numbers.
Never fabricates results -- everything printed here comes from actually
running the pipeline.

Usage: python scripts/benchmark.py --duration 30 --rate 200
"""
import argparse
import asyncio
import logging
import resource
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import redis.asyncio as aioredis

from config import settings
from producer.event_generator import EventGenerator, RedisPublisher
from processor.stream_processor import StreamProcessor
from storage.database import Database

logging.basicConfig(level="WARNING", format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


async def run_benchmark(duration: float, rate: float, sources: int) -> None:
    r = aioredis.Redis(host=settings.redis_host, port=settings.redis_port, db=settings.redis_db, decode_responses=True)
    await r.delete(settings.stream_name)
    await r.delete(settings.dead_letter_stream)
    try:
        await r.xgroup_destroy(settings.stream_name, settings.consumer_group)
    except Exception:
        pass

    db = Database()
    with db.cursor() as cur:
        cur.execute("DELETE FROM events")
        cur.execute("DELETE FROM anomalies")
        cur.execute("DELETE FROM aggregates")
        cur.execute("DELETE FROM source_health")
        cur.execute("DELETE FROM processing_stats")

    generator = EventGenerator(num_sources=sources, events_per_sec=rate, seed=42)
    publisher = RedisPublisher(redis_client=r)
    processor = StreamProcessor(db=db)

    produced = 0
    produce_start = time.time()

    async def produce():
        nonlocal produced
        async for event in generator.stream():
            await publisher.publish(event)
            produced += 1
            if time.time() - produce_start >= duration:
                break

    async def process():
        await processor.run()

    print(f"Running benchmark: target_rate={rate}/s sources={sources} duration={duration}s ...")
    produce_task = asyncio.create_task(produce())
    process_task = asyncio.create_task(process())

    await produce_task
    # let the processor drain the backlog
    drain_deadline = time.time() + 10
    while processor._processed < produced and time.time() < drain_deadline:
        await asyncio.sleep(0.2)
    processor.stop()
    await asyncio.sleep(0.3)
    process_task.cancel()
    try:
        await process_task
    except asyncio.CancelledError:
        pass

    elapsed = time.time() - produce_start
    stats = db.get_statistics()
    peak_rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024

    with db.cursor() as cur:
        cur.execute("SELECT AVG(avg_latency_ms) AS a, MAX(avg_latency_ms) AS m FROM processing_stats")
        row = cur.fetchone()
        avg_proc_latency = row["a"] or 0.0
        max_proc_latency = row["m"] or 0.0
        cur.execute("SELECT AVG(processed_at - timestamp) AS a FROM events")
        detect_row = cur.fetchone()
        avg_detection_latency = (detect_row["a"] or 0.0) * 1000

    print("\n--- Benchmark Results ---")
    print(f"Events produced:            {produced}")
    print(f"Events processed:           {processor._processed}")
    print(f"Duplicates skipped:         {processor._duplicates}")
    print(f"Validation errors:          {processor._validation_errors}")
    print(f"Wall clock duration:        {elapsed:.2f}s")
    print(f"Throughput (produced):      {produced / elapsed:.1f} events/sec")
    print(f"Throughput (processed):     {processor._processed / elapsed:.1f} events/sec")
    print(f"Avg processing latency:     {avg_proc_latency:.3f} ms/event (in-process compute time)")
    print(f"Max processing latency:     {max_proc_latency:.3f} ms/event")
    print(f"Avg end-to-end detect lag:  {avg_detection_latency:.2f} ms (publish -> stored)")
    print(f"Anomalies detected:         {stats['total_anomalies']}")
    print(f"Peak RSS memory:            {peak_rss_mb:.1f} MB")
    print("--------------------------\n")

    await publisher.close()
    await r.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=30.0)
    parser.add_argument("--rate", type=float, default=200.0)
    parser.add_argument("--sources", type=int, default=12)
    args = parser.parse_args()
    asyncio.run(run_benchmark(args.duration, args.rate, args.sources))

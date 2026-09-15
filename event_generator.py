"""
Synthetic event generator for PulseGrid.

Simulates a fleet of microservice instances, each continuously emitting
health telemetry (CPU, memory, latency, error rate, throughput). Behaviour
per source drifts slowly over time, has natural noise, occasional spikes,
injected anomalies, and occasional dropped/missing heartbeats -- mimicking
a real production fleet closely enough to exercise the downstream pipeline.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import AsyncIterator, Optional

import redis.asyncio as aioredis

from config import SOURCE_TYPES, settings
from producer.schemas import ServiceHealthEvent

logger = logging.getLogger("pulsegrid.producer")


@dataclass
class SourceProfile:
    """Baseline operating characteristics for one simulated service instance."""

    source: str
    instance_id: str
    cpu_baseline: float
    memory_baseline: float
    latency_baseline: float
    error_baseline: float
    throughput_baseline: float
    # slow random-walk drift state
    drift: dict = field(default_factory=lambda: {"cpu": 0.0, "memory": 0.0, "latency": 0.0})

    def step(self, rng: random.Random) -> dict:
        """Advance the drift state slightly and return current baselines."""
        self.drift["cpu"] = _clamp(self.drift["cpu"] + rng.uniform(-0.6, 0.6), -10, 10)
        self.drift["memory"] = _clamp(self.drift["memory"] + rng.uniform(-0.4, 0.4), -8, 8)
        self.drift["latency"] = _clamp(self.drift["latency"] + rng.uniform(-3, 3), -30, 30)
        return {
            "cpu": self.cpu_baseline + self.drift["cpu"],
            "memory": self.memory_baseline + self.drift["memory"],
            "latency": self.latency_baseline + self.drift["latency"],
            "error": self.error_baseline,
            "throughput": self.throughput_baseline,
        }


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def build_source_profiles(count: int, rng: random.Random) -> list[SourceProfile]:
    profiles = []
    for i in range(count):
        service = SOURCE_TYPES[i % len(SOURCE_TYPES)]
        instance_id = f"{service}-{i:02d}"
        profiles.append(
            SourceProfile(
                source=service,
                instance_id=instance_id,
                cpu_baseline=rng.uniform(15, 45),
                memory_baseline=rng.uniform(30, 60),
                latency_baseline=rng.uniform(20, 120),
                error_baseline=rng.uniform(0.0, 0.01),
                throughput_baseline=rng.uniform(50, 400),
            )
        )
    return profiles


class EventGenerator:
    """Produces ServiceHealthEvent objects for a fleet of simulated sources."""

    def __init__(
        self,
        num_sources: int = settings.event_sources,
        events_per_sec: float = settings.base_events_per_sec,
        anomaly_probability: float = settings.anomaly_probability,
        missing_probability: float = settings.missing_event_probability,
        seed: Optional[int] = None,
    ):
        self.rng = random.Random(seed)
        self.profiles = build_source_profiles(num_sources, self.rng)
        self.events_per_sec = events_per_sec
        self.anomaly_probability = anomaly_probability
        self.missing_probability = missing_probability
        self._active_anomalies: dict[str, dict] = {}  # source -> {metric, remaining_ticks, magnitude}

    def _maybe_start_anomaly(self, profile: SourceProfile) -> None:
        if profile.instance_id in self._active_anomalies:
            return
        if self.rng.random() < self.anomaly_probability:
            metric = self.rng.choice(["cpu", "memory", "latency", "error", "throughput"])
            self._active_anomalies[profile.instance_id] = {
                "metric": metric,
                "remaining": self.rng.randint(3, 12),
                "magnitude": self.rng.uniform(2.5, 6.0),
            }
            logger.debug("Injected anomaly on %s metric=%s", profile.instance_id, metric)

    def _apply_anomaly(self, profile: SourceProfile, values: dict) -> tuple[dict, str]:
        anomaly = self._active_anomalies.get(profile.instance_id)
        event_type = "heartbeat"
        if anomaly:
            metric, magnitude = anomaly["metric"], anomaly["magnitude"]
            if metric == "cpu":
                values["cpu"] = _clamp(values["cpu"] * magnitude, 0, 100)
            elif metric == "memory":
                values["memory"] = _clamp(values["memory"] * magnitude, 0, 100)
            elif metric == "latency":
                values["latency"] = values["latency"] * magnitude
            elif metric == "error":
                values["error"] = _clamp(values["error"] + 0.15 * magnitude, 0, 1)
            elif metric == "throughput":
                values["throughput"] = max(0.0, values["throughput"] / magnitude)
            event_type = "anomaly_injected"
            anomaly["remaining"] -= 1
            if anomaly["remaining"] <= 0:
                del self._active_anomalies[profile.instance_id]
        return values, event_type

    def generate_one(self, profile: SourceProfile) -> Optional[ServiceHealthEvent]:
        """Generate a single event for a profile, or None if simulating a dropped heartbeat."""
        if self.rng.random() < self.missing_probability:
            return None  # simulated missed heartbeat / network blip

        self._maybe_start_anomaly(profile)
        values = profile.step(self.rng)
        # natural per-tick noise
        values["cpu"] = _clamp(values["cpu"] + self.rng.gauss(0, 2.5), 0, 100)
        values["memory"] = _clamp(values["memory"] + self.rng.gauss(0, 1.5), 0, 100)
        values["latency"] = max(1.0, values["latency"] + self.rng.gauss(0, 5))
        values["error"] = _clamp(values["error"] + max(0, self.rng.gauss(0, 0.002)), 0, 1)
        values["throughput"] = max(0.0, values["throughput"] + self.rng.gauss(0, 15))

        values, event_type = self._apply_anomaly(profile, values)

        status = "ok"
        if values["error"] > 0.2 or values["latency"] > 800:
            status = "down"
        elif values["error"] > 0.05 or values["cpu"] > 90 or values["latency"] > 400:
            status = "degraded"

        return ServiceHealthEvent(
            event_id=str(uuid.uuid4()),
            timestamp=time.time(),
            source=profile.source,
            instance_id=profile.instance_id,
            event_type=event_type,
            cpu_percent=round(values["cpu"], 2),
            memory_percent=round(values["memory"], 2),
            latency_ms=round(values["latency"], 2),
            error_rate=round(values["error"], 4),
            requests_per_sec=round(values["throughput"], 2),
            status=status,
            metadata={"generator_version": "1.0"},
        )

    async def stream(self) -> AsyncIterator[ServiceHealthEvent]:
        """Infinite async generator yielding events at the configured aggregate rate."""
        per_source_interval = len(self.profiles) / max(self.events_per_sec, 0.1)
        while True:
            for profile in self.profiles:
                event = self.generate_one(profile)
                if event is not None:
                    yield event
                await asyncio.sleep(per_source_interval / max(len(self.profiles), 1))


class RedisPublisher:
    """Publishes events onto a Redis Stream (the message broker for PulseGrid)."""

    def __init__(self, redis_client: Optional[aioredis.Redis] = None):
        self.redis = redis_client or aioredis.Redis(
            host=settings.redis_host, port=settings.redis_port, db=settings.redis_db, decode_responses=True
        )

    async def publish(self, event: ServiceHealthEvent) -> str:
        fields = event.to_redis_fields()
        msg_id = await self.redis.xadd(
            settings.stream_name, fields, maxlen=settings.stream_max_len, approximate=True
        )
        return msg_id

    async def close(self) -> None:
        await self.redis.aclose()


async def run_producer(duration_sec: Optional[float] = None, seed: Optional[int] = None) -> int:
    """Run the producer loop, publishing to Redis until duration_sec elapses (or forever)."""
    generator = EventGenerator(seed=seed)
    publisher = RedisPublisher()
    start = time.time()
    count = 0
    logger.info(
        "Producer starting: %d sources, target %.1f events/sec",
        len(generator.profiles),
        generator.events_per_sec,
    )
    try:
        async for event in generator.stream():
            await publisher.publish(event)
            count += 1
            if count % 200 == 0:
                elapsed = time.time() - start
                logger.info("Produced %d events (%.1f events/sec avg)", count, count / max(elapsed, 0.001))
            if duration_sec is not None and (time.time() - start) >= duration_sec:
                break
    finally:
        await publisher.close()
    logger.info("Producer stopped after %d events", count)
    return count


if __name__ == "__main__":
    logging.basicConfig(level=settings.log_level, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    asyncio.run(run_producer())

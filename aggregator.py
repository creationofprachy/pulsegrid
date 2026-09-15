"""
Tumbling-window aggregation for PulseGrid.

Every `window_sec` seconds, per-source averages (CPU/memory/latency/error
rate/throughput) plus the anomaly count observed in that window are flushed
to the `aggregates` table. This keeps the historical-analytics view cheap to
query without re-scanning the raw events table.
"""
from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class WindowBucket:
    count: int = 0
    sums: dict = field(default_factory=lambda: defaultdict(float))
    anomaly_count: int = 0


class TumblingAggregator:
    def __init__(self, window_sec: int):
        self.window_sec = window_sec
        self._window_start = self._aligned_now()
        self._buckets: dict[str, WindowBucket] = defaultdict(WindowBucket)

    def _aligned_now(self) -> float:
        now = time.time()
        return now - (now % self.window_sec)

    def add_event(self, source: str, metrics: dict[str, float], anomaly_count: int = 0) -> None:
        bucket = self._buckets[source]
        bucket.count += 1
        bucket.anomaly_count += anomaly_count
        for k, v in metrics.items():
            bucket.sums[k] += v

    def should_flush(self) -> bool:
        return time.time() - self._window_start >= self.window_sec

    def flush(self) -> list[dict]:
        """Return finished-window aggregate rows and reset internal state."""
        window_start = self._window_start
        window_end = window_start + self.window_sec
        rows = []
        for source, bucket in self._buckets.items():
            if bucket.count == 0:
                continue
            rows.append(
                {
                    "window_start": window_start,
                    "window_end": window_end,
                    "source": source,
                    "event_count": bucket.count,
                    "avg_cpu": bucket.sums["cpu"] / bucket.count,
                    "avg_memory": bucket.sums["memory"] / bucket.count,
                    "avg_latency": bucket.sums["latency"] / bucket.count,
                    "avg_error_rate": bucket.sums["error"] / bucket.count,
                    "avg_throughput": bucket.sums["throughput"] / bucket.count,
                    "anomaly_count": bucket.anomaly_count,
                }
            )
        self._buckets = defaultdict(WindowBucket)
        self._window_start = self._aligned_now()
        return rows

"""
SQLite storage layer for PulseGrid.

Uses a single synchronous sqlite3 connection guarded by a lock, wrapped with
asyncio.to_thread() at call sites in async code. SQLite in WAL mode handles
this workload comfortably at our target throughput and keeps the project
dependency-light (no external DB server required).
"""
from __future__ import annotations

import logging
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Optional

from config import settings
from storage.models import SCHEMA

logger = logging.getLogger("pulsegrid.storage")


class Database:
    def __init__(self, db_path: str = settings.db_path):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._lock = threading.Lock()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn"):
            conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
            self._local.conn = conn
        return self._local.conn

    def _init_schema(self) -> None:
        conn = self._connect()
        with self._lock:
            conn.executescript(SCHEMA)
            conn.commit()
        logger.info("Database schema ready at %s", self.db_path)

    @contextmanager
    def cursor(self):
        conn = self._connect()
        cur = conn.cursor()
        try:
            with self._lock:
                yield cur
                conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    # ---------------------------------------------------------------- events
    def insert_event(self, event: dict) -> bool:
        """Idempotent insert. Returns True if a new row was inserted, False if it was a duplicate."""
        with self.cursor() as cur:
            cur.execute(
                """INSERT OR IGNORE INTO events
                   (event_id, timestamp, source, instance_id, event_type, cpu_percent,
                    memory_percent, latency_ms, error_rate, requests_per_sec, status, processed_at)
                   VALUES (:event_id, :timestamp, :source, :instance_id, :event_type, :cpu_percent,
                           :memory_percent, :latency_ms, :error_rate, :requests_per_sec, :status, :processed_at)""",
                event,
            )
            return cur.rowcount > 0

    def upsert_source_health(self, instance_id: str, source: str, timestamp: float, status: str, is_anomaly: bool) -> None:
        with self.cursor() as cur:
            cur.execute(
                """INSERT INTO source_health (instance_id, source, last_seen, last_status, total_events, total_anomalies)
                   VALUES (?, ?, ?, ?, 1, ?)
                   ON CONFLICT(instance_id) DO UPDATE SET
                     last_seen = excluded.last_seen,
                     last_status = excluded.last_status,
                     total_events = total_events + 1,
                     total_anomalies = total_anomalies + excluded.total_anomalies""",
                (instance_id, source, timestamp, status, 1 if is_anomaly else 0),
            )

    def insert_anomaly(self, anomaly: dict) -> int:
        with self.cursor() as cur:
            cur.execute(
                """INSERT INTO anomalies
                   (event_id, timestamp, source, instance_id, metric, observed_value, baseline_mean,
                    baseline_std, expected_low, expected_high, severity, reason, detection_method)
                   VALUES (:event_id, :timestamp, :source, :instance_id, :metric, :observed_value,
                           :baseline_mean, :baseline_std, :expected_low, :expected_high, :severity,
                           :reason, :detection_method)""",
                anomaly,
            )
            return cur.lastrowid

    def upsert_aggregate(self, agg: dict) -> None:
        with self.cursor() as cur:
            cur.execute(
                """INSERT INTO aggregates
                   (window_start, window_end, source, event_count, avg_cpu, avg_memory,
                    avg_latency, avg_error_rate, avg_throughput, anomaly_count)
                   VALUES (:window_start, :window_end, :source, :event_count, :avg_cpu, :avg_memory,
                           :avg_latency, :avg_error_rate, :avg_throughput, :anomaly_count)
                   ON CONFLICT(window_start, source) DO UPDATE SET
                     event_count = excluded.event_count,
                     avg_cpu = excluded.avg_cpu,
                     avg_memory = excluded.avg_memory,
                     avg_latency = excluded.avg_latency,
                     avg_error_rate = excluded.avg_error_rate,
                     avg_throughput = excluded.avg_throughput,
                     anomaly_count = excluded.anomaly_count""",
                agg,
            )

    def record_processing_stats(self, processed: int, duplicates: int, errors: int, avg_latency_ms: float) -> None:
        with self.cursor() as cur:
            cur.execute(
                """INSERT INTO processing_stats
                   (recorded_at, events_processed, duplicates_skipped, validation_errors, avg_latency_ms)
                   VALUES (?, ?, ?, ?, ?)""",
                (time.time(), processed, duplicates, errors, avg_latency_ms),
            )

    # --------------------------------------------------------------- queries
    def get_recent_events(self, limit: int = 50, source: Optional[str] = None) -> list[dict]:
        query = "SELECT * FROM events"
        params: list = []
        if source:
            query += " WHERE source = ?"
            params.append(source)
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        with self.cursor() as cur:
            cur.execute(query, params)
            return [dict(r) for r in cur.fetchall()]

    def get_anomalies(self, limit: int = 50, severity: Optional[str] = None, source: Optional[str] = None) -> list[dict]:
        query = "SELECT * FROM anomalies WHERE 1=1"
        params: list = []
        if severity:
            query += " AND severity = ?"
            params.append(severity)
        if source:
            query += " AND source = ?"
            params.append(source)
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        with self.cursor() as cur:
            cur.execute(query, params)
            return [dict(r) for r in cur.fetchall()]

    def get_sources(self) -> list[dict]:
        with self.cursor() as cur:
            cur.execute("SELECT * FROM source_health ORDER BY source, instance_id")
            return [dict(r) for r in cur.fetchall()]

    def get_statistics(self) -> dict:
        with self.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS c FROM events")
            total_events = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM anomalies")
            total_anomalies = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(DISTINCT instance_id) AS c FROM source_health")
            active_sources = cur.fetchone()["c"]
            cur.execute(
                "SELECT AVG(latency_ms) AS a FROM events WHERE timestamp > ?", (time.time() - 60,)
            )
            row = cur.fetchone()
            recent_avg_latency = row["a"] if row and row["a"] is not None else 0.0
            cur.execute(
                "SELECT COUNT(*) AS c FROM events WHERE status != 'ok' AND timestamp > ?",
                (time.time() - 60,),
            )
            recent_error_count = cur.fetchone()["c"]
        return {
            "total_events": total_events,
            "total_anomalies": total_anomalies,
            "active_sources": active_sources,
            "recent_avg_latency_ms": round(recent_avg_latency, 2),
            "recent_error_events_60s": recent_error_count,
        }

    def get_aggregates(self, source: Optional[str] = None, limit: int = 100) -> list[dict]:
        query = "SELECT * FROM aggregates"
        params: list = []
        if source:
            query += " WHERE source = ?"
            params.append(source)
        query += " ORDER BY window_start DESC LIMIT ?"
        params.append(limit)
        with self.cursor() as cur:
            cur.execute(query, params)
            return [dict(r) for r in cur.fetchall()]

    def close(self) -> None:
        if hasattr(self._local, "conn"):
            self._local.conn.close()
            del self._local.conn

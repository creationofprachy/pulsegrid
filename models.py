"""SQL schema definitions for PulseGrid's SQLite storage layer."""

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS events (
    event_id        TEXT PRIMARY KEY,
    timestamp       REAL NOT NULL,
    source          TEXT NOT NULL,
    instance_id     TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    cpu_percent     REAL NOT NULL,
    memory_percent  REAL NOT NULL,
    latency_ms      REAL NOT NULL,
    error_rate      REAL NOT NULL,
    requests_per_sec REAL NOT NULL,
    status          TEXT NOT NULL,
    processed_at    REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
CREATE INDEX IF NOT EXISTS idx_events_source ON events(source);
CREATE INDEX IF NOT EXISTS idx_events_instance ON events(instance_id);

CREATE TABLE IF NOT EXISTS anomalies (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        TEXT NOT NULL,
    timestamp       REAL NOT NULL,
    source          TEXT NOT NULL,
    instance_id     TEXT NOT NULL,
    metric          TEXT NOT NULL,
    observed_value  REAL NOT NULL,
    baseline_mean   REAL,
    baseline_std    REAL,
    expected_low    REAL,
    expected_high   REAL,
    severity        TEXT NOT NULL,
    reason          TEXT NOT NULL,
    detection_method TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_anomalies_timestamp ON anomalies(timestamp);
CREATE INDEX IF NOT EXISTS idx_anomalies_source ON anomalies(source);
CREATE INDEX IF NOT EXISTS idx_anomalies_severity ON anomalies(severity);

CREATE TABLE IF NOT EXISTS aggregates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    window_start    REAL NOT NULL,
    window_end      REAL NOT NULL,
    source          TEXT NOT NULL,
    event_count     INTEGER NOT NULL,
    avg_cpu         REAL,
    avg_memory      REAL,
    avg_latency     REAL,
    avg_error_rate  REAL,
    avg_throughput  REAL,
    anomaly_count   INTEGER NOT NULL DEFAULT 0,
    UNIQUE(window_start, source)
);
CREATE INDEX IF NOT EXISTS idx_aggregates_window ON aggregates(window_start);

CREATE TABLE IF NOT EXISTS source_health (
    instance_id     TEXT PRIMARY KEY,
    source          TEXT NOT NULL,
    last_seen       REAL NOT NULL,
    last_status     TEXT NOT NULL,
    total_events    INTEGER NOT NULL DEFAULT 0,
    total_anomalies INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS processing_stats (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at         REAL NOT NULL,
    events_processed    INTEGER NOT NULL,
    duplicates_skipped  INTEGER NOT NULL,
    validation_errors   INTEGER NOT NULL,
    avg_latency_ms      REAL
);
"""

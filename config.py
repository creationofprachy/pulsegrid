"""
Central configuration for PulseGrid.
All tunables live here and can be overridden via environment variables (.env).
"""
import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, default))


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, default))


def _env_str(name: str, default: str) -> str:
    return os.getenv(name, default)


@dataclass
class Settings:
    # Redis / broker
    redis_host: str = field(default_factory=lambda: _env_str("REDIS_HOST", "localhost"))
    redis_port: int = field(default_factory=lambda: _env_int("REDIS_PORT", 6379))
    redis_db: int = field(default_factory=lambda: _env_int("REDIS_DB", 0))
    stream_name: str = field(default_factory=lambda: _env_str("STREAM_NAME", "pulsegrid:events"))
    dead_letter_stream: str = field(
        default_factory=lambda: _env_str("DEAD_LETTER_STREAM", "pulsegrid:events:dlq")
    )
    consumer_group: str = field(default_factory=lambda: _env_str("CONSUMER_GROUP", "pulsegrid-processors"))
    consumer_name: str = field(default_factory=lambda: _env_str("CONSUMER_NAME", "processor-1"))
    stream_max_len: int = field(default_factory=lambda: _env_int("STREAM_MAX_LEN", 200_000))

    # Storage
    db_path: str = field(default_factory=lambda: _env_str("DB_PATH", str(BASE_DIR / "data" / "pulsegrid.db")))

    # Producer
    event_sources: int = field(default_factory=lambda: _env_int("EVENT_SOURCES", 12))
    base_events_per_sec: float = field(default_factory=lambda: _env_float("BASE_EVENTS_PER_SEC", 20.0))
    anomaly_probability: float = field(default_factory=lambda: _env_float("ANOMALY_PROBABILITY", 0.03))
    missing_event_probability: float = field(
        default_factory=lambda: _env_float("MISSING_EVENT_PROBABILITY", 0.02)
    )

    # Processing / anomaly detection
    rolling_window_size: int = field(default_factory=lambda: _env_int("ROLLING_WINDOW_SIZE", 50))
    ewma_alpha: float = field(default_factory=lambda: _env_float("EWMA_ALPHA", 0.3))
    zscore_threshold: float = field(default_factory=lambda: _env_float("ZSCORE_THRESHOLD", 3.0))
    min_samples_for_detection: int = field(
        default_factory=lambda: _env_int("MIN_SAMPLES_FOR_DETECTION", 15)
    )
    heartbeat_timeout_sec: float = field(
        default_factory=lambda: _env_float("HEARTBEAT_TIMEOUT_SEC", 8.0)
    )
    aggregation_window_sec: int = field(default_factory=lambda: _env_int("AGGREGATION_WINDOW_SEC", 10))

    # Fault tolerance
    max_retries: int = field(default_factory=lambda: _env_int("MAX_RETRIES", 3))
    pending_claim_idle_ms: int = field(default_factory=lambda: _env_int("PENDING_CLAIM_IDLE_MS", 30_000))
    batch_size: int = field(default_factory=lambda: _env_int("BATCH_SIZE", 50))
    block_ms: int = field(default_factory=lambda: _env_int("BLOCK_MS", 2000))

    # API
    api_host: str = field(default_factory=lambda: _env_str("API_HOST", "0.0.0.0"))
    api_port: int = field(default_factory=lambda: _env_int("API_PORT", 8000))
    log_level: str = field(default_factory=lambda: _env_str("LOG_LEVEL", "INFO"))


settings = Settings()

METRICS = ("cpu_percent", "memory_percent", "latency_ms", "error_rate", "requests_per_sec")

SOURCE_TYPES = ("api-gateway", "auth-service", "payments-service", "orders-service", "search-service")

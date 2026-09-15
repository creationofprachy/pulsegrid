"""
Event schema definitions for PulseGrid.

Every event flowing through the system is a "service health event": a single
point-in-time measurement emitted by one simulated microservice instance.
"""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field, field_validator


class ServiceHealthEvent(BaseModel):
    """A single telemetry event emitted by a simulated service instance."""

    event_id: str = Field(..., description="Unique event identifier (UUID4)")
    timestamp: float = Field(..., description="Unix epoch seconds when the event was generated")
    source: str = Field(..., description="Logical service name, e.g. 'payments-service'")
    instance_id: str = Field(..., description="Specific instance/host emitting the event")
    event_type: str = Field(default="heartbeat", description="heartbeat | anomaly_injected")
    cpu_percent: float = Field(..., ge=0, le=100)
    memory_percent: float = Field(..., ge=0, le=100)
    latency_ms: float = Field(..., ge=0)
    error_rate: float = Field(..., ge=0, le=1)
    requests_per_sec: float = Field(..., ge=0)
    status: str = Field(default="ok", description="ok | degraded | down")
    metadata: dict = Field(default_factory=dict)

    @field_validator("source")
    @classmethod
    def source_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("source must not be empty")
        return v

    def to_redis_fields(self) -> dict:
        """Flatten to a dict of str->str suitable for XADD."""
        import json

        d = self.model_dump()
        d["metadata"] = json.dumps(d["metadata"])
        return {k: str(v) for k, v in d.items()}

    @classmethod
    def from_redis_fields(cls, fields: dict) -> "ServiceHealthEvent":
        import json

        data = dict(fields)
        data["timestamp"] = float(data["timestamp"])
        data["cpu_percent"] = float(data["cpu_percent"])
        data["memory_percent"] = float(data["memory_percent"])
        data["latency_ms"] = float(data["latency_ms"])
        data["error_rate"] = float(data["error_rate"])
        data["requests_per_sec"] = float(data["requests_per_sec"])
        data["metadata"] = json.loads(data.get("metadata", "{}") or "{}")
        return cls(**data)


class EventValidationError(Exception):
    """Raised when a raw payload cannot be turned into a valid ServiceHealthEvent."""


def validate_raw_event(fields: dict) -> ServiceHealthEvent:
    """Validate & parse a raw Redis stream payload, raising EventValidationError on failure."""
    try:
        return ServiceHealthEvent.from_redis_fields(fields)
    except Exception as exc:  # noqa: BLE001 - we want to funnel all failures
        raise EventValidationError(str(exc)) from exc

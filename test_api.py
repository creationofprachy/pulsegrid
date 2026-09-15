import time

import pytest
from fastapi.testclient import TestClient

from api import routes as api_routes
from api.main import app
from storage.database import Database


@pytest.fixture
def client(temp_db_path, monkeypatch):
    test_db = Database(db_path=temp_db_path)
    monkeypatch.setattr(api_routes, "_db", test_db)
    test_db.insert_event(
        {
            "event_id": "evt-api-1",
            "timestamp": time.time(),
            "source": "api-gateway",
            "instance_id": "api-gateway-00",
            "event_type": "heartbeat",
            "cpu_percent": 40.0,
            "memory_percent": 30.0,
            "latency_ms": 45.0,
            "error_rate": 0.01,
            "requests_per_sec": 100.0,
            "status": "ok",
            "processed_at": time.time(),
        }
    )
    test_db.insert_anomaly(
        {
            "event_id": "evt-api-1",
            "timestamp": time.time(),
            "source": "api-gateway",
            "instance_id": "api-gateway-00",
            "metric": "cpu",
            "observed_value": 95.0,
            "baseline_mean": 40.0,
            "baseline_std": 5.0,
            "expected_low": 25.0,
            "expected_high": 55.0,
            "severity": "high",
            "reason": "test",
            "detection_method": "rolling_zscore",
        }
    )
    test_db.upsert_source_health("api-gateway-00", "api-gateway", time.time(), "ok", True)
    with TestClient(app) as c:
        yield c


def test_health_endpoint(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert "status" in body
    assert "redis_connected" in body


def test_statistics_endpoint(client):
    resp = client.get("/api/statistics")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_events"] == 1
    assert body["total_anomalies"] == 1


def test_events_endpoint(client):
    resp = client.get("/api/events")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["source"] == "api-gateway"


def test_events_endpoint_filters_by_source(client):
    resp = client.get("/api/events?source=nonexistent")
    assert resp.status_code == 200
    assert resp.json() == []


def test_anomalies_endpoint(client):
    resp = client.get("/api/anomalies")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["severity"] == "high"


def test_sources_endpoint(client):
    resp = client.get("/api/sources")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["instance_id"] == "api-gateway-00"


def test_metrics_endpoint(client):
    resp = client.get("/metrics")
    assert resp.status_code == 200
    body = resp.json()
    assert "total_events" in body
    assert "uptime_sec" in body


def test_docs_available(client):
    resp = client.get("/docs")
    assert resp.status_code == 200

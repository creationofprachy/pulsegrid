import time

from storage.database import Database


def make_event(event_id="evt-1", ts=None, source="api-gateway", instance="api-gateway-00"):
    return {
        "event_id": event_id,
        "timestamp": ts or time.time(),
        "source": source,
        "instance_id": instance,
        "event_type": "heartbeat",
        "cpu_percent": 42.0,
        "memory_percent": 33.0,
        "latency_ms": 55.0,
        "error_rate": 0.01,
        "requests_per_sec": 120.0,
        "status": "ok",
        "processed_at": time.time(),
    }


def test_insert_event_and_duplicate_is_ignored(temp_db_path):
    db = Database(db_path=temp_db_path)
    assert db.insert_event(make_event("evt-1")) is True
    assert db.insert_event(make_event("evt-1")) is False  # duplicate, idempotent
    events = db.get_recent_events(limit=10)
    assert len(events) == 1


def test_get_recent_events_filters_by_source(temp_db_path):
    db = Database(db_path=temp_db_path)
    db.insert_event(make_event("evt-a", source="auth-service", instance="auth-service-00"))
    db.insert_event(make_event("evt-b", source="orders-service", instance="orders-service-00"))
    auth_events = db.get_recent_events(source="auth-service")
    assert len(auth_events) == 1
    assert auth_events[0]["source"] == "auth-service"


def test_insert_and_query_anomaly(temp_db_path):
    db = Database(db_path=temp_db_path)
    db.insert_anomaly(
        {
            "event_id": "evt-1",
            "timestamp": time.time(),
            "source": "api-gateway",
            "instance_id": "api-gateway-00",
            "metric": "cpu",
            "observed_value": 99.0,
            "baseline_mean": 40.0,
            "baseline_std": 5.0,
            "expected_low": 25.0,
            "expected_high": 55.0,
            "severity": "high",
            "reason": "test anomaly",
            "detection_method": "rolling_zscore",
        }
    )
    anomalies = db.get_anomalies()
    assert len(anomalies) == 1
    assert anomalies[0]["severity"] == "high"


def test_upsert_source_health_increments_counts(temp_db_path):
    db = Database(db_path=temp_db_path)
    db.upsert_source_health("api-gateway-00", "api-gateway", time.time(), "ok", False)
    db.upsert_source_health("api-gateway-00", "api-gateway", time.time(), "ok", True)
    sources = db.get_sources()
    assert len(sources) == 1
    assert sources[0]["total_events"] == 2
    assert sources[0]["total_anomalies"] == 1


def test_statistics_reflect_inserted_data(temp_db_path):
    db = Database(db_path=temp_db_path)
    db.insert_event(make_event("evt-1"))
    db.upsert_source_health("api-gateway-00", "api-gateway", time.time(), "ok", False)
    stats = db.get_statistics()
    assert stats["total_events"] == 1
    assert stats["active_sources"] == 1


def test_upsert_aggregate_is_idempotent_on_conflict(temp_db_path):
    db = Database(db_path=temp_db_path)
    row = {
        "window_start": 1000.0,
        "window_end": 1010.0,
        "source": "api-gateway",
        "event_count": 5,
        "avg_cpu": 40.0,
        "avg_memory": 30.0,
        "avg_latency": 50.0,
        "avg_error_rate": 0.01,
        "avg_throughput": 100.0,
        "anomaly_count": 1,
    }
    db.upsert_aggregate(row)
    row["event_count"] = 10
    db.upsert_aggregate(row)
    aggs = db.get_aggregates(source="api-gateway")
    assert len(aggs) == 1
    assert aggs[0]["event_count"] == 10

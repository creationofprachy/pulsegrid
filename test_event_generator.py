import time

from producer.event_generator import EventGenerator, build_source_profiles
import random

from producer.schemas import EventValidationError, ServiceHealthEvent, validate_raw_event


def test_build_source_profiles_count():
    profiles = build_source_profiles(5, random.Random(1))
    assert len(profiles) == 5
    assert all(p.instance_id for p in profiles)


def test_generate_one_produces_valid_event_or_none():
    gen = EventGenerator(num_sources=3, seed=1, missing_probability=0.0)
    profile = gen.profiles[0]
    event = gen.generate_one(profile)
    assert event is not None
    assert isinstance(event, ServiceHealthEvent)
    assert 0 <= event.cpu_percent <= 100
    assert 0 <= event.error_rate <= 1
    assert event.status in {"ok", "degraded", "down"}


def test_missing_events_are_simulated():
    gen = EventGenerator(num_sources=1, seed=2, missing_probability=1.0)
    profile = gen.profiles[0]
    event = gen.generate_one(profile)
    assert event is None


def test_event_roundtrip_through_redis_fields():
    gen = EventGenerator(num_sources=1, seed=3, missing_probability=0.0)
    event = gen.generate_one(gen.profiles[0])
    fields = event.to_redis_fields()
    restored = validate_raw_event(fields)
    assert restored.event_id == event.event_id
    assert restored.source == event.source
    assert restored.cpu_percent == event.cpu_percent


def test_invalid_event_raises_validation_error():
    bad_fields = {
        "event_id": "x",
        "timestamp": "not-a-number",
        "source": "svc",
        "instance_id": "svc-01",
        "cpu_percent": "10",
        "memory_percent": "10",
        "latency_ms": "10",
        "error_rate": "0.1",
        "requests_per_sec": "10",
    }
    try:
        validate_raw_event(bad_fields)
        assert False, "expected EventValidationError"
    except EventValidationError:
        pass


def test_anomaly_injection_eventually_triggers():
    gen = EventGenerator(num_sources=1, seed=7, anomaly_probability=1.0, missing_probability=0.0)
    profile = gen.profiles[0]
    saw_anomaly_type = False
    for _ in range(5):
        event = gen.generate_one(profile)
        if event and event.event_type == "anomaly_injected":
            saw_anomaly_type = True
            break
    assert saw_anomaly_type

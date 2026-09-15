from processor.anomaly_detector import AnomalyDetector


def test_no_anomaly_reported_with_insufficient_samples():
    detector = AnomalyDetector(min_samples=15)
    anomalies = detector.evaluate("inst-1", {"cpu": 50.0})
    assert anomalies == []


def test_stable_metrics_produce_no_anomalies():
    detector = AnomalyDetector(min_samples=10, window_size=20, zscore_threshold=3.0)
    for i in range(40):
        anomalies = detector.evaluate("inst-1", {"cpu": 50.0 + (i % 3) * 0.1})
    assert anomalies == []


def test_sudden_spike_is_detected():
    detector = AnomalyDetector(min_samples=10, window_size=20, zscore_threshold=3.0)
    for _ in range(25):
        detector.evaluate("inst-1", {"cpu": 50.0})
    anomalies = detector.evaluate("inst-1", {"cpu": 99.0})
    assert len(anomalies) == 1
    assert anomalies[0].metric == "cpu"
    assert anomalies[0].severity in {"medium", "high", "critical"}


def test_anomaly_includes_required_fields():
    detector = AnomalyDetector(min_samples=10, window_size=20, zscore_threshold=2.0)
    for _ in range(15):
        detector.evaluate("inst-2", {"latency": 100.0})
    anomalies = detector.evaluate("inst-2", {"latency": 900.0})
    assert len(anomalies) == 1
    a = anomalies[0]
    assert a.observed_value == 900.0
    assert a.baseline_mean > 0
    assert a.reason
    assert a.detection_method.startswith("rolling_zscore")


def test_sustained_anomaly_escalates_severity():
    detector = AnomalyDetector(min_samples=10, window_size=30, zscore_threshold=3.0)
    for _ in range(15):
        detector.evaluate("inst-3", {"error": 0.01})
    reasons = []
    for _ in range(4):
        anomalies = detector.evaluate("inst-3", {"error": 0.5})
        if anomalies:
            reasons.append(anomalies[0])
    assert len(reasons) == 4
    # after 3 consecutive anomalous readings, detection should flag it as sustained
    assert "sustained" in reasons[-1].reason
    assert reasons[-1].detection_method == "rolling_zscore+sustained"
    assert reasons[-1].severity in {"high", "critical"}


def test_normal_noise_does_not_flag_after_baseline_established():
    detector = AnomalyDetector(min_samples=10, window_size=30, zscore_threshold=3.0)
    values = [50, 51, 49, 50, 52, 48, 50, 51, 49, 50, 50, 51, 49, 50, 52]
    flagged = 0
    for v in values:
        anomalies = detector.evaluate("inst-4", {"cpu": float(v)})
        flagged += len(anomalies)
    assert flagged == 0

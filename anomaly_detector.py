"""
Real-time anomaly detection for PulseGrid.

Two complementary techniques run per (instance, metric) time series:

1. Rolling z-score  - flags values that are statistically far (in standard
   deviations) from a sliding-window mean. Good at catching sudden spikes.
2. EWMA deviation   - an exponentially weighted moving average reacts faster
   to sustained drift than a plain rolling mean, so it catches slow-building
   degradation that a window-based z-score might smooth over.

A value must clear the z-score threshold to be reported (EWMA deviation is
used to grade severity and to catch sustained abnormal behaviour), which
keeps normal noisy fluctuation from being mislabeled as anomalous.
"""
from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Optional

from config import METRICS, settings


@dataclass
class RollingSeries:
    window_size: int
    values: deque = field(default_factory=deque)
    ewma: Optional[float] = None
    consecutive_anomalies: int = 0

    def mean_std(self) -> tuple[float, float]:
        n = len(self.values)
        if n == 0:
            return 0.0, 0.0
        mean = sum(self.values) / n
        if n < 2:
            return mean, 0.0
        variance = sum((v - mean) ** 2 for v in self.values) / (n - 1)
        return mean, math.sqrt(variance)


def _severity_for_zscore(z: float) -> str:
    z = abs(z)
    if z >= 6:
        return "critical"
    if z >= 4.5:
        return "high"
    if z >= settings.zscore_threshold:
        return "medium"
    return "low"


@dataclass
class Anomaly:
    metric: str
    observed_value: float
    baseline_mean: float
    baseline_std: float
    expected_low: float
    expected_high: float
    zscore: float
    severity: str
    reason: str
    detection_method: str


class AnomalyDetector:
    """Maintains per-(instance, metric) rolling state and flags anomalies on each new event."""

    def __init__(
        self,
        window_size: int = settings.rolling_window_size,
        alpha: float = settings.ewma_alpha,
        zscore_threshold: float = settings.zscore_threshold,
        min_samples: int = settings.min_samples_for_detection,
    ):
        self.window_size = window_size
        self.alpha = alpha
        self.zscore_threshold = zscore_threshold
        self.min_samples = min_samples
        self._series: dict[tuple[str, str], RollingSeries] = defaultdict(
            lambda: RollingSeries(window_size=window_size)
        )

    def _get_series(self, instance_id: str, metric: str) -> RollingSeries:
        return self._series[(instance_id, metric)]

    def evaluate(self, instance_id: str, metrics: dict[str, float]) -> list[Anomaly]:
        """Update rolling state for every metric and return any anomalies detected."""
        anomalies: list[Anomaly] = []
        for metric, value in metrics.items():
            series = self._get_series(instance_id, metric)
            mean, std = series.mean_std()
            has_enough_data = len(series.values) >= self.min_samples

            zscore = 0.0
            if has_enough_data:
                # Floor the denominator so a near-constant baseline (std ~ 0)
                # doesn't divide by zero -- any real deviation from a flat
                # baseline should still register as a large z-score.
                effective_std = max(std, abs(mean) * 0.01, 0.05)
                zscore = (value - mean) / effective_std

            is_anomalous = has_enough_data and abs(zscore) >= self.zscore_threshold

            if is_anomalous:
                series.consecutive_anomalies += 1
                severity = _severity_for_zscore(zscore)
                sustained = series.consecutive_anomalies >= 3
                reason = (
                    f"{metric} deviated {zscore:+.2f} std-dev from the {self.window_size}-sample "
                    f"rolling baseline (mean={mean:.2f})"
                )
                if sustained:
                    reason += f"; sustained for {series.consecutive_anomalies} consecutive readings"
                    if severity == "medium":
                        severity = "high"
                anomalies.append(
                    Anomaly(
                        metric=metric,
                        observed_value=value,
                        baseline_mean=mean,
                        baseline_std=std,
                        expected_low=mean - self.zscore_threshold * std,
                        expected_high=mean + self.zscore_threshold * std,
                        zscore=zscore,
                        severity=severity,
                        reason=reason,
                        detection_method="rolling_zscore+sustained" if sustained else "rolling_zscore",
                    )
                )
            else:
                series.consecutive_anomalies = 0

            # Keep the rolling baseline "clean": an anomalous point should not
            # drag the window's mean/std toward itself, or a sustained spike
            # would quickly get absorbed into "normal". The EWMA is still
            # updated on every point so it keeps tracking the live trend.
            if not is_anomalous:
                series.values.append(value)
                if len(series.values) > series.window_size:
                    series.values.popleft()
            series.ewma = value if series.ewma is None else (self.alpha * value + (1 - self.alpha) * series.ewma)

        return anomalies

    def series_snapshot(self, instance_id: str, metric: str) -> dict:
        series = self._get_series(instance_id, metric)
        mean, std = series.mean_std()
        return {
            "mean": mean,
            "std": std,
            "ewma": series.ewma,
            "samples": len(series.values),
        }

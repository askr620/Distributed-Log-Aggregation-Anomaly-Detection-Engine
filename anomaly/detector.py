"""Pure anomaly detection logic using rolling counts and Z-score."""

from dataclasses import dataclass
from datetime import UTC, datetime
from math import sqrt
from statistics import mean
from typing import Any


ANOMALY_LEVELS = {"ERROR", "CRITICAL"}
CRITICAL_Z_SCORE = 7.0


@dataclass(frozen=True)
class DetectionConfig:
    """Thresholds used by the anomaly detector."""

    z_score_threshold: float = 2.5
    min_samples: int = 10


def compute_z_score(current_count: int, history_counts: list[int]) -> float:
    """Return how unusual the current count is compared with history."""
    if not history_counts:
        return 0.0

    avg = mean(history_counts)
    variance = sum((count - avg) ** 2 for count in history_counts) / len(history_counts)
    std_dev = sqrt(variance)
    if std_dev == 0:
        return 0.0 if current_count == avg else float(current_count - avg)

    return (current_count - avg) / std_dev


def detect_anomaly(
    *,
    service: str,
    level: str,
    current_count: int,
    history_counts: list[int],
    config: DetectionConfig,
) -> dict[str, Any] | None:
    """Return an anomaly event when current traffic is far outside history."""
    non_zero_samples = [count for count in history_counts if count > 0]
    if level not in ANOMALY_LEVELS:
        return None

    if len(non_zero_samples) < config.min_samples:
        return None

    z_score = compute_z_score(current_count, history_counts)
    if z_score <= config.z_score_threshold:
        return None

    avg = mean(history_counts)
    variance = sum((count - avg) ** 2 for count in history_counts) / len(history_counts)
    std_dev = sqrt(variance)

    severity = "CRITICAL" if z_score >= CRITICAL_Z_SCORE else level

    return {
        "service": service,
        "tenant_id": "default",
        "level": level,
        "severity": severity,
        "metric": "error_rate",
        "current_count": current_count,
        "mean": round(float(avg), 4),
        "std_dev": round(float(std_dev), 4),
        "z_score": round(float(z_score), 4),
        "threshold": config.z_score_threshold,
        "fired_at": datetime.now(UTC).isoformat(),
    }

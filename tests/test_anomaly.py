"""Unit tests for pure anomaly detection logic."""

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from anomaly.detector import DetectionConfig, compute_z_score, detect_anomaly  # noqa: E402


def test_z_score_spike() -> None:
    z_score = compute_z_score(50, [2, 3, 2, 3, 2, 3, 2, 3, 2, 3])

    assert z_score > 2.5


def test_z_score_normal() -> None:
    z_score = compute_z_score(3, [2, 3, 2, 3, 2, 3, 2, 3, 2, 3])

    assert z_score < 2.5


def test_error_drop_does_not_trigger() -> None:
    anomaly = detect_anomaly(
        service="payment-service",
        level="ERROR",
        current_count=1,
        history_counts=[10, 11, 10, 11, 10, 11, 10, 11, 10, 11],
        config=DetectionConfig(z_score_threshold=2.5, min_samples=10),
    )

    assert anomaly is None


def test_z_score_empty_history() -> None:
    assert compute_z_score(10, []) == 0.0


def test_below_min_samples_no_anomaly() -> None:
    anomaly = detect_anomaly(
        service="payment-service",
        level="ERROR",
        current_count=50,
        history_counts=[2, 3, 0, 0, 0, 0, 0, 0, 0, 0],
        config=DetectionConfig(z_score_threshold=2.5, min_samples=10),
    )

    assert anomaly is None


def test_critical_level_triggers() -> None:
    anomaly = detect_anomaly(
        service="payment-service",
        level="CRITICAL",
        current_count=50,
        history_counts=[2, 3, 2, 3, 2, 3, 2, 3, 2, 3],
        config=DetectionConfig(z_score_threshold=2.5, min_samples=10),
    )

    assert anomaly is not None
    assert anomaly["level"] == "CRITICAL"


def test_high_z_score_error_becomes_critical_severity() -> None:
    anomaly = detect_anomaly(
        service="payment-service",
        level="ERROR",
        current_count=460,
        history_counts=[40, 42, 41, 44, 43, 45, 42, 43, 44, 41],
        config=DetectionConfig(z_score_threshold=2.5, min_samples=10),
    )

    assert anomaly is not None
    assert anomaly["level"] == "ERROR"
    assert anomaly["severity"] == "CRITICAL"

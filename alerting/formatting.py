"""Human-readable formatting helpers for anomaly alerts."""

from typing import Any


def format_anomaly_message(anomaly: dict[str, Any]) -> str:
    """Create a plain-text anomaly alert message."""
    severity = anomaly.get("severity") or anomaly.get("level", "UNKNOWN")
    return (
        f"Anomaly Detected - {anomaly.get('service', 'unknown-service')}\n"
        f"Severity: {severity}\n"
        f"Raw level: {anomaly.get('level', 'UNKNOWN')}\n"
        f"Current count: {anomaly.get('current_count', 0)}\n"
        f"Normal mean: {anomaly.get('mean', 0)}\n"
        f"Z-score: {anomaly.get('z_score', 0)}\n"
        f"Threshold: {anomaly.get('threshold', 0)}\n"
        f"Time: {anomaly.get('fired_at', '')}"
    )

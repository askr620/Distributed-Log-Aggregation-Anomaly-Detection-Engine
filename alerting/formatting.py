"""Human-readable formatting helpers for anomaly alerts."""

from typing import Any


def format_anomaly_message(anomaly: dict[str, Any]) -> str:
    """Create a plain-text anomaly alert message, including AI analysis when available."""
    severity = anomaly.get("severity") or anomaly.get("level", "UNKNOWN")
    base = (
        f"Anomaly Detected - {anomaly.get('service', 'unknown-service')}\n"
        f"Severity: {severity}\n"
        f"Raw level: {anomaly.get('level', 'UNKNOWN')}\n"
        f"Current count: {anomaly.get('current_count', 0)}\n"
        f"Normal mean: {anomaly.get('mean', 0)}\n"
        f"Z-score: {anomaly.get('z_score', 0)}\n"
        f"Threshold: {anomaly.get('threshold', 0)}\n"
        f"Time: {anomaly.get('fired_at', '')}"
    )

    ai = anomaly.get("ai_analysis")
    if ai:
        checks = ai.get("recommended_checks") or []
        checks_text = "\n".join(f"  {i + 1}. {c}" for i, c in enumerate(checks))
        ai_section = (
            f"\n\nAI Incident Analysis:\n"
            f"Possible cause: {ai.get('possible_cause', 'unknown')}\n"
            f"Affected service: {ai.get('affected_service', anomaly.get('service', 'unknown'))}\n"
            f"Recommended checks:\n{checks_text}"
        )
        return base + ai_section

    return base

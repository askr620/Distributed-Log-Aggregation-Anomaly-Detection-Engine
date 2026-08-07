"""AI-powered incident analyzer using an LLM to explain anomalies."""

import logging
from typing import Any

import httpx

from consumers.config import settings


logger = logging.getLogger(__name__)

OPENAI_CHAT_URL = "https://api.groq.com/openai/v1/chat/completions"

_SYSTEM_PROMPT = (
    "You are an expert site reliability engineer. "
    "When given a log anomaly event, respond with a concise JSON object containing:\n"
    "  - possible_cause: one sentence root cause\n"
    "  - affected_service: service name\n"
    "  - recommended_checks: list of 3 short action items\n"
    "Respond ONLY with valid JSON. No markdown, no extra text."
)


def _build_user_prompt(anomaly: dict[str, Any]) -> str:
    """Turn an anomaly dict into a human-readable prompt for the LLM."""
    return (
        f"Incident: {anomaly.get('current_count', '?')} {anomaly.get('level', 'ERROR')} "
        f"errors detected in service '{anomaly.get('service', 'unknown')}'.\n"
        f"Z-score: {anomaly.get('z_score', '?')} (threshold: {anomaly.get('threshold', '?')}).\n"
        f"Normal mean: {anomaly.get('mean', '?')}, std_dev: {anomaly.get('std_dev', '?')}.\n"
        f"Severity: {anomaly.get('severity', anomaly.get('level', 'ERROR'))}.\n"
        f"Time: {anomaly.get('fired_at', 'unknown')}.\n"
        "Analyze this incident and return the JSON described."
    )


async def analyze_anomaly(anomaly: dict[str, Any]) -> dict[str, Any] | None:
    """
    Send the anomaly to OpenAI and return structured AI analysis.

    Returns None if the API key is not configured or the call fails.
    """
    api_key = settings.openai_api_key
    if not api_key:
        logger.warning("OPENAI_API_KEY not set — skipping AI analysis")
        return None

    payload = {
        "model": settings.openai_model,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": _build_user_prompt(anomaly)},
        ],
        "temperature": 0.2,
        "max_tokens": 300,
    }

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.post(
                OPENAI_CHAT_URL,
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=payload,
            )
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"].strip()

            import json  # noqa: PLC0415
            result = json.loads(content)
            logger.info("AI analysis for %s/%s: %s", anomaly.get("service"), anomaly.get("level"), result)
            return result

    except Exception:
        logger.exception("AI analysis failed for anomaly %s", anomaly)
        return None

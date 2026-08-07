"""Slack webhook alert sender."""

import logging
from typing import Any

import httpx

from alerting.base_alert import BaseAlert
from alerting.formatting import format_anomaly_message
from consumers.config import settings


logger = logging.getLogger(__name__)


class SlackAlert(BaseAlert):
    """Send anomaly alerts to Slack through an incoming webhook."""

    async def send(self, anomaly: dict[str, Any]) -> None:
        """Send one Slack message, or log if webhook is not configured."""
        message = format_anomaly_message(anomaly)
        if not settings.slack_webhook_url:
            logger.warning("Slack webhook not configured. Alert message:\n%s", message)
            return

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(settings.slack_webhook_url, json={"text": message})
                if response.status_code >= 300:
                    logger.error("Slack webhook failed with status %s: %s", response.status_code, response.text)
        except Exception:
            logger.exception("Failed to send Slack alert")

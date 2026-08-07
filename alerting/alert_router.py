"""Route anomaly alerts to the correct delivery channels."""

import logging
from typing import Any

from alerting.email_alert import EmailAlert
from alerting.slack_alert import SlackAlert


logger = logging.getLogger(__name__)


class AlertRouter:
    """Choose alert channels based on anomaly severity."""

    def __init__(self, slack: SlackAlert | None = None, email: EmailAlert | None = None) -> None:
        self.slack = slack or SlackAlert()
        self.email = email or EmailAlert()

    async def route(self, anomaly: dict[str, Any]) -> None:
        """Send an anomaly to the appropriate channel."""
        level = str(anomaly.get("level", "")).upper()
        if level == "CRITICAL":
            await self.slack.send(anomaly)
            await self.email.send(anomaly)
        elif level == "ERROR":
            await self.slack.send(anomaly)
        else:
            logger.info("No external alert route for level %s: %s", level, anomaly)

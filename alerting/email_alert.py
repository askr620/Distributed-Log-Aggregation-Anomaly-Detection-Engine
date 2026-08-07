"""SMTP email alert sender."""

import asyncio
import logging
import smtplib
from email.message import EmailMessage
from typing import Any

from alerting.base_alert import BaseAlert
from alerting.formatting import format_anomaly_message
from consumers.config import settings


logger = logging.getLogger(__name__)


class EmailAlert(BaseAlert):
    """Send anomaly alerts by email using SMTP."""

    async def send(self, anomaly: dict[str, Any]) -> None:
        """Send one email alert without blocking the event loop."""
        if not settings.smtp_host or settings.smtp_host == "localhost":
            logger.warning("SMTP not configured for real delivery. Alert message:\n%s", format_anomaly_message(anomaly))
            return

        await asyncio.to_thread(self._send_sync, anomaly)

    def _send_sync(self, anomaly: dict[str, Any]) -> None:
        """Blocking SMTP delivery, wrapped by asyncio.to_thread."""
        message = EmailMessage()
        service = anomaly.get("service", "unknown-service")
        level = anomaly.get("level", "UNKNOWN")
        message["From"] = settings.alert_email_from
        message["To"] = settings.alert_email_to
        message["Subject"] = f"[ANOMALY] {service} {level} spike detected"
        message.set_content(format_anomaly_message(anomaly))

        try:
            with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=10) as smtp:
                smtp.starttls()
                if settings.smtp_user and settings.smtp_password:
                    smtp.login(settings.smtp_user, settings.smtp_password)
                smtp.send_message(message)
        except Exception:
            logger.exception("Failed to send email alert")

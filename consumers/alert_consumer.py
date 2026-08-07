"""Kafka consumer that stores anomaly events and routes alerts."""

import asyncio
import logging
import signal
from datetime import datetime
from typing import Any

from aiokafka import AIOKafkaConsumer
from sqlalchemy import insert

from alerting.ai_analyzer import analyze_anomaly
from alerting.alert_router import AlertRouter
from consumers.base_consumer import decode_message, start_with_retry
from consumers.config import settings
from storage.db import async_session_factory, dispose_engine
from storage.models import AnomalyEvent


logger = logging.getLogger(__name__)


def install_shutdown_handlers(stop_event: asyncio.Event) -> None:
    """Install best-effort shutdown handlers."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: stop_event.set())


def normalize_anomaly(anomaly: dict[str, Any]) -> dict[str, Any] | None:
    """Convert an anomaly dict into database insert values."""
    try:
        row: dict[str, Any] = {
            "tenant_id": str(anomaly.get("tenant_id", "default")),
            "service": str(anomaly["service"]),
            "level": str(anomaly["level"]),
            "metric": str(anomaly["metric"]),
            "current_count": int(anomaly["current_count"]),
            "mean": float(anomaly["mean"]),
            "std_dev": float(anomaly["std_dev"]),
            "z_score": float(anomaly["z_score"]),
            "threshold": float(anomaly["threshold"]),
        }
        fired_at = anomaly.get("fired_at")
        if isinstance(fired_at, str):
            row["fired_at"] = datetime.fromisoformat(fired_at.replace("Z", "+00:00"))
        elif isinstance(fired_at, datetime):
            row["fired_at"] = fired_at
        return row
    except (KeyError, TypeError, ValueError):
        logger.exception("Skipping malformed anomaly event: %s", anomaly)
        return None


async def save_anomaly(anomaly: dict[str, Any]) -> None:
    """Store one anomaly event in TimescaleDB."""
    row = normalize_anomaly(anomaly)
    if row is None:
        return

    try:
        async with async_session_factory() as session:
            async with session.begin():
                await session.execute(insert(AnomalyEvent), [row])
    except Exception:
        logger.exception("Failed to save anomaly event")


async def consume_forever() -> None:
    """Read anomaly events from Kafka, save them, and send alerts."""
    stop_event = asyncio.Event()
    install_shutdown_handlers(stop_event)
    router = AlertRouter()

    consumer = AIOKafkaConsumer(
        settings.anomaly_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id="alert-group",
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )
    await start_with_retry(consumer, "alert Kafka consumer")
    logger.info("Alert consumer started")

    try:
        while not stop_event.is_set():
            message = await consumer.getone()
            anomaly = decode_message(message.value)
            if anomaly is None:
                continue
            await save_anomaly(anomaly)
            ai_analysis = await analyze_anomaly(anomaly)
            if ai_analysis:
                anomaly["ai_analysis"] = ai_analysis
            await router.route(anomaly)
    finally:
        await consumer.stop()
        await dispose_engine()
        logger.info("Alert consumer stopped")


def main() -> None:
    """Run the alert consumer service."""
    logging.basicConfig(level=logging.INFO)
    asyncio.run(consume_forever())


if __name__ == "__main__":
    main()

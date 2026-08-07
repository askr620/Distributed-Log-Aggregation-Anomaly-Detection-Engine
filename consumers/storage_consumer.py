"""Kafka consumer that batch-writes raw log events to TimescaleDB."""

import asyncio
import logging
import signal
from datetime import datetime
from typing import Any

from aiokafka import AIOKafkaConsumer
from sqlalchemy import insert

from consumers.base_consumer import decode_message, start_with_retry
from consumers.config import settings
from storage.db import async_session_factory, dispose_engine
from storage.models import LogEvent


logger = logging.getLogger(__name__)
BATCH_SIZE = 100
FLUSH_INTERVAL_SECONDS = 2.0


def normalize_log_event(event: dict[str, Any]) -> dict[str, Any] | None:
    """Convert an incoming event dict into DB insert values."""
    try:
        row: dict[str, Any] = {
            "service": str(event["service"]),
            "tenant_id": str(event.get("tenant_id", "default")),
            "level": str(event["level"]),
            "message": str(event["message"]),
            "metadata_": event.get("metadata") or {},
        }

        timestamp = event.get("timestamp")
        if isinstance(timestamp, str):
            row["created_at"] = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        elif isinstance(timestamp, datetime):
            row["created_at"] = timestamp

        return row
    except KeyError:
        logger.exception("Skipping malformed log event: %s", event)
        return None
    except ValueError:
        logger.exception("Skipping log event with invalid timestamp: %s", event)
        return None


async def flush_batch(batch: list[dict[str, Any]]) -> None:
    """Bulk insert a batch of log events into TimescaleDB."""
    if not batch:
        return

    rows = [row for event in batch if (row := normalize_log_event(event)) is not None]
    if not rows:
        batch.clear()
        return

    try:
        async with async_session_factory() as session:
            async with session.begin():
                await session.execute(insert(LogEvent), rows)
        logger.info("Inserted %s log events", len(rows))
    except Exception:
        logger.exception("Failed to insert log batch; skipping batch")
    finally:
        batch.clear()


def install_shutdown_handlers(stop_event: asyncio.Event) -> None:
    """Install best-effort shutdown handlers for local and container runs."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: stop_event.set())


async def consume_forever() -> None:
    """Read raw log events from Kafka and flush them to the database."""
    stop_event = asyncio.Event()
    install_shutdown_handlers(stop_event)

    consumer = AIOKafkaConsumer(
        settings.log_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id="storage-group",
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )

    batch: list[dict[str, Any]] = []
    await start_with_retry(consumer, "storage Kafka consumer")
    logger.info("Storage consumer started")

    try:
        while not stop_event.is_set():
            messages = await consumer.getmany(timeout_ms=int(FLUSH_INTERVAL_SECONDS * 1000), max_records=BATCH_SIZE)
            for records in messages.values():
                for message in records:
                    event = decode_message(message.value)
                    if event is not None:
                        batch.append(event)

                    if len(batch) >= BATCH_SIZE:
                        await flush_batch(batch)

            if batch:
                await flush_batch(batch)
    finally:
        await flush_batch(batch)
        await consumer.stop()
        await dispose_engine()
        logger.info("Storage consumer stopped")


def main() -> None:
    """Run the storage consumer service."""
    logging.basicConfig(level=logging.INFO)
    asyncio.run(consume_forever())


if __name__ == "__main__":
    main()

"""Kafka producer singleton used by the ingestion API."""

import asyncio
import json
import logging
from typing import Any

from aiokafka import AIOKafkaProducer

from config import settings


logger = logging.getLogger(__name__)
MAX_START_ATTEMPTS = 20
START_RETRY_SECONDS = 3


class KafkaLogProducer:
    """Small wrapper around AIOKafkaProducer for log-event publishing."""

    def __init__(self) -> None:
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        """Create and start the Kafka producer."""
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
            key_serializer=lambda value: value.encode("utf-8") if value else None,
        )
        for attempt in range(1, MAX_START_ATTEMPTS + 1):
            try:
                await self._producer.start()
                break
            except Exception:
                if attempt == MAX_START_ATTEMPTS:
                    logger.exception("Kafka producer failed to start")
                    raise
                logger.warning("Kafka not ready for producer, retrying in %s seconds", START_RETRY_SECONDS)
                await asyncio.sleep(START_RETRY_SECONDS)
        logger.info("Kafka producer started")

    async def stop(self) -> None:
        """Gracefully stop the Kafka producer."""
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None
            logger.info("Kafka producer stopped")

    async def publish_log_event(self, event: dict[str, Any]) -> None:
        """Publish a validated log event to the raw logs topic."""
        if self._producer is None:
            logger.error("Kafka producer is not started; dropping log event")
            return

        service = str(event.get("service", "unknown-service"))
        try:
            await self._producer.send(settings.log_topic, value=event, key=service)
        except Exception:
            logger.exception("Failed to publish log event to Kafka")


kafka_log_producer = KafkaLogProducer()

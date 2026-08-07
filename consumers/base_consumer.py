"""Shared helpers for Kafka consumer services."""

import json
import logging
import asyncio
from typing import Any


logger = logging.getLogger(__name__)
MAX_START_ATTEMPTS = 30
START_RETRY_SECONDS = 3


def decode_message(raw_value: bytes) -> dict[str, Any] | None:
    """Decode a Kafka message payload into a dictionary."""
    try:
        value = json.loads(raw_value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        logger.exception("Skipping invalid JSON message")
        return None

    if not isinstance(value, dict):
        logger.error("Skipping non-object Kafka message: %r", value)
        return None

    return value


async def start_with_retry(client: Any, label: str) -> None:
    """Start an async Kafka client, retrying while Docker dependencies boot."""
    for attempt in range(1, MAX_START_ATTEMPTS + 1):
        try:
            await client.start()
            return
        except Exception:
            if attempt == MAX_START_ATTEMPTS:
                logger.exception("%s failed to start", label)
                raise
            logger.warning("%s not ready, retrying in %s seconds", label, START_RETRY_SECONDS)
            await asyncio.sleep(START_RETRY_SECONDS)

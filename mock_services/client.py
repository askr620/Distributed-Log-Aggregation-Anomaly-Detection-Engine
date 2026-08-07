"""Shared HTTP client helper for mock services."""

import asyncio
import logging
from typing import Any

import httpx

from mock_services.config import settings


logger = logging.getLogger(__name__)


async def send_log(client: httpx.AsyncClient, payload: dict[str, Any]) -> None:
    """Send one log event to the ingestion API."""
    try:
        response = await client.post(
            f"{settings.ingestion_url}/logs",
            headers={"X-API-Key": settings.api_key},
            json=payload,
            timeout=5.0,
        )
        if response.status_code >= 300:
            logger.error("Ingestion rejected log: %s %s", response.status_code, response.text)
    except Exception:
        logger.exception("Failed to send log to ingestion API")
        await asyncio.sleep(1)


async def wait_for_ingestion() -> None:
    """Wait until the ingestion API health endpoint responds."""
    async with httpx.AsyncClient(timeout=3.0) as client:
        while True:
            try:
                response = await client.get(f"{settings.ingestion_url}/health")
                if response.status_code == 200:
                    return
            except Exception:
                logger.info("Waiting for ingestion API...")
            await asyncio.sleep(2)

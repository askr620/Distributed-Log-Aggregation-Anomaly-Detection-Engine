"""Mock order service that continuously sends order logs."""

import asyncio
import logging
import random
from uuid import uuid4

import httpx

from mock_services.client import send_log, wait_for_ingestion


MESSAGES = {
    "INFO": ["Order placed", "Order confirmed", "Order delivered"],
    "WARN": ["Order cancellation requested", "Inventory check slow"],
    "ERROR": ["Order confirmation failed", "Inventory reservation failed"],
}


def choose_level() -> str:
    """Choose a realistic order log level."""
    return random.choices(["INFO", "WARN", "ERROR"], weights=[85, 10, 5], k=1)[0]


async def main() -> None:
    """Run the order mock service."""
    logging.basicConfig(level=logging.INFO)
    await wait_for_ingestion()
    async with httpx.AsyncClient() as client:
        while True:
            level = choose_level()
            await send_log(
                client,
                {
                    "service": "order-service",
                    "level": level,
                    "message": random.choice(MESSAGES[level]),
                    "metadata": {
                        "order_id": str(uuid4()),
                        "user_id": str(uuid4()),
                        "item_count": random.randint(1, 8),
                        "total_amount": round(random.uniform(200, 7000), 2),
                    },
                },
            )
            await asyncio.sleep(0.3)


if __name__ == "__main__":
    asyncio.run(main())

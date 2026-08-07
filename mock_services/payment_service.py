"""Mock payment service that sends normal logs and periodic error spikes."""

import asyncio
import logging
import random
import time
from uuid import uuid4

import httpx

from mock_services.client import send_log, wait_for_ingestion


MESSAGES = {
    "INFO": ["Payment processed", "Refund processed", "Payment authorized"],
    "WARN": ["Gateway latency high", "Payment retry scheduled"],
    "ERROR": ["Payment failed", "Gateway timeout", "Card authorization failed"],
    "CRITICAL": ["Payment gateway outage", "All payment attempts failing"],
}


def choose_normal_level() -> str:
    """Choose a normal payment log level."""
    return random.choices(["INFO", "WARN", "ERROR"], weights=[90, 7, 3], k=1)[0]


def payment_payload(level: str) -> dict:
    """Create one payment log payload."""
    return {
        "service": "payment-service",
        "level": level,
        "message": random.choice(MESSAGES[level]),
        "metadata": {
            "amount": round(random.uniform(100, 12000), 2),
            "gateway": random.choice(["razorpay", "stripe", "adyen"]),
            "user_id": str(uuid4()),
            "order_id": str(uuid4()),
        },
    }


async def send_spike(client: httpx.AsyncClient, duration_seconds: int = 30) -> None:
    """Send a burst of ERROR logs to trigger anomaly detection."""
    logging.warning("Starting payment-service ERROR spike")
    deadline = time.monotonic() + duration_seconds
    while time.monotonic() < deadline:
        tasks = [send_log(client, payment_payload("ERROR")) for _ in range(20)]
        await asyncio.gather(*tasks)
        await asyncio.sleep(1)
    logging.warning("Payment-service ERROR spike finished")


async def main() -> None:
    """Run the payment mock service."""
    logging.basicConfig(level=logging.INFO)
    await wait_for_ingestion()
    next_spike = time.monotonic() + random.randint(120, 300)

    async with httpx.AsyncClient() as client:
        while True:
            if time.monotonic() >= next_spike:
                await send_spike(client)
                next_spike = time.monotonic() + random.randint(120, 300)

            level = choose_normal_level()
            await send_log(client, payment_payload(level))
            await asyncio.sleep(0.5)


if __name__ == "__main__":
    asyncio.run(main())

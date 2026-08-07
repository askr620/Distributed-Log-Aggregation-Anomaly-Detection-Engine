"""Mock authentication service that continuously sends auth logs."""

import asyncio
import logging
import random
from uuid import uuid4

import httpx

from mock_services.client import send_log, wait_for_ingestion


MESSAGES = {
    "INFO": ["User logged in", "User logged out", "Token refreshed"],
    "WARN": ["Repeated login attempt", "Password reset requested"],
    "ERROR": ["Invalid token verification failed", "Login provider timeout"],
}


def choose_level() -> str:
    """Choose a realistic auth log level."""
    return random.choices(["INFO", "WARN", "ERROR"], weights=[90, 5, 5], k=1)[0]


async def main() -> None:
    """Run the auth mock service."""
    logging.basicConfig(level=logging.INFO)
    await wait_for_ingestion()
    async with httpx.AsyncClient() as client:
        while True:
            level = choose_level()
            await send_log(
                client,
                {
                    "service": "auth-service",
                    "level": level,
                    "message": random.choice(MESSAGES[level]),
                    "metadata": {
                        "user_id": str(uuid4()),
                        "ip_address": f"10.0.0.{random.randint(1, 254)}",
                        "auth_method": random.choice(["password", "google", "github"]),
                    },
                },
            )
            await asyncio.sleep(0.2)


if __name__ == "__main__":
    asyncio.run(main())

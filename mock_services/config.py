"""Configuration for mock log-generating services."""

from dataclasses import dataclass
from os import getenv

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class MockSettings:
    """Runtime settings for mock services."""

    ingestion_url: str = getenv("INGESTION_URL", "http://localhost:8000")
    api_key: str = getenv("API_KEY", "super-secret-key-123")


settings = MockSettings()

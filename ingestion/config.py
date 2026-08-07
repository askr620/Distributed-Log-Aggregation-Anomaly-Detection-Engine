"""Environment-based configuration for the ingestion service."""

from dataclasses import dataclass
from os import getenv

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    """Runtime settings loaded from environment variables."""

    api_key: str = getenv("API_KEY", "super-secret-key-123")
    kafka_bootstrap_servers: str = getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    log_topic: str = getenv("LOG_TOPIC", "raw-logs")


settings = Settings()

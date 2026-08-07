"""Environment-based configuration for consumer services."""

from dataclasses import dataclass
from os import getenv

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class ConsumerSettings:
    """Runtime settings shared by Kafka consumers."""

    kafka_bootstrap_servers: str = getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    log_topic: str = getenv("LOG_TOPIC", "raw-logs")
    anomaly_topic: str = getenv("ANOMALY_TOPIC", "anomaly-events")
    redis_url: str = getenv("REDIS_URL", "redis://localhost:6379/0")
    database_url: str = getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://loguser:logpass@localhost:5432/logsdb",
    )
    window_size_seconds: int = int(getenv("WINDOW_SIZE_SECONDS", "60"))
    z_score_threshold: float = float(getenv("Z_SCORE_THRESHOLD", "2.5"))
    min_samples: int = int(getenv("MIN_SAMPLES", "10"))
    alert_cooldown_seconds: int = int(getenv("ALERT_COOLDOWN_SECONDS", "180"))
    dashboard_username: str = getenv("DASHBOARD_USERNAME", "admin")
    dashboard_password: str = getenv("DASHBOARD_PASSWORD", "admin123")
    dashboard_token: str = getenv("DASHBOARD_TOKEN", "dev-dashboard-token")
    slack_webhook_url: str = getenv("SLACK_WEBHOOK_URL", "")
    alert_email_from: str = getenv("ALERT_EMAIL_FROM", "alerts@example.com")
    alert_email_to: str = getenv("ALERT_EMAIL_TO", "you@example.com")
    smtp_host: str = getenv("SMTP_HOST", "localhost")
    smtp_port: int = int(getenv("SMTP_PORT", "1025"))
    smtp_user: str = getenv("SMTP_USER", "")
    smtp_password: str = getenv("SMTP_PASSWORD", "")
    openai_api_key: str = getenv("OPENAI_API_KEY", "")
    openai_model: str = getenv("OPENAI_MODEL", "llama-3.1-8b-instant")


settings = ConsumerSettings()

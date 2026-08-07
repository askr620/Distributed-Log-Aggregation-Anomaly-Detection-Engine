"""Tests for the FastAPI log ingestion API."""

import sys
from pathlib import Path

from fastapi.testclient import TestClient


PROJECT_ROOT = Path(__file__).resolve().parents[1]
INGESTION_ROOT = PROJECT_ROOT / "ingestion"
sys.path.insert(0, str(INGESTION_ROOT))

from main import app  # noqa: E402
from kafka_client.producer import kafka_log_producer  # noqa: E402


class FakeProducer:
    """In-memory producer used to avoid Kafka in API tests."""

    def __init__(self) -> None:
        self.events: list[dict] = []

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def publish_log_event(self, event: dict) -> None:
        self.events.append(event)


def make_client() -> tuple[TestClient, FakeProducer]:
    """Create a test client with Kafka publishing replaced by a fake."""
    fake = FakeProducer()
    kafka_log_producer.start = fake.start
    kafka_log_producer.stop = fake.stop
    kafka_log_producer.publish_log_event = fake.publish_log_event
    return TestClient(app), fake


def test_health_endpoint() -> None:
    client, _ = make_client()
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_valid_log_accepted() -> None:
    client, fake = make_client()

    response = client.post(
        "/logs",
        headers={"X-API-Key": "super-secret-key-123"},
        json={
            "service": "payment-service",
            "level": "ERROR",
            "message": "Payment failed",
            "metadata": {"order_id": "ord_123"},
        },
    )

    assert response.status_code == 202
    assert response.json() == {"status": "accepted"}
    assert fake.events[0]["service"] == "payment-service"


def test_missing_api_key_returns_401() -> None:
    client, _ = make_client()

    response = client.post(
        "/logs",
        json={
            "service": "payment-service",
            "level": "ERROR",
            "message": "Payment failed",
        },
    )

    assert response.status_code == 401


def test_wrong_api_key_returns_401() -> None:
    client, _ = make_client()

    response = client.post(
        "/logs",
        headers={"X-API-Key": "wrong"},
        json={
            "service": "payment-service",
            "level": "ERROR",
            "message": "Payment failed",
        },
    )

    assert response.status_code == 401


def test_batch_log_accepted() -> None:
    client, fake = make_client()

    response = client.post(
        "/logs/batch",
        headers={"X-API-Key": "super-secret-key-123"},
        json={
            "logs": [
                {"service": "auth-service", "level": "INFO", "message": "User logged in"},
                {"service": "order-service", "level": "WARN", "message": "Slow order lookup"},
                {"service": "payment-service", "level": "ERROR", "message": "Payment timeout"},
            ]
        },
    )

    assert response.status_code == 202
    assert response.json() == {"status": "accepted", "count": 3}
    assert len(fake.events) == 3


def test_invalid_level_rejected() -> None:
    client, _ = make_client()

    response = client.post(
        "/logs",
        headers={"X-API-Key": "super-secret-key-123"},
        json={
            "service": "payment-service",
            "level": "BAD",
            "message": "Invalid level should fail",
        },
    )

    assert response.status_code == 422

"""Pydantic models for structured log ingestion."""

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


LogLevel = Literal["INFO", "WARN", "ERROR", "CRITICAL"]


class LogEvent(BaseModel):
    """Validated log event sent by a microservice."""

    service: str = Field(..., min_length=1, max_length=120)
    tenant_id: str = Field(default="default", min_length=1, max_length=120)
    level: LogLevel
    message: str = Field(..., min_length=1, max_length=5000)
    metadata: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("service", "tenant_id", "message")
    @classmethod
    def strip_non_empty_text(cls, value: str) -> str:
        """Trim text fields and reject values that become empty."""
        stripped = value.strip()
        if not stripped:
            raise ValueError("field cannot be empty")
        return stripped


class LogBatchRequest(BaseModel):
    """Request body for batch log ingestion."""

    logs: list[LogEvent] = Field(..., min_length=1, max_length=1000)

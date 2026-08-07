"""Base interface for alert delivery channels."""

from abc import ABC, abstractmethod
from typing import Any


class BaseAlert(ABC):
    """Abstract alert channel."""

    @abstractmethod
    async def send(self, anomaly: dict[str, Any]) -> None:
        """Send one anomaly alert."""

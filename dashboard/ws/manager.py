"""Connection manager for live dashboard WebSocket clients."""

import json
import logging
from typing import Any

from fastapi import WebSocket


logger = logging.getLogger(__name__)


class ConnectionManager:
    """Track active WebSocket clients and broadcast messages."""

    def __init__(self) -> None:
        self.active_connections: set[WebSocket] = set()

    async def connect(self, websocket: WebSocket) -> None:
        """Accept and register one WebSocket client."""
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info("Dashboard client connected; total=%s", len(self.active_connections))

    def disconnect(self, websocket: WebSocket) -> None:
        """Remove one WebSocket client."""
        self.active_connections.discard(websocket)
        logger.info("Dashboard client disconnected; total=%s", len(self.active_connections))

    async def broadcast(self, payload: dict[str, Any]) -> None:
        """Broadcast JSON payload to all connected clients."""
        if not self.active_connections:
            return

        message = json.dumps(payload, default=str)
        stale: list[WebSocket] = []
        for websocket in self.active_connections:
            try:
                await websocket.send_text(message)
            except Exception:
                logger.exception("Failed to send WebSocket message")
                stale.append(websocket)

        for websocket in stale:
            self.disconnect(websocket)


manager = ConnectionManager()

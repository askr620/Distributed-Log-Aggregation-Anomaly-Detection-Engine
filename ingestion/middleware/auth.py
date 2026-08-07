"""API-key authentication dependency for ingestion endpoints."""

from fastapi import Header, HTTPException, status

from config import settings


async def verify_api_key(x_api_key: str | None = Header(default=None)) -> None:
    """Reject requests that do not include the configured API key."""
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-API-Key header",
        )

    if x_api_key != settings.api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )

"""FastAPI application entrypoint for structured log ingestion."""

import logging
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

from fastapi import FastAPI

from kafka_client.producer import kafka_log_producer
from routers.logs import router as logs_router


logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    """Start and stop service dependencies with the FastAPI lifecycle."""
    await kafka_log_producer.start()
    try:
        yield
    finally:
        await kafka_log_producer.stop()


app = FastAPI(
    title="Distributed Log Ingestion API",
    version="0.1.0",
    lifespan=lifespan,
)
app.include_router(logs_router)

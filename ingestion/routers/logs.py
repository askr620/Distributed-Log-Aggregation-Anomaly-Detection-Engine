"""HTTP routes for accepting structured log events."""

from fastapi import APIRouter, BackgroundTasks, Depends, status

from kafka_client.producer import kafka_log_producer
from middleware.auth import verify_api_key
from schemas.log_event import LogBatchRequest, LogEvent


router = APIRouter(tags=["logs"])


@router.get("/health")
async def health() -> dict[str, str]:
    """Return a simple health status."""
    return {"status": "ok"}


@router.post("/logs", status_code=status.HTTP_202_ACCEPTED)
async def ingest_log(
    log_event: LogEvent,
    background_tasks: BackgroundTasks,
    _: None = Depends(verify_api_key),
) -> dict[str, str]:
    """Accept one log event and enqueue Kafka publishing after the response."""
    background_tasks.add_task(kafka_log_producer.publish_log_event, log_event.model_dump(mode="json"))
    return {"status": "accepted"}


@router.post("/logs/batch", status_code=status.HTTP_202_ACCEPTED)
async def ingest_log_batch(
    batch: LogBatchRequest,
    background_tasks: BackgroundTasks,
    _: None = Depends(verify_api_key),
) -> dict[str, int | str]:
    """Accept multiple log events and enqueue Kafka publishing after the response."""
    for log_event in batch.logs:
        background_tasks.add_task(kafka_log_producer.publish_log_event, log_event.model_dump(mode="json"))
    return {"status": "accepted", "count": len(batch.logs)}

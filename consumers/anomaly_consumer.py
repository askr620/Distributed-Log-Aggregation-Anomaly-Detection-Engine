"""Kafka consumer that detects log anomalies and publishes anomaly events."""

import asyncio
import json
import logging
import signal
import time
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from redis.asyncio import Redis
from sqlalchemy import text

from anomaly.detector import DetectionConfig, detect_anomaly
from anomaly.redis_window import RedisSlidingWindow
from consumers.base_consumer import decode_message, start_with_retry
from consumers.config import settings
from storage.db import async_session_factory, dispose_engine


logger = logging.getLogger(__name__)
RULE_CACHE_TTL_SECONDS = 10.0
RuleCacheKey = tuple[str, str, str]
rule_threshold_cache: dict[RuleCacheKey, tuple[float, float]] = {}
SEVERITY_PRIORITY = {"UNKNOWN": 0, "INFO": 1, "WARN": 2, "ERROR": 3, "CRITICAL": 4}


def install_shutdown_handlers(stop_event: asyncio.Event) -> None:
    """Install best-effort shutdown handlers for local and container runs."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: stop_event.set())


def extract_service_level(event: dict[str, Any]) -> tuple[str, str] | None:
    """Read service and level from a raw log event."""
    service = event.get("service")
    level = event.get("level")
    if not isinstance(service, str) or not isinstance(level, str):
        logger.error("Skipping event without string service/level: %s", event)
        return None
    return service, level


async def process_event(
    event: dict[str, Any],
    window: RedisSlidingWindow,
    config: DetectionConfig,
) -> dict[str, Any] | None:
    """Update Redis counters and return an anomaly event when detected."""
    service_level = extract_service_level(event)
    if service_level is None:
        return None

    service, level = service_level
    tenant_id = str(event.get("tenant_id", "default"))
    try:
        bucket, current_count = await window.increment_current(service, level)
        history_counts = await window.get_previous_counts(service, level, bucket, count=config.min_samples)
    except Exception:
        logger.exception("Redis unavailable or failed; skipping anomaly detection")
        return None

    rule_threshold = await get_alert_rule_threshold(
        tenant_id=tenant_id,
        service=service,
        level=level,
        fallback=config.z_score_threshold,
    )
    active_config = DetectionConfig(
        z_score_threshold=rule_threshold,
        min_samples=config.min_samples,
    )
    return detect_anomaly(
        service=service,
        level=level,
        current_count=current_count,
        history_counts=history_counts,
        config=active_config,
    )


async def get_alert_rule_threshold(*, tenant_id: str, service: str, level: str, fallback: float) -> float:
    """Return the best matching enabled alert rule threshold, or the default."""
    cache_key = (tenant_id, service, level)
    now = time.monotonic()
    cached = rule_threshold_cache.get(cache_key)
    if cached and now - cached[0] < RULE_CACHE_TTL_SECONDS:
        return cached[1]

    threshold = fallback
    try:
        async with async_session_factory() as session:
            row = (await session.execute(
                text(
                    """
                    SELECT z_score_threshold
                    FROM alert_rules
                    WHERE tenant_id = :tenant_id
                      AND enabled = TRUE
                      AND level = :level
                      AND service IN (:service, '*')
                    ORDER BY CASE WHEN service = :service THEN 0 ELSE 1 END, id DESC
                    LIMIT 1
                    """
                ),
                {"tenant_id": tenant_id, "service": service, "level": level},
            )).mappings().first()
        if row is not None:
            threshold = float(row["z_score_threshold"])
    except Exception:
        logger.exception("Failed to load alert rule threshold; using default threshold")

    rule_threshold_cache[cache_key] = (now, threshold)
    return threshold


async def cooldown_allows_publish(redis: Redis, anomaly: dict[str, Any]) -> bool:
    """Return false for duplicates, but allow escalation to higher severity."""
    tenant_id = str(anomaly.get("tenant_id", "default"))
    key = f"anomaly:cooldown:{tenant_id}:{anomaly['service']}:{anomaly['level']}"
    severity = str(anomaly.get("severity") or anomaly.get("level") or "UNKNOWN")
    existing = await redis.get(key)
    if existing is None:
        await redis.set(key, severity, ex=settings.alert_cooldown_seconds)
        return True

    existing_severity = existing.decode("utf-8") if isinstance(existing, bytes) else str(existing)
    if SEVERITY_PRIORITY.get(severity, 0) > SEVERITY_PRIORITY.get(existing_severity, 0):
        await redis.set(key, severity, ex=settings.alert_cooldown_seconds)
        return True

    return False


async def consume_forever() -> None:
    """Read raw logs, detect anomalies, and publish anomaly events."""
    stop_event = asyncio.Event()
    install_shutdown_handlers(stop_event)

    redis = Redis.from_url(settings.redis_url, decode_responses=False)
    window = RedisSlidingWindow(redis, window_size_seconds=settings.window_size_seconds)
    config = DetectionConfig(
        z_score_threshold=settings.z_score_threshold,
        min_samples=settings.min_samples,
    )

    consumer = AIOKafkaConsumer(
        settings.log_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id="anomaly-group",
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8") if value else None,
    )

    await start_with_retry(consumer, "anomaly Kafka consumer")
    await start_with_retry(producer, "anomaly Kafka producer")
    logger.info("Anomaly consumer started")

    try:
        while not stop_event.is_set():
            message = await consumer.getone()
            event = decode_message(message.value)
            if event is None:
                continue

            anomaly = await process_event(event, window, config)
            if anomaly is not None:
                anomaly["tenant_id"] = str(event.get("tenant_id", "default"))
                if not await cooldown_allows_publish(redis, anomaly):
                    logger.info("Skipping duplicate anomaly during cooldown: %s", anomaly)
                    continue
                await producer.send(settings.anomaly_topic, value=anomaly, key=anomaly["service"])
                logger.warning("Published anomaly event: %s", anomaly)
    finally:
        await consumer.stop()
        await producer.stop()
        await redis.aclose()
        await dispose_engine()
        logger.info("Anomaly consumer stopped")


def main() -> None:
    """Run the anomaly consumer service."""
    logging.basicConfig(level=logging.INFO)
    asyncio.run(consume_forever())


if __name__ == "__main__":
    main()

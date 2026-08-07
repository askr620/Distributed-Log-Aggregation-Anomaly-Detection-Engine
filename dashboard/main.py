"""FastAPI WebSocket dashboard and observability APIs."""

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from secrets import compare_digest
from typing import Any

import httpx
from aiokafka import AIOKafkaConsumer
from fastapi import Depends, FastAPI, Header, HTTPException, Query, WebSocket, WebSocketDisconnect, status
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from sqlalchemy import text

from consumers.base_consumer import decode_message, start_with_retry
from consumers.config import settings
from dashboard.ws.manager import manager
from storage.db import async_session_factory, dispose_engine
from alerting.ai_analyzer import analyze_anomaly


logger = logging.getLogger(__name__)


class LoginRequest(BaseModel):
    """Dashboard login request."""

    username: str
    password: str


class RumEventRequest(BaseModel):
    """Browser real-user monitoring event."""

    session_id: str = Field(..., min_length=1, max_length=120)
    path: str = Field(..., min_length=1, max_length=500)
    event_type: str = Field(..., min_length=1, max_length=80)
    value: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    tenant_id: str = "default"


class AlertRuleRequest(BaseModel):
    """Create/update alert rule request."""

    name: str = Field(..., min_length=1, max_length=160)
    service: str = Field(default="*", min_length=1, max_length=120)
    level: str = Field(default="ERROR", pattern="^(ERROR|CRITICAL|WARN|INFO)$")
    z_score_threshold: float = Field(default=2.5, gt=0)
    enabled: bool = True
    tenant_id: str = "default"


class AlertRulePatchRequest(BaseModel):
    """Update alert rule enabled state."""

    enabled: bool


async def require_auth(authorization: str | None = Header(default=None)) -> None:
    """Protect dashboard JSON APIs with a simple bearer token."""
    expected = f"Bearer {settings.dashboard_token}"
    if not authorization or not compare_digest(authorization, expected):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")


async def ensure_dashboard_tables() -> None:
    """Apply lightweight schema upgrades for already-created local volumes."""
    statements = [
        "ALTER TABLE log_events ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default'",
        "ALTER TABLE anomaly_events ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default'",
        """
        CREATE TABLE IF NOT EXISTS alert_rules (
            id BIGSERIAL PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            name TEXT NOT NULL,
            service TEXT NOT NULL DEFAULT '*',
            level TEXT NOT NULL DEFAULT 'ERROR',
            z_score_threshold DOUBLE PRECISION NOT NULL DEFAULT 2.5,
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS rum_events (
            id BIGSERIAL,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            session_id TEXT NOT NULL,
            path TEXT NOT NULL,
            event_type TEXT NOT NULL,
            value DOUBLE PRECISION,
            metadata JSONB NOT NULL DEFAULT '{}',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        "SELECT create_hypertable('rum_events', 'created_at', if_not_exists => TRUE)",
        """
        CREATE TABLE IF NOT EXISTS synthetic_checks (
            id BIGSERIAL,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            target TEXT NOT NULL,
            status TEXT NOT NULL,
            latency_ms DOUBLE PRECISION,
            status_code INTEGER,
            error TEXT,
            checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        "SELECT create_hypertable('synthetic_checks', 'checked_at', if_not_exists => TRUE)",
    ]
    async with async_session_factory() as session:
        async with session.begin():
            for statement in statements:
                await session.execute(text(statement))


async def consume_anomalies(stop_event: asyncio.Event) -> None:
    """Read anomaly events from Kafka and broadcast to browser clients."""
    consumer = AIOKafkaConsumer(
        settings.anomaly_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id="dashboard-group",
        enable_auto_commit=True,
        auto_offset_reset="latest",
    )
    await start_with_retry(consumer, "dashboard Kafka consumer")
    logger.info("Dashboard Kafka consumer started")
    try:
        while not stop_event.is_set():
            try:
                message = await asyncio.wait_for(consumer.getone(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            anomaly = decode_message(message.value)
            if anomaly is not None:
                await manager.broadcast(anomaly)
    finally:
        await consumer.stop()
        logger.info("Dashboard Kafka consumer stopped")


async def run_synthetic_checks(stop_event: asyncio.Event) -> None:
    """Periodically check important local service endpoints."""
    targets = {"ingestion-health": "http://ingestion:8000/health", "dashboard-health": "http://dashboard:8001/health"}
    async with httpx.AsyncClient(timeout=5.0) as client:
        while not stop_event.is_set():
            for target, url in targets.items():
                started = time.perf_counter()
                status_text = "ok"
                status_code: int | None = None
                error: str | None = None
                try:
                    response = await client.get(url)
                    status_code = response.status_code
                    if response.status_code >= 300:
                        status_text = "failed"
                except Exception as exc:
                    status_text = "failed"
                    error = str(exc)[:500]
                latency_ms = round((time.perf_counter() - started) * 1000, 2)
                async with async_session_factory() as session:
                    async with session.begin():
                        await session.execute(
                            text(
                                """
                                INSERT INTO synthetic_checks (tenant_id, target, status, latency_ms, status_code, error)
                                VALUES ('default', :target, :status, :latency_ms, :status_code, :error)
                                """
                            ),
                            {"target": target, "status": status_text, "latency_ms": latency_ms, "status_code": status_code, "error": error},
                        )
            await asyncio.sleep(30)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    """Start dashboard background tasks."""
    await ensure_dashboard_tables()
    stop_event = asyncio.Event()
    tasks = [
        asyncio.create_task(consume_anomalies(stop_event)),
        asyncio.create_task(run_synthetic_checks(stop_event)),
    ]
    try:
        yield
    finally:
        stop_event.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        await dispose_engine()


app = FastAPI(title="Log Engine Dashboard", lifespan=lifespan)


@app.middleware("http")
async def no_store_dashboard_api_cache(request, call_next):
    """Prevent browsers from caching dashboard data APIs."""
    response = await call_next(request)
    if request.url.path.startswith("/api/") or request.url.path in {"/", "/advanced"}:
        response.headers["Cache-Control"] = "no-store"
    return response


@app.get("/health")
async def health() -> dict[str, str]:
    """Return dashboard health status."""
    return {"status": "ok"}


@app.get("/")
async def index() -> HTMLResponse:
    """Serve the static dashboard page."""
    with open("dashboard/static/index.html", encoding="utf-8") as file:
        return HTMLResponse(file.read(), headers={"Cache-Control": "no-store"})


@app.get("/advanced")
async def advanced_dashboard() -> HTMLResponse:
    """Serve the same dashboard for old /advanced links."""
    with open("dashboard/static/index.html", encoding="utf-8") as file:
        return HTMLResponse(file.read(), headers={"Cache-Control": "no-store"})


@app.post("/api/login")
async def login(payload: LoginRequest) -> dict[str, str]:
    """Return a static bearer token when dashboard credentials match."""
    valid_user = compare_digest(payload.username, settings.dashboard_username)
    valid_password = compare_digest(payload.password, settings.dashboard_password)
    if not (valid_user and valid_password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password")
    return {"token": settings.dashboard_token}


@app.get("/api/me", dependencies=[Depends(require_auth)])
async def current_dashboard_user() -> dict[str, str]:
    """Confirm the current dashboard token is valid."""
    return {"username": settings.dashboard_username, "tenant_id": "default", "status": "authenticated"}


@app.get("/api/logs", dependencies=[Depends(require_auth)])
async def search_logs(
    tenant_id: str = "default",
    service: str | None = None,
    level: str | None = None,
    q: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
) -> list[dict[str, Any]]:
    """Search recent logs by tenant, service, level, and message text."""
    clauses = ["tenant_id = :tenant_id"]
    params: dict[str, Any] = {"tenant_id": tenant_id, "limit": limit}
    if service:
        clauses.append("service = :service")
        params["service"] = service
    if level:
        clauses.append("level = :level")
        params["level"] = level
    if q:
        clauses.append("message ILIKE :q")
        params["q"] = f"%{q}%"
    where = " AND ".join(clauses)
    query = text(
        f"""
        SELECT service, tenant_id, level, message, metadata, created_at
        FROM log_events
        WHERE {where}
        ORDER BY created_at DESC
        LIMIT :limit
        """
    )
    async with async_session_factory() as session:
        rows = (await session.execute(query, params)).mappings().all()
    return [dict(row) for row in rows]


@app.get("/api/metrics/summary", dependencies=[Depends(require_auth)])
async def metrics_summary(tenant_id: str = "default") -> dict[str, Any]:
    """Return log-derived metrics for charts and summary cards."""
    async with async_session_factory() as session:
        recent_totals = (await session.execute(
            text(
                """
                SELECT
                    count(*) AS total,
                    count(*) FILTER (WHERE level IN ('ERROR', 'CRITICAL')) AS errors,
                    count(DISTINCT service) AS services
                FROM log_events
                WHERE tenant_id = :tenant_id AND created_at > NOW() - INTERVAL '15 minutes'
                """
            ),
            {"tenant_id": tenant_id},
        )).mappings().one()
        all_time_totals = (await session.execute(
            text(
                """
                SELECT
                    count(*) AS total,
                    count(*) FILTER (WHERE level IN ('ERROR', 'CRITICAL')) AS errors,
                    count(DISTINCT service) AS services,
                    COALESCE(max(id), 0) AS latest_log_id,
                    COALESCE(max(id) FILTER (WHERE level IN ('ERROR', 'CRITICAL')), 0) AS latest_error_id
                FROM log_events
                WHERE tenant_id = :tenant_id
                """
            ),
            {"tenant_id": tenant_id},
        )).mappings().one()
        anomaly_total = (await session.execute(
            text(
                """
                SELECT count(*) AS total
                FROM anomaly_events
                WHERE tenant_id = :tenant_id
                """
            ),
            {"tenant_id": tenant_id},
        )).scalar_one()
        by_service = (await session.execute(
            text(
                """
                SELECT service, count(*) AS count
                FROM log_events
                WHERE tenant_id = :tenant_id AND created_at > NOW() - INTERVAL '15 minutes'
                GROUP BY service
                ORDER BY count DESC
                """
            ),
            {"tenant_id": tenant_id},
        )).mappings().all()
        timeline = (await session.execute(
            text(
                """
                SELECT date_trunc('minute', created_at) AS minute, level, count(*) AS count
                FROM log_events
                WHERE tenant_id = :tenant_id AND created_at > NOW() - INTERVAL '15 minutes'
                GROUP BY minute, level
                ORDER BY minute ASC
                """
            ),
            {"tenant_id": tenant_id},
        )).mappings().all()
    return {
        "totals": dict(recent_totals),
        "all_time_totals": dict(all_time_totals),
        "anomaly_total": anomaly_total,
        "by_service": [dict(row) for row in by_service],
        "timeline": [dict(row) for row in timeline],
    }


@app.get("/api/anomalies", dependencies=[Depends(require_auth)])
async def anomaly_history(tenant_id: str = "default", limit: int = Query(default=50, ge=1, le=200)) -> list[dict[str, Any]]:
    """Return recent persisted anomaly events for dashboard history."""
    async with async_session_factory() as session:
        rows = (await session.execute(
            text(
                """
                SELECT id, tenant_id, service, level, metric, current_count, mean, std_dev, z_score, threshold, fired_at
                FROM anomaly_events
                WHERE tenant_id = :tenant_id
                ORDER BY fired_at DESC
                LIMIT :limit
                """
            ),
            {"tenant_id": tenant_id, "limit": limit},
        )).mappings().all()
    return [dict(row) for row in rows]


@app.get("/api/infrastructure/status", dependencies=[Depends(require_auth)])
async def infrastructure_status() -> list[dict[str, Any]]:
    """Check basic connectivity for local infrastructure services."""
    checks = [("kafka", "kafka", 9092), ("redis", "redis", 6379), ("timescaledb", "timescaledb", 5432), ("ingestion", "ingestion", 8000)]
    results = []
    for name, host, port in checks:
        started = time.perf_counter()
        status_text = "ok"
        error = None
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=3.0)
            writer.close()
            await writer.wait_closed()
        except Exception as exc:
            status_text = "failed"
            error = str(exc)
        results.append({"name": name, "status": status_text, "latency_ms": round((time.perf_counter() - started) * 1000, 2), "error": error})
    return results


@app.post("/api/rum", status_code=202)
async def create_rum_event(payload: RumEventRequest) -> dict[str, str]:
    """Store one browser-side real-user monitoring event."""
    async with async_session_factory() as session:
        async with session.begin():
            await session.execute(
                text(
                    """
                    INSERT INTO rum_events (tenant_id, session_id, path, event_type, value, metadata)
                    VALUES (:tenant_id, :session_id, :path, :event_type, :value, CAST(:metadata AS JSONB))
                    """
                ),
                {**payload.model_dump(exclude={"metadata"}), "metadata": json.dumps(payload.metadata)},
            )
    return {"status": "accepted"}


@app.get("/api/rum/summary", dependencies=[Depends(require_auth)])
async def rum_summary(tenant_id: str = "default") -> dict[str, Any]:
    """Return simple browser monitoring statistics."""
    async with async_session_factory() as session:
        rows = (await session.execute(
            text(
                """
                SELECT event_type, count(*) AS count, avg(value) AS avg_value
                FROM rum_events
                WHERE tenant_id = :tenant_id AND created_at > NOW() - INTERVAL '1 hour'
                GROUP BY event_type
                ORDER BY count DESC
                """
            ),
            {"tenant_id": tenant_id},
        )).mappings().all()
    return {"events": [dict(row) for row in rows]}


@app.get("/api/synthetics", dependencies=[Depends(require_auth)])
async def synthetic_results(tenant_id: str = "default") -> list[dict[str, Any]]:
    """Return recent synthetic health check results."""
    async with async_session_factory() as session:
        rows = (await session.execute(
            text(
                """
                SELECT DISTINCT ON (target) target, status, latency_ms, status_code, error, checked_at
                FROM synthetic_checks
                WHERE tenant_id = :tenant_id
                ORDER BY target, checked_at DESC
                """
            ),
            {"tenant_id": tenant_id},
        )).mappings().all()
    return [dict(row) for row in rows]


@app.get("/api/security/summary", dependencies=[Depends(require_auth)])
async def security_summary(tenant_id: str = "default") -> dict[str, Any]:
    """Return log-derived security signals."""
    async with async_session_factory() as session:
        rows = (await session.execute(
            text(
                """
                SELECT service, count(*) AS suspicious_events
                FROM log_events
                WHERE tenant_id = :tenant_id
                  AND created_at > NOW() - INTERVAL '30 minutes'
                  AND (
                    service = 'auth-service'
                    OR message ILIKE '%token%'
                    OR message ILIKE '%login%'
                    OR message ILIKE '%password%'
                  )
                  AND level IN ('WARN', 'ERROR', 'CRITICAL')
                GROUP BY service
                ORDER BY suspicious_events DESC
                """
            ),
            {"tenant_id": tenant_id},
        )).mappings().all()
    return {"signals": [dict(row) for row in rows]}


@app.get("/api/alert-rules", dependencies=[Depends(require_auth)])
async def list_alert_rules(tenant_id: str = "default") -> list[dict[str, Any]]:
    """List configured alert rules."""
    async with async_session_factory() as session:
        rows = (await session.execute(
            text("SELECT id, tenant_id, name, service, level, z_score_threshold, enabled, created_at FROM alert_rules WHERE tenant_id = :tenant_id ORDER BY id DESC"),
            {"tenant_id": tenant_id},
        )).mappings().all()
    return [dict(row) for row in rows]


@app.post("/api/alert-rules", dependencies=[Depends(require_auth)])
async def create_alert_rule(payload: AlertRuleRequest) -> dict[str, Any]:
    """Create a dashboard alert rule record."""
    async with async_session_factory() as session:
        async with session.begin():
            row = (await session.execute(
                text(
                    """
                    INSERT INTO alert_rules (tenant_id, name, service, level, z_score_threshold, enabled)
                    VALUES (:tenant_id, :name, :service, :level, :z_score_threshold, :enabled)
                    RETURNING id, tenant_id, name, service, level, z_score_threshold, enabled, created_at
                    """
                ),
                payload.model_dump(),
            )).mappings().one()
    return dict(row)


@app.patch("/api/alert-rules/{rule_id}", dependencies=[Depends(require_auth)])
async def update_alert_rule(rule_id: int, payload: AlertRulePatchRequest, tenant_id: str = "default") -> dict[str, Any]:
    """Enable or disable one alert rule."""
    async with async_session_factory() as session:
        async with session.begin():
            row = (await session.execute(
                text(
                    """
                    UPDATE alert_rules
                    SET enabled = :enabled
                    WHERE id = :rule_id AND tenant_id = :tenant_id
                    RETURNING id, tenant_id, name, service, level, z_score_threshold, enabled, created_at
                    """
                ),
                {"rule_id": rule_id, "tenant_id": tenant_id, "enabled": payload.enabled},
            )).mappings().first()
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert rule not found")
    return dict(row)


@app.delete("/api/alert-rules/{rule_id}", dependencies=[Depends(require_auth)])
async def delete_alert_rule(rule_id: int, tenant_id: str = "default") -> dict[str, str]:
    """Delete one alert rule."""
    async with async_session_factory() as session:
        async with session.begin():
            result = await session.execute(
                text("DELETE FROM alert_rules WHERE id = :rule_id AND tenant_id = :tenant_id"),
                {"rule_id": rule_id, "tenant_id": tenant_id},
            )
    if result.rowcount == 0:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert rule not found")
    return {"status": "deleted"}


@app.get("/api/anomalies/{anomaly_id}/analysis", dependencies=[Depends(require_auth)])
async def get_anomaly_analysis(anomaly_id: int, tenant_id: str = "default") -> dict[str, Any]:
    """Run AI incident analysis for a specific stored anomaly event."""
    async with async_session_factory() as session:
        row = (await session.execute(
            text(
                """
                SELECT tenant_id, service, level, metric, current_count, mean, std_dev, z_score, threshold, fired_at
                FROM anomaly_events
                WHERE id = :anomaly_id AND tenant_id = :tenant_id
                """
            ),
            {"anomaly_id": anomaly_id, "tenant_id": tenant_id},
        )).mappings().first()

    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Anomaly not found")

    anomaly = dict(row)
    if anomaly.get("fired_at"):
        anomaly["fired_at"] = anomaly["fired_at"].isoformat()

    analysis = await analyze_anomaly(anomaly)
    if analysis is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="AI analysis unavailable — check OPENAI_API_KEY in your .env",
        )
    return {"anomaly_id": anomaly_id, "analysis": analysis}


@app.websocket("/ws/alerts")
async def websocket_alerts(websocket: WebSocket) -> None:
    """Keep one browser client connected for live alert updates."""
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

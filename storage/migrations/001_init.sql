CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS log_events (
    id BIGSERIAL,
    tenant_id TEXT NOT NULL DEFAULT 'default',
    service TEXT NOT NULL,
    level TEXT NOT NULL,
    message TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE log_events ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default';

SELECT create_hypertable('log_events', 'created_at', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_log_events_service_created_at
    ON log_events (service, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_log_events_tenant_created_at
    ON log_events (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_log_events_level_created_at
    ON log_events (level, created_at DESC);

CREATE TABLE IF NOT EXISTS anomaly_events (
    id BIGSERIAL,
    tenant_id TEXT NOT NULL DEFAULT 'default',
    service TEXT NOT NULL,
    level TEXT NOT NULL,
    metric TEXT NOT NULL,
    current_count INTEGER NOT NULL,
    mean DOUBLE PRECISION NOT NULL,
    std_dev DOUBLE PRECISION NOT NULL,
    z_score DOUBLE PRECISION NOT NULL,
    threshold DOUBLE PRECISION NOT NULL,
    fired_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE anomaly_events ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default';

SELECT create_hypertable('anomaly_events', 'fired_at', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_anomaly_events_service_fired_at
    ON anomaly_events (service, fired_at DESC);

CREATE TABLE IF NOT EXISTS alert_rules (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL DEFAULT 'default',
    name TEXT NOT NULL,
    service TEXT NOT NULL DEFAULT '*',
    level TEXT NOT NULL DEFAULT 'ERROR',
    z_score_threshold DOUBLE PRECISION NOT NULL DEFAULT 2.5,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS rum_events (
    id BIGSERIAL,
    tenant_id TEXT NOT NULL DEFAULT 'default',
    session_id TEXT NOT NULL,
    path TEXT NOT NULL,
    event_type TEXT NOT NULL,
    value DOUBLE PRECISION,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
SELECT create_hypertable('rum_events', 'created_at', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS synthetic_checks (
    id BIGSERIAL,
    tenant_id TEXT NOT NULL DEFAULT 'default',
    target TEXT NOT NULL,
    status TEXT NOT NULL,
    latency_ms DOUBLE PRECISION,
    status_code INTEGER,
    error TEXT,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
SELECT create_hypertable('synthetic_checks', 'checked_at', if_not_exists => TRUE);

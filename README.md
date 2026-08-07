# Distributed Log Aggregation and Anomaly Detection Engine

This is a complete Python backend project that collects structured logs from multiple services, streams them through Kafka, stores them in TimescaleDB, detects abnormal error spikes with Redis sliding windows and Z-score analysis, sends alerts, and shows live anomalies on a WebSocket dashboard. It also includes Datadog-style observability features such as log search, metrics, infrastructure checks, synthetic monitoring, real-user monitoring, security signals, alert rules, authentication, and tenant filtering.

In simple words:

```text
Services send logs -> Kafka distributes logs -> consumers process logs
-> Redis detects spikes -> TimescaleDB stores history -> dashboard and alerts show problems
```

## What Is Built

- FastAPI ingestion API with `/logs`, `/logs/batch`, and `/health`
- Pydantic validation for structured log events
- API-key authentication using `X-API-Key`
- Kafka producer for the `raw-logs` topic
- Storage consumer that batch-inserts raw logs into TimescaleDB
- Redis sliding-window counters for anomaly detection
- Z-score anomaly detector for `ERROR` and `CRITICAL` spikes
- Anomaly consumer that publishes events to `anomaly-events`
- Alert consumer that stores anomaly events and routes Slack/email alerts
- WebSocket dashboard at `http://localhost:8001`
- Dashboard login with bearer-token protected APIs
- Log search with tenant, service, level, and text filters
- Metrics summary for total logs, error counts, and top services
- Infrastructure monitoring for Kafka, Redis, TimescaleDB, and ingestion
- Synthetic health checks for ingestion and dashboard endpoints
- Real User Monitoring (RUM) browser events for page loads and JS errors
- Security monitoring summary for suspicious auth failures
- Alert rules UI for creating service/level/Z-score rules
- Alert cooldown and deduplication to avoid repeated duplicate alerts
- Multi-tenancy using `tenant_id`
- Optional mock auth, payment, and order services for demo/testing
- Optional payment mock creates periodic ERROR spikes to test anomaly detection
- Docker Compose setup for the full local system
- Tests for ingestion and anomaly logic

## Architecture

```text
auth-mock / payment-mock / order-mock
        |
        v
FastAPI ingestion API :8000
        |
        v
Kafka topic: raw-logs
        |
        +--> storage-consumer  --> TimescaleDB log_events
        |
        +--> anomaly-consumer  --> Redis sliding windows
                                   |
                                   v
                         Kafka topic: anomaly-events
                                   |
                                   +--> alert-consumer --> Slack/email + TimescaleDB anomaly_events
                                   |
                                   +--> dashboard      --> WebSocket browser UI :8001
```

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Language | Python 3.12 | Main language for APIs, consumers, and mock services |
| API | FastAPI | Receives logs and runs the dashboard WebSocket server |
| Server | Uvicorn | Runs FastAPI apps |
| Validation | Pydantic | Checks incoming log JSON |
| Stream broker | Apache Kafka | Moves log and anomaly events between services |
| Kafka client | aiokafka | Async Kafka producer and consumers in Python |
| Cache/counters | Redis | Fast sliding-window counters for anomaly detection |
| Database | TimescaleDB | Time-series storage for logs and anomalies |
| ORM | SQLAlchemy async | Async database writes through Python models |
| DB driver | asyncpg | Async PostgreSQL/TimescaleDB driver |
| Alerts | Slack webhook / SMTP | Sends anomaly notifications |
| Realtime UI | WebSocket | Pushes live anomalies to browser |
| Containers | Docker Compose | Runs the complete system locally |
| Tests | pytest | Tests validation, API behavior, and anomaly math |

## Folder Structure

```text
anomaly/          Z-score and Redis sliding-window logic
alerting/         Slack, email, and alert routing code
consumers/        Kafka consumers for storage, anomaly detection, and alerting
dashboard/        FastAPI WebSocket dashboard and static HTML UI
ingestion/        FastAPI log ingestion API
mock_services/    Fake auth, payment, and order services
storage/          SQLAlchemy models and TimescaleDB migration
tests/            Unit/API tests
```

## Run With Real Logs

First make sure Docker Desktop is running.

```bash
docker compose up -d --build
```

Open the services:

```text
Ingestion API: http://localhost:8000
Dashboard:     http://localhost:8001
```

Dashboard login:

```text
Username: admin
Password: admin123
```

In real mode, fake mock services do not start. Your real website, app, backend, or microservices must send logs to `http://localhost:8000/logs`.

## Run Demo Mode With Fake Logs

Use this only when you want sample logs for testing the dashboard:

```bash
docker compose --profile demo up -d --build
```

Demo mode starts `auth-mock`, `payment-mock`, and `order-mock`. The payment mock creates periodic ERROR spikes, which should eventually appear on the dashboard.

## Real-Time Integration

Read [REAL_TIME_INTEGRATION.md](REAL_TIME_INTEGRATION.md) for Python and Node.js examples showing how a real service sends real logs into this system.

## Dashboard Features

| Feature | What it does |
|---|---|
| Live Anomaly Feed | Shows ERROR/CRITICAL spikes detected by the backend in real time |
| Log Search | Searches stored logs by tenant, service, level, and message text |
| Metrics Monitoring | Shows total logs, errors, and busiest services |
| Better Dashboards | Organizes logs, metrics, alerts, infra, synthetic checks, security, and RUM in one UI |
| Alert Rules UI | Lets you create alert rules for service, level, and Z-score threshold |
| Alert Cooldown / Deduplication | Stops the same alert from firing repeatedly during a cooldown window |
| Infrastructure Monitoring | Checks whether Kafka, Redis, TimescaleDB, and ingestion are reachable |
| Real User Monitoring | Records dashboard browser page-load and JavaScript error events |
| Synthetic Monitoring | Automatically calls health endpoints and stores uptime/latency results |
| Security Monitoring | Highlights suspicious auth failure patterns from logs |
| Authentication And Users | Protects dashboard APIs with login and bearer token authentication |
| Multi-Tenancy | Separates data using `tenant_id`, so one system can store logs for many tenants |

## Send A Manual Log

PowerShell:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8000/logs" `
  -Headers @{"X-API-Key"="super-secret-key-123"} `
  -ContentType "application/json" `
  -Body '{"service":"payment-service","level":"ERROR","message":"manual payment failure","metadata":{"source":"manual"}}'
```

## Trigger A Manual Spike

Run this while Docker Compose is up:

```powershell
1..80 | ForEach-Object {
  Invoke-RestMethod `
    -Method Post `
    -Uri "http://localhost:8000/logs" `
    -Headers @{"X-API-Key"="super-secret-key-123"} `
    -ContentType "application/json" `
    -Body '{"service":"payment-service","level":"ERROR","message":"manual spike","metadata":{"source":"manual-spike"}}'
}
```

Note: anomaly detection compares current traffic against previous time windows. Let the mock services run for a few minutes first so Redis has history.

## Query The Database

Open a shell inside TimescaleDB:

```bash
docker compose exec timescaledb psql -U loguser -d logsdb
```

Useful queries:

```sql
SELECT service, level, count(*)
FROM log_events
GROUP BY service, level
ORDER BY service, level;

SELECT *
FROM anomaly_events
ORDER BY fired_at DESC
LIMIT 10;
```

## Run Tests

Install local test dependencies first:

```bash
pip install -r ingestion/requirements.txt -r consumers/requirements.txt -r dashboard/requirements.txt -r requirements-dev.txt
```

Then run:

```bash
pytest
```

## Environment Variables

Copy `.env.example` to `.env` and change values if needed.

Important values:

| Variable | Meaning |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka address used inside Docker |
| `LOG_TOPIC` | Kafka topic for raw logs |
| `ANOMALY_TOPIC` | Kafka topic for anomaly events |
| `REDIS_URL` | Redis connection URL |
| `DATABASE_URL` | TimescaleDB connection URL |
| `API_KEY` | Required value for `X-API-Key` |
| `WINDOW_SIZE_SECONDS` | Size of each anomaly detection time bucket |
| `Z_SCORE_THRESHOLD` | How unusual traffic must be before alerting |
| `MIN_SAMPLES` | Number of previous non-zero windows needed |
| `ALERT_COOLDOWN_SECONDS` | Time to suppress duplicate alerts for the same tenant/service/level |
| `DASHBOARD_USERNAME` | Dashboard login username |
| `DASHBOARD_PASSWORD` | Dashboard login password |
| `DASHBOARD_TOKEN` | Bearer token used by protected dashboard APIs |
| `SLACK_WEBHOOK_URL` | Optional Slack webhook URL |
| `SMTP_*` | Optional email alert settings |



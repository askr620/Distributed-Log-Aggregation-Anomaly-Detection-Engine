# Real-Time Integration Guide

This project can run with real logs from real software. The mock services are only for demo/testing.

## Real Mode Vs Demo Mode

Real mode starts the monitoring platform only:

```bash
docker compose up -d --build
```

This starts:

```text
ingestion
dashboard
kafka
zookeeper
redis
timescaledb
storage-consumer
anomaly-consumer
alert-consumer
```

Demo mode also starts fake log generators:

```bash
docker compose --profile demo up -d --build
```

This additionally starts:

```text
auth-mock
payment-mock
order-mock
```

Use demo mode only when you want fake logs for testing.

## How Real Services Send Logs

Any real backend, website API, mobile app backend, admin panel, or microservice sends logs to:

```text
POST http://YOUR_SERVER_IP:8000/logs
```

Required header:

```text
X-API-Key: super-secret-key-123
```

Required JSON body:

```json
{
  "tenant_id": "client-a",
  "service": "payment-service",
  "level": "ERROR",
  "message": "Payment gateway timeout",
  "metadata": {
    "user_id": "123",
    "order_id": "ORD-1001",
    "gateway": "razorpay"
  }
}
```

## Service Names Are Automatic

You do not manually create service names in this project.

If a real service sends:

```json
{
  "service": "booking-service",
  "level": "ERROR",
  "message": "Booking failed"
}
```

Then `booking-service` automatically appears in:

- Log Search
- Metrics Monitoring
- Service Distribution
- Anomaly Feed
- Alert Rules

## Python Example

```python
import logging
import requests

INGESTION_URL = "http://YOUR_SERVER_IP:8000/logs"
API_KEY = "super-secret-key-123"


def send_log(service, level, message, metadata=None):
    try:
        requests.post(
            INGESTION_URL,
            headers={"X-API-Key": API_KEY},
            json={
                "tenant_id": "client-a",
                "service": service,
                "level": level,
                "message": message,
                "metadata": metadata or {},
            },
            timeout=3,
        )
    except Exception:
        logging.exception("Could not send log to monitoring system")


try:
    # real application code
    charge_payment()
except Exception as exc:
    send_log(
        service="payment-service",
        level="ERROR",
        message="Payment failed",
        metadata={"error": str(exc)},
    )
    raise
```

## Node.js Example

```js
async function sendLog(service, level, message, metadata = {}) {
  await fetch("http://YOUR_SERVER_IP:8000/logs", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-API-Key": "super-secret-key-123"
    },
    body: JSON.stringify({
      tenant_id: "client-a",
      service,
      level,
      message,
      metadata
    })
  });
}

try {
  await chargePayment();
} catch (error) {
  await sendLog("payment-service", "ERROR", "Payment failed", {
    error: error.message
  });
  throw error;
}
```

## What Happens After A Real Log Arrives

```text
real service
  -> ingestion API
  -> Kafka raw-logs topic
  -> storage-consumer saves in TimescaleDB
  -> anomaly-consumer checks Redis counters and Z-score
  -> alert-consumer saves/sends alert
  -> dashboard shows live result
```

## Real Errors

Real errors come from the client application code.

Examples:

- Payment gateway timeout
- Database connection failed
- Booking inventory unavailable
- Login provider timeout
- Admin permission denied
- API request too slow
- Mobile app backend exception

Your project does not need to know these services in advance. It only needs the logs.

## Important Production Notes

Before giving this to a real client, change:

- `API_KEY`
- `DASHBOARD_PASSWORD`
- database password
- Slack/email settings if alerts are needed

For internet/public deployment, also add:

- HTTPS
- proper user accounts
- rate limiting
- stronger tenant isolation
- backup and data retention policy

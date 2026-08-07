FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY mock_services/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mock_services ./mock_services

CMD ["python", "-m", "mock_services.payment_service"]

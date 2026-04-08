FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /app

# Copy dependency files first for layer caching
COPY pyproject.toml .

# Install dependencies (no project itself yet)
RUN uv sync --no-install-project --no-dev

# Copy application source
COPY main.py .

# Sync again to install the project
RUN uv sync --no-dev

ENV DB_PATH=/data/logs.db \
    MQTT_BROKER=localhost:1883 \
    MQTT_TOPIC_PREFIX=picologs \
    MAX_MESSAGES=500 \
    CLEANUP_INTERVAL_SECONDS=60 \
    MQTT_RECONNECT_DELAY_SECONDS=5

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

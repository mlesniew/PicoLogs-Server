# PicoLogs Server

A lightweight log aggregation server that captures logs published over MQTT and exposes them via REST and WebSocket APIs.

Designed to work with the [PicoLogs](https://github.com/mlesniew/picologs) Arduino/PlatformIO library.

## Features

- **MQTT subscriber** – listens on a configurable topic prefix (`picologs/#` by default)
- **Per-source line buffering** – correctly reassembles log lines split across multiple MQTT payloads
- **SQLite storage** – persistent (file) or ephemeral (in-memory) storage with automatic cleanup
- **REST API** – query sources and log messages with optional filters
- **WebSocket streaming** – tail logs in real-time from any source

## Quick Start

### With Docker (recommended)

```bash
docker run -d \
  --name picologs-server \
  -p 8000:8000 \
  -e MQTT_BROKER=your-broker:1883 \
  -v picologs-data:/data \
  ghcr.io/mlesniew/picologs-server
```

### With UV (local development)

```bash
# Install dependencies
uv sync

# Run the server
uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

## Configuration

All configuration is done via environment variables:

| Variable | Default | Description |
|---|---|---|
| `MQTT_BROKER` | `localhost:1883` | MQTT broker address (`host:port`) |
| `MQTT_TOPIC_PREFIX` | `picologs` | MQTT topic prefix to subscribe to (`<prefix>/#`) |
| `DB_PATH` | `:memory:` | SQLite database path (`:memory:` for in-memory) |
| `MAX_MESSAGES` | `500` | Maximum number of messages retained in the database |
| `CLEANUP_INTERVAL_SECONDS` | `60` | How often (in seconds) to run the cleanup task |
| `MQTT_RECONNECT_DELAY_SECONDS` | `5` | Seconds to wait before reconnecting after MQTT disconnection |

## API Reference

### `GET /sources`

Returns a JSON array of all source identifiers that have sent at least one log message.

**Example:**
```bash
curl http://localhost:8000/sources
```
```json
["device1", "device1/status", "sensor/temperature"]
```

---

### `GET /logs`

Returns a JSON array of log messages, optionally filtered.

**Query parameters:**

| Parameter | Type | Description |
|---|---|---|
| `sources` | string | Comma-separated list of source identifiers to include |
| `since` | integer | Unix timestamp in **milliseconds**; only return messages newer than this |

**Example – all logs:**
```bash
curl http://localhost:8000/logs
```

**Example – filtered by source:**
```bash
curl "http://localhost:8000/logs?sources=device1,sensor/temperature"
```

**Example – since a timestamp:**
```bash
curl "http://localhost:8000/logs?since=1700000000000"
```

**Response format:**
```json
[
  {"ts": 1700000001234, "source": "device1", "message": "Boot complete"},
  {"ts": 1700000002345, "source": "device1", "message": "Connecting to WiFi..."}
]
```

---

### `WS /tail`

WebSocket endpoint for streaming logs in real-time.

**Query parameters:**

| Parameter | Type | Description |
|---|---|---|
| `sources` | string | Comma-separated list of source identifiers to filter |
| `since` | integer | Unix timestamp in milliseconds; historical messages start from here |

On connection, the server first sends all stored messages matching the filters, then streams new messages as they arrive.

> **Note:** Clients must clear previously received data on reconnect — messages may be re-delivered from history.

**Example (Python):**

> **Note:** This example uses the `websockets` library (`pip install websockets`), which is not included in server dependencies but is useful for client scripts.

```python
import asyncio
import websockets
import json

async def tail():
    uri = "ws://localhost:8000/tail?sources=device1"
    async with websockets.connect(uri) as ws:
        async for raw in ws:
            msg = json.loads(raw)
            print(f"[{msg['ts']}] {msg['source']}: {msg['message']}")

asyncio.run(tail())
```

**Example (JavaScript):**
```javascript
const ws = new WebSocket("ws://localhost:8000/tail?sources=device1");
ws.onmessage = (event) => {
  const msg = JSON.parse(event.data);
  console.log(`[${msg.ts}] ${msg.source}: ${msg.message}`);
};
```

## MQTT Message Format

Devices publish newline-delimited log lines to topics under the configured prefix:

```
picologs/<source-identifier>
```

For example, a device publishing to `picologs/device1/status` will have the source identifier `device1/status`.

**Payload format:**
```
Log line one\nLog line two\nIncomplete line (no trailing newline – buffered until next message)
```

Payloads must be UTF-8 encoded text. The server buffers incomplete lines (those not terminated by `\n`) in RAM and flushes them when the rest of the line arrives.

## Docker Compose Example

```yaml
services:
  mosquitto:
    image: eclipse-mosquitto:2
    ports:
      - "1883:1883"
    volumes:
      - ./mosquitto.conf:/mosquitto/config/mosquitto.conf

  picologs-server:
    image: ghcr.io/mlesniew/picologs-server
    ports:
      - "8000:8000"
    environment:
      MQTT_BROKER: mosquitto:1883
      DB_PATH: /data/logs.db
      MAX_MESSAGES: "2000"
    volumes:
      - picologs-data:/data
    depends_on:
      - mosquitto

volumes:
  picologs-data:
```

## Architecture

```
MQTT Broker
    │
    │  picologs/#
    ▼
┌─────────────────────────────────────────┐
│            PicoLogs Server              │
│                                         │
│  aiomqtt subscriber                     │
│    └─► per-source line buffer           │
│          └─► aiosqlite (SQLite DB)      │
│                └─► WebSocket broadcast  │
│                                         │
│  FastAPI REST API                       │
│    GET /sources                         │
│    GET /logs                            │
│    WS  /tail                            │
└─────────────────────────────────────────┘
```

## License

See [LICENSE](LICENSE).

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager

import aiosqlite
import aiomqtt
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MQTT_BROKER = os.environ.get("MQTT_BROKER", "localhost:1883")
MQTT_TOPIC_PREFIX = os.environ.get("MQTT_TOPIC_PREFIX", "picologs")
DB_PATH = os.environ.get("DB_PATH", ":memory:")
MAX_MESSAGES = int(os.environ.get("MAX_MESSAGES", "500"))
CLEANUP_INTERVAL_SECONDS = int(os.environ.get("CLEANUP_INTERVAL_SECONDS", "60"))
MQTT_RECONNECT_DELAY_SECONDS = int(os.environ.get("MQTT_RECONNECT_DELAY_SECONDS", "5"))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("picologs")

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------

# Shared database connection (single connection, serialized via asyncio)
db: aiosqlite.Connection | None = None

# Per-source incomplete-line buffers
source_buffers: dict[str, str] = {}

# WebSocket clients waiting for new messages: list of asyncio.Queue instances
ws_queues: list[asyncio.Queue] = []


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

async def init_db(conn: aiosqlite.Connection) -> None:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            ts        INTEGER NOT NULL,
            source    TEXT    NOT NULL,
            message   TEXT    NOT NULL
        )
        """
    )
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages (ts)")
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_messages_source ON messages (source)"
    )
    await conn.commit()


async def store_message(conn: aiosqlite.Connection, source: str, message: str) -> dict:
    ts = int(time.time() * 1000)
    await conn.execute(
        "INSERT INTO messages (ts, source, message) VALUES (?, ?, ?)",
        (ts, source, message),
    )
    await conn.commit()
    return {"ts": ts, "source": source, "message": message}


async def cleanup_old_messages(conn: aiosqlite.Connection) -> None:
    cursor = await conn.execute("SELECT COUNT(*) FROM messages")
    row = await cursor.fetchone()
    count = row[0]
    if count > MAX_MESSAGES:
        excess = count - MAX_MESSAGES
        await conn.execute(
            """
            DELETE FROM messages WHERE id IN (
                SELECT id FROM messages ORDER BY ts ASC LIMIT ?
            )
            """,
            (excess,),
        )
        await conn.commit()
        logger.info("Cleaned up %d old message(s), keeping %d", excess, MAX_MESSAGES)


# ---------------------------------------------------------------------------
# WebSocket broadcast
# ---------------------------------------------------------------------------

def broadcast(msg: dict) -> None:
    """Put a message on every active WebSocket queue."""
    for q in ws_queues:
        q.put_nowait(msg)


# ---------------------------------------------------------------------------
# MQTT subscriber
# ---------------------------------------------------------------------------

def _parse_broker(broker_str: str) -> tuple[str, int]:
    """Parse 'host:port' string, returning (host, port)."""
    if ":" in broker_str:
        host, port_str = broker_str.rsplit(":", 1)
        return host, int(port_str)
    return broker_str, 1883


async def mqtt_loop() -> None:
    """Connect to MQTT broker and process messages forever, reconnecting on failure."""
    host, port = _parse_broker(MQTT_BROKER)
    topic = f"{MQTT_TOPIC_PREFIX}/#"
    prefix_with_slash = f"{MQTT_TOPIC_PREFIX}/"

    while True:
        try:
            logger.info("Connecting to MQTT broker at %s:%d …", host, port)
            async with aiomqtt.Client(hostname=host, port=port) as client:
                logger.info("Connected to MQTT broker. Subscribing to %s", topic)
                await client.subscribe(topic)
                async for message in client.messages:
                    topic_str = str(message.topic)
                    if topic_str.startswith(prefix_with_slash):
                        source = topic_str[len(prefix_with_slash):]
                    else:
                        source = topic_str

                    payload = message.payload
                    if isinstance(payload, (bytes, bytearray)):
                        payload = payload.decode("utf-8", errors="replace")

                    await handle_payload(source, payload)
        except aiomqtt.MqttError as exc:
            logger.warning(
                "MQTT connection error: %s. Reconnecting in %ds …",
                exc,
                MQTT_RECONNECT_DELAY_SECONDS,
            )
            await asyncio.sleep(MQTT_RECONNECT_DELAY_SECONDS)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Unexpected error in MQTT loop: %s. Reconnecting in %ds …",
                exc,
                MQTT_RECONNECT_DELAY_SECONDS,
            )
            await asyncio.sleep(MQTT_RECONNECT_DELAY_SECONDS)


async def handle_payload(source: str, payload: str) -> None:
    """Process a raw MQTT payload for the given source."""
    global db

    buffered = source_buffers.get(source, "")
    combined = buffered + payload

    if "\n" in combined:
        parts = combined.split("\n")
        # The last element is the incomplete tail (may be empty string)
        source_buffers[source] = parts[-1]
        complete_lines = parts[:-1]
        for line in complete_lines:
            if line:  # skip empty lines
                msg = await store_message(db, source, line)
                broadcast(msg)
    else:
        # No newline yet: accumulate in buffer
        source_buffers[source] = combined


# ---------------------------------------------------------------------------
# Cleanup task
# ---------------------------------------------------------------------------

async def cleanup_task() -> None:
    """Periodically remove oldest messages when the limit is exceeded."""
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)
        try:
            await cleanup_old_messages(db)
        except Exception as exc:  # noqa: BLE001
            logger.error("Error during cleanup: %s", exc)


# ---------------------------------------------------------------------------
# Application lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db

    logger.info("Opening database: %s", DB_PATH)
    db = await aiosqlite.connect(DB_PATH)
    await init_db(db)

    mqtt_task = asyncio.create_task(mqtt_loop(), name="mqtt_loop")
    cleanup = asyncio.create_task(cleanup_task(), name="cleanup_task")

    try:
        yield
    finally:
        mqtt_task.cancel()
        cleanup.cancel()
        await asyncio.gather(mqtt_task, cleanup, return_exceptions=True)
        await db.close()
        logger.info("Database closed.")


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

app = FastAPI(title="PicoLogs Server", lifespan=lifespan)


@app.get("/sources")
async def get_sources():
    """Return a JSON list of all known source identifiers."""
    async with db.execute("SELECT DISTINCT source FROM messages ORDER BY source") as cur:
        rows = await cur.fetchall()
    return JSONResponse([row[0] for row in rows])


@app.get("/logs")
async def get_logs(sources: str | None = None, since: int | None = None):
    """
    Return log messages matching the given filters.

    - **sources**: optional comma-separated list of source identifiers
    - **since**: optional unix timestamp in milliseconds; only messages newer than this
    """
    query = "SELECT ts, source, message FROM messages"
    conditions: list[str] = []
    params: list = []

    if since is not None:
        conditions.append("ts > ?")
        params.append(since)

    if sources:
        source_list = [s.strip() for s in sources.split(",") if s.strip()]
        if source_list:
            placeholders = ",".join("?" * len(source_list))
            conditions.append(f"source IN ({placeholders})")
            params.extend(source_list)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY ts ASC"

    async with db.execute(query, params) as cur:
        rows = await cur.fetchall()

    return JSONResponse(
        [{"ts": row[0], "source": row[1], "message": row[2]} for row in rows]
    )


@app.websocket("/tail")
async def tail_websocket(websocket: WebSocket, sources: str | None = None, since: int | None = None):
    """
    WebSocket endpoint that streams log messages.

    On connect: sends historical messages matching filters, then streams new ones.
    """
    await websocket.accept()

    # Build source filter
    source_list: list[str] = []
    if sources:
        source_list = [s.strip() for s in sources.split(",") if s.strip()]

    # Send historical messages first
    query = "SELECT ts, source, message FROM messages"
    conditions: list[str] = []
    params: list = []

    if since is not None:
        conditions.append("ts > ?")
        params.append(since)

    if source_list:
        placeholders = ",".join("?" * len(source_list))
        conditions.append(f"source IN ({placeholders})")
        params.extend(source_list)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY ts ASC"

    async with db.execute(query, params) as cur:
        rows = await cur.fetchall()

    for row in rows:
        await websocket.send_json({"ts": row[0], "source": row[1], "message": row[2]})

    # Register queue for live messages
    q: asyncio.Queue = asyncio.Queue()
    ws_queues.append(q)

    try:
        while True:
            msg = await q.get()
            # Apply source filter to live messages
            if source_list and msg["source"] not in source_list:
                continue
            await websocket.send_json(msg)
    except WebSocketDisconnect:
        pass
    except Exception as exc:  # noqa: BLE001
        logger.debug("WebSocket error: %s", exc)
    finally:
        ws_queues.remove(q)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, log_level="info")


if __name__ == "__main__":
    main()

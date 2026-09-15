"""
PulseGrid API server.

Serves the REST analytics API, a WebSocket endpoint for live dashboard
updates, and the static dashboard UI itself.
"""
from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from api.routes import get_db, router
from api.websocket_manager import manager
from config import BASE_DIR, settings

logging.basicConfig(level=settings.log_level, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("pulsegrid.api")

_start_time = time.time()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("PulseGrid API starting up")
    get_db()  # ensure schema exists
    await manager.start_listener()
    yield
    await manager.stop_listener()
    logger.info("PulseGrid API shutting down")


app = FastAPI(
    title="PulseGrid",
    description="Real-time infrastructure monitoring, anomaly detection, and analytics platform.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api", tags=["analytics"])


@app.get("/health", tags=["system"], summary="Liveness/readiness health check")
async def health():
    redis_ok = False
    try:
        r = aioredis.Redis(host=settings.redis_host, port=settings.redis_port, db=settings.redis_db)
        redis_ok = await r.ping()
        await r.aclose()
    except Exception:  # noqa: BLE001
        redis_ok = False

    db_ok = True
    try:
        get_db().get_statistics()
    except Exception:  # noqa: BLE001
        db_ok = False

    status = "ok" if (redis_ok and db_ok) else "degraded"
    return {
        "status": status,
        "uptime_sec": round(time.time() - _start_time, 1),
        "redis_connected": redis_ok,
        "database_connected": db_ok,
        "websocket_clients": len(manager.active),
    }


@app.get("/metrics", tags=["system"], summary="Quick snapshot of processing metrics")
async def metrics():
    stats = get_db().get_statistics()
    stats["websocket_clients"] = len(manager.active)
    stats["uptime_sec"] = round(time.time() - _start_time, 1)
    return stats


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # We don't require anything from the client; just keep the
            # connection alive and drop it cleanly on disconnect.
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:  # noqa: BLE001
        manager.disconnect(websocket)


dashboard_dir = BASE_DIR / "dashboard"
if dashboard_dir.exists():
    app.mount("/", StaticFiles(directory=str(dashboard_dir), html=True), name="dashboard")

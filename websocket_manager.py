"""
Bridges the processor's Redis pub/sub channel to any number of connected
WebSocket dashboard clients. Decouples the API process from the processor
process entirely -- either can restart independently.
"""
from __future__ import annotations

import asyncio
import logging

import redis.asyncio as aioredis
from fastapi import WebSocket

from config import settings
from processor.stream_processor import UPDATE_CHANNEL

logger = logging.getLogger("pulsegrid.api.ws")


class ConnectionManager:
    def __init__(self):
        self.active: set[WebSocket] = set()
        self._redis: aioredis.Redis | None = None
        self._listener_task: asyncio.Task | None = None

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self.active.add(ws)
        logger.info("WebSocket client connected (%d active)", len(self.active))

    def disconnect(self, ws: WebSocket) -> None:
        self.active.discard(ws)
        logger.info("WebSocket client disconnected (%d active)", len(self.active))

    async def broadcast(self, message: str) -> None:
        dead = []
        for ws in self.active:
            try:
                await ws.send_text(message)
            except Exception:  # noqa: BLE001
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

    async def start_listener(self) -> None:
        self._redis = aioredis.Redis(
            host=settings.redis_host, port=settings.redis_port, db=settings.redis_db, decode_responses=True
        )
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(UPDATE_CHANNEL)
        logger.info("Subscribed to '%s' for live dashboard updates", UPDATE_CHANNEL)

        async def _listen():
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                await self.broadcast(message["data"])

        self._listener_task = asyncio.create_task(_listen())

    async def stop_listener(self) -> None:
        if self._listener_task:
            self._listener_task.cancel()
        if self._redis:
            await self._redis.aclose()


manager = ConnectionManager()

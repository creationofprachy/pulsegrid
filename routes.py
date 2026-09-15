"""REST endpoints exposing PulseGrid's processed analytics."""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query

from storage.database import Database

router = APIRouter()
_db: Optional[Database] = None


def get_db() -> Database:
    global _db
    if _db is None:
        _db = Database()
    return _db


@router.get("/statistics", summary="Aggregate system-wide statistics")
async def statistics():
    return get_db().get_statistics()


@router.get("/events", summary="Most recent processed events")
async def events(limit: int = Query(50, ge=1, le=1000), source: Optional[str] = None):
    return get_db().get_recent_events(limit=limit, source=source)


@router.get("/anomalies", summary="Most recent detected anomalies")
async def anomalies(
    limit: int = Query(50, ge=1, le=1000),
    severity: Optional[str] = None,
    source: Optional[str] = None,
):
    return get_db().get_anomalies(limit=limit, severity=severity, source=source)


@router.get("/sources", summary="Known sources/instances and their live status")
async def sources():
    return get_db().get_sources()


@router.get("/aggregates", summary="Tumbling-window aggregate history for charts")
async def aggregates(source: Optional[str] = None, limit: int = Query(100, ge=1, le=2000)):
    return get_db().get_aggregates(source=source, limit=limit)

import asyncio

from fastapi import APIRouter

from app.services.sync_service import sync_service

router = APIRouter(prefix="/api/sync", tags=["sync"])


@router.post("/trigger")
async def trigger_sync(full: bool = False):
    if sync_service.is_syncing:
        return {"status": "already_syncing", "stats": sync_service.sync_stats}

    if full:
        asyncio.create_task(sync_service.full_sync())
    else:
        asyncio.create_task(sync_service.incremental_sync())

    return {"status": "started", "type": "full" if full else "incremental"}


@router.get("/status")
async def sync_status():
    return {
        "is_syncing": sync_service.is_syncing,
        "last_sync": sync_service.last_sync.isoformat() if sync_service.last_sync else None,
        "stats": sync_service.sync_stats,
    }

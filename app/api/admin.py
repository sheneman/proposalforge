import asyncio
from datetime import datetime

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import Opportunity, Agency
from app.models.sync_log import SyncLog
from app.services.sync_service import sync_service
from app.tasks import scheduler

router = APIRouter(prefix="/admin", tags=["admin"])
templates = Jinja2Templates(directory="app/templates")


@router.get("", response_class=HTMLResponse)
async def admin_dashboard(request: Request):
    return templates.TemplateResponse("admin.html", {"request": request})


@router.get("/sync/live", response_class=HTMLResponse)
async def sync_live(request: Request):
    stats = dict(sync_service.sync_stats)
    elapsed = None
    if stats.get("started"):
        try:
            started = datetime.fromisoformat(stats["started"])
            elapsed = (datetime.utcnow() - started).total_seconds()
        except (ValueError, TypeError):
            pass

    return templates.TemplateResponse("partials/admin/sync_live.html", {
        "request": request,
        "is_syncing": sync_service.is_syncing,
        "stats": stats,
        "elapsed": elapsed,
        "last_sync": sync_service.last_sync,
    })


@router.post("/sync/trigger", response_class=HTMLResponse)
async def trigger_sync(request: Request, full: bool = False):
    if not sync_service.is_syncing:
        if full:
            asyncio.create_task(sync_service.full_sync())
        else:
            asyncio.create_task(sync_service.incremental_sync())

    # Small delay to let sync start
    await asyncio.sleep(0.2)

    return await sync_live(request)


@router.post("/sync/cancel", response_class=HTMLResponse)
async def cancel_sync(request: Request):
    sync_service.cancel_sync()
    await asyncio.sleep(0.5)
    return await sync_live(request)


@router.get("/sync/history", response_class=HTMLResponse)
async def sync_history(request: Request, db: AsyncSession = Depends(get_db)):
    stmt = select(SyncLog).order_by(SyncLog.started_at.desc()).limit(20)
    result = await db.execute(stmt)
    logs = result.scalars().all()

    return templates.TemplateResponse("partials/admin/sync_history.html", {
        "request": request,
        "logs": logs,
    })


@router.get("/data/health", response_class=HTMLResponse)
async def data_health(request: Request, db: AsyncSession = Depends(get_db)):
    # Total opportunities
    total = (await db.execute(select(func.count(Opportunity.id)))).scalar() or 0

    # By status
    status_counts = {}
    stmt = select(Opportunity.status, func.count(Opportunity.id)).group_by(Opportunity.status)
    rows = (await db.execute(stmt)).all()
    for status, count in rows:
        status_counts[status] = count

    # Total agencies
    agency_count = (await db.execute(select(func.count(Agency.code)))).scalar() or 0

    # Data freshness
    oldest_sync = (await db.execute(
        select(func.min(Opportunity.last_synced_at)).where(Opportunity.last_synced_at.isnot(None))
    )).scalar()
    median_sync_age = None
    try:
        avg_sync = (await db.execute(
            select(func.avg(
                func.timestampdiff(text("HOUR"), Opportunity.last_synced_at, func.now())
            )).where(Opportunity.last_synced_at.isnot(None))
        )).scalar()
        if avg_sync is not None:
            median_sync_age = round(float(avg_sync), 1)
    except Exception:
        pass

    # Data quality - missing fields
    missing_close_date = (await db.execute(
        select(func.count(Opportunity.id)).where(Opportunity.close_date.is_(None))
    )).scalar() or 0
    missing_award_ceiling = (await db.execute(
        select(func.count(Opportunity.id)).where(Opportunity.award_ceiling.is_(None))
    )).scalar() or 0
    missing_description = (await db.execute(
        select(func.count(Opportunity.id)).where(
            (Opportunity.synopsis_description.is_(None)) | (Opportunity.synopsis_description == "")
        )
    )).scalar() or 0

    # Classification breakdown
    team_based = (await db.execute(
        select(func.count(Opportunity.id)).where(Opportunity.is_team_based == True)
    )).scalar() or 0
    multi_inst = (await db.execute(
        select(func.count(Opportunity.id)).where(Opportunity.is_multi_institution == True)
    )).scalar() or 0
    multi_disc = (await db.execute(
        select(func.count(Opportunity.id)).where(Opportunity.is_multi_disciplinary == True)
    )).scalar() or 0

    return templates.TemplateResponse("partials/admin/data_health.html", {
        "request": request,
        "total": total,
        "status_counts": status_counts,
        "agency_count": agency_count,
        "oldest_sync": oldest_sync,
        "median_sync_age_hours": median_sync_age,
        "missing_close_date": missing_close_date,
        "missing_award_ceiling": missing_award_ceiling,
        "missing_description": missing_description,
        "team_based": team_based,
        "multi_institution": multi_inst,
        "multi_disciplinary": multi_disc,
    })


@router.get("/scheduler", response_class=HTMLResponse)
async def scheduler_info(request: Request):
    next_run = scheduler.get_next_run_time()
    return templates.TemplateResponse("partials/admin/scheduler.html", {
        "request": request,
        "enabled": scheduler.is_enabled(),
        "interval_hours": scheduler.get_interval_hours(),
        "next_run": next_run,
    })


@router.post("/scheduler/toggle", response_class=HTMLResponse)
async def toggle_scheduler(request: Request):
    scheduler.toggle_scheduler()
    return await scheduler_info(request)


@router.post("/scheduler/interval", response_class=HTMLResponse)
async def update_interval(request: Request, hours: int = 6):
    if hours in (1, 3, 6, 12, 24):
        scheduler.update_interval(hours)
    return await scheduler_info(request)

import asyncio
from datetime import datetime

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import Opportunity, Agency
from app.models.sync_log import SyncLog
from app.services.sync_service import sync_service
from app.services.settings_service import settings_service
from app.tasks import scheduler

router = APIRouter(prefix="/admin", tags=["admin"])
templates = Jinja2Templates(directory="app/templates")


@router.get("", response_class=HTMLResponse)
async def admin_dashboard(request: Request):
    return templates.TemplateResponse("admin.html", {"request": request})


@router.get("/sync/live", response_class=HTMLResponse)
async def sync_live(request: Request, db: AsyncSession = Depends(get_db)):
    # If this worker owns the sync, publish stats to Redis and return them
    if sync_service.is_syncing:
        stats = dict(sync_service.sync_stats)
        elapsed = None
        if stats.get("started"):
            try:
                started = datetime.fromisoformat(stats["started"])
                elapsed = (datetime.utcnow() - started).total_seconds()
            except (ValueError, TypeError):
                pass
        # Publish to Redis so other workers can read the live stats
        await sync_service._publish_stats()
        return templates.TemplateResponse("partials/admin/sync_live.html", {
            "request": request,
            "is_syncing": True,
            "stats": stats,
            "elapsed": elapsed,
            "last_sync": sync_service.last_sync,
        })

    # This worker doesn't own the sync — check Redis for shared stats from the worker that does
    shared = await sync_service.get_shared_stats()
    if shared and shared.get("is_syncing"):
        stats = shared["stats"]
        elapsed = None
        if stats.get("started"):
            try:
                started = datetime.fromisoformat(stats["started"])
                elapsed = (datetime.utcnow() - started).total_seconds()
            except (ValueError, TypeError):
                pass
        return templates.TemplateResponse("partials/admin/sync_live.html", {
            "request": request,
            "is_syncing": True,
            "stats": stats,
            "elapsed": elapsed,
            "last_sync": None,
        })

    # No sync running — show last completed sync
    last_log = (await db.execute(
        select(SyncLog).where(SyncLog.status == "completed").order_by(SyncLog.completed_at.desc()).limit(1)
    )).scalar_one_or_none()

    last_sync = last_log.completed_at if last_log else sync_service.last_sync

    return templates.TemplateResponse("partials/admin/sync_live.html", {
        "request": request,
        "is_syncing": False,
        "stats": shared.get("stats", {}) if shared else {},
        "elapsed": None,
        "last_sync": last_sync,
    })


@router.post("/sync/trigger", response_class=HTMLResponse)
async def trigger_sync(request: Request, full: bool = False, refresh: bool = False):
    if not sync_service.is_syncing:
        if refresh:
            asyncio.create_task(sync_service.full_sync(skip_discovery=True))
        elif full:
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


# --- LLM Backend Settings ---

@router.get("/llm", response_class=HTMLResponse)
async def llm_settings(request: Request, db: AsyncSession = Depends(get_db)):
    llm = await settings_service.get_llm_settings(db)
    return templates.TemplateResponse("partials/admin/llm_settings.html", {
        "request": request,
        "llm": llm,
        "saved": False,
    })


@router.post("/llm", response_class=HTMLResponse)
async def save_llm_settings(request: Request, db: AsyncSession = Depends(get_db)):
    form = await request.form()
    base_url = (form.get("base_url") or "").strip()
    model = (form.get("model") or "").strip()
    api_key = (form.get("api_key") or "").strip()

    # Always save all three fields (empty string clears a setting)
    await settings_service.save_llm_settings(db, base_url=base_url, model=model, api_key=api_key)

    llm = await settings_service.get_llm_settings(db)
    return templates.TemplateResponse("partials/admin/llm_settings.html", {
        "request": request,
        "llm": llm,
        "saved": True,
    })


@router.post("/llm/test")
async def test_llm_connection(request: Request, db: AsyncSession = Depends(get_db)):
    # Read values from the form so Test works even without saving first
    body = await request.json()
    base_url = (body.get("base_url") or "").strip()
    model = (body.get("model") or "").strip()
    api_key = (body.get("api_key") or "").strip()

    # Fall back to DB/config if the form fields are empty
    if not base_url or not model:
        llm = await settings_service.get_llm_settings(db)
        base_url = base_url or llm["base_url"] or ""
        model = model or llm["model"] or ""
        api_key = api_key or llm["api_key"] or ""

    if not base_url or not model:
        return JSONResponse(content={
            "success": False,
            "message": "Endpoint and model must be configured first.",
        })

    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(base_url=base_url, api_key=api_key or "not-needed")
        response = await client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": "Say OK"}],
            max_tokens=10,
            timeout=15,
        )
        reply = (response.choices[0].message.content or "").strip()[:50]
        return JSONResponse(content={
            "success": True,
            "message": f"Connected. Response: \"{reply}\"",
        })
    except Exception as e:
        return JSONResponse(content={
            "success": False,
            "message": str(e)[:200],
        })

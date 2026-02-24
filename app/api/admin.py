import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.models import Opportunity, Agency
from app.models.sync_log import SyncLog
from app.models.researcher import (
    Researcher, ResearcherKeyword, ResearcherAffiliation,
    ResearcherEducation, Publication,
)
from app.services.sync_service import sync_service
from app.services.researcher_sync_service import researcher_sync_service
from app.services.match_service import match_service
from app.services.settings_service import settings_service, TIMEZONE_CHOICES
from app.tasks import scheduler

router = APIRouter(prefix="/admin", tags=["admin"])
templates = Jinja2Templates(directory="app/templates")


# --- Timezone Jinja2 filter ---

def tz_filter(dt_value, tz_name="UTC"):
    if dt_value is None:
        return ""
    if dt_value.tzinfo is None:
        dt_value = dt_value.replace(tzinfo=ZoneInfo("UTC"))
    return dt_value.astimezone(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M %Z")

templates.env.filters["tz"] = tz_filter


# --- Auth helpers ---

def _is_admin(request: Request) -> bool:
    return request.session.get("is_admin", False)


def require_admin(request: Request):
    if not _is_admin(request):
        raise HTTPException(status_code=403, detail="Admin authentication required")


# --- Main dashboard ---

@router.get("", response_class=HTMLResponse)
async def admin_dashboard(request: Request):
    return templates.TemplateResponse("admin.html", {
        "request": request,
        "is_admin": _is_admin(request),
    })


# --- Login/Logout ---

@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if _is_admin(request):
        return RedirectResponse("/admin", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@router.post("/login", response_class=HTMLResponse)
async def login(request: Request):
    form = await request.form()
    username = (form.get("username") or "").strip()
    password = (form.get("password") or "").strip()

    if (
        settings.ADMIN_PASSWORD
        and username == settings.ADMIN_USERNAME
        and password == settings.ADMIN_PASSWORD
    ):
        request.session["is_admin"] = True
        return RedirectResponse("/admin", status_code=302)

    return templates.TemplateResponse("login.html", {
        "request": request,
        "error": "Invalid username or password",
    })


@router.post("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/admin", status_code=302)


# ====================================================================
# Section 1: Data Sources — Grants.gov sync live
# ====================================================================

@router.get("/sync/live", response_class=HTMLResponse)
async def sync_live(request: Request, db: AsyncSession = Depends(get_db)):
    tz = await settings_service.get_timezone(db)

    if sync_service.is_syncing:
        stats = dict(sync_service.sync_stats)
        elapsed = None
        if stats.get("started"):
            try:
                started = datetime.fromisoformat(stats["started"])
                elapsed = (datetime.utcnow() - started).total_seconds()
            except (ValueError, TypeError):
                pass
        await sync_service._publish_stats()
        return templates.TemplateResponse("partials/admin/sync_live.html", {
            "request": request,
            "is_syncing": True,
            "stats": stats,
            "elapsed": elapsed,
            "last_sync": sync_service.last_sync,
            "is_admin": _is_admin(request),
            "tz": tz,
        })

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
            "is_admin": _is_admin(request),
            "tz": tz,
        })

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
        "is_admin": _is_admin(request),
        "tz": tz,
    })


@router.post("/sync/trigger", response_class=HTMLResponse, dependencies=[Depends(require_admin)])
async def trigger_sync(request: Request, full: bool = False, refresh: bool = False):
    if not sync_service.is_syncing:
        if refresh:
            asyncio.create_task(sync_service.full_sync(skip_discovery=True))
        elif full:
            asyncio.create_task(sync_service.full_sync())
        else:
            asyncio.create_task(sync_service.incremental_sync())

    await asyncio.sleep(0.2)
    return await sync_live(request)


@router.post("/sync/cancel", response_class=HTMLResponse, dependencies=[Depends(require_admin)])
async def cancel_sync(request: Request):
    cancelled = sync_service.cancel_sync()
    await asyncio.sleep(0.5)
    if not cancelled:
        from app.services.cache_service import cache_service
        await cache_service.delete("pf:sync_stats")
    return await sync_live(request)


# ====================================================================
# Section 1: Data Sources — CollabNet sync live
# ====================================================================

@router.get("/researcher-sync/live", response_class=HTMLResponse)
async def researcher_sync_live(request: Request, db: AsyncSession = Depends(get_db)):
    tz = await settings_service.get_timezone(db)

    if researcher_sync_service.is_syncing:
        stats = dict(researcher_sync_service.sync_stats)
        elapsed = None
        if stats.get("started"):
            try:
                started = datetime.fromisoformat(stats["started"])
                elapsed = (datetime.utcnow() - started).total_seconds()
            except (ValueError, TypeError):
                pass
        await researcher_sync_service._publish_stats()
        return templates.TemplateResponse("partials/admin/researcher_sync_live.html", {
            "request": request,
            "is_syncing": True,
            "stats": stats,
            "elapsed": elapsed,
            "last_sync": researcher_sync_service.last_sync,
            "is_admin": _is_admin(request),
            "tz": tz,
        })

    shared = await researcher_sync_service.get_shared_stats()
    if shared and shared.get("is_syncing"):
        stats = shared["stats"]
        elapsed = None
        if stats.get("started"):
            try:
                started = datetime.fromisoformat(stats["started"])
                elapsed = (datetime.utcnow() - started).total_seconds()
            except (ValueError, TypeError):
                pass
        return templates.TemplateResponse("partials/admin/researcher_sync_live.html", {
            "request": request,
            "is_syncing": True,
            "stats": stats,
            "elapsed": elapsed,
            "last_sync": None,
            "is_admin": _is_admin(request),
            "tz": tz,
        })

    last_log = (await db.execute(
        select(SyncLog)
        .where(SyncLog.sync_type == "researcher_full", SyncLog.status == "completed")
        .order_by(SyncLog.completed_at.desc()).limit(1)
    )).scalar_one_or_none()

    last_sync = last_log.completed_at if last_log else researcher_sync_service.last_sync

    return templates.TemplateResponse("partials/admin/researcher_sync_live.html", {
        "request": request,
        "is_syncing": False,
        "stats": shared.get("stats", {}) if shared else {},
        "elapsed": None,
        "last_sync": last_sync,
        "is_admin": _is_admin(request),
        "tz": tz,
    })


@router.post("/researcher-sync/trigger", response_class=HTMLResponse, dependencies=[Depends(require_admin)])
async def trigger_researcher_sync(request: Request):
    if not researcher_sync_service.is_syncing:
        asyncio.create_task(researcher_sync_service.full_sync())
    await asyncio.sleep(0.2)
    return await researcher_sync_live(request)


@router.post("/researcher-sync/cancel", response_class=HTMLResponse, dependencies=[Depends(require_admin)])
async def cancel_researcher_sync(request: Request):
    cancelled = researcher_sync_service.cancel_sync()
    await asyncio.sleep(0.5)
    if not cancelled:
        from app.services.cache_service import cache_service
        await cache_service.delete("pf:researcher_sync_stats")
    return await researcher_sync_live(request)


@router.post("/matches/recompute", response_class=HTMLResponse, dependencies=[Depends(require_admin)])
async def trigger_match_recompute(request: Request):
    if not match_service.is_computing:
        asyncio.create_task(match_service.recompute_all_matches())
        await asyncio.sleep(0.2)
    return await match_recompute_status(request)


@router.get("/matches/status", response_class=HTMLResponse)
async def match_recompute_status(request: Request):
    # Check this worker first
    if match_service.is_computing:
        stats = dict(match_service.match_stats)
        return templates.TemplateResponse("partials/admin/match_status.html", {
            "request": request,
            "is_computing": True,
            "stats": stats,
        })

    # Check Redis for stats from another worker
    shared = await match_service.get_shared_match_stats()
    if shared and shared.get("is_computing"):
        return templates.TemplateResponse("partials/admin/match_status.html", {
            "request": request,
            "is_computing": True,
            "stats": shared.get("stats", {}),
        })

    # Not computing — show last result if available
    stats = {}
    if shared:
        stats = shared.get("stats", {})
    elif match_service.match_stats:
        stats = dict(match_service.match_stats)

    return templates.TemplateResponse("partials/admin/match_status.html", {
        "request": request,
        "is_computing": False,
        "stats": stats,
    })


# ====================================================================
# Section 1: Data Sources — Data Health
# ====================================================================

@router.get("/data/health/grants", response_class=HTMLResponse)
async def data_health_grants(request: Request, db: AsyncSession = Depends(get_db)):
    tz = await settings_service.get_timezone(db)

    total = (await db.execute(select(func.count(Opportunity.id)))).scalar() or 0

    status_counts = {}
    stmt = select(Opportunity.status, func.count(Opportunity.id)).group_by(Opportunity.status)
    rows = (await db.execute(stmt)).all()
    for status, count in rows:
        status_counts[status] = count

    agency_count = (await db.execute(select(func.count(Agency.code)))).scalar() or 0

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

    team_based = (await db.execute(
        select(func.count(Opportunity.id)).where(Opportunity.is_team_based == True)
    )).scalar() or 0
    multi_inst = (await db.execute(
        select(func.count(Opportunity.id)).where(Opportunity.is_multi_institution == True)
    )).scalar() or 0
    multi_disc = (await db.execute(
        select(func.count(Opportunity.id)).where(Opportunity.is_multi_disciplinary == True)
    )).scalar() or 0

    return templates.TemplateResponse("partials/admin/data_health_grants.html", {
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
        "tz": tz,
    })


@router.get("/data/health/collabnet", response_class=HTMLResponse)
async def data_health_collabnet(request: Request, db: AsyncSession = Depends(get_db)):
    total_researchers = (await db.execute(select(func.count(Researcher.id)))).scalar() or 0
    active = (await db.execute(
        select(func.count(Researcher.id)).where(Researcher.status == "ACTIVE")
    )).scalar() or 0
    inactive = total_researchers - active
    with_summaries = (await db.execute(
        select(func.count(Researcher.id)).where(
            Researcher.ai_summary.isnot(None), Researcher.ai_summary != ""
        )
    )).scalar() or 0
    total_publications = (await db.execute(select(func.count(Publication.id)))).scalar() or 0
    total_keywords = (await db.execute(select(func.count(ResearcherKeyword.id)))).scalar() or 0
    total_affiliations = (await db.execute(select(func.count(ResearcherAffiliation.id)))).scalar() or 0
    total_education = (await db.execute(select(func.count(ResearcherEducation.id)))).scalar() or 0

    return templates.TemplateResponse("partials/admin/data_health_collabnet.html", {
        "request": request,
        "total_researchers": total_researchers,
        "active": active,
        "inactive": inactive,
        "with_summaries": with_summaries,
        "total_publications": total_publications,
        "total_keywords": total_keywords,
        "total_affiliations": total_affiliations,
        "total_education": total_education,
    })


# Keep old endpoint as redirect for compatibility
@router.get("/data/health", response_class=HTMLResponse)
async def data_health_redirect(request: Request, db: AsyncSession = Depends(get_db)):
    return await data_health_grants(request, db)


# ====================================================================
# Section 2: Sync History
# ====================================================================

@router.get("/sync/history", response_class=HTMLResponse)
async def sync_history(request: Request, db: AsyncSession = Depends(get_db)):
    tz = await settings_service.get_timezone(db)

    stmt = select(SyncLog).order_by(SyncLog.started_at.desc()).limit(20)
    result = await db.execute(stmt)
    logs = result.scalars().all()

    return templates.TemplateResponse("partials/admin/sync_history.html", {
        "request": request,
        "logs": logs,
        "tz": tz,
    })


# ====================================================================
# Section 3: Per-Source Schedulers
# ====================================================================

@router.get("/scheduler/grants", response_class=HTMLResponse)
async def scheduler_grants(request: Request, db: AsyncSession = Depends(get_db)):
    tz = await settings_service.get_timezone(db)
    next_run = scheduler.get_next_run_time("incremental_sync")
    return templates.TemplateResponse("partials/admin/scheduler_grants.html", {
        "request": request,
        "enabled": scheduler.is_grants_enabled(),
        "interval_hours": scheduler.get_grants_interval_hours(),
        "next_run": next_run,
        "is_admin": _is_admin(request),
        "tz": tz,
    })


@router.post("/scheduler/grants/toggle", response_class=HTMLResponse, dependencies=[Depends(require_admin)])
async def toggle_grants_scheduler(request: Request, db: AsyncSession = Depends(get_db)):
    new_state = scheduler.toggle_grants_scheduler()
    await settings_service.save_grants_scheduler_settings(db, enabled=new_state)
    return await scheduler_grants(request, db)


@router.post("/scheduler/grants/interval", response_class=HTMLResponse, dependencies=[Depends(require_admin)])
async def update_grants_interval(request: Request, hours: int = 6, db: AsyncSession = Depends(get_db)):
    if hours in (1, 3, 6, 12, 24):
        scheduler.update_grants_interval(hours)
        await settings_service.save_grants_scheduler_settings(db, interval_hours=hours)
    return await scheduler_grants(request, db)


@router.get("/scheduler/collabnet", response_class=HTMLResponse)
async def scheduler_collabnet(request: Request, db: AsyncSession = Depends(get_db)):
    tz = await settings_service.get_timezone(db)
    next_run = scheduler.get_next_run_time("researcher_sync")
    sched = scheduler.get_collabnet_schedule()
    return templates.TemplateResponse("partials/admin/scheduler_collabnet.html", {
        "request": request,
        "enabled": scheduler.is_collabnet_enabled(),
        "day": sched["day"],
        "hour": sched["hour"],
        "minute": sched["minute"],
        "next_run": next_run,
        "is_admin": _is_admin(request),
        "tz": tz,
    })


@router.post("/scheduler/collabnet/toggle", response_class=HTMLResponse, dependencies=[Depends(require_admin)])
async def toggle_collabnet_scheduler(request: Request, db: AsyncSession = Depends(get_db)):
    new_state = scheduler.toggle_collabnet_scheduler()
    await settings_service.save_collabnet_scheduler_settings(db, enabled=new_state)
    return await scheduler_collabnet(request, db)


@router.post("/scheduler/collabnet/schedule", response_class=HTMLResponse, dependencies=[Depends(require_admin)])
async def update_collabnet_schedule(request: Request, db: AsyncSession = Depends(get_db)):
    form = await request.form()
    day = (form.get("day") or "fri").strip().lower()
    hour = int(form.get("hour") or 1)
    minute = int(form.get("minute") or 0)

    if day in scheduler.DAY_CHOICES and 0 <= hour <= 23 and 0 <= minute <= 59:
        scheduler.update_collabnet_schedule(day, hour, minute)
        await settings_service.save_collabnet_scheduler_settings(db, day=day, hour=hour, minute=minute)

    return await scheduler_collabnet(request, db)


# Keep old scheduler endpoint for compat
@router.get("/scheduler", response_class=HTMLResponse)
async def scheduler_info(request: Request, db: AsyncSession = Depends(get_db)):
    return await scheduler_grants(request, db)


@router.post("/scheduler/toggle", response_class=HTMLResponse, dependencies=[Depends(require_admin)])
async def toggle_scheduler_compat(request: Request, db: AsyncSession = Depends(get_db)):
    return await toggle_grants_scheduler(request, db)


@router.post("/scheduler/interval", response_class=HTMLResponse, dependencies=[Depends(require_admin)])
async def update_interval_compat(request: Request, hours: int = 6, db: AsyncSession = Depends(get_db)):
    return await update_grants_interval(request, hours, db)


# ====================================================================
# Section 4: Model Endpoints — LLM
# ====================================================================

@router.get("/llm", response_class=HTMLResponse)
async def llm_settings(request: Request, db: AsyncSession = Depends(get_db)):
    llm = await settings_service.get_llm_settings(db)
    return templates.TemplateResponse("partials/admin/llm_settings.html", {
        "request": request,
        "llm": llm,
        "saved": False,
        "is_admin": _is_admin(request),
    })


@router.post("/llm", response_class=HTMLResponse, dependencies=[Depends(require_admin)])
async def save_llm_settings(request: Request, db: AsyncSession = Depends(get_db)):
    form = await request.form()
    base_url = (form.get("base_url") or "").strip()
    model = (form.get("model") or "").strip()
    api_key = (form.get("api_key") or "").strip()

    await settings_service.save_llm_settings(db, base_url=base_url, model=model, api_key=api_key)

    llm = await settings_service.get_llm_settings(db)
    return templates.TemplateResponse("partials/admin/llm_settings.html", {
        "request": request,
        "llm": llm,
        "saved": True,
        "is_admin": _is_admin(request),
    })


@router.post("/llm/test", dependencies=[Depends(require_admin)])
async def test_llm_connection(request: Request, db: AsyncSession = Depends(get_db)):
    body = await request.json()
    base_url = (body.get("base_url") or "").strip()
    model = (body.get("model") or "").strip()
    api_key = (body.get("api_key") or "").strip()

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


# ====================================================================
# Section 4: Model Endpoints — Embedding
# ====================================================================

@router.get("/embedding", response_class=HTMLResponse)
async def embedding_settings(request: Request, db: AsyncSession = Depends(get_db)):
    embed = await settings_service.get_embedding_settings(db)
    return templates.TemplateResponse("partials/admin/embedding_settings.html", {
        "request": request,
        "embed": embed,
        "saved": False,
        "is_admin": _is_admin(request),
    })


@router.post("/embedding", response_class=HTMLResponse, dependencies=[Depends(require_admin)])
async def save_embedding_settings(request: Request, db: AsyncSession = Depends(get_db)):
    form = await request.form()
    base_url = (form.get("base_url") or "").strip()
    model = (form.get("model") or "").strip()
    api_key = (form.get("api_key") or "").strip()

    await settings_service.save_embedding_settings(db, base_url=base_url, model=model, api_key=api_key)

    embed = await settings_service.get_embedding_settings(db)
    return templates.TemplateResponse("partials/admin/embedding_settings.html", {
        "request": request,
        "embed": embed,
        "saved": True,
        "is_admin": _is_admin(request),
    })


@router.post("/embedding/test", dependencies=[Depends(require_admin)])
async def test_embedding_connection(request: Request, db: AsyncSession = Depends(get_db)):
    body = await request.json()
    base_url = (body.get("base_url") or "").strip()
    model = (body.get("model") or "").strip()
    api_key = (body.get("api_key") or "").strip()

    if not base_url or not model:
        embed = await settings_service.get_embedding_settings(db)
        base_url = base_url or embed["base_url"] or ""
        model = model or embed["model"] or ""
        api_key = api_key or embed["api_key"] or ""

    if not base_url or not model:
        return JSONResponse(content={
            "success": False,
            "message": "Endpoint and model must be configured first.",
        })

    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(base_url=base_url, api_key=api_key or "not-needed")
        response = await client.embeddings.create(
            model=model,
            input="test embedding",
            timeout=15,
        )
        dim = len(response.data[0].embedding)
        return JSONResponse(content={
            "success": True,
            "message": f"Connected. Embedding dimension: {dim}",
        })
    except Exception as e:
        return JSONResponse(content={
            "success": False,
            "message": str(e)[:200],
        })


# ====================================================================
# Section 4: Model Endpoints — Re-ranker
# ====================================================================

@router.get("/reranker", response_class=HTMLResponse)
async def reranker_settings(request: Request, db: AsyncSession = Depends(get_db)):
    reranker = await settings_service.get_reranker_settings(db)
    return templates.TemplateResponse("partials/admin/reranker_settings.html", {
        "request": request,
        "reranker": reranker,
        "saved": False,
        "is_admin": _is_admin(request),
    })


@router.post("/reranker", response_class=HTMLResponse, dependencies=[Depends(require_admin)])
async def save_reranker_settings(request: Request, db: AsyncSession = Depends(get_db)):
    form = await request.form()
    base_url = (form.get("base_url") or "").strip()
    model = (form.get("model") or "").strip()
    api_key = (form.get("api_key") or "").strip()

    await settings_service.save_reranker_settings(db, base_url=base_url, model=model, api_key=api_key)

    reranker = await settings_service.get_reranker_settings(db)
    return templates.TemplateResponse("partials/admin/reranker_settings.html", {
        "request": request,
        "reranker": reranker,
        "saved": True,
        "is_admin": _is_admin(request),
    })


@router.post("/reranker/test", dependencies=[Depends(require_admin)])
async def test_reranker_connection(request: Request, db: AsyncSession = Depends(get_db)):
    body = await request.json()
    base_url = (body.get("base_url") or "").strip()
    model = (body.get("model") or "").strip()
    api_key = (body.get("api_key") or "").strip()

    if not base_url or not model:
        reranker = await settings_service.get_reranker_settings(db)
        base_url = base_url or reranker["base_url"] or ""
        model = model or reranker["model"] or ""
        api_key = api_key or reranker["api_key"] or ""

    if not base_url or not model:
        return JSONResponse(content={
            "success": False,
            "message": "Endpoint and model must be configured first.",
        })

    try:
        import httpx
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        # Use a generic reranking request format
        payload = {
            "model": model,
            "query": "test query",
            "documents": ["document one", "document two"],
        }

        async with httpx.AsyncClient(timeout=15) as client:
            # Try /rerank endpoint first (common for re-ranker APIs)
            url = base_url.rstrip("/")
            resp = await client.post(f"{url}/rerank", json=payload, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            n_results = len(data.get("results", data.get("data", [])))
            return JSONResponse(content={
                "success": True,
                "message": f"Connected. Returned {n_results} ranked results.",
            })
    except Exception as e:
        return JSONResponse(content={
            "success": False,
            "message": str(e)[:200],
        })


# ====================================================================
# Section 5: App Settings (Timezone)
# ====================================================================

@router.get("/settings", response_class=HTMLResponse)
async def app_settings_view(request: Request, db: AsyncSession = Depends(get_db)):
    tz = await settings_service.get_timezone(db)
    return templates.TemplateResponse("partials/admin/app_settings.html", {
        "request": request,
        "timezone": tz,
        "timezone_choices": TIMEZONE_CHOICES,
        "saved": False,
        "is_admin": _is_admin(request),
    })


@router.post("/settings", response_class=HTMLResponse, dependencies=[Depends(require_admin)])
async def save_app_settings(request: Request, db: AsyncSession = Depends(get_db)):
    form = await request.form()
    timezone = (form.get("timezone") or "US/Pacific").strip()

    await settings_service.save_timezone(db, timezone)

    tz = await settings_service.get_timezone(db)
    return templates.TemplateResponse("partials/admin/app_settings.html", {
        "request": request,
        "timezone": tz,
        "timezone_choices": TIMEZONE_CHOICES,
        "saved": True,
        "is_admin": _is_admin(request),
    })

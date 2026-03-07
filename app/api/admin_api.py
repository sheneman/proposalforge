"""JSON API endpoints for the React admin SPA."""

import asyncio
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select, func, text, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.models import Opportunity, Agency
from app.models.sync_log import SyncLog
from app.models.researcher import (
    Researcher, ResearcherKeyword, ResearcherAffiliation,
    ResearcherEducation, ResearcherIdentifier,
    Publication, Grant, Project, Activity,
)
from app.services.pipeline_service import pipeline_service
from app.services.sync_service import sync_service
from app.services.researcher_sync_service import researcher_sync_service
from app.services.match_service import match_service
from app.services.settings_service import settings_service
from app.tasks import scheduler

router = APIRouter(prefix="/admin/api", tags=["admin-api"])


# --- Auth ---

def _is_admin(request: Request) -> bool:
    return request.session.get("is_admin", False)


def require_admin(request: Request):
    if not _is_admin(request):
        raise HTTPException(status_code=403, detail="Admin authentication required")


@router.get("/auth/status")
async def auth_status(request: Request):
    return {"is_admin": _is_admin(request)}


# ====================================================================
# Pipeline
# ====================================================================

@router.get("/pipeline/status")
async def pipeline_status(request: Request):
    return await pipeline_service.get_status()


@router.post("/pipeline/start", dependencies=[Depends(require_admin)])
async def pipeline_start(request: Request):
    body = await request.json()
    types = body.get("types", ["posted", "forecasted"])
    if not types:
        raise HTTPException(400, "At least one opportunity type required")
    try:
        await pipeline_service.start(types)
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    return {"ok": True}


@router.post("/pipeline/cancel", dependencies=[Depends(require_admin)])
async def pipeline_cancel(request: Request):
    await pipeline_service.cancel()
    return {"ok": True}


# ====================================================================
# Settings
# ====================================================================

@router.get("/settings/llm")
async def get_llm(db: AsyncSession = Depends(get_db)):
    return await settings_service.get_llm_settings(db)


@router.post("/settings/llm", dependencies=[Depends(require_admin)])
async def save_llm(request: Request, db: AsyncSession = Depends(get_db)):
    body = await request.json()
    await settings_service.save_llm_settings(
        db,
        base_url=body.get("base_url", ""),
        model=body.get("model", ""),
        api_key=body.get("api_key", ""),
    )
    return {"ok": True}


@router.post("/settings/llm/test", dependencies=[Depends(require_admin)])
async def test_llm(request: Request, db: AsyncSession = Depends(get_db)):
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
        return {"ok": False, "error": "Endpoint and model must be configured first."}

    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(base_url=base_url, api_key=api_key or "not-needed")
        response = await client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": "Say OK"}],
            max_tokens=10, timeout=15,
        )
        reply = (response.choices[0].message.content or "").strip()[:50]
        return {"ok": True, "message": f'Connected. Response: "{reply}"'}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


@router.get("/settings/embedding")
async def get_embedding(db: AsyncSession = Depends(get_db)):
    return await settings_service.get_embedding_settings(db)


@router.post("/settings/embedding", dependencies=[Depends(require_admin)])
async def save_embedding(request: Request, db: AsyncSession = Depends(get_db)):
    body = await request.json()
    await settings_service.save_embedding_settings(
        db,
        base_url=body.get("base_url", ""),
        model=body.get("model", ""),
        api_key=body.get("api_key", ""),
    )
    return {"ok": True}


@router.post("/settings/embedding/test", dependencies=[Depends(require_admin)])
async def test_embedding(request: Request, db: AsyncSession = Depends(get_db)):
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
        return {"ok": False, "error": "Endpoint and model must be configured first."}

    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(base_url=base_url, api_key=api_key or "not-needed")
        response = await client.embeddings.create(
            model=model, input="test embedding", timeout=15,
        )
        dim = len(response.data[0].embedding)
        return {"ok": True, "message": f"Connected. Embedding dimension: {dim}"}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


@router.get("/settings/reranker")
async def get_reranker(db: AsyncSession = Depends(get_db)):
    return await settings_service.get_reranker_settings(db)


@router.post("/settings/reranker", dependencies=[Depends(require_admin)])
async def save_reranker(request: Request, db: AsyncSession = Depends(get_db)):
    body = await request.json()
    await settings_service.save_reranker_settings(
        db,
        base_url=body.get("base_url", ""),
        model=body.get("model", ""),
        api_key=body.get("api_key", ""),
    )
    return {"ok": True}


@router.post("/settings/reranker/test", dependencies=[Depends(require_admin)])
async def test_reranker(request: Request, db: AsyncSession = Depends(get_db)):
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
        return {"ok": False, "error": "Endpoint and model must be configured first."}

    try:
        import httpx
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        payload = {
            "model": model,
            "query": "test query",
            "documents": ["document one", "document two"],
        }
        async with httpx.AsyncClient(timeout=15) as client:
            url = base_url.rstrip("/")
            resp = await client.post(f"{url}/rerank", json=payload, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            n_results = len(data.get("results", data.get("data", [])))
            return {"ok": True, "message": f"Connected. Returned {n_results} ranked results."}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


@router.get("/settings/ocr")
async def get_ocr(db: AsyncSession = Depends(get_db)):
    ocr = await settings_service.get_ocr_settings(db)
    return {
        "base_url": ocr.get("endpoint_url", ""),
        "model": "",
        "api_key": "",
        "ocr_method": ocr.get("method", "dotsocr"),
        "doc_workers": str(ocr.get("doc_workers", 4)),
    }


@router.post("/settings/ocr", dependencies=[Depends(require_admin)])
async def save_ocr(request: Request, db: AsyncSession = Depends(get_db)):
    body = await request.json()
    method = body.get("ocr_method", "dotsocr")
    endpoint_url = body.get("base_url", "")
    doc_workers = int(body.get("doc_workers", 4))
    await settings_service.save_ocr_settings(
        db, method=method, endpoint_url=endpoint_url, doc_workers=doc_workers,
    )
    return {"ok": True}


@router.post("/settings/ocr/test", dependencies=[Depends(require_admin)])
async def test_ocr(request: Request, db: AsyncSession = Depends(get_db)):
    body = await request.json()
    endpoint_url = (body.get("base_url") or "").strip()

    if not endpoint_url:
        ocr = await settings_service.get_ocr_settings(db)
        endpoint_url = ocr.get("endpoint_url", "")

    if not endpoint_url:
        return {"ok": False, "error": "OCR endpoint URL must be configured first."}

    try:
        import httpx
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(endpoint_url, follow_redirects=True)
            return {"ok": True, "message": f"Connected. Status: {resp.status_code}"}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


@router.get("/settings/app")
async def get_app_settings(db: AsyncSession = Depends(get_db)):
    tz = await settings_service.get_timezone(db)
    return {"timezone": tz}


@router.post("/settings/app", dependencies=[Depends(require_admin)])
async def save_app_settings(request: Request, db: AsyncSession = Depends(get_db)):
    body = await request.json()
    tz = body.get("timezone", "UTC")
    await settings_service.save_timezone(db, tz)
    return {"ok": True}


# ====================================================================
# Health
# ====================================================================

@router.get("/health/grants")
async def health_grants(db: AsyncSession = Depends(get_db)):
    stats_row = (await db.execute(select(
        func.count(Opportunity.id),
        func.sum(func.IF(Opportunity.close_date.is_(None), 1, 0)),
        func.sum(func.IF(Opportunity.award_ceiling.is_(None), 1, 0)),
        func.sum(func.IF(
            (Opportunity.synopsis_description.is_(None)) | (Opportunity.synopsis_description == ""),
            1, 0,
        )),
    ))).one()

    total = stats_row[0] or 0
    status_counts = {}
    rows = (await db.execute(
        select(Opportunity.status, func.count(Opportunity.id)).group_by(Opportunity.status)
    )).all()
    for status, count in rows:
        status_counts[status] = count

    agency_count = (await db.execute(select(func.count(Agency.code)))).scalar() or 0

    from app.services.document_service import document_service
    doc_counts = await document_service.get_document_counts()

    return {
        "total_opportunities": total,
        "agencies": agency_count,
        "missing_close_date": int(stats_row[1] or 0),
        "missing_award_ceiling": int(stats_row[2] or 0),
        "missing_description": int(stats_row[3] or 0),
        "documents": doc_counts.get("total", 0),
        "downloaded": doc_counts.get("downloaded", 0),
        "ocr_completed": doc_counts.get("ocr_completed", 0),
        "classified": doc_counts.get("classified", 0),
        **{f"status_{k}": v for k, v in status_counts.items()},
    }


@router.get("/health/collabnet")
async def health_collabnet(db: AsyncSession = Depends(get_db)):
    total = (await db.execute(select(func.count(Researcher.id)))).scalar() or 0
    active = (await db.execute(
        select(func.count(Researcher.id)).where(Researcher.status == "ACTIVE")
    )).scalar() or 0
    publications = (await db.execute(select(func.count(Publication.id)))).scalar() or 0
    keywords = (await db.execute(select(func.count(ResearcherKeyword.id)))).scalar() or 0
    grants = (await db.execute(select(func.count(Grant.id)))).scalar() or 0

    return {
        "total_researchers": total,
        "active": active,
        "inactive": total - active,
        "publications": publications,
        "keywords": keywords,
        "grants": grants,
    }


# ====================================================================
# Sync History
# ====================================================================

@router.get("/sync/history")
async def sync_history(db: AsyncSession = Depends(get_db)):
    stmt = select(SyncLog).order_by(SyncLog.started_at.desc()).limit(20)
    result = await db.execute(stmt)
    logs = result.scalars().all()

    return [
        {
            "id": log.id,
            "sync_type": log.sync_type,
            "status": log.status,
            "started_at": log.started_at.isoformat() if log.started_at else None,
            "completed_at": log.completed_at.isoformat() if log.completed_at else None,
            "duration_seconds": log.duration_seconds,
            "total_items": log.total_items or 0,
            "success_count": log.success_count or 0,
            "error_count": log.error_count or 0,
            "error_message": log.error_message,
        }
        for log in logs
    ]


# ====================================================================
# Scheduler
# ====================================================================

@router.get("/scheduler/grants")
async def scheduler_grants():
    next_run = scheduler.get_next_run_time("incremental_sync")
    return {
        "enabled": scheduler.is_grants_enabled(),
        "interval_hours": scheduler.get_grants_interval_hours(),
        "next_run": next_run.isoformat() if next_run else None,
    }


@router.post("/scheduler/grants/toggle", dependencies=[Depends(require_admin)])
async def toggle_grants_scheduler(db: AsyncSession = Depends(get_db)):
    new_state = scheduler.toggle_grants_scheduler()
    await settings_service.save_grants_scheduler_settings(db, enabled=new_state)
    return {"ok": True, "enabled": new_state}


@router.post("/scheduler/grants/interval", dependencies=[Depends(require_admin)])
async def update_grants_interval(request: Request, db: AsyncSession = Depends(get_db)):
    body = await request.json()
    hours = body.get("hours", 6)
    if hours in (1, 3, 6, 12, 24):
        scheduler.update_grants_interval(hours)
        await settings_service.save_grants_scheduler_settings(db, interval_hours=hours)
    return {"ok": True}


@router.get("/scheduler/collabnet")
async def scheduler_collabnet():
    next_run = scheduler.get_next_run_time("researcher_sync")
    sched = scheduler.get_collabnet_schedule()
    return {
        "enabled": scheduler.is_collabnet_enabled(),
        "schedule_day": sched["day"],
        "schedule_hour": sched["hour"],
        "schedule_minute": sched["minute"],
        "next_run": next_run.isoformat() if next_run else None,
    }


@router.post("/scheduler/collabnet/toggle", dependencies=[Depends(require_admin)])
async def toggle_collabnet_scheduler(db: AsyncSession = Depends(get_db)):
    new_state = scheduler.toggle_collabnet_scheduler()
    await settings_service.save_collabnet_scheduler_settings(db, enabled=new_state)
    return {"ok": True, "enabled": new_state}


@router.post("/scheduler/collabnet/schedule", dependencies=[Depends(require_admin)])
async def update_collabnet_schedule(request: Request, db: AsyncSession = Depends(get_db)):
    body = await request.json()
    day = (body.get("day") or "fri").strip().lower()
    hour = int(body.get("hour", 1))
    minute = int(body.get("minute", 0))
    if day in scheduler.DAY_CHOICES and 0 <= hour <= 23 and 0 <= minute <= 59:
        scheduler.update_collabnet_schedule(day, hour, minute)
        await settings_service.save_collabnet_scheduler_settings(db, day=day, hour=hour, minute=minute)
    return {"ok": True}


# ====================================================================
# Researcher Sync
# ====================================================================

@router.get("/researcher-sync/status")
async def researcher_sync_status():
    shared = await researcher_sync_service.get_shared_stats()
    if shared:
        return {"is_syncing": shared.get("is_syncing", False), "stats": shared.get("stats", {})}
    return {"is_syncing": researcher_sync_service.is_syncing, "stats": researcher_sync_service.sync_stats}


@router.post("/researcher-sync/trigger", dependencies=[Depends(require_admin)])
async def trigger_researcher_sync():
    if researcher_sync_service.is_syncing:
        raise HTTPException(409, "Researcher sync already running")
    asyncio.create_task(researcher_sync_service.full_sync())
    return {"ok": True}


@router.post("/researcher-sync/cancel", dependencies=[Depends(require_admin)])
async def cancel_researcher_sync():
    researcher_sync_service.cancel_sync()
    return {"ok": True}


# ====================================================================
# Matches
# ====================================================================

@router.get("/matches/status")
async def matches_status():
    return {"is_computing": match_service.is_computing, "stats": match_service.match_stats}


@router.post("/matches/recompute", dependencies=[Depends(require_admin)])
async def recompute_matches():
    if match_service.is_computing:
        raise HTTPException(409, "Match computation already running")
    asyncio.create_task(match_service.recompute_all_matches())
    return {"ok": True}

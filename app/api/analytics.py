import logging
from datetime import date

from fastapi import APIRouter, Depends, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.services.analytics_service import analytics_service
from app.services.chat_service import chat_service
from app.services.search_service import search_service
from app.services.sync_service import sync_service

logger = logging.getLogger(__name__)

router = APIRouter(tags=["analytics"])
templates = Jinja2Templates(directory="app/templates")


def _parse_filters(
    status: str | None = None,
    agency: str | None = None,
    category: str | None = None,
    date_start: date | None = None,
    date_end: date | None = None,
) -> dict:
    """Parse common query params into filter kwargs."""
    return {
        "status": status.split(",") if status else None,
        "agency_codes": agency.split(",") if agency else None,
        "category_codes": category.split(",") if category else None,
        "date_start": date_start,
        "date_end": date_end,
    }


# --- HTML Page ---

@router.get("/analytics", response_class=HTMLResponse)
async def analytics_page(request: Request, db: AsyncSession = Depends(get_db)):
    agencies = await search_service.get_agencies(db)
    categories = await search_service.get_categories(db)
    return templates.TemplateResponse("analytics.html", {
        "request": request,
        "agencies": agencies,
        "categories": categories,
        "last_sync": sync_service.last_sync,
    })


# --- KPI Endpoint ---

@router.get("/analytics/api/kpis")
async def api_kpis(
    status: str | None = None,
    agency: str | None = None,
    category: str | None = None,
    date_start: date | None = None,
    date_end: date | None = None,
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_filters(status, agency, category, date_start, date_end)
    return await analytics_service.summary_kpis(db, **filters)


# --- Timeline Tab ---

@router.get("/analytics/api/timeline")
async def api_timeline(
    status: str | None = None,
    agency: str | None = None,
    category: str | None = None,
    date_start: date | None = None,
    date_end: date | None = None,
    granularity: str = "month",
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_filters(status, agency, category, date_start, date_end)
    return await analytics_service.opportunities_over_time(db, granularity=granularity, **filters)


@router.get("/analytics/api/close-dates")
async def api_close_dates(
    status: str | None = None,
    agency: str | None = None,
    category: str | None = None,
    date_start: date | None = None,
    date_end: date | None = None,
    granularity: str = "month",
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_filters(status, agency, category, date_start, date_end)
    return await analytics_service.close_dates_over_time(db, granularity=granularity, **filters)


# --- Funding Tab ---

@router.get("/analytics/api/funding-distribution")
async def api_funding_distribution(
    status: str | None = None,
    agency: str | None = None,
    category: str | None = None,
    date_start: date | None = None,
    date_end: date | None = None,
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_filters(status, agency, category, date_start, date_end)
    return await analytics_service.award_ceiling_distribution(db, **filters)


@router.get("/analytics/api/funding-by-agency")
async def api_funding_by_agency(
    status: str | None = None,
    agency: str | None = None,
    category: str | None = None,
    date_start: date | None = None,
    date_end: date | None = None,
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_filters(status, agency, category, date_start, date_end)
    return await analytics_service.funding_by_agency(db, **filters)


@router.get("/analytics/api/funding-by-category")
async def api_funding_by_category(
    status: str | None = None,
    agency: str | None = None,
    category: str | None = None,
    date_start: date | None = None,
    date_end: date | None = None,
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_filters(status, agency, category, date_start, date_end)
    return await analytics_service.funding_by_category(db, **filters)


@router.get("/analytics/api/funding-trends")
async def api_funding_trends(
    status: str | None = None,
    agency: str | None = None,
    category: str | None = None,
    date_start: date | None = None,
    date_end: date | None = None,
    granularity: str = "month",
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_filters(status, agency, category, date_start, date_end)
    return await analytics_service.funding_trends(db, granularity=granularity, **filters)


@router.get("/analytics/api/floor-vs-ceiling")
async def api_floor_vs_ceiling(
    status: str | None = None,
    agency: str | None = None,
    category: str | None = None,
    date_start: date | None = None,
    date_end: date | None = None,
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_filters(status, agency, category, date_start, date_end)
    return await analytics_service.floor_vs_ceiling(db, **filters)


# --- Agency Tab ---

@router.get("/analytics/api/agency-comparison")
async def api_agency_comparison(
    status: str | None = None,
    agency: str | None = None,
    category: str | None = None,
    date_start: date | None = None,
    date_end: date | None = None,
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_filters(status, agency, category, date_start, date_end)
    return await analytics_service.agency_comparison(db, **filters)


@router.get("/analytics/api/agency-activity")
async def api_agency_activity(
    status: str | None = None,
    agency: str | None = None,
    category: str | None = None,
    date_start: date | None = None,
    date_end: date | None = None,
    granularity: str = "month",
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_filters(status, agency, category, date_start, date_end)
    return await analytics_service.agency_activity_over_time(db, granularity=granularity, **filters)


@router.get("/analytics/api/agency-category")
async def api_agency_category(
    status: str | None = None,
    agency: str | None = None,
    category: str | None = None,
    date_start: date | None = None,
    date_end: date | None = None,
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_filters(status, agency, category, date_start, date_end)
    return await analytics_service.agency_category_heatmap(db, **filters)


# --- Category Tab ---

@router.get("/analytics/api/category-funding")
async def api_category_funding(
    status: str | None = None,
    agency: str | None = None,
    category: str | None = None,
    date_start: date | None = None,
    date_end: date | None = None,
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_filters(status, agency, category, date_start, date_end)
    return await analytics_service.category_funding(db, **filters)


@router.get("/analytics/api/classification")
async def api_classification(
    status: str | None = None,
    agency: str | None = None,
    category: str | None = None,
    date_start: date | None = None,
    date_end: date | None = None,
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_filters(status, agency, category, date_start, date_end)
    return await analytics_service.classification_breakdown(db, **filters)


@router.get("/analytics/api/classification-trends")
async def api_classification_trends(
    status: str | None = None,
    agency: str | None = None,
    category: str | None = None,
    date_start: date | None = None,
    date_end: date | None = None,
    granularity: str = "month",
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_filters(status, agency, category, date_start, date_end)
    return await analytics_service.classification_trends(db, granularity=granularity, **filters)


# --- Chat Endpoint ---

@router.post("/analytics/api/chat")
async def api_chat(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    body = await request.json()
    message = body.get("message", "").strip()
    history = body.get("history", [])

    if not message:
        return JSONResponse(
            status_code=400,
            content={"error": "Message is required"},
        )

    result = await chat_service.chat(db, message, history)
    return result

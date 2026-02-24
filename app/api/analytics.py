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


def _parse_researcher_filters(
    department: str | None = None,
    researcher_status: str | None = None,
    keyword: str | None = None,
) -> dict:
    """Parse researcher-specific filter params."""
    return {
        "departments": department.split(",") if department else None,
        "researcher_status": researcher_status.split(",") if researcher_status else None,
        "keyword": keyword or None,
    }


def _parse_match_filters(
    min_score: float | None = None,
    agency: str | None = None,
    department: str | None = None,
) -> dict:
    """Parse match-specific filter params."""
    return {
        "min_score": min_score,
        "agency_codes": agency.split(",") if agency else None,
        "departments": department.split(",") if department else None,
    }


# --- HTML Page ---

@router.get("/analytics", response_class=HTMLResponse)
async def analytics_page(request: Request, db: AsyncSession = Depends(get_db)):
    agencies = await search_service.get_agencies(db)
    categories = await search_service.get_categories(db)
    departments = await analytics_service.get_departments(db)
    return templates.TemplateResponse("analytics.html", {
        "request": request,
        "agencies": agencies,
        "categories": categories,
        "departments": departments,
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
    return await analytics_service.cross_domain_kpis(db, **filters)


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


# --- Researcher Endpoints ---

@router.get("/analytics/api/researchers/by-department")
async def api_researchers_by_department(
    department: str | None = None,
    researcher_status: str | None = None,
    keyword: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_researcher_filters(department, researcher_status, keyword)
    return await analytics_service.researchers_by_department(db, **filters)


@router.get("/analytics/api/researchers/status-breakdown")
async def api_researchers_status(
    department: str | None = None,
    researcher_status: str | None = None,
    keyword: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_researcher_filters(department, researcher_status, keyword)
    return await analytics_service.researcher_status_breakdown(db, **filters)


@router.get("/analytics/api/researchers/top-keywords")
async def api_researchers_keywords(
    department: str | None = None,
    researcher_status: str | None = None,
    keyword: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_researcher_filters(department, researcher_status, keyword)
    return await analytics_service.top_research_keywords(db, **filters)


@router.get("/analytics/api/researchers/publications-over-time")
async def api_publications_over_time(
    department: str | None = None,
    researcher_status: str | None = None,
    keyword: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_researcher_filters(department, researcher_status, keyword)
    return await analytics_service.publications_over_time(db, **filters)


@router.get("/analytics/api/researchers/grant-funding-by-funder")
async def api_grant_funding_by_funder(
    department: str | None = None,
    researcher_status: str | None = None,
    keyword: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_researcher_filters(department, researcher_status, keyword)
    return await analytics_service.grant_funding_by_funder(db, **filters)


@router.get("/analytics/api/researchers/activity-types")
async def api_activity_types(
    department: str | None = None,
    researcher_status: str | None = None,
    keyword: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_researcher_filters(department, researcher_status, keyword)
    return await analytics_service.activity_types(db, **filters)


@router.get("/analytics/api/researchers/engagement-summary")
async def api_researcher_engagement(
    department: str | None = None,
    researcher_status: str | None = None,
    keyword: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_researcher_filters(department, researcher_status, keyword)
    return await analytics_service.researcher_engagement(db, **filters)


# --- Match Endpoints ---

@router.get("/analytics/api/matches/score-distribution")
async def api_match_score_distribution(
    min_score: float | None = None,
    agency: str | None = None,
    department: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_match_filters(min_score, agency, department)
    return await analytics_service.match_score_distribution(db, **filters)


@router.get("/analytics/api/matches/component-breakdown")
async def api_match_components(
    min_score: float | None = None,
    agency: str | None = None,
    department: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_match_filters(min_score, agency, department)
    return await analytics_service.match_component_breakdown(db, **filters)


@router.get("/analytics/api/matches/top-researchers")
async def api_match_top_researchers(
    min_score: float | None = None,
    agency: str | None = None,
    department: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_match_filters(min_score, agency, department)
    return await analytics_service.top_matched_researchers(db, **filters)


@router.get("/analytics/api/matches/top-opportunities")
async def api_match_top_opportunities(
    min_score: float | None = None,
    agency: str | None = None,
    department: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_match_filters(min_score, agency, department)
    return await analytics_service.top_matched_opportunities(db, **filters)


@router.get("/analytics/api/matches/by-department")
async def api_match_by_department(
    min_score: float | None = None,
    agency: str | None = None,
    department: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_match_filters(min_score, agency, department)
    return await analytics_service.match_quality_by_department(db, **filters)


@router.get("/analytics/api/matches/by-agency")
async def api_match_by_agency(
    min_score: float | None = None,
    agency: str | None = None,
    department: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_match_filters(min_score, agency, department)
    return await analytics_service.match_quality_by_agency(db, **filters)


@router.get("/analytics/api/matches/coverage")
async def api_match_coverage(
    min_score: float | None = None,
    agency: str | None = None,
    department: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    filters = _parse_match_filters(min_score, agency, department)
    return await analytics_service.match_coverage(db, **filters)


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

    # Include model name so the UI can display it
    from app.services.settings_service import settings_service
    llm = await settings_service.get_llm_settings(db)
    result["model"] = llm.get("model", "")

    return result

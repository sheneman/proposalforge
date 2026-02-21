from datetime import date

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.api.opportunities import _serialize_opp
from app.services.search_service import search_service

router = APIRouter(tags=["search"])
templates = Jinja2Templates(directory="app/templates")


@router.get("/api/search")
async def api_search(
    q: str | None = None,
    status: str | None = None,
    agency: str | None = None,
    category: str | None = None,
    close_date_start: date | None = None,
    close_date_end: date | None = None,
    award_min: float | None = None,
    award_max: float | None = None,
    team_based: bool | None = None,
    multi_institution: bool | None = None,
    multi_disciplinary: bool | None = None,
    sort_by: str = "close_date",
    sort_order: str = "asc",
    page: int = 1,
    per_page: int = 25,
    db: AsyncSession = Depends(get_db),
):
    status_list = status.split(",") if status else None
    agency_list = agency.split(",") if agency else None
    category_list = category.split(",") if category else None

    results = await search_service.search(
        session=db,
        query=q,
        status=status_list,
        agency_codes=agency_list,
        category_codes=category_list,
        close_date_start=close_date_start,
        close_date_end=close_date_end,
        award_ceiling_min=award_min,
        award_ceiling_max=award_max,
        is_team_based=team_based,
        is_multi_institution=multi_institution,
        is_multi_disciplinary=multi_disciplinary,
        sort_by=sort_by,
        sort_order=sort_order,
        page=page,
        per_page=per_page,
    )

    facets = await search_service.get_facets(db, status_list)

    return {
        "opportunities": [_serialize_opp(o) for o in results["opportunities"]],
        "total": results["total"],
        "page": results["page"],
        "per_page": results["per_page"],
        "total_pages": results["total_pages"],
        "facets": facets,
    }


@router.get("/api/agencies")
async def get_agencies(db: AsyncSession = Depends(get_db)):
    return await search_service.get_agencies(db)


@router.get("/api/categories")
async def get_categories(db: AsyncSession = Depends(get_db)):
    return await search_service.get_categories(db)


@router.get("/api/stats")
async def get_stats(db: AsyncSession = Depends(get_db)):
    return await search_service.get_stats(db)

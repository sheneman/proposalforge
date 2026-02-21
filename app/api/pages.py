from datetime import date

from fastapi import APIRouter, Depends, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import Opportunity
from app.services.search_service import search_service
from app.services.sync_service import sync_service

router = APIRouter(tags=["pages"])
templates = Jinja2Templates(directory="app/templates")


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, db: AsyncSession = Depends(get_db)):
    stats = await search_service.get_stats(db)
    return templates.TemplateResponse("index.html", {
        "request": request,
        "stats": stats,
        "last_sync": sync_service.last_sync,
    })


@router.get("/search", response_class=HTMLResponse)
async def search_page(
    request: Request,
    q: str | None = None,
    status: str | None = None,
    agency: str | None = None,
    category: str | None = None,
    sort_by: str = "close_date",
    sort_order: str = "asc",
    page: int = 1,
    db: AsyncSession = Depends(get_db),
):
    status_list = status.split(",") if status else ["posted", "forecasted"]
    agency_list = agency.split(",") if agency else None
    category_list = category.split(",") if category else None

    results = await search_service.search(
        session=db,
        query=q,
        status=status_list,
        agency_codes=agency_list,
        category_codes=category_list,
        sort_by=sort_by,
        sort_order=sort_order,
        page=page,
    )
    facets = await search_service.get_facets(db, status_list)

    context = {
        "request": request,
        "opportunities": results["opportunities"],
        "total": results["total"],
        "page": results["page"],
        "total_pages": results["total_pages"],
        "facets": facets,
        "query": q or "",
        "status": status or "posted,forecasted",
        "agency": agency or "",
        "category": category or "",
        "sort_by": sort_by,
        "sort_order": sort_order,
        "today": date.today(),
    }

    # If HTMX request, return partial
    if request.headers.get("HX-Request"):
        return templates.TemplateResponse("partials/search_results.html", context)

    return templates.TemplateResponse("search.html", context)


@router.get("/opportunity/{opp_id}", response_class=HTMLResponse)
async def opportunity_detail(request: Request, opp_id: int, db: AsyncSession = Depends(get_db)):
    stmt = select(Opportunity).where(Opportunity.opportunity_id == opp_id)
    result = await db.execute(stmt)
    opp = result.scalar_one_or_none()

    if not opp:
        return templates.TemplateResponse("404.html", {"request": request}, status_code=404)

    return templates.TemplateResponse("opportunity.html", {
        "request": request,
        "opp": opp,
    })

import logging

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import Opportunity
from app.services.match_service import match_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/matches", tags=["matches"])
templates = Jinja2Templates(directory="app/templates")


@router.get("/opportunity/{opp_id}/researchers")
async def matching_researchers(
    request: Request,
    opp_id: int,
    limit: int = Query(default=10, le=50),
    db: AsyncSession = Depends(get_db),
):
    """Get top matching researchers for an opportunity (by grants.gov opportunity_id)."""
    stmt = select(Opportunity.id).where(Opportunity.opportunity_id == opp_id)
    result = await db.execute(stmt)
    row = result.first()
    if not row:
        if request.headers.get("HX-Request"):
            return HTMLResponse("")
        return JSONResponse(status_code=404, content={"error": "Opportunity not found"})

    matches = await match_service.get_matches_for_opportunity(db, row[0], limit=limit)

    # Return HTML partial for HTMX requests
    if request.headers.get("HX-Request"):
        return templates.TemplateResponse("partials/opportunity_matches.html", {
            "request": request,
            "matches": matches,
        })

    return {"matches": matches}


@router.get("/researcher/{researcher_id}/opportunities")
async def matching_opportunities(
    request: Request,
    researcher_id: int,
    limit: int = Query(default=20, le=100),
    db: AsyncSession = Depends(get_db),
):
    """Get top matching opportunities for a researcher."""
    matches = await match_service.get_matches_for_researcher(db, researcher_id, limit=limit)

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse("partials/researcher_matches.html", {
            "request": request,
            "matches": matches,
        })

    return {"matches": matches}

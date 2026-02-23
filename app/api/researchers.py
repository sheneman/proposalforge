import logging
from typing import Any

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.researcher import (
    Researcher, Publication, ResearcherPublication, ResearcherOpportunityMatch,
)
from app.models import Opportunity
from app.services.researcher_search_service import researcher_search_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/researchers", tags=["researchers"])


@router.get("")
async def search_researchers(
    q: str | None = None,
    department: str | None = None,
    keyword: str | None = None,
    status: str | None = None,
    has_summary: bool | None = None,
    sort_by: str = "name",
    sort_order: str = "asc",
    page: int = 1,
    per_page: int = 25,
    db: AsyncSession = Depends(get_db),
):
    results = await researcher_search_service.search(
        session=db,
        query=q,
        department=department,
        keyword=keyword,
        status=status,
        has_summary=has_summary,
        sort_by=sort_by,
        sort_order=sort_order,
        page=page,
        per_page=per_page,
    )

    return {
        "researchers": [
            _serialize_researcher(r)
            for r in results["researchers"]
        ],
        "total": results["total"],
        "page": results["page"],
        "per_page": results["per_page"],
        "total_pages": results["total_pages"],
    }


@router.get("/stats")
async def researcher_stats(db: AsyncSession = Depends(get_db)):
    return await researcher_search_service.get_stats(db)


@router.get("/facets")
async def researcher_facets(db: AsyncSession = Depends(get_db)):
    return await researcher_search_service.get_facets(db)


@router.get("/{researcher_id}")
async def get_researcher(researcher_id: int, db: AsyncSession = Depends(get_db)):
    detail = await researcher_search_service.get_researcher_detail(db, researcher_id)
    if not detail:
        return JSONResponse(status_code=404, content={"error": "Researcher not found"})

    return {
        "researcher": _serialize_researcher(detail["researcher"]),
        "publications": [_serialize_publication(p) for p in detail["publications"]],
    }


@router.get("/{researcher_id}/matches")
async def get_researcher_matches(
    researcher_id: int,
    limit: int = Query(default=20, le=100),
    db: AsyncSession = Depends(get_db),
):
    stmt = (
        select(ResearcherOpportunityMatch, Opportunity)
        .join(Opportunity, Opportunity.id == ResearcherOpportunityMatch.opportunity_id)
        .where(ResearcherOpportunityMatch.researcher_id == researcher_id)
        .order_by(ResearcherOpportunityMatch.score.desc())
        .limit(limit)
    )
    result = await db.execute(stmt)
    rows = result.all()

    matches = []
    for match, opp in rows:
        matches.append({
            "opportunity_id": opp.opportunity_id,
            "title": opp.title,
            "agency_code": opp.agency_code,
            "status": opp.status,
            "close_date": opp.close_date.isoformat() if opp.close_date else None,
            "award_ceiling": float(opp.award_ceiling) if opp.award_ceiling else None,
            "score": round(match.score, 3),
            "keyword_score": round(match.keyword_score, 3),
            "text_score": round(match.text_score, 3),
            "agency_score": round(match.agency_score, 3),
        })

    return {"matches": matches}


@router.get("/{researcher_id}/publications")
async def get_researcher_publications(
    researcher_id: int,
    page: int = 1,
    per_page: int = 25,
    db: AsyncSession = Depends(get_db),
):
    count_stmt = (
        select(func.count(ResearcherPublication.id))
        .where(ResearcherPublication.researcher_id == researcher_id)
    )
    total = (await db.execute(count_stmt)).scalar() or 0

    offset = (page - 1) * per_page
    stmt = (
        select(Publication)
        .join(ResearcherPublication, ResearcherPublication.publication_id == Publication.id)
        .where(ResearcherPublication.researcher_id == researcher_id)
        .order_by(Publication.publication_date.desc())
        .offset(offset)
        .limit(per_page)
    )
    result = await db.execute(stmt)
    pubs = result.scalars().all()

    return {
        "publications": [_serialize_publication(p) for p in pubs],
        "total": total,
        "page": page,
        "total_pages": (total + per_page - 1) // per_page if total else 0,
    }


def _serialize_researcher(r: Researcher) -> dict[str, Any]:
    return {
        "id": r.id,
        "collabnet_id": r.collabnet_id,
        "primary_id": r.primary_id,
        "first_name": r.first_name,
        "last_name": r.last_name,
        "full_name": r.full_name,
        "email": r.email,
        "photo_url": r.photo_url,
        "profile_url": r.profile_url,
        "position_title": r.position_title,
        "position_code": r.position_code,
        "status": r.status,
        "ai_summary": r.ai_summary,
        "keyword_text": r.keyword_text,
        "keywords": [kw.keyword for kw in r.keywords] if r.keywords else [],
        "affiliations": [
            {"name": a.organization_name, "code": a.organization_code, "is_current": a.is_current}
            for a in r.affiliations
        ] if r.affiliations else [],
        "education": [
            {"institution": e.institution, "degree": e.degree, "field": e.field_of_study}
            for e in r.education
        ] if r.education else [],
    }


def _serialize_publication(p: Publication) -> dict[str, Any]:
    return {
        "id": p.id,
        "title": p.title,
        "abstract": p.abstract[:500] if p.abstract else None,
        "keywords": p.keywords,
        "doi": p.doi,
        "uri": p.uri,
        "resource_type": p.resource_type,
        "publication_date": p.publication_date,
        "publication_info": p.publication_info,
        "affiliation": p.affiliation,
    }

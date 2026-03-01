from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import Opportunity, OpportunityDocument

router = APIRouter(prefix="/api/opportunities", tags=["opportunities"])
templates = Jinja2Templates(directory="app/templates")


@router.get("")
async def list_opportunities(
    status: str | None = None,
    agency_code: str | None = None,
    page: int = 1,
    per_page: int = 25,
    db: AsyncSession = Depends(get_db),
):
    stmt = select(Opportunity)

    if status:
        stmt = stmt.where(Opportunity.status == status)
    if agency_code:
        stmt = stmt.where(Opportunity.agency_code == agency_code)

    stmt = stmt.order_by(Opportunity.close_date.asc().nullslast())
    stmt = stmt.offset((page - 1) * per_page).limit(per_page)

    result = await db.execute(stmt)
    opportunities = result.scalars().unique().all()

    return {
        "opportunities": [_serialize_opp(o) for o in opportunities],
        "page": page,
        "per_page": per_page,
    }


@router.get("/{opp_id}")
async def get_opportunity(opp_id: int, db: AsyncSession = Depends(get_db)):
    stmt = select(Opportunity).where(Opportunity.opportunity_id == opp_id)
    result = await db.execute(stmt)
    opp = result.scalar_one_or_none()

    if not opp:
        raise HTTPException(status_code=404, detail="Opportunity not found")

    return _serialize_opp_detail(opp)


@router.get("/{opp_id}/documents")
async def get_opportunity_documents(
    opp_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    # Resolve Grants.gov opportunity_id -> internal id
    stmt = select(Opportunity.id).where(Opportunity.opportunity_id == opp_id)
    result = await db.execute(stmt)
    internal_id = result.scalar_one_or_none()

    if internal_id is None:
        raise HTTPException(status_code=404, detail="Opportunity not found")

    stmt = (
        select(OpportunityDocument)
        .where(OpportunityDocument.opportunity_id == internal_id)
        .order_by(OpportunityDocument.doc_category, OpportunityDocument.file_name)
    )
    result = await db.execute(stmt)
    documents = result.scalars().all()

    # HTMX request -> return partial
    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(
            "partials/opportunity_documents.html",
            {"request": request, "documents": documents},
        )

    # Plain API request -> return JSON
    return {
        "documents": [
            {
                "id": d.id,
                "file_name": d.file_name,
                "file_description": d.file_description,
                "folder_name": d.folder_name,
                "doc_category": d.doc_category,
                "file_size": d.file_size,
                "mime_type": d.mime_type,
                "download_status": d.download_status,
                "ocr_status": d.ocr_status,
                "classify_status": d.classify_status,
                "embed_status": d.embed_status,
                "extracted_text_length": d.extracted_text_length,
                "chunk_count": d.chunk_count,
                "error_message": d.error_message,
            }
            for d in documents
        ]
    }


def _serialize_opp(opp: Opportunity) -> dict:
    return {
        "opportunity_id": opp.opportunity_id,
        "opportunity_number": opp.opportunity_number,
        "title": opp.title,
        "status": opp.status,
        "agency_code": opp.agency_code,
        "agency_name": opp.agency.name if opp.agency else None,
        "posting_date": opp.posting_date.isoformat() if opp.posting_date else None,
        "close_date": opp.close_date.isoformat() if opp.close_date else None,
        "close_date_description": opp.close_date_description,
        "award_ceiling": float(opp.award_ceiling) if opp.award_ceiling else None,
        "award_floor": float(opp.award_floor) if opp.award_floor else None,
        "category": opp.category,
        "funding_instrument_description": opp.funding_instrument_description,
        "is_team_based": opp.is_team_based,
        "is_multi_institution": opp.is_multi_institution,
        "is_multi_disciplinary": opp.is_multi_disciplinary,
        "grants_gov_url": opp.grants_gov_url,
    }


def _serialize_opp_detail(opp: Opportunity) -> dict:
    base = _serialize_opp(opp)
    base.update({
        "estimated_total_funding": float(opp.estimated_total_funding) if opp.estimated_total_funding else None,
        "expected_number_of_awards": opp.expected_number_of_awards,
        "cost_sharing": opp.cost_sharing,
        "synopsis_description": opp.synopsis_description,
        "contact_name": opp.contact_name,
        "contact_email": opp.contact_email,
        "contact_phone": opp.contact_phone,
        "is_multi_jurisdiction": opp.is_multi_jurisdiction,
        "category_explanation": opp.category_explanation,
        "archive_date": opp.archive_date.isoformat() if opp.archive_date else None,
        "applicant_types": [
            {"code": at.type_code, "name": at.type_name}
            for at in opp.applicant_types
        ],
        "funding_instruments": [
            {"code": fi.instrument_code, "name": fi.instrument_name}
            for fi in opp.funding_instruments
        ],
        "funding_categories": [
            {"code": fc.category_code, "name": fc.category_name}
            for fc in opp.funding_categories
        ],
        "alns": [
            {"number": a.aln_number, "title": a.program_title}
            for a in opp.alns
        ],
    })
    return base

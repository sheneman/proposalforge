import os

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
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
            {"request": request, "documents": documents, "opp_id": opp_id},
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


async def _resolve_document(
    opp_id: int, doc_id: int, db: AsyncSession
) -> tuple[OpportunityDocument, str]:
    """Validate and resolve a document, returning (doc, resolved_path)."""
    stmt = select(Opportunity.id).where(Opportunity.opportunity_id == opp_id)
    result = await db.execute(stmt)
    internal_id = result.scalar_one_or_none()
    if internal_id is None:
        raise HTTPException(status_code=404, detail="Opportunity not found")

    stmt = select(OpportunityDocument).where(
        OpportunityDocument.id == doc_id,
        OpportunityDocument.opportunity_id == internal_id,
    )
    result = await db.execute(stmt)
    doc = result.scalar_one_or_none()
    if doc is None:
        raise HTTPException(status_code=404, detail="Document not found")
    if doc.download_status != "downloaded" or not doc.local_path:
        raise HTTPException(status_code=404, detail="Document file not available")

    resolved = os.path.realpath(doc.local_path)
    storage_root = os.path.realpath(settings.DOCUMENT_STORAGE_PATH)
    if not resolved.startswith(storage_root + os.sep):
        raise HTTPException(status_code=403, detail="Access denied")
    if not os.path.isfile(resolved):
        raise HTTPException(status_code=404, detail="File not found on disk")

    return doc, resolved


@router.get("/{opp_id}/documents/{doc_id}/download")
async def download_document(
    opp_id: int,
    doc_id: int,
    inline: int = 0,
    db: AsyncSession = Depends(get_db),
):
    doc, resolved = await _resolve_document(opp_id, doc_id, db)
    media_type = doc.mime_type or "application/octet-stream"

    headers = {}
    if inline:
        headers["Content-Disposition"] = f'inline; filename="{doc.file_name}"'
        return FileResponse(
            path=resolved,
            media_type=media_type,
            headers=headers,
        )

    return FileResponse(
        path=resolved,
        filename=doc.file_name,
        media_type=media_type,
    )


@router.get("/{opp_id}/documents/{doc_id}/preview")
async def preview_document(
    opp_id: int,
    doc_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    doc, resolved = await _resolve_document(opp_id, doc_id, db)

    is_pdf = (
        (doc.mime_type or "").lower() == "application/pdf"
        or doc.file_name.lower().endswith(".pdf")
    )
    download_url = f"/api/opportunities/{opp_id}/documents/{doc_id}/download"

    text_content = None
    if not is_pdf:
        text_path = resolved + ".txt"
        if os.path.isfile(text_path):
            with open(text_path, "r", encoding="utf-8", errors="replace") as f:
                text_content = f.read(500_000)

    return templates.TemplateResponse(
        "partials/document_preview.html",
        {
            "request": request,
            "doc": doc,
            "is_pdf": is_pdf,
            "download_url": download_url,
            "text_content": text_content,
        },
    )


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

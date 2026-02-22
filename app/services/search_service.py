import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import select, func, text, case, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Opportunity, Agency, OpportunityFundingCategory
from app.services.cache_service import cache_service, FACET_TTL, AGENCY_LIST_TTL, STATS_TTL

logger = logging.getLogger(__name__)


class SearchService:

    async def search(
        self,
        session: AsyncSession,
        query: str | None = None,
        status: list[str] | None = None,
        agency_codes: list[str] | None = None,
        category_codes: list[str] | None = None,
        close_date_start: date | None = None,
        close_date_end: date | None = None,
        award_ceiling_min: float | None = None,
        award_ceiling_max: float | None = None,
        is_team_based: bool | None = None,
        is_multi_institution: bool | None = None,
        is_multi_disciplinary: bool | None = None,
        sort_by: str = "close_date",
        sort_order: str = "asc",
        page: int = 1,
        per_page: int = 25,
    ) -> dict[str, Any]:
        # Build base query
        stmt = select(Opportunity)
        count_stmt = select(func.count(Opportunity.id))

        conditions = []

        # Full-text search
        if query and query.strip():
            ft = text(
                "MATCH(opportunities.title, opportunities.synopsis_description) "
                "AGAINST(:query IN BOOLEAN MODE)"
            )
            conditions.append(ft)

        # Status filter
        if status:
            conditions.append(Opportunity.status.in_(status))

        # Agency filter
        if agency_codes:
            conditions.append(Opportunity.agency_code.in_(agency_codes))

        # Category filter (via join)
        if category_codes:
            stmt = stmt.join(
                OpportunityFundingCategory,
                OpportunityFundingCategory.opportunity_id == Opportunity.id,
            )
            count_stmt = count_stmt.join(
                OpportunityFundingCategory,
                OpportunityFundingCategory.opportunity_id == Opportunity.id,
            )
            conditions.append(OpportunityFundingCategory.category_code.in_(category_codes))

        # Date range
        if close_date_start:
            conditions.append(Opportunity.close_date >= close_date_start)
        if close_date_end:
            conditions.append(Opportunity.close_date <= close_date_end)

        # Award ceiling range
        if award_ceiling_min is not None:
            conditions.append(Opportunity.award_ceiling >= award_ceiling_min)
        if award_ceiling_max is not None:
            conditions.append(Opportunity.award_ceiling <= award_ceiling_max)

        # Boolean flags
        if is_team_based is not None:
            conditions.append(Opportunity.is_team_based == is_team_based)
        if is_multi_institution is not None:
            conditions.append(Opportunity.is_multi_institution == is_multi_institution)
        if is_multi_disciplinary is not None:
            conditions.append(Opportunity.is_multi_disciplinary == is_multi_disciplinary)

        if conditions:
            where_clause = and_(*conditions)
            stmt = stmt.where(where_clause)
            count_stmt = count_stmt.where(where_clause)

        # Bind the query parameter if full-text search
        params = {}
        if query and query.strip():
            params["query"] = query.strip()

        # Get total count
        count_result = await session.execute(count_stmt, params)
        total = count_result.scalar()

        # Sorting
        sort_column = {
            "close_date": Opportunity.close_date,
            "posting_date": Opportunity.posting_date,
            "award_ceiling": Opportunity.award_ceiling,
            "title": Opportunity.title,
        }.get(sort_by, Opportunity.close_date)

        # MariaDB doesn't support NULLS LAST, use ISNULL() trick instead
        if sort_order == "desc":
            stmt = stmt.order_by(func.isnull(sort_column), sort_column.desc())
        else:
            stmt = stmt.order_by(func.isnull(sort_column), sort_column.asc())

        # Pagination
        offset = (page - 1) * per_page
        stmt = stmt.offset(offset).limit(per_page)

        result = await session.execute(stmt, params)
        opportunities = result.scalars().unique().all()

        return {
            "opportunities": opportunities,
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": (total + per_page - 1) // per_page if total else 0,
        }

    async def get_facets(self, session: AsyncSession, status: list[str] | None = None) -> dict[str, Any]:
        cache_key = f"pf:facets:{','.join(status) if status else 'all'}"
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        base_condition = Opportunity.status.in_(status) if status else True

        # Agency counts
        agency_stmt = (
            select(
                Opportunity.agency_code,
                Agency.name,
                func.count(Opportunity.id).label("count"),
            )
            .join(Agency, Agency.code == Opportunity.agency_code, isouter=True)
            .where(base_condition)
            .group_by(Opportunity.agency_code, Agency.name)
            .order_by(func.count(Opportunity.id).desc())
            .limit(50)
        )
        agency_result = await session.execute(agency_stmt)
        agency_counts = [
            {"code": row[0], "name": row[1] or row[0], "count": row[2]}
            for row in agency_result.all()
            if row[0]
        ]

        # Category counts
        cat_stmt = (
            select(
                OpportunityFundingCategory.category_code,
                OpportunityFundingCategory.category_name,
                func.count(func.distinct(OpportunityFundingCategory.opportunity_id)).label("count"),
            )
            .join(Opportunity, Opportunity.id == OpportunityFundingCategory.opportunity_id)
            .where(base_condition)
            .group_by(OpportunityFundingCategory.category_code, OpportunityFundingCategory.category_name)
            .order_by(func.count(func.distinct(OpportunityFundingCategory.opportunity_id)).desc())
        )
        cat_result = await session.execute(cat_stmt)
        category_counts = [
            {"code": row[0], "name": row[1], "count": row[2]}
            for row in cat_result.all()
        ]

        # Deadline buckets
        today = date.today()
        week_end = today + timedelta(days=7)
        month_end = today + timedelta(days=30)
        quarter_end = today + timedelta(days=90)

        deadline_stmt = select(
            func.sum(case((and_(Opportunity.close_date >= today, Opportunity.close_date <= week_end), 1), else_=0)).label("this_week"),
            func.sum(case((and_(Opportunity.close_date >= today, Opportunity.close_date <= month_end), 1), else_=0)).label("this_month"),
            func.sum(case((and_(Opportunity.close_date >= today, Opportunity.close_date <= quarter_end), 1), else_=0)).label("this_quarter"),
            func.sum(case((Opportunity.close_date.is_(None), 1), else_=0)).label("no_deadline"),
        ).where(base_condition)

        deadline_result = await session.execute(deadline_stmt)
        dr = deadline_result.one()
        deadline_buckets = {
            "this_week": dr[0] or 0,
            "this_month": dr[1] or 0,
            "this_quarter": dr[2] or 0,
            "no_deadline": dr[3] or 0,
        }

        # Award ceiling ranges
        ceiling_stmt = select(
            func.sum(case((Opportunity.award_ceiling < 100000, 1), else_=0)).label("under_100k"),
            func.sum(case((and_(Opportunity.award_ceiling >= 100000, Opportunity.award_ceiling < 500000), 1), else_=0)).label("100k_500k"),
            func.sum(case((and_(Opportunity.award_ceiling >= 500000, Opportunity.award_ceiling < 1000000), 1), else_=0)).label("500k_1m"),
            func.sum(case((Opportunity.award_ceiling >= 1000000, 1), else_=0)).label("over_1m"),
        ).where(base_condition)

        ceiling_result = await session.execute(ceiling_stmt)
        cr = ceiling_result.one()
        ceiling_ranges = {
            "under_100k": cr[0] or 0,
            "100k_500k": cr[1] or 0,
            "500k_1m": cr[2] or 0,
            "over_1m": cr[3] or 0,
        }

        facets = {
            "agencies": agency_counts,
            "categories": category_counts,
            "deadlines": deadline_buckets,
            "ceilings": ceiling_ranges,
        }

        await cache_service.set(cache_key, facets, FACET_TTL)
        return facets

    async def get_stats(self, session: AsyncSession) -> dict[str, Any]:
        cached = await cache_service.get("pf:stats")
        if cached:
            return cached

        today = date.today()
        week_end = today + timedelta(days=7)
        month_end = today + timedelta(days=30)
        week_ago = today - timedelta(days=7)

        stats_stmt = select(
            func.count(Opportunity.id).label("total_open"),
            func.sum(case((and_(Opportunity.close_date >= today, Opportunity.close_date <= week_end), 1), else_=0)).label("closing_this_week"),
            func.sum(case((and_(Opportunity.close_date >= today, Opportunity.close_date <= month_end), 1), else_=0)).label("closing_this_month"),
            func.sum(case((Opportunity.posting_date >= week_ago, 1), else_=0)).label("new_this_week"),
        ).where(Opportunity.status.in_(["posted", "forecasted"]))

        result = await session.execute(stats_stmt)
        row = result.one()

        # Top agencies
        agency_stmt = (
            select(
                Agency.name,
                func.count(Opportunity.id).label("count"),
            )
            .join(Agency, Agency.code == Opportunity.agency_code)
            .where(Opportunity.status.in_(["posted", "forecasted"]))
            .group_by(Agency.name)
            .order_by(func.count(Opportunity.id).desc())
            .limit(15)
        )
        agency_result = await session.execute(agency_stmt)
        top_agencies = [{"name": r[0], "count": r[1]} for r in agency_result.all()]

        # Top categories
        cat_stmt = (
            select(
                OpportunityFundingCategory.category_name,
                func.count(func.distinct(OpportunityFundingCategory.opportunity_id)).label("count"),
            )
            .join(Opportunity, Opportunity.id == OpportunityFundingCategory.opportunity_id)
            .where(Opportunity.status.in_(["posted", "forecasted"]))
            .group_by(OpportunityFundingCategory.category_name)
            .order_by(func.count(func.distinct(OpportunityFundingCategory.opportunity_id)).desc())
            .limit(10)
        )
        cat_result = await session.execute(cat_stmt)
        top_categories = [{"name": r[0], "count": r[1]} for r in cat_result.all()]

        # Closed/archived summary stats
        closed_stmt = select(
            func.count(Opportunity.id).label("total"),
            func.sum(case((Opportunity.award_ceiling.isnot(None), Opportunity.award_ceiling), else_=0)).label("total_funding"),
            func.avg(case((Opportunity.award_ceiling.isnot(None), Opportunity.award_ceiling), else_=None)).label("avg_ceiling"),
        ).where(Opportunity.status == "closed")
        closed_result = await session.execute(closed_stmt)
        closed_row = closed_result.one()

        archived_stmt = select(
            func.count(Opportunity.id).label("total"),
            func.sum(case((Opportunity.award_ceiling.isnot(None), Opportunity.award_ceiling), else_=0)).label("total_funding"),
            func.avg(case((Opportunity.award_ceiling.isnot(None), Opportunity.award_ceiling), else_=None)).label("avg_ceiling"),
        ).where(Opportunity.status == "archived")
        archived_result = await session.execute(archived_stmt)
        archived_row = archived_result.one()

        # Top agencies for closed
        closed_agency_stmt = (
            select(
                Agency.name,
                func.count(Opportunity.id).label("count"),
            )
            .join(Agency, Agency.code == Opportunity.agency_code)
            .where(Opportunity.status == "closed")
            .group_by(Agency.name)
            .order_by(func.count(Opportunity.id).desc())
            .limit(10)
        )
        closed_agency_result = await session.execute(closed_agency_stmt)
        closed_top_agencies = [{"name": r[0], "count": r[1]} for r in closed_agency_result.all()]

        # Top agencies for archived
        archived_agency_stmt = (
            select(
                Agency.name,
                func.count(Opportunity.id).label("count"),
            )
            .join(Agency, Agency.code == Opportunity.agency_code)
            .where(Opportunity.status == "archived")
            .group_by(Agency.name)
            .order_by(func.count(Opportunity.id).desc())
            .limit(10)
        )
        archived_agency_result = await session.execute(archived_agency_stmt)
        archived_top_agencies = [{"name": r[0], "count": r[1]} for r in archived_agency_result.all()]

        stats = {
            "total_open": row[0] or 0,
            "closing_this_week": row[1] or 0,
            "closing_this_month": row[2] or 0,
            "new_this_week": row[3] or 0,
            "top_agencies": top_agencies,
            "top_categories": top_categories,
            "closed": {
                "total": closed_row[0] or 0,
                "total_funding": float(closed_row[1] or 0),
                "avg_ceiling": float(closed_row[2] or 0),
                "top_agencies": closed_top_agencies,
            },
            "archived": {
                "total": archived_row[0] or 0,
                "total_funding": float(archived_row[1] or 0),
                "avg_ceiling": float(archived_row[2] or 0),
                "top_agencies": archived_top_agencies,
            },
        }

        await cache_service.set("pf:stats", stats, STATS_TTL)
        return stats

    async def get_agencies(self, session: AsyncSession) -> list[dict]:
        cached = await cache_service.get("pf:agencies")
        if cached:
            return cached

        stmt = (
            select(
                Agency.code,
                Agency.name,
                func.count(Opportunity.id).label("count"),
            )
            .join(Opportunity, Opportunity.agency_code == Agency.code, isouter=True)
            .group_by(Agency.code, Agency.name)
            .order_by(Agency.name)
        )
        result = await session.execute(stmt)
        agencies = [{"code": r[0], "name": r[1], "count": r[2]} for r in result.all()]

        await cache_service.set("pf:agencies", agencies, AGENCY_LIST_TTL)
        return agencies

    async def get_categories(self, session: AsyncSession) -> list[dict]:
        cached = await cache_service.get("pf:categories")
        if cached:
            return cached

        stmt = (
            select(
                OpportunityFundingCategory.category_code,
                OpportunityFundingCategory.category_name,
                func.count(func.distinct(OpportunityFundingCategory.opportunity_id)).label("count"),
            )
            .group_by(OpportunityFundingCategory.category_code, OpportunityFundingCategory.category_name)
            .order_by(func.count(func.distinct(OpportunityFundingCategory.opportunity_id)).desc())
        )
        result = await session.execute(stmt)
        categories = [{"code": r[0], "name": r[1], "count": r[2]} for r in result.all()]

        await cache_service.set("pf:categories", categories, AGENCY_LIST_TTL)
        return categories


search_service = SearchService()

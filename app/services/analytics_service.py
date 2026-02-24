import logging
from datetime import date
from decimal import Decimal
from typing import Any

from sqlalchemy import select, func, text, case, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    Opportunity, Agency, OpportunityFundingCategory,
    Researcher, ResearcherKeyword, ResearcherAffiliation,
    Publication, ResearcherPublication,
    Grant, ResearcherGrant,
    Project, ResearcherProject,
    Activity, ResearcherActivity,
    ResearcherOpportunityMatch,
)
from app.services.cache_service import cache_service

logger = logging.getLogger(__name__)

ANALYTICS_TTL = 300  # 5 minutes


def _serialize(val):
    """Convert Decimal/date to JSON-safe types."""
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, date):
        return val.isoformat()
    return val


class AnalyticsService:

    def _build_conditions(
        self,
        status: list[str] | None = None,
        agency_codes: list[str] | None = None,
        category_codes: list[str] | None = None,
        date_start: date | None = None,
        date_end: date | None = None,
    ) -> list:
        """Build WHERE conditions from common filter params."""
        conditions = []
        if status:
            conditions.append(Opportunity.status.in_(status))
        if agency_codes:
            conditions.append(Opportunity.agency_code.in_(agency_codes))
        if date_start:
            conditions.append(Opportunity.posting_date >= date_start)
        if date_end:
            conditions.append(Opportunity.posting_date <= date_end)
        return conditions

    def _needs_category_join(self, category_codes: list[str] | None) -> bool:
        return bool(category_codes)

    def _date_format(self, granularity: str) -> str:
        """Return MariaDB DATE_FORMAT pattern for granularity."""
        if granularity == "year":
            return "%Y"
        if granularity == "quarter":
            return "%Y-Q"
        if granularity == "week":
            return "%x-W%v"
        return "%Y-%m"  # month default

    def _date_trunc_expr(self, col, granularity: str):
        """Return a SQL expression for grouping by time period."""
        if granularity == "year":
            return func.date_format(col, "%Y")
        if granularity == "quarter":
            return func.concat(
                func.year(col), "-Q", func.quarter(col)
            )
        if granularity == "week":
            return func.date_format(col, "%x-W%v")
        return func.date_format(col, "%Y-%m")

    def _cache_key(self, prefix: str, **kwargs) -> str:
        parts = [f"pf:analytics:{prefix}"]
        for k, v in sorted(kwargs.items()):
            if v is not None:
                parts.append(f"{k}={v}")
        return ":".join(parts)

    # --- KPIs ---

    async def summary_kpis(
        self,
        session: AsyncSession,
        status: list[str] | None = None,
        agency_codes: list[str] | None = None,
        category_codes: list[str] | None = None,
        date_start: date | None = None,
        date_end: date | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "kpis", status=status, agency=agency_codes,
            category=category_codes, ds=date_start, de=date_end,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        conditions = self._build_conditions(status, agency_codes, category_codes, date_start, date_end)

        stmt = select(
            func.count(Opportunity.id).label("total_opportunities"),
            func.sum(Opportunity.award_ceiling).label("total_funding"),
            func.avg(Opportunity.award_ceiling).label("avg_ceiling"),
            func.count(func.distinct(Opportunity.agency_code)).label("unique_agencies"),
        )

        if category_codes:
            stmt = stmt.join(
                OpportunityFundingCategory,
                OpportunityFundingCategory.opportunity_id == Opportunity.id,
            )
            conditions.append(OpportunityFundingCategory.category_code.in_(category_codes))

        if conditions:
            stmt = stmt.where(and_(*conditions))

        result = await session.execute(stmt)
        row = result.one()

        # Unique categories count (separate query)
        cat_stmt = (
            select(func.count(func.distinct(OpportunityFundingCategory.category_code)))
            .join(Opportunity, Opportunity.id == OpportunityFundingCategory.opportunity_id)
        )
        cat_conditions = self._build_conditions(status, agency_codes, None, date_start, date_end)
        if category_codes:
            cat_conditions.append(OpportunityFundingCategory.category_code.in_(category_codes))
        if cat_conditions:
            cat_stmt = cat_stmt.where(and_(*cat_conditions))
        cat_result = await session.execute(cat_stmt)
        unique_cats = cat_result.scalar() or 0

        kpis = {
            "total_opportunities": row[0] or 0,
            "total_funding": float(row[1] or 0),
            "avg_ceiling": float(row[2] or 0),
            "unique_agencies": row[3] or 0,
            "unique_categories": unique_cats,
        }

        await cache_service.set(cache_key, kpis, ANALYTICS_TTL)
        return kpis

    # --- Timeline Tab ---

    async def opportunities_over_time(
        self,
        session: AsyncSession,
        granularity: str = "month",
        status: list[str] | None = None,
        agency_codes: list[str] | None = None,
        category_codes: list[str] | None = None,
        date_start: date | None = None,
        date_end: date | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "opp_time", gran=granularity, status=status, agency=agency_codes,
            category=category_codes, ds=date_start, de=date_end,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        period = self._date_trunc_expr(Opportunity.posting_date, granularity)
        conditions = self._build_conditions(status, agency_codes, category_codes, date_start, date_end)
        conditions.append(Opportunity.posting_date.isnot(None))

        stmt = (
            select(
                period.label("period"),
                Opportunity.status,
                func.count(Opportunity.id).label("count"),
            )
        )

        if category_codes:
            stmt = stmt.join(
                OpportunityFundingCategory,
                OpportunityFundingCategory.opportunity_id == Opportunity.id,
            )
            conditions.append(OpportunityFundingCategory.category_code.in_(category_codes))

        stmt = stmt.where(and_(*conditions)).group_by(
            period, Opportunity.status
        ).order_by(period)

        result = await session.execute(stmt)
        rows = result.all()

        # Pivot into Chart.js format: one dataset per status
        periods = sorted(set(r[0] for r in rows))
        statuses = sorted(set(r[1] for r in rows))
        data_map = {}
        for r in rows:
            data_map[(r[0], r[1])] = r[2]

        status_colors = {
            "posted": "#2c5282",
            "forecasted": "#d4a843",
            "closed": "#6b9bd2",
            "archived": "#a8c8e8",
        }

        datasets = []
        for s in statuses:
            datasets.append({
                "label": s.capitalize(),
                "data": [int(data_map.get((p, s), 0)) for p in periods],
                "borderColor": status_colors.get(s, "#1a365d"),
                "backgroundColor": status_colors.get(s, "#1a365d") + "33",
                "fill": True,
                "tension": 0.3,
            })

        chart_data = {"labels": periods, "datasets": datasets}
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data

    async def close_dates_over_time(
        self,
        session: AsyncSession,
        granularity: str = "month",
        status: list[str] | None = None,
        agency_codes: list[str] | None = None,
        category_codes: list[str] | None = None,
        date_start: date | None = None,
        date_end: date | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "close_time", gran=granularity, status=status, agency=agency_codes,
            category=category_codes, ds=date_start, de=date_end,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        period = self._date_trunc_expr(Opportunity.close_date, granularity)
        conditions = self._build_conditions(status, agency_codes, category_codes, date_start, date_end)
        conditions.append(Opportunity.close_date.isnot(None))

        stmt = select(
            period.label("period"),
            func.count(Opportunity.id).label("count"),
        )

        if category_codes:
            stmt = stmt.join(
                OpportunityFundingCategory,
                OpportunityFundingCategory.opportunity_id == Opportunity.id,
            )
            conditions.append(OpportunityFundingCategory.category_code.in_(category_codes))

        stmt = stmt.where(and_(*conditions)).group_by(period).order_by(period)

        result = await session.execute(stmt)
        rows = result.all()

        chart_data = {
            "labels": [r[0] for r in rows],
            "datasets": [{
                "label": "Close Dates",
                "data": [int(r[1]) for r in rows],
                "backgroundColor": "#1a365d",
                "borderRadius": 4,
            }],
        }
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data

    # --- Funding Tab ---

    async def award_ceiling_distribution(
        self,
        session: AsyncSession,
        status: list[str] | None = None,
        agency_codes: list[str] | None = None,
        category_codes: list[str] | None = None,
        date_start: date | None = None,
        date_end: date | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "ceil_dist", status=status, agency=agency_codes,
            category=category_codes, ds=date_start, de=date_end,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        conditions = self._build_conditions(status, agency_codes, category_codes, date_start, date_end)
        conditions.append(Opportunity.award_ceiling.isnot(None))

        buckets = [
            ("< $50K", 0, 50000),
            ("$50K-100K", 50000, 100000),
            ("$100K-250K", 100000, 250000),
            ("$250K-500K", 250000, 500000),
            ("$500K-1M", 500000, 1000000),
            ("$1M-5M", 1000000, 5000000),
            ("$5M-10M", 5000000, 10000000),
            ("> $10M", 10000000, None),
        ]

        case_exprs = []
        for label, low, high in buckets:
            if high is None:
                case_exprs.append(
                    func.sum(case((Opportunity.award_ceiling >= low, 1), else_=0))
                )
            else:
                case_exprs.append(
                    func.sum(case(
                        (and_(Opportunity.award_ceiling >= low, Opportunity.award_ceiling < high), 1),
                        else_=0,
                    ))
                )

        stmt = select(*case_exprs)

        if category_codes:
            stmt = stmt.join(
                OpportunityFundingCategory,
                OpportunityFundingCategory.opportunity_id == Opportunity.id,
            )
            conditions.append(OpportunityFundingCategory.category_code.in_(category_codes))

        stmt = stmt.where(and_(*conditions)) if conditions else stmt

        result = await session.execute(stmt)
        row = result.one()

        colors = [
            "#c4dbf0", "#a8c8e8", "#8bb5e0", "#6b9bd2",
            "#4a80b8", "#3b6ba5", "#2c5282", "#1a365d",
        ]

        chart_data = {
            "labels": [b[0] for b in buckets],
            "datasets": [{
                "label": "Opportunities",
                "data": [int(row[i] or 0) for i in range(len(buckets))],
                "backgroundColor": colors,
                "borderRadius": 4,
            }],
        }
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data

    async def funding_by_agency(
        self,
        session: AsyncSession,
        top_n: int = 15,
        status: list[str] | None = None,
        agency_codes: list[str] | None = None,
        category_codes: list[str] | None = None,
        date_start: date | None = None,
        date_end: date | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "fund_agency", top=top_n, status=status, agency=agency_codes,
            category=category_codes, ds=date_start, de=date_end,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        conditions = self._build_conditions(status, agency_codes, category_codes, date_start, date_end)
        conditions.append(Opportunity.award_ceiling.isnot(None))

        stmt = (
            select(
                Agency.name,
                func.sum(Opportunity.award_ceiling).label("total"),
            )
            .join(Agency, Agency.code == Opportunity.agency_code)
        )

        if category_codes:
            stmt = stmt.join(
                OpportunityFundingCategory,
                OpportunityFundingCategory.opportunity_id == Opportunity.id,
            )
            conditions.append(OpportunityFundingCategory.category_code.in_(category_codes))

        stmt = (
            stmt.where(and_(*conditions))
            .group_by(Agency.name)
            .order_by(func.sum(Opportunity.award_ceiling).desc())
            .limit(top_n)
        )

        result = await session.execute(stmt)
        rows = result.all()

        chart_data = {
            "labels": [r[0] or "Unknown" for r in rows],
            "datasets": [{
                "label": "Total Award Ceiling ($)",
                "data": [float(r[1] or 0) for r in rows],
                "backgroundColor": "#2c5282",
                "borderRadius": 4,
            }],
        }
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data

    async def funding_by_category(
        self,
        session: AsyncSession,
        status: list[str] | None = None,
        agency_codes: list[str] | None = None,
        category_codes: list[str] | None = None,
        date_start: date | None = None,
        date_end: date | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "fund_cat", status=status, agency=agency_codes,
            category=category_codes, ds=date_start, de=date_end,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        conditions = self._build_conditions(status, agency_codes, None, date_start, date_end)
        conditions.append(Opportunity.award_ceiling.isnot(None))

        stmt = (
            select(
                OpportunityFundingCategory.category_name,
                func.sum(Opportunity.award_ceiling).label("total"),
            )
            .join(Opportunity, Opportunity.id == OpportunityFundingCategory.opportunity_id)
        )

        if category_codes:
            conditions.append(OpportunityFundingCategory.category_code.in_(category_codes))

        stmt = (
            stmt.where(and_(*conditions))
            .group_by(OpportunityFundingCategory.category_name)
            .order_by(func.sum(Opportunity.award_ceiling).desc())
            .limit(15)
        )

        result = await session.execute(stmt)
        rows = result.all()

        palette = [
            "#1a365d", "#2c5282", "#3b6ba5", "#4a80b8", "#6b9bd2",
            "#8bb5e0", "#a8c8e8", "#c4dbf0", "#d4a843", "#e8c97a",
            "#2d6a4f", "#52b788", "#b5838d", "#6d6875", "#e5989b",
        ]

        chart_data = {
            "labels": [r[0] or "Unknown" for r in rows],
            "datasets": [{
                "label": "Total Award Ceiling ($)",
                "data": [float(r[1] or 0) for r in rows],
                "backgroundColor": palette[:len(rows)],
                "borderRadius": 4,
            }],
        }
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data

    async def funding_trends(
        self,
        session: AsyncSession,
        granularity: str = "month",
        status: list[str] | None = None,
        agency_codes: list[str] | None = None,
        category_codes: list[str] | None = None,
        date_start: date | None = None,
        date_end: date | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "fund_trends", gran=granularity, status=status, agency=agency_codes,
            category=category_codes, ds=date_start, de=date_end,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        period = self._date_trunc_expr(Opportunity.posting_date, granularity)
        conditions = self._build_conditions(status, agency_codes, category_codes, date_start, date_end)
        conditions.append(Opportunity.posting_date.isnot(None))

        stmt = select(
            period.label("period"),
            func.sum(Opportunity.award_ceiling).label("total_ceiling"),
            func.avg(Opportunity.award_ceiling).label("avg_ceiling"),
            func.sum(Opportunity.estimated_total_funding).label("est_total"),
        )

        if category_codes:
            stmt = stmt.join(
                OpportunityFundingCategory,
                OpportunityFundingCategory.opportunity_id == Opportunity.id,
            )
            conditions.append(OpportunityFundingCategory.category_code.in_(category_codes))

        stmt = stmt.where(and_(*conditions)).group_by(period).order_by(period)

        result = await session.execute(stmt)
        rows = result.all()

        periods = [r[0] for r in rows]

        chart_data = {
            "labels": periods,
            "datasets": [
                {
                    "label": "Total Ceiling",
                    "data": [float(r[1] or 0) for r in rows],
                    "borderColor": "#1a365d",
                    "backgroundColor": "#1a365d33",
                    "fill": False,
                    "tension": 0.3,
                    "yAxisID": "y",
                },
                {
                    "label": "Avg Ceiling",
                    "data": [float(r[2] or 0) for r in rows],
                    "borderColor": "#d4a843",
                    "backgroundColor": "#d4a84333",
                    "fill": False,
                    "tension": 0.3,
                    "yAxisID": "y1",
                },
                {
                    "label": "Est. Total Funding",
                    "data": [float(r[3] or 0) for r in rows],
                    "borderColor": "#52b788",
                    "backgroundColor": "#52b78833",
                    "fill": False,
                    "tension": 0.3,
                    "borderDash": [5, 5],
                    "yAxisID": "y",
                },
            ],
        }
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data

    async def floor_vs_ceiling(
        self,
        session: AsyncSession,
        top_n: int = 20,
        status: list[str] | None = None,
        agency_codes: list[str] | None = None,
        category_codes: list[str] | None = None,
        date_start: date | None = None,
        date_end: date | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "floor_ceil", top=top_n, status=status, agency=agency_codes,
            category=category_codes, ds=date_start, de=date_end,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        conditions = self._build_conditions(status, agency_codes, category_codes, date_start, date_end)
        conditions.append(Opportunity.award_ceiling.isnot(None))

        stmt = (
            select(
                Agency.name,
                func.avg(Opportunity.award_floor).label("avg_floor"),
                func.avg(Opportunity.award_ceiling).label("avg_ceiling"),
            )
            .join(Agency, Agency.code == Opportunity.agency_code)
        )

        if category_codes:
            stmt = stmt.join(
                OpportunityFundingCategory,
                OpportunityFundingCategory.opportunity_id == Opportunity.id,
            )
            conditions.append(OpportunityFundingCategory.category_code.in_(category_codes))

        stmt = (
            stmt.where(and_(*conditions))
            .group_by(Agency.name)
            .order_by(func.avg(Opportunity.award_ceiling).desc())
            .limit(top_n)
        )

        result = await session.execute(stmt)
        rows = result.all()

        chart_data = {
            "labels": [r[0] or "Unknown" for r in rows],
            "datasets": [
                {
                    "label": "Avg Floor ($)",
                    "data": [float(r[1] or 0) for r in rows],
                    "backgroundColor": "#6b9bd2",
                    "borderRadius": 4,
                },
                {
                    "label": "Avg Ceiling ($)",
                    "data": [float(r[2] or 0) for r in rows],
                    "backgroundColor": "#1a365d",
                    "borderRadius": 4,
                },
            ],
        }
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data

    # --- Agency Tab ---

    async def agency_comparison(
        self,
        session: AsyncSession,
        top_n: int = 15,
        status: list[str] | None = None,
        agency_codes: list[str] | None = None,
        category_codes: list[str] | None = None,
        date_start: date | None = None,
        date_end: date | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "agency_comp", top=top_n, status=status, agency=agency_codes,
            category=category_codes, ds=date_start, de=date_end,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        conditions = self._build_conditions(None, agency_codes, category_codes, date_start, date_end)

        # Get top agencies by total count
        top_stmt = (
            select(Agency.name, func.count(Opportunity.id).label("cnt"))
            .join(Agency, Agency.code == Opportunity.agency_code)
        )
        if category_codes:
            top_stmt = top_stmt.join(
                OpportunityFundingCategory,
                OpportunityFundingCategory.opportunity_id == Opportunity.id,
            )
            conditions_for_top = list(conditions)
            conditions_for_top.append(OpportunityFundingCategory.category_code.in_(category_codes))
            if conditions_for_top:
                top_stmt = top_stmt.where(and_(*conditions_for_top))
        elif conditions:
            top_stmt = top_stmt.where(and_(*conditions))

        top_stmt = top_stmt.group_by(Agency.name).order_by(func.count(Opportunity.id).desc()).limit(top_n)
        top_result = await session.execute(top_stmt)
        top_agencies = [r[0] for r in top_result.all()]

        if not top_agencies:
            chart_data = {"labels": [], "datasets": []}
            await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
            return chart_data

        # Now get counts per status for those agencies
        conditions2 = self._build_conditions(None, agency_codes, category_codes, date_start, date_end)

        stmt = (
            select(
                Agency.name,
                Opportunity.status,
                func.count(Opportunity.id).label("count"),
            )
            .join(Agency, Agency.code == Opportunity.agency_code)
            .where(Agency.name.in_(top_agencies))
        )

        if category_codes:
            stmt = stmt.join(
                OpportunityFundingCategory,
                OpportunityFundingCategory.opportunity_id == Opportunity.id,
            )
            conditions2.append(OpportunityFundingCategory.category_code.in_(category_codes))

        if conditions2:
            stmt = stmt.where(and_(*conditions2))

        stmt = stmt.group_by(Agency.name, Opportunity.status)

        result = await session.execute(stmt)
        rows = result.all()

        data_map = {}
        for r in rows:
            data_map[(r[0], r[1])] = r[2]

        all_statuses = ["posted", "forecasted", "closed", "archived"]
        status_colors = {
            "posted": "#2c5282",
            "forecasted": "#d4a843",
            "closed": "#6b9bd2",
            "archived": "#a8c8e8",
        }

        # Filter to only statuses with data if status filter applied
        show_statuses = status if status else all_statuses

        datasets = []
        for s in show_statuses:
            if s in all_statuses:
                datasets.append({
                    "label": s.capitalize(),
                    "data": [int(data_map.get((a, s), 0)) for a in top_agencies],
                    "backgroundColor": status_colors.get(s, "#1a365d"),
                })

        chart_data = {"labels": top_agencies, "datasets": datasets}
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data

    async def agency_activity_over_time(
        self,
        session: AsyncSession,
        top_n: int = 8,
        granularity: str = "month",
        status: list[str] | None = None,
        agency_codes: list[str] | None = None,
        category_codes: list[str] | None = None,
        date_start: date | None = None,
        date_end: date | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "agency_activity", top=top_n, gran=granularity, status=status,
            agency=agency_codes, category=category_codes, ds=date_start, de=date_end,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        conditions = self._build_conditions(status, agency_codes, category_codes, date_start, date_end)
        conditions.append(Opportunity.posting_date.isnot(None))

        # Get top agencies
        top_stmt = (
            select(Agency.name, func.count(Opportunity.id).label("cnt"))
            .join(Agency, Agency.code == Opportunity.agency_code)
        )
        top_conditions = list(conditions)
        if category_codes:
            top_stmt = top_stmt.join(
                OpportunityFundingCategory,
                OpportunityFundingCategory.opportunity_id == Opportunity.id,
            )
            top_conditions.append(OpportunityFundingCategory.category_code.in_(category_codes))
        if top_conditions:
            top_stmt = top_stmt.where(and_(*top_conditions))
        top_stmt = top_stmt.group_by(Agency.name).order_by(func.count(Opportunity.id).desc()).limit(top_n)
        top_result = await session.execute(top_stmt)
        top_agencies = [r[0] for r in top_result.all()]

        if not top_agencies:
            chart_data = {"labels": [], "datasets": []}
            await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
            return chart_data

        period = self._date_trunc_expr(Opportunity.posting_date, granularity)

        stmt = (
            select(
                period.label("period"),
                Agency.name,
                func.count(Opportunity.id).label("count"),
            )
            .join(Agency, Agency.code == Opportunity.agency_code)
            .where(Agency.name.in_(top_agencies))
        )

        conds = list(conditions)
        if category_codes:
            stmt = stmt.join(
                OpportunityFundingCategory,
                OpportunityFundingCategory.opportunity_id == Opportunity.id,
            )
            conds.append(OpportunityFundingCategory.category_code.in_(category_codes))
        if conds:
            stmt = stmt.where(and_(*conds))

        stmt = stmt.group_by(period, Agency.name).order_by(period)

        result = await session.execute(stmt)
        rows = result.all()

        periods = sorted(set(r[0] for r in rows))
        data_map = {}
        for r in rows:
            data_map[(r[0], r[1])] = r[2]

        colors = [
            "#1a365d", "#d4a843", "#2d6a4f", "#b5838d",
            "#6b9bd2", "#e8c97a", "#52b788", "#6d6875",
        ]

        datasets = []
        for i, agency in enumerate(top_agencies):
            datasets.append({
                "label": agency[:30],
                "data": [int(data_map.get((p, agency), 0)) for p in periods],
                "borderColor": colors[i % len(colors)],
                "fill": False,
                "tension": 0.3,
            })

        chart_data = {"labels": periods, "datasets": datasets}
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data

    async def agency_category_heatmap(
        self,
        session: AsyncSession,
        top_n: int = 10,
        status: list[str] | None = None,
        agency_codes: list[str] | None = None,
        category_codes: list[str] | None = None,
        date_start: date | None = None,
        date_end: date | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "agency_cat_heat", top=top_n, status=status, agency=agency_codes,
            category=category_codes, ds=date_start, de=date_end,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        conditions = self._build_conditions(status, agency_codes, None, date_start, date_end)

        stmt = (
            select(
                Agency.name,
                OpportunityFundingCategory.category_name,
                func.count(func.distinct(Opportunity.id)).label("count"),
            )
            .join(Agency, Agency.code == Opportunity.agency_code)
            .join(OpportunityFundingCategory, OpportunityFundingCategory.opportunity_id == Opportunity.id)
        )

        if category_codes:
            conditions.append(OpportunityFundingCategory.category_code.in_(category_codes))
        if conditions:
            stmt = stmt.where(and_(*conditions))

        stmt = stmt.group_by(Agency.name, OpportunityFundingCategory.category_name)

        result = await session.execute(stmt)
        rows = result.all()

        # Get top agencies and categories by count
        agency_totals = {}
        cat_totals = {}
        for r in rows:
            agency_totals[r[0]] = agency_totals.get(r[0], 0) + r[2]
            cat_totals[r[1]] = cat_totals.get(r[1], 0) + r[2]

        top_agency_names = sorted(agency_totals, key=agency_totals.get, reverse=True)[:top_n]
        top_cat_names = sorted(cat_totals, key=cat_totals.get, reverse=True)[:top_n]

        data_map = {}
        for r in rows:
            if r[0] in top_agency_names and r[1] in top_cat_names:
                data_map[(r[0], r[1])] = r[2]

        # Bubble chart format
        data_points = []
        for ai, agency in enumerate(top_agency_names):
            for ci, cat in enumerate(top_cat_names):
                count = data_map.get((agency, cat), 0)
                if count > 0:
                    data_points.append({
                        "x": ci,
                        "y": ai,
                        "r": min(max(count ** 0.5 * 3, 3), 30),
                        "count": count,
                    })

        chart_data = {
            "agencies": top_agency_names,
            "categories": top_cat_names,
            "datasets": [{
                "label": "Opportunities",
                "data": data_points,
                "backgroundColor": "#2c528266",
                "borderColor": "#2c5282",
            }],
        }
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data

    # --- Category Tab ---

    async def category_funding(
        self,
        session: AsyncSession,
        status: list[str] | None = None,
        agency_codes: list[str] | None = None,
        category_codes: list[str] | None = None,
        date_start: date | None = None,
        date_end: date | None = None,
    ) -> dict[str, Any]:
        # Reuse funding_by_category
        return await self.funding_by_category(
            session, status=status, agency_codes=agency_codes,
            category_codes=category_codes, date_start=date_start, date_end=date_end,
        )

    async def classification_breakdown(
        self,
        session: AsyncSession,
        status: list[str] | None = None,
        agency_codes: list[str] | None = None,
        category_codes: list[str] | None = None,
        date_start: date | None = None,
        date_end: date | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "classif_break", status=status, agency=agency_codes,
            category=category_codes, ds=date_start, de=date_end,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        conditions = self._build_conditions(status, agency_codes, category_codes, date_start, date_end)

        stmt = select(
            func.sum(case((Opportunity.is_team_based == True, 1), else_=0)).label("team_based"),
            func.sum(case((Opportunity.is_multi_institution == True, 1), else_=0)).label("multi_institution"),
            func.sum(case((Opportunity.is_multi_disciplinary == True, 1), else_=0)).label("multi_disciplinary"),
            func.sum(case((Opportunity.is_multi_jurisdiction == True, 1), else_=0)).label("multi_jurisdiction"),
        )

        if category_codes:
            stmt = stmt.join(
                OpportunityFundingCategory,
                OpportunityFundingCategory.opportunity_id == Opportunity.id,
            )
            conditions.append(OpportunityFundingCategory.category_code.in_(category_codes))

        if conditions:
            stmt = stmt.where(and_(*conditions))

        result = await session.execute(stmt)
        row = result.one()

        labels = ["Team-Based", "Multi-Institution", "Multi-Disciplinary", "Multi-Jurisdiction"]
        values = [int(row[0] or 0), int(row[1] or 0), int(row[2] or 0), int(row[3] or 0)]
        colors = ["#1a365d", "#2c5282", "#d4a843", "#52b788"]

        chart_data = {
            "labels": labels,
            "datasets": [{
                "label": "Opportunities",
                "data": values,
                "backgroundColor": colors,
                "borderRadius": 4,
            }],
        }
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data

    async def classification_trends(
        self,
        session: AsyncSession,
        granularity: str = "month",
        status: list[str] | None = None,
        agency_codes: list[str] | None = None,
        category_codes: list[str] | None = None,
        date_start: date | None = None,
        date_end: date | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "classif_trends", gran=granularity, status=status, agency=agency_codes,
            category=category_codes, ds=date_start, de=date_end,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        period = self._date_trunc_expr(Opportunity.posting_date, granularity)
        conditions = self._build_conditions(status, agency_codes, category_codes, date_start, date_end)
        conditions.append(Opportunity.posting_date.isnot(None))

        stmt = select(
            period.label("period"),
            func.sum(case((Opportunity.is_team_based == True, 1), else_=0)).label("team_based"),
            func.sum(case((Opportunity.is_multi_institution == True, 1), else_=0)).label("multi_institution"),
            func.sum(case((Opportunity.is_multi_disciplinary == True, 1), else_=0)).label("multi_disciplinary"),
            func.sum(case((Opportunity.is_multi_jurisdiction == True, 1), else_=0)).label("multi_jurisdiction"),
        )

        if category_codes:
            stmt = stmt.join(
                OpportunityFundingCategory,
                OpportunityFundingCategory.opportunity_id == Opportunity.id,
            )
            conditions.append(OpportunityFundingCategory.category_code.in_(category_codes))

        stmt = stmt.where(and_(*conditions)).group_by(period).order_by(period)

        result = await session.execute(stmt)
        rows = result.all()

        periods = [r[0] for r in rows]

        chart_data = {
            "labels": periods,
            "datasets": [
                {
                    "label": "Team-Based",
                    "data": [int(r[1] or 0) for r in rows],
                    "borderColor": "#1a365d",
                    "fill": False,
                    "tension": 0.3,
                },
                {
                    "label": "Multi-Institution",
                    "data": [int(r[2] or 0) for r in rows],
                    "borderColor": "#2c5282",
                    "fill": False,
                    "tension": 0.3,
                },
                {
                    "label": "Multi-Disciplinary",
                    "data": [int(r[3] or 0) for r in rows],
                    "borderColor": "#d4a843",
                    "fill": False,
                    "tension": 0.3,
                },
                {
                    "label": "Multi-Jurisdiction",
                    "data": [int(r[4] or 0) for r in rows],
                    "borderColor": "#52b788",
                    "fill": False,
                    "tension": 0.3,
                },
            ],
        }
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data


    # --- Researcher Helpers ---

    def _build_researcher_conditions(
        self,
        departments: list[str] | None = None,
        researcher_status: list[str] | None = None,
        keyword: str | None = None,
    ) -> list:
        conditions = []
        if researcher_status:
            conditions.append(Researcher.status.in_(researcher_status))
        return conditions

    def _build_match_conditions(
        self,
        min_score: float | None = None,
        agency_codes: list[str] | None = None,
        departments: list[str] | None = None,
    ) -> list:
        conditions = []
        if min_score is not None:
            conditions.append(ResearcherOpportunityMatch.score >= min_score)
        return conditions

    # --- Expanded KPIs ---

    async def cross_domain_kpis(
        self,
        session: AsyncSession,
        status: list[str] | None = None,
        agency_codes: list[str] | None = None,
        category_codes: list[str] | None = None,
        date_start: date | None = None,
        date_end: date | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "xkpis", status=status, agency=agency_codes,
            category=category_codes, ds=date_start, de=date_end,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        # Opportunity KPIs (reuse existing)
        opp_kpis = await self.summary_kpis(
            session, status=status, agency_codes=agency_codes,
            category_codes=category_codes, date_start=date_start, date_end=date_end,
        )

        # Researcher count
        res_result = await session.execute(select(func.count(Researcher.id)))
        researcher_count = res_result.scalar() or 0

        # Publication count
        pub_result = await session.execute(select(func.count(Publication.id)))
        publication_count = pub_result.scalar() or 0

        # VERSO Grant count
        grant_result = await session.execute(select(func.count(Grant.id)))
        grant_count = grant_result.scalar() or 0

        # Match stats
        match_result = await session.execute(
            select(
                func.count(ResearcherOpportunityMatch.id),
                func.avg(ResearcherOpportunityMatch.score),
            )
        )
        match_row = match_result.one()

        kpis = {
            **opp_kpis,
            "researchers": researcher_count,
            "publications": publication_count,
            "verso_grants": grant_count,
            "total_matches": match_row[0] or 0,
            "avg_match_score": round(float(match_row[1] or 0), 1),
        }

        await cache_service.set(cache_key, kpis, ANALYTICS_TTL)
        return kpis

    # --- Researcher Tab (7 charts) ---

    async def researchers_by_department(
        self,
        session: AsyncSession,
        departments: list[str] | None = None,
        researcher_status: list[str] | None = None,
        keyword: str | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "res_dept", dept=departments, rstatus=researcher_status, kw=keyword,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        conditions = [ResearcherAffiliation.organization_name.isnot(None)]
        if researcher_status:
            conditions.append(Researcher.status.in_(researcher_status))
        if departments:
            conditions.append(ResearcherAffiliation.organization_name.in_(departments))
        if keyword:
            conditions.append(ResearcherKeyword.keyword.ilike(f"%{keyword}%"))

        stmt = (
            select(
                ResearcherAffiliation.organization_name,
                func.count(func.distinct(ResearcherAffiliation.researcher_id)).label("count"),
            )
            .join(Researcher, Researcher.id == ResearcherAffiliation.researcher_id)
        )
        if keyword:
            stmt = stmt.join(ResearcherKeyword, ResearcherKeyword.researcher_id == Researcher.id)

        stmt = (
            stmt.where(and_(*conditions))
            .group_by(ResearcherAffiliation.organization_name)
            .order_by(func.count(func.distinct(ResearcherAffiliation.researcher_id)).desc())
            .limit(15)
        )

        result = await session.execute(stmt)
        rows = result.all()

        chart_data = {
            "labels": [r[0] or "Unknown" for r in rows],
            "datasets": [{
                "label": "Researchers",
                "data": [int(r[1]) for r in rows],
                "backgroundColor": "#2c5282",
                "borderRadius": 4,
            }],
        }
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data

    async def researcher_status_breakdown(
        self,
        session: AsyncSession,
        departments: list[str] | None = None,
        researcher_status: list[str] | None = None,
        keyword: str | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "res_status", dept=departments, rstatus=researcher_status, kw=keyword,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        conditions = []
        if researcher_status:
            conditions.append(Researcher.status.in_(researcher_status))
        if departments:
            conditions.append(ResearcherAffiliation.organization_name.in_(departments))
        if keyword:
            conditions.append(ResearcherKeyword.keyword.ilike(f"%{keyword}%"))

        stmt = select(
            Researcher.status,
            func.count(Researcher.id).label("count"),
        )
        if departments:
            stmt = stmt.join(ResearcherAffiliation, ResearcherAffiliation.researcher_id == Researcher.id)
        if keyword:
            stmt = stmt.join(ResearcherKeyword, ResearcherKeyword.researcher_id == Researcher.id)

        if conditions:
            stmt = stmt.where(and_(*conditions))

        stmt = stmt.group_by(Researcher.status).order_by(func.count(Researcher.id).desc())

        result = await session.execute(stmt)
        rows = result.all()

        colors = ["#2c5282", "#d4a843", "#6b9bd2", "#a8c8e8", "#52b788"]

        chart_data = {
            "labels": [r[0] or "Unknown" for r in rows],
            "datasets": [{
                "label": "Researchers",
                "data": [int(r[1]) for r in rows],
                "backgroundColor": colors[:len(rows)],
            }],
        }
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data

    async def top_research_keywords(
        self,
        session: AsyncSession,
        departments: list[str] | None = None,
        researcher_status: list[str] | None = None,
        keyword: str | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "res_keywords", dept=departments, rstatus=researcher_status, kw=keyword,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        conditions = [ResearcherKeyword.keyword.isnot(None)]
        if researcher_status:
            conditions.append(Researcher.status.in_(researcher_status))
        if departments:
            conditions.append(ResearcherAffiliation.organization_name.in_(departments))
        if keyword:
            conditions.append(ResearcherKeyword.keyword.ilike(f"%{keyword}%"))

        stmt = (
            select(
                ResearcherKeyword.keyword,
                func.count(ResearcherKeyword.id).label("count"),
            )
            .join(Researcher, Researcher.id == ResearcherKeyword.researcher_id)
        )
        if departments:
            stmt = stmt.join(ResearcherAffiliation, ResearcherAffiliation.researcher_id == Researcher.id)

        stmt = (
            stmt.where(and_(*conditions))
            .group_by(ResearcherKeyword.keyword)
            .order_by(func.count(ResearcherKeyword.id).desc())
            .limit(20)
        )

        result = await session.execute(stmt)
        rows = result.all()

        chart_data = {
            "labels": [r[0] for r in rows],
            "datasets": [{
                "label": "Occurrences",
                "data": [int(r[1]) for r in rows],
                "backgroundColor": "#1a365d",
                "borderRadius": 4,
            }],
        }
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data

    async def publications_over_time(
        self,
        session: AsyncSession,
        departments: list[str] | None = None,
        researcher_status: list[str] | None = None,
        keyword: str | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "pub_time", dept=departments, rstatus=researcher_status, kw=keyword,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        conditions = [Publication.publication_date.isnot(None)]
        if researcher_status or departments or keyword:
            # Need to join through researcher
            stmt = (
                select(
                    func.substring(Publication.publication_date, 1, 7).label("period"),
                    func.count(func.distinct(Publication.id)).label("count"),
                )
                .join(ResearcherPublication, ResearcherPublication.publication_id == Publication.id)
                .join(Researcher, Researcher.id == ResearcherPublication.researcher_id)
            )
            if researcher_status:
                conditions.append(Researcher.status.in_(researcher_status))
            if departments:
                stmt = stmt.join(ResearcherAffiliation, ResearcherAffiliation.researcher_id == Researcher.id)
                conditions.append(ResearcherAffiliation.organization_name.in_(departments))
            if keyword:
                stmt = stmt.join(ResearcherKeyword, ResearcherKeyword.researcher_id == Researcher.id)
                conditions.append(ResearcherKeyword.keyword.ilike(f"%{keyword}%"))
        else:
            stmt = select(
                func.substring(Publication.publication_date, 1, 7).label("period"),
                func.count(Publication.id).label("count"),
            )

        stmt = (
            stmt.where(and_(*conditions))
            .group_by("period")
            .order_by(text("period"))
        )

        result = await session.execute(stmt)
        rows = result.all()

        chart_data = {
            "labels": [r[0] for r in rows],
            "datasets": [{
                "label": "Publications",
                "data": [int(r[1]) for r in rows],
                "borderColor": "#2c5282",
                "backgroundColor": "#2c528233",
                "fill": True,
                "tension": 0.3,
            }],
        }
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data

    async def grant_funding_by_funder(
        self,
        session: AsyncSession,
        departments: list[str] | None = None,
        researcher_status: list[str] | None = None,
        keyword: str | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "grant_funder", dept=departments, rstatus=researcher_status, kw=keyword,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        conditions = [Grant.funder.isnot(None), Grant.amount.isnot(None)]

        if researcher_status or departments or keyword:
            stmt = (
                select(
                    Grant.funder,
                    func.sum(Grant.amount).label("total"),
                )
                .join(ResearcherGrant, ResearcherGrant.grant_id == Grant.id)
                .join(Researcher, Researcher.id == ResearcherGrant.researcher_id)
            )
            if researcher_status:
                conditions.append(Researcher.status.in_(researcher_status))
            if departments:
                stmt = stmt.join(ResearcherAffiliation, ResearcherAffiliation.researcher_id == Researcher.id)
                conditions.append(ResearcherAffiliation.organization_name.in_(departments))
            if keyword:
                stmt = stmt.join(ResearcherKeyword, ResearcherKeyword.researcher_id == Researcher.id)
                conditions.append(ResearcherKeyword.keyword.ilike(f"%{keyword}%"))
        else:
            stmt = select(
                Grant.funder,
                func.sum(Grant.amount).label("total"),
            )

        stmt = (
            stmt.where(and_(*conditions))
            .group_by(Grant.funder)
            .order_by(func.sum(Grant.amount).desc())
            .limit(15)
        )

        result = await session.execute(stmt)
        rows = result.all()

        chart_data = {
            "labels": [r[0] or "Unknown" for r in rows],
            "datasets": [{
                "label": "Total Funding ($)",
                "data": [float(r[1] or 0) for r in rows],
                "backgroundColor": "#d4a843",
                "borderRadius": 4,
            }],
        }
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data

    async def activity_types(
        self,
        session: AsyncSession,
        departments: list[str] | None = None,
        researcher_status: list[str] | None = None,
        keyword: str | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "act_types", dept=departments, rstatus=researcher_status, kw=keyword,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        conditions = [Activity.activity_type.isnot(None)]

        if researcher_status or departments or keyword:
            stmt = (
                select(
                    Activity.activity_type,
                    func.count(func.distinct(Activity.id)).label("count"),
                )
                .join(ResearcherActivity, ResearcherActivity.activity_id == Activity.id)
                .join(Researcher, Researcher.id == ResearcherActivity.researcher_id)
            )
            if researcher_status:
                conditions.append(Researcher.status.in_(researcher_status))
            if departments:
                stmt = stmt.join(ResearcherAffiliation, ResearcherAffiliation.researcher_id == Researcher.id)
                conditions.append(ResearcherAffiliation.organization_name.in_(departments))
            if keyword:
                stmt = stmt.join(ResearcherKeyword, ResearcherKeyword.researcher_id == Researcher.id)
                conditions.append(ResearcherKeyword.keyword.ilike(f"%{keyword}%"))
        else:
            stmt = select(
                Activity.activity_type,
                func.count(Activity.id).label("count"),
            )

        stmt = (
            stmt.where(and_(*conditions))
            .group_by(Activity.activity_type)
            .order_by(func.count(Activity.id).desc())
        )

        result = await session.execute(stmt)
        rows = result.all()

        colors = [
            "#1a365d", "#2c5282", "#d4a843", "#e8c97a", "#2d6a4f",
            "#52b788", "#b5838d", "#6d6875", "#e5989b", "#a8c8e8",
        ]

        chart_data = {
            "labels": [r[0] for r in rows],
            "datasets": [{
                "label": "Activities",
                "data": [int(r[1]) for r in rows],
                "backgroundColor": colors[:len(rows)],
            }],
        }
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data

    async def researcher_engagement(
        self,
        session: AsyncSession,
        departments: list[str] | None = None,
        researcher_status: list[str] | None = None,
        keyword: str | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "res_engage", dept=departments, rstatus=researcher_status, kw=keyword,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        conditions = []
        if researcher_status:
            conditions.append(Researcher.status.in_(researcher_status))
        if departments:
            conditions.append(ResearcherAffiliation.organization_name.in_(departments))
        if keyword:
            conditions.append(ResearcherKeyword.keyword.ilike(f"%{keyword}%"))

        # Subqueries for counts per researcher
        pub_sub = (
            select(
                ResearcherPublication.researcher_id,
                func.count(ResearcherPublication.id).label("pub_count"),
            )
            .group_by(ResearcherPublication.researcher_id)
            .subquery()
        )
        grant_sub = (
            select(
                ResearcherGrant.researcher_id,
                func.count(ResearcherGrant.id).label("grant_count"),
            )
            .group_by(ResearcherGrant.researcher_id)
            .subquery()
        )
        proj_sub = (
            select(
                ResearcherProject.researcher_id,
                func.count(ResearcherProject.id).label("proj_count"),
            )
            .group_by(ResearcherProject.researcher_id)
            .subquery()
        )
        act_sub = (
            select(
                ResearcherActivity.researcher_id,
                func.count(ResearcherActivity.id).label("act_count"),
            )
            .group_by(ResearcherActivity.researcher_id)
            .subquery()
        )

        # Total engagement = sum of all counts
        total_expr = (
            func.coalesce(pub_sub.c.pub_count, 0)
            + func.coalesce(grant_sub.c.grant_count, 0)
            + func.coalesce(proj_sub.c.proj_count, 0)
            + func.coalesce(act_sub.c.act_count, 0)
        )

        stmt = (
            select(
                Researcher.full_name,
                func.coalesce(pub_sub.c.pub_count, 0).label("publications"),
                func.coalesce(grant_sub.c.grant_count, 0).label("grants"),
                func.coalesce(proj_sub.c.proj_count, 0).label("projects"),
                func.coalesce(act_sub.c.act_count, 0).label("activities"),
            )
            .outerjoin(pub_sub, pub_sub.c.researcher_id == Researcher.id)
            .outerjoin(grant_sub, grant_sub.c.researcher_id == Researcher.id)
            .outerjoin(proj_sub, proj_sub.c.researcher_id == Researcher.id)
            .outerjoin(act_sub, act_sub.c.researcher_id == Researcher.id)
        )

        if departments:
            stmt = stmt.join(ResearcherAffiliation, ResearcherAffiliation.researcher_id == Researcher.id)
        if keyword:
            stmt = stmt.join(ResearcherKeyword, ResearcherKeyword.researcher_id == Researcher.id)

        if conditions:
            stmt = stmt.where(and_(*conditions))

        stmt = stmt.order_by(total_expr.desc()).limit(15)

        result = await session.execute(stmt)
        rows = result.all()

        chart_data = {
            "labels": [r[0] or "Unknown" for r in rows],
            "datasets": [
                {
                    "label": "Publications",
                    "data": [int(r[1]) for r in rows],
                    "backgroundColor": "#2c5282",
                },
                {
                    "label": "Grants",
                    "data": [int(r[2]) for r in rows],
                    "backgroundColor": "#d4a843",
                },
                {
                    "label": "Projects",
                    "data": [int(r[3]) for r in rows],
                    "backgroundColor": "#52b788",
                },
                {
                    "label": "Activities",
                    "data": [int(r[4]) for r in rows],
                    "backgroundColor": "#b5838d",
                },
            ],
        }
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data

    # --- Match Tab (7 charts) ---

    async def match_score_distribution(
        self,
        session: AsyncSession,
        min_score: float | None = None,
        agency_codes: list[str] | None = None,
        departments: list[str] | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "match_dist", minscore=min_score, agency=agency_codes, dept=departments,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        buckets = [
            ("0-10", 0, 10), ("10-20", 10, 20), ("20-30", 20, 30),
            ("30-40", 30, 40), ("40-50", 40, 50), ("50-60", 50, 60),
            ("60-70", 60, 70), ("70-80", 70, 80), ("80-90", 80, 90),
            ("90-100", 90, 101),
        ]

        case_exprs = []
        for label, low, high in buckets:
            case_exprs.append(
                func.sum(case(
                    (and_(
                        ResearcherOpportunityMatch.score >= low,
                        ResearcherOpportunityMatch.score < high,
                    ), 1),
                    else_=0,
                ))
            )

        stmt = select(*case_exprs).select_from(ResearcherOpportunityMatch)

        conditions = self._build_match_conditions(min_score, agency_codes, departments)
        if agency_codes:
            stmt = stmt.join(Opportunity, Opportunity.id == ResearcherOpportunityMatch.opportunity_id)
            conditions.append(Opportunity.agency_code.in_(agency_codes))
        if departments:
            stmt = stmt.join(Researcher, Researcher.id == ResearcherOpportunityMatch.researcher_id)
            stmt = stmt.join(ResearcherAffiliation, ResearcherAffiliation.researcher_id == Researcher.id)
            conditions.append(ResearcherAffiliation.organization_name.in_(departments))

        if conditions:
            stmt = stmt.where(and_(*conditions))

        result = await session.execute(stmt)
        row = result.one()

        colors = [
            "#c4dbf0", "#a8c8e8", "#8bb5e0", "#6b9bd2", "#4a80b8",
            "#3b6ba5", "#2c5282", "#1a365d", "#d4a843", "#e8c97a",
        ]

        chart_data = {
            "labels": [b[0] for b in buckets],
            "datasets": [{
                "label": "Matches",
                "data": [int(row[i] or 0) for i in range(len(buckets))],
                "backgroundColor": colors,
                "borderRadius": 4,
            }],
        }
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data

    async def match_component_breakdown(
        self,
        session: AsyncSession,
        min_score: float | None = None,
        agency_codes: list[str] | None = None,
        departments: list[str] | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "match_comp", minscore=min_score, agency=agency_codes, dept=departments,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        stmt = (
            select(
                func.avg(ResearcherOpportunityMatch.keyword_score).label("avg_keyword"),
                func.avg(ResearcherOpportunityMatch.text_score).label("avg_text"),
                func.avg(ResearcherOpportunityMatch.agency_score).label("avg_agency"),
            )
            .select_from(ResearcherOpportunityMatch)
        )

        conditions = self._build_match_conditions(min_score, agency_codes, departments)
        if agency_codes:
            stmt = stmt.join(Opportunity, Opportunity.id == ResearcherOpportunityMatch.opportunity_id)
            conditions.append(Opportunity.agency_code.in_(agency_codes))
        if departments:
            stmt = stmt.join(Researcher, Researcher.id == ResearcherOpportunityMatch.researcher_id)
            stmt = stmt.join(ResearcherAffiliation, ResearcherAffiliation.researcher_id == Researcher.id)
            conditions.append(ResearcherAffiliation.organization_name.in_(departments))

        if conditions:
            stmt = stmt.where(and_(*conditions))

        result = await session.execute(stmt)
        row = result.one()

        chart_data = {
            "labels": ["Keyword Score", "Text Score", "Agency Score"],
            "datasets": [{
                "label": "Average Score",
                "data": [
                    round(float(row[0] or 0), 1),
                    round(float(row[1] or 0), 1),
                    round(float(row[2] or 0), 1),
                ],
                "backgroundColor": ["#2c5282", "#d4a843", "#52b788"],
                "borderRadius": 4,
            }],
        }
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data

    async def top_matched_researchers(
        self,
        session: AsyncSession,
        min_score: float | None = None,
        agency_codes: list[str] | None = None,
        departments: list[str] | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "match_top_res", minscore=min_score, agency=agency_codes, dept=departments,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        conditions = self._build_match_conditions(min_score, agency_codes, departments)

        stmt = (
            select(
                Researcher.full_name,
                func.avg(ResearcherOpportunityMatch.score).label("avg_score"),
            )
            .join(Researcher, Researcher.id == ResearcherOpportunityMatch.researcher_id)
        )

        if agency_codes:
            stmt = stmt.join(Opportunity, Opportunity.id == ResearcherOpportunityMatch.opportunity_id)
            conditions.append(Opportunity.agency_code.in_(agency_codes))
        if departments:
            stmt = stmt.join(ResearcherAffiliation, ResearcherAffiliation.researcher_id == Researcher.id)
            conditions.append(ResearcherAffiliation.organization_name.in_(departments))

        if conditions:
            stmt = stmt.where(and_(*conditions))

        stmt = (
            stmt.group_by(Researcher.id, Researcher.full_name)
            .order_by(func.avg(ResearcherOpportunityMatch.score).desc())
            .limit(15)
        )

        result = await session.execute(stmt)
        rows = result.all()

        chart_data = {
            "labels": [r[0] or "Unknown" for r in rows],
            "datasets": [{
                "label": "Avg Match Score",
                "data": [round(float(r[1] or 0), 1) for r in rows],
                "backgroundColor": "#2c5282",
                "borderRadius": 4,
            }],
        }
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data

    async def top_matched_opportunities(
        self,
        session: AsyncSession,
        min_score: float | None = None,
        agency_codes: list[str] | None = None,
        departments: list[str] | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "match_top_opp", minscore=min_score, agency=agency_codes, dept=departments,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        # Count of strong matches (score >= 30) per opportunity
        threshold = max(min_score or 30, 30)
        conditions = [ResearcherOpportunityMatch.score >= threshold]

        stmt = (
            select(
                Opportunity.title,
                func.count(ResearcherOpportunityMatch.id).label("strong_matches"),
            )
            .join(Opportunity, Opportunity.id == ResearcherOpportunityMatch.opportunity_id)
        )

        if agency_codes:
            conditions.append(Opportunity.agency_code.in_(agency_codes))
        if departments:
            stmt = stmt.join(Researcher, Researcher.id == ResearcherOpportunityMatch.researcher_id)
            stmt = stmt.join(ResearcherAffiliation, ResearcherAffiliation.researcher_id == Researcher.id)
            conditions.append(ResearcherAffiliation.organization_name.in_(departments))

        if conditions:
            stmt = stmt.where(and_(*conditions))

        stmt = (
            stmt.group_by(Opportunity.id, Opportunity.title)
            .order_by(func.count(ResearcherOpportunityMatch.id).desc())
            .limit(15)
        )

        result = await session.execute(stmt)
        rows = result.all()

        chart_data = {
            "labels": [(r[0] or "Unknown")[:60] for r in rows],
            "datasets": [{
                "label": f"Strong Matches (score >= {threshold})",
                "data": [int(r[1]) for r in rows],
                "backgroundColor": "#d4a843",
                "borderRadius": 4,
            }],
        }
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data

    async def match_quality_by_department(
        self,
        session: AsyncSession,
        min_score: float | None = None,
        agency_codes: list[str] | None = None,
        departments: list[str] | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "match_dept", minscore=min_score, agency=agency_codes, dept=departments,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        conditions = [ResearcherAffiliation.organization_name.isnot(None)]
        conditions.extend(self._build_match_conditions(min_score, agency_codes, departments))

        stmt = (
            select(
                ResearcherAffiliation.organization_name,
                func.avg(ResearcherOpportunityMatch.score).label("avg_score"),
            )
            .join(Researcher, Researcher.id == ResearcherOpportunityMatch.researcher_id)
            .join(ResearcherAffiliation, ResearcherAffiliation.researcher_id == Researcher.id)
        )

        if agency_codes:
            stmt = stmt.join(Opportunity, Opportunity.id == ResearcherOpportunityMatch.opportunity_id)
            conditions.append(Opportunity.agency_code.in_(agency_codes))
        if departments:
            conditions.append(ResearcherAffiliation.organization_name.in_(departments))

        if conditions:
            stmt = stmt.where(and_(*conditions))

        stmt = (
            stmt.group_by(ResearcherAffiliation.organization_name)
            .order_by(func.avg(ResearcherOpportunityMatch.score).desc())
            .limit(15)
        )

        result = await session.execute(stmt)
        rows = result.all()

        chart_data = {
            "labels": [r[0] or "Unknown" for r in rows],
            "datasets": [{
                "label": "Avg Match Score",
                "data": [round(float(r[1] or 0), 1) for r in rows],
                "backgroundColor": "#2c5282",
                "borderRadius": 4,
            }],
        }
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data

    async def match_quality_by_agency(
        self,
        session: AsyncSession,
        min_score: float | None = None,
        agency_codes: list[str] | None = None,
        departments: list[str] | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "match_agency", minscore=min_score, agency=agency_codes, dept=departments,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        conditions = self._build_match_conditions(min_score, agency_codes, departments)

        stmt = (
            select(
                Agency.name,
                func.avg(ResearcherOpportunityMatch.score).label("avg_score"),
            )
            .join(Opportunity, Opportunity.id == ResearcherOpportunityMatch.opportunity_id)
            .join(Agency, Agency.code == Opportunity.agency_code)
        )

        if agency_codes:
            conditions.append(Opportunity.agency_code.in_(agency_codes))
        if departments:
            stmt = stmt.join(Researcher, Researcher.id == ResearcherOpportunityMatch.researcher_id)
            stmt = stmt.join(ResearcherAffiliation, ResearcherAffiliation.researcher_id == Researcher.id)
            conditions.append(ResearcherAffiliation.organization_name.in_(departments))

        if conditions:
            stmt = stmt.where(and_(*conditions))

        stmt = (
            stmt.group_by(Agency.name)
            .order_by(func.avg(ResearcherOpportunityMatch.score).desc())
            .limit(15)
        )

        result = await session.execute(stmt)
        rows = result.all()

        chart_data = {
            "labels": [r[0] or "Unknown" for r in rows],
            "datasets": [{
                "label": "Avg Match Score",
                "data": [round(float(r[1] or 0), 1) for r in rows],
                "backgroundColor": "#1a365d",
                "borderRadius": 4,
            }],
        }
        await cache_service.set(cache_key, chart_data, ANALYTICS_TTL)
        return chart_data

    async def match_coverage(
        self,
        session: AsyncSession,
        min_score: float | None = None,
        agency_codes: list[str] | None = None,
        departments: list[str] | None = None,
    ) -> dict[str, Any]:
        cache_key = self._cache_key(
            "match_coverage", minscore=min_score, agency=agency_codes, dept=departments,
        )
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        threshold = min_score or 30

        # Total opportunities and researchers
        total_opps = (await session.execute(select(func.count(Opportunity.id)))).scalar() or 1
        total_researchers = (await session.execute(select(func.count(Researcher.id)))).scalar() or 1

        # Opportunities with at least one match >= threshold
        opps_with_match = (await session.execute(
            select(func.count(func.distinct(ResearcherOpportunityMatch.opportunity_id)))
            .where(ResearcherOpportunityMatch.score >= threshold)
        )).scalar() or 0

        # Researchers with at least one match >= threshold
        res_with_match = (await session.execute(
            select(func.count(func.distinct(ResearcherOpportunityMatch.researcher_id)))
            .where(ResearcherOpportunityMatch.score >= threshold)
        )).scalar() or 0

        # Total matches and strong matches
        match_stats = (await session.execute(
            select(
                func.count(ResearcherOpportunityMatch.id),
                func.sum(case(
                    (ResearcherOpportunityMatch.score >= threshold, 1),
                    else_=0,
                )),
            )
        )).one()

        data = {
            "threshold": threshold,
            "total_opportunities": total_opps,
            "opportunities_with_match": opps_with_match,
            "opportunity_coverage_pct": round(opps_with_match / total_opps * 100, 1),
            "total_researchers": total_researchers,
            "researchers_with_match": res_with_match,
            "researcher_coverage_pct": round(res_with_match / total_researchers * 100, 1),
            "total_matches": int(match_stats[0] or 0),
            "strong_matches": int(match_stats[1] or 0),
        }

        await cache_service.set(cache_key, data, ANALYTICS_TTL)
        return data

    # --- Sidebar Data ---

    async def get_departments(self, session: AsyncSession) -> list[dict]:
        """Get list of departments for filter sidebar."""
        cache_key = "pf:analytics:departments"
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        stmt = (
            select(
                ResearcherAffiliation.organization_name,
                func.count(func.distinct(ResearcherAffiliation.researcher_id)).label("count"),
            )
            .where(ResearcherAffiliation.organization_name.isnot(None))
            .group_by(ResearcherAffiliation.organization_name)
            .order_by(func.count(func.distinct(ResearcherAffiliation.researcher_id)).desc())
        )

        result = await session.execute(stmt)
        rows = result.all()

        departments = [{"name": r[0], "count": r[1]} for r in rows]
        await cache_service.set(cache_key, departments, 3600)
        return departments


analytics_service = AnalyticsService()

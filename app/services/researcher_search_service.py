import logging
from typing import Any

from sqlalchemy import select, func, text, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.researcher import (
    Researcher, ResearcherKeyword, ResearcherAffiliation, Publication, ResearcherPublication,
)
from app.services.cache_service import cache_service, FACET_TTL, STATS_TTL

logger = logging.getLogger(__name__)


class ResearcherSearchService:

    async def search(
        self,
        session: AsyncSession,
        query: str | None = None,
        department: str | None = None,
        keyword: str | None = None,
        status: str | None = None,
        has_summary: bool | None = None,
        sort_by: str = "name",
        sort_order: str = "asc",
        page: int = 1,
        per_page: int = 25,
    ) -> dict[str, Any]:
        stmt = select(Researcher)
        count_stmt = select(func.count(Researcher.id))

        conditions = []
        params = {}

        # Full-text search (with LIKE fallback for short queries)
        if query and query.strip():
            q = query.strip()
            # MariaDB ft_min_word_len default is 4; short terms need LIKE fallback
            if len(q) < 4:
                like_pattern = f"%{q}%"
                conditions.append(text(
                    "(researchers.full_name LIKE :like_q OR "
                    "researchers.keyword_text LIKE :like_q OR "
                    "researchers.position_title LIKE :like_q)"
                ))
                params["like_q"] = like_pattern
            else:
                # Add wildcard for prefix matching in boolean mode
                ft_query = " ".join(f"+{word}*" for word in q.split() if word)
                ft = text(
                    "MATCH(researchers.full_name, researchers.keyword_text, "
                    "researchers.ai_summary, researchers.position_title) "
                    "AGAINST(:query IN BOOLEAN MODE)"
                )
                conditions.append(ft)
                params["query"] = ft_query

        # Status filter
        if status:
            conditions.append(Researcher.status == status.upper())

        # Has AI summary filter
        if has_summary is True:
            conditions.append(Researcher.ai_summary.isnot(None))
            conditions.append(Researcher.ai_summary != "")
        elif has_summary is False:
            conditions.append(
                (Researcher.ai_summary.is_(None)) | (Researcher.ai_summary == "")
            )

        # Department filter (via affiliations join)
        if department:
            stmt = stmt.join(
                ResearcherAffiliation,
                ResearcherAffiliation.researcher_id == Researcher.id,
            )
            count_stmt = count_stmt.join(
                ResearcherAffiliation,
                ResearcherAffiliation.researcher_id == Researcher.id,
            )
            conditions.append(ResearcherAffiliation.organization_name == department)

        # Keyword filter (via keywords join)
        if keyword:
            stmt = stmt.join(
                ResearcherKeyword,
                ResearcherKeyword.researcher_id == Researcher.id,
            )
            count_stmt = count_stmt.join(
                ResearcherKeyword,
                ResearcherKeyword.researcher_id == Researcher.id,
            )
            conditions.append(ResearcherKeyword.keyword == keyword)

        if conditions:
            where_clause = and_(*conditions)
            stmt = stmt.where(where_clause)
            count_stmt = count_stmt.where(where_clause)

        # Count
        count_result = await session.execute(count_stmt, params)
        total = count_result.scalar()

        # Sorting
        sort_column = {
            "name": Researcher.full_name,
            "position": Researcher.position_title,
            "updated": Researcher.updated_at,
        }.get(sort_by, Researcher.full_name)

        if sort_order == "desc":
            stmt = stmt.order_by(func.isnull(sort_column), sort_column.desc())
        else:
            stmt = stmt.order_by(func.isnull(sort_column), sort_column.asc())

        # Pagination
        offset = (page - 1) * per_page
        stmt = stmt.offset(offset).limit(per_page)

        result = await session.execute(stmt, params)
        researchers = result.scalars().unique().all()

        return {
            "researchers": researchers,
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": (total + per_page - 1) // per_page if total else 0,
        }

    async def get_facets(self, session: AsyncSession) -> dict[str, Any]:
        cached = await cache_service.get("pf:researcher_facets")
        if cached:
            return cached

        # Department counts (from affiliations)
        dept_stmt = (
            select(
                ResearcherAffiliation.organization_name,
                func.count(func.distinct(ResearcherAffiliation.researcher_id)).label("count"),
            )
            .where(ResearcherAffiliation.organization_name.isnot(None))
            .group_by(ResearcherAffiliation.organization_name)
            .order_by(func.count(func.distinct(ResearcherAffiliation.researcher_id)).desc())
            .limit(50)
        )
        dept_result = await session.execute(dept_stmt)
        departments = [
            {"name": row[0], "count": row[1]}
            for row in dept_result.all()
            if row[0]
        ]

        # Top keywords
        kw_stmt = (
            select(
                ResearcherKeyword.keyword,
                func.count(func.distinct(ResearcherKeyword.researcher_id)).label("count"),
            )
            .group_by(ResearcherKeyword.keyword)
            .order_by(func.count(func.distinct(ResearcherKeyword.researcher_id)).desc())
            .limit(30)
        )
        kw_result = await session.execute(kw_stmt)
        keywords = [
            {"keyword": row[0], "count": row[1]}
            for row in kw_result.all()
        ]

        facets = {
            "departments": departments,
            "keywords": keywords,
        }

        await cache_service.set("pf:researcher_facets", facets, FACET_TTL)
        return facets

    async def get_stats(self, session: AsyncSession) -> dict[str, Any]:
        cached = await cache_service.get("pf:researcher_stats")
        if cached:
            return cached

        total = (await session.execute(select(func.count(Researcher.id)))).scalar() or 0
        active = (await session.execute(
            select(func.count(Researcher.id)).where(Researcher.status == "ACTIVE")
        )).scalar() or 0
        with_summary = (await session.execute(
            select(func.count(Researcher.id)).where(
                Researcher.ai_summary.isnot(None),
                Researcher.ai_summary != "",
            )
        )).scalar() or 0
        total_pubs = (await session.execute(select(func.count(Publication.id)))).scalar() or 0

        stats = {
            "total_researchers": total,
            "active_researchers": active,
            "with_summary": with_summary,
            "total_publications": total_pubs,
        }

        await cache_service.set("pf:researcher_stats", stats, STATS_TTL)
        return stats

    async def get_researcher_detail(self, session: AsyncSession, researcher_id: int) -> dict | None:
        """Get full researcher detail including publications."""
        stmt = select(Researcher).where(Researcher.id == researcher_id)
        result = await session.execute(stmt)
        researcher = result.scalar_one_or_none()
        if not researcher:
            return None

        # Fetch publications via join table
        pub_stmt = (
            select(Publication)
            .join(ResearcherPublication, ResearcherPublication.publication_id == Publication.id)
            .where(ResearcherPublication.researcher_id == researcher_id)
            .order_by(Publication.publication_date.desc())
        )
        pub_result = await session.execute(pub_stmt)
        publications = pub_result.scalars().all()

        return {
            "researcher": researcher,
            "publications": publications,
        }

    async def get_departments(self, session: AsyncSession) -> list[dict]:
        cached = await cache_service.get("pf:researcher_departments")
        if cached:
            return cached

        stmt = (
            select(
                ResearcherAffiliation.organization_name,
                func.count(func.distinct(ResearcherAffiliation.researcher_id)).label("count"),
            )
            .where(ResearcherAffiliation.organization_name.isnot(None))
            .group_by(ResearcherAffiliation.organization_name)
            .order_by(ResearcherAffiliation.organization_name)
        )
        result = await session.execute(stmt)
        departments = [{"name": row[0], "count": row[1]} for row in result.all() if row[0]]

        await cache_service.set("pf:researcher_departments", departments, FACET_TTL)
        return departments


researcher_search_service = ResearcherSearchService()

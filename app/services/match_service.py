import logging
import re
from datetime import datetime

from sqlalchemy import select, text, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session
from app.models.researcher import (
    Researcher, ResearcherKeyword, ResearcherOpportunityMatch,
    Publication, ResearcherPublication,
)
from app.models import Opportunity, OpportunityFundingCategory
from app.services.cache_service import cache_service

logger = logging.getLogger(__name__)

# Scoring weights
KEYWORD_WEIGHT = 0.40
TEXT_WEIGHT = 0.35
CATEGORY_WEIGHT = 0.15
PUBLICATION_WEIGHT = 0.10

# Minimum score threshold to store a match
MIN_SCORE_THRESHOLD = 0.05

# Word tokenizer
WORD_RE = re.compile(r'\b[a-zA-Z]{3,}\b')
STOP_WORDS = frozenset([
    "the", "and", "for", "are", "but", "not", "you", "all", "can", "had", "her",
    "was", "one", "our", "out", "has", "have", "from", "with", "this", "that",
    "will", "each", "make", "like", "been", "many", "some", "them", "than",
    "its", "over", "such", "into", "other", "also", "may", "which", "would",
    "about", "their", "these", "could", "should", "shall", "must", "through",
    "between", "before", "after", "during", "being", "those", "what", "when",
    "where", "there", "here", "both", "does", "did", "very", "just", "more",
    "most", "only", "under", "while", "within", "without", "upon",
    "including", "provide", "program", "funding", "federal", "grant", "opportunity",
    "application", "applicant", "project", "research", "support", "award",
])


def _tokenize(text_val: str | None) -> set[str]:
    if not text_val:
        return set()
    words = set(w.lower() for w in WORD_RE.findall(text_val))
    return words - STOP_WORDS


def _jaccard_similarity(set_a: set, set_b: set) -> float:
    if not set_a or not set_b:
        return 0.0
    intersection = set_a & set_b
    union = set_a | set_b
    return len(intersection) / len(union) if union else 0.0


MATCH_STATS_KEY = "pf:match_recompute_stats"


class MatchService:

    def __init__(self):
        self.is_computing = False
        self.match_stats: dict = {}

    async def _publish_match_stats(self):
        """Publish current match recomputation stats to Redis."""
        try:
            await cache_service.set(MATCH_STATS_KEY, {
                "is_computing": self.is_computing,
                "stats": dict(self.match_stats),
            }, ttl=120)
        except Exception:
            pass

    async def get_shared_match_stats(self) -> dict | None:
        """Read match stats from Redis (for other workers)."""
        return await cache_service.get(MATCH_STATS_KEY)

    async def recompute_all_matches(self):
        """Batch recompute all researcher-opportunity matches."""
        logger.info("Starting match recomputation...")
        self.is_computing = True
        self.match_stats = {
            "phase": "loading",
            "started": datetime.utcnow().isoformat(),
            "researchers": 0,
            "opportunities": 0,
            "matches_found": 0,
            "matches_inserted": 0,
            "completed": False,
            "error": None,
        }
        await self._publish_match_stats()

        try:
            try:
                from sklearn.feature_extraction.text import TfidfVectorizer
                from sklearn.metrics.pairwise import cosine_similarity
                has_sklearn = True
            except ImportError:
                logger.warning("scikit-learn not installed, using keyword-only matching")
                has_sklearn = False

            async with async_session() as session:
                # Load all active researchers
                r_stmt = select(Researcher).where(Researcher.status == "ACTIVE")
                r_result = await session.execute(r_stmt)
                researchers = r_result.scalars().all()

                if not researchers:
                    logger.info("No researchers found, skipping match computation")
                    self.match_stats.update({"phase": "done", "completed": True, "error": "No active researchers found"})
                    await self._publish_match_stats()
                    self.is_computing = False
                    return

                # Load researcher keywords
                self.match_stats.update({"phase": "loading", "researchers": len(researchers)})
                await self._publish_match_stats()

                researcher_keywords: dict[int, set[str]] = {}
                for r in researchers:
                    kw_stmt = select(ResearcherKeyword.keyword).where(ResearcherKeyword.researcher_id == r.id)
                    kw_result = await session.execute(kw_stmt)
                    keywords = set(kw.lower() for kw, in kw_result.all())
                    # Also add words from keyword_text
                    keywords |= _tokenize(r.keyword_text)
                    researcher_keywords[r.id] = keywords

                # Load researcher publication keywords
                researcher_pub_keywords: dict[int, set[str]] = {}
                for r in researchers:
                    pub_stmt = (
                        select(Publication.keywords, Publication.title)
                        .join(ResearcherPublication, ResearcherPublication.publication_id == Publication.id)
                        .where(ResearcherPublication.researcher_id == r.id)
                    )
                    pub_result = await session.execute(pub_stmt)
                    pub_words = set()
                    for kw_text, title in pub_result.all():
                        pub_words |= _tokenize(kw_text)
                        pub_words |= _tokenize(title)
                    researcher_pub_keywords[r.id] = pub_words

                # Load all active opportunities with categories
                o_stmt = select(Opportunity).where(
                    Opportunity.status.in_(["posted", "forecasted"])
                )
                o_result = await session.execute(o_stmt)
                opportunities = o_result.scalars().all()

                if not opportunities:
                    logger.info("No active opportunities found, skipping match computation")
                    self.match_stats.update({"phase": "done", "completed": True, "error": "No active opportunities found"})
                    await self._publish_match_stats()
                    self.is_computing = False
                    return

                self.match_stats.update({"opportunities": len(opportunities)})
                await self._publish_match_stats()

                # Load opportunity categories
                opp_categories: dict[int, set[str]] = {}
                for opp in opportunities:
                    cat_stmt = (
                        select(OpportunityFundingCategory.category_name)
                        .where(OpportunityFundingCategory.opportunity_id == opp.id)
                    )
                    cat_result = await session.execute(cat_stmt)
                    cats = set(c.lower() for c, in cat_result.all() if c)
                    # Also add words from title and category names
                    cats |= _tokenize(opp.title)
                    opp_categories[opp.id] = cats

                # Build TF-IDF matrix if sklearn available
                self.match_stats["phase"] = "tfidf"
                await self._publish_match_stats()

                text_scores_matrix = {}
                if has_sklearn:
                    # Build researcher text corpus
                    researcher_texts = []
                    researcher_ids_ordered = []
                    for r in researchers:
                        doc = " ".join(filter(None, [r.ai_summary, r.keyword_text, r.position_title]))
                        researcher_texts.append(doc)
                        researcher_ids_ordered.append(r.id)

                    # Build opportunity text corpus
                    opp_texts = []
                    opp_ids_ordered = []
                    for opp in opportunities:
                        doc = " ".join(filter(None, [opp.title, opp.synopsis_description]))
                        opp_texts.append(doc)
                        opp_ids_ordered.append(opp.id)

                    # Compute TF-IDF
                    all_texts = researcher_texts + opp_texts
                    if all_texts and any(t.strip() for t in all_texts):
                        vectorizer = TfidfVectorizer(
                            max_features=10000,
                            stop_words="english",
                            min_df=2,
                            max_df=0.95,
                        )
                        try:
                            tfidf_matrix = vectorizer.fit_transform(all_texts)
                            r_matrix = tfidf_matrix[:len(researcher_texts)]
                            o_matrix = tfidf_matrix[len(researcher_texts):]

                            # Compute cosine similarity matrix: researchers x opportunities
                            sim_matrix = cosine_similarity(r_matrix, o_matrix)

                            for i, r_id in enumerate(researcher_ids_ordered):
                                for j, o_id in enumerate(opp_ids_ordered):
                                    text_scores_matrix[(r_id, o_id)] = float(sim_matrix[i, j])
                        except ValueError as e:
                            logger.warning(f"TF-IDF computation failed: {e}")

                logger.info(f"Computing matches for {len(researchers)} researchers x {len(opportunities)} opportunities...")

                # Compute scores and build batch
                self.match_stats["phase"] = "scoring"
                await self._publish_match_stats()

                matches_to_insert = []
                now = datetime.utcnow()
                total_pairs = len(researchers) * len(opportunities)
                scored = 0

                for r in researchers:
                    r_kw = researcher_keywords.get(r.id, set())
                    r_pub_kw = researcher_pub_keywords.get(r.id, set())

                    for opp in opportunities:
                        o_kw = opp_categories.get(opp.id, set())

                        # 1. Keyword score (Jaccard)
                        keyword_score = _jaccard_similarity(r_kw, o_kw)

                        # 2. Text similarity (TF-IDF)
                        text_score = text_scores_matrix.get((r.id, opp.id), 0.0)

                        # 3. Category alignment — check if researcher affiliation words overlap with opp categories
                        r_aff_words = set()
                        for aff in (r.affiliations or []):
                            if aff.organization_name:
                                r_aff_words |= _tokenize(aff.organization_name)
                        agency_score = _jaccard_similarity(r_aff_words, o_kw) if r_aff_words else 0.0

                        # 4. Publication relevance
                        pub_score = _jaccard_similarity(r_pub_kw, o_kw) if r_pub_kw else 0.0

                        # Composite score
                        score = (
                            KEYWORD_WEIGHT * keyword_score
                            + TEXT_WEIGHT * text_score
                            + CATEGORY_WEIGHT * agency_score
                            + PUBLICATION_WEIGHT * pub_score
                        )

                        if score >= MIN_SCORE_THRESHOLD:
                            matches_to_insert.append({
                                "researcher_id": r.id,
                                "opportunity_id": opp.id,
                                "score": round(score, 6),
                                "keyword_score": round(keyword_score, 6),
                                "text_score": round(text_score, 6),
                                "agency_score": round(agency_score, 6),
                                "computed_at": now,
                            })

                    scored += len(opportunities)
                    # Publish progress every 50 researchers
                    if scored % (50 * len(opportunities)) < len(opportunities):
                        self.match_stats.update({
                            "scored": scored,
                            "total_pairs": total_pairs,
                            "matches_found": len(matches_to_insert),
                        })
                        await self._publish_match_stats()

                self.match_stats.update({
                    "scored": total_pairs,
                    "total_pairs": total_pairs,
                    "matches_found": len(matches_to_insert),
                })

            # Write matches in batches (new session to avoid long transaction)
            self.match_stats["phase"] = "writing"
            await self._publish_match_stats()

            async with async_session() as session:
                async with session.begin():
                    # Clear existing matches
                    await session.execute(text("DELETE FROM researcher_opportunity_matches"))

                # Insert in batches
                batch_size = 5000
                total_inserted = 0
                for i in range(0, len(matches_to_insert), batch_size):
                    batch = matches_to_insert[i:i + batch_size]
                    async with session.begin():
                        for m in batch:
                            session.add(ResearcherOpportunityMatch(**m))
                    total_inserted += len(batch)
                    self.match_stats["matches_inserted"] = total_inserted
                    await self._publish_match_stats()

            logger.info(f"Match recomputation complete: {total_inserted} matches stored")
            self.match_stats.update({
                "phase": "done",
                "completed": True,
                "matches_inserted": total_inserted,
            })
            await self._publish_match_stats()
            await cache_service.delete_pattern("pf:matches:*")

        except Exception as e:
            logger.error(f"Match recomputation failed: {e}", exc_info=True)
            self.match_stats.update({"phase": "done", "completed": True, "error": str(e)[:200]})
            await self._publish_match_stats()
        finally:
            self.is_computing = False

    async def get_matches_for_opportunity(
        self, session: AsyncSession, opportunity_id: int, limit: int = 20,
    ) -> list[dict]:
        """Get top matching researchers for an opportunity (by internal id)."""
        cache_key = f"pf:matches:opp:{opportunity_id}:{limit}"
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        stmt = (
            select(ResearcherOpportunityMatch, Researcher)
            .join(Researcher, Researcher.id == ResearcherOpportunityMatch.researcher_id)
            .where(ResearcherOpportunityMatch.opportunity_id == opportunity_id)
            .order_by(ResearcherOpportunityMatch.score.desc())
            .limit(limit)
        )
        result = await session.execute(stmt)
        rows = result.all()

        matches = []
        for match, researcher in rows:
            matches.append({
                "researcher_id": researcher.id,
                "full_name": researcher.full_name,
                "position_title": researcher.position_title,
                "photo_url": researcher.photo_url,
                "score": round(match.score, 3),
                "keyword_score": round(match.keyword_score, 3),
                "text_score": round(match.text_score, 3),
                "keywords": [kw.keyword for kw in researcher.keywords[:5]] if researcher.keywords else [],
            })

        await cache_service.set(cache_key, matches, 300)
        return matches

    async def get_matches_for_researcher(
        self, session: AsyncSession, researcher_id: int, limit: int = 20,
    ) -> list[dict]:
        """Get top matching opportunities for a researcher."""
        cache_key = f"pf:matches:res:{researcher_id}:{limit}"
        cached = await cache_service.get(cache_key)
        if cached:
            return cached

        stmt = (
            select(ResearcherOpportunityMatch, Opportunity)
            .join(Opportunity, Opportunity.id == ResearcherOpportunityMatch.opportunity_id)
            .where(ResearcherOpportunityMatch.researcher_id == researcher_id)
            .order_by(ResearcherOpportunityMatch.score.desc())
            .limit(limit)
        )
        result = await session.execute(stmt)
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
            })

        await cache_service.set(cache_key, matches, 300)
        return matches


match_service = MatchService()

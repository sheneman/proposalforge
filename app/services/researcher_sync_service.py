import asyncio
import logging
import re
from datetime import datetime

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session
from app.models.researcher import (
    Researcher, ResearcherKeyword, ResearcherAffiliation,
    ResearcherEducation, Publication, ResearcherPublication,
)
from app.models.sync_log import SyncLog
from app.services.collabnet_client import collabnet_client
from app.services.cache_service import cache_service

logger = logging.getLogger(__name__)

SYNC_STATS_KEY = "pf:researcher_sync_stats"
SYNC_STATS_TTL = 3600

# Strip HTML tags from AI summaries
HTML_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text_val: str | None) -> str | None:
    if not text_val:
        return None
    return HTML_TAG_RE.sub("", text_val).strip()


def _extract_contact(contacts: list | None, contact_type: str) -> str | None:
    """Extract a contact value from the contacts array by type."""
    if not contacts:
        return None
    for c in contacts:
        if isinstance(c, dict):
            ct = (c.get("type") or c.get("contact_type") or "").lower()
            if ct == contact_type:
                return c.get("value") or c.get("contact_value")
    return None


class ResearcherSyncService:
    def __init__(self):
        self.is_syncing = False
        self.last_sync: datetime | None = None
        self.sync_stats: dict = {}
        self._cancel_requested = False
        self._current_log_id: int | None = None
        self._task: asyncio.Task | None = None

    async def _publish_stats(self):
        try:
            data = {"is_syncing": self.is_syncing, "stats": self.sync_stats}
            if self.last_sync:
                data["last_sync"] = self.last_sync.isoformat()
            await cache_service.set(SYNC_STATS_KEY, data, SYNC_STATS_TTL)
        except Exception:
            pass

    @staticmethod
    async def get_shared_stats() -> dict | None:
        return await cache_service.get(SYNC_STATS_KEY)

    async def _create_sync_log(self, sync_type: str) -> int:
        async with async_session() as session:
            async with session.begin():
                log = SyncLog(
                    sync_type=sync_type,
                    status="running",
                    started_at=datetime.utcnow(),
                )
                session.add(log)
                await session.flush()
                log_id = log.id
        return log_id

    async def _finish_sync_log(self, log_id: int, status: str, stats: dict, error_msg: str | None = None):
        now = datetime.utcnow()
        async with async_session() as session:
            async with session.begin():
                log = await session.get(SyncLog, log_id)
                if log:
                    log.status = status
                    log.completed_at = now
                    log.duration_seconds = (now - log.started_at).total_seconds()
                    log.total_items = stats.get("total", 0)
                    log.success_count = stats.get("success", 0)
                    log.error_count = stats.get("errors", 0)
                    log.error_message = error_msg
        await self._publish_stats()

    def cancel_sync(self):
        if self.is_syncing:
            self._cancel_requested = True
            # Cancel the asyncio task to interrupt any pending await (e.g. HTTP requests)
            if self._task and not self._task.done():
                self._task.cancel()
            return True
        return False

    async def _upsert_researcher(self, session: AsyncSession, data: dict) -> Researcher | None:
        """Upsert a single researcher from CollabNet API data."""
        try:
            collabnet_id = str(data.get("_id") or data.get("id") or "")
            if not collabnet_id:
                return None

            primary_id = data.get("primary_id") or data.get("username") or ""
            if not primary_id:
                return None

            # Look for existing
            stmt = select(Researcher).where(Researcher.collabnet_id == collabnet_id)
            result = await session.execute(stmt)
            researcher = result.scalar_one_or_none()

            # Extract name
            first_name = data.get("first_name") or data.get("researcher_researcher_first_name") or ""
            last_name = data.get("last_name") or data.get("researcher_researcher_last_name") or ""
            full_name = data.get("full_name") or data.get("name") or f"{first_name} {last_name}".strip()

            # Extract contacts
            contacts = data.get("contacts") or data.get("contact_info") or []
            email = _extract_contact(contacts, "email") or data.get("email")
            phone = _extract_contact(contacts, "phone") or data.get("phone")

            # Photo and profile — CollabNet uses researcher_photo_url / researcher_profile_identifier_url
            photo_url = (
                data.get("researcher_photo_url")
                or data.get("photo_url")
                or data.get("photoUrl")
            )
            profile_url = (
                data.get("researcher_profile_identifier_url")
                or data.get("profile_url")
                or data.get("profileUrl")
            )

            # Position — CollabNet uses researcher_position_desc / researcher_position_value
            position_title = (
                data.get("researcher_position_desc")
                or data.get("position_title")
                or data.get("job_description")
            )
            position_code = (
                data.get("researcher_position_value")
                or data.get("position_code")
            )

            # Status — CollabNet uses status_value
            raw_status = data.get("status_value") or data.get("status") or "ACTIVE"
            status = raw_status.upper()

            # Keywords — CollabNet uses researcher_researcher_keyword: [{value: "..."}]
            keywords_list = []
            raw_keywords = data.get("researcher_researcher_keyword") or data.get("keywords") or data.get("research_keywords") or []
            if isinstance(raw_keywords, str):
                keywords_list = [k.strip() for k in raw_keywords.split(",") if k.strip()]
            elif isinstance(raw_keywords, list):
                for kw in raw_keywords:
                    if isinstance(kw, dict):
                        val = kw.get("value") or kw.get("keyword") or ""
                        if val:
                            keywords_list.append(val.strip())
                    elif isinstance(kw, str) and kw.strip():
                        keywords_list.append(kw.strip())
            keyword_text = ", ".join(keywords_list) if keywords_list else None

            values = dict(
                collabnet_id=collabnet_id,
                primary_id=primary_id,
                first_name=first_name,
                last_name=last_name,
                full_name=full_name,
                email=email,
                phone=phone,
                photo_url=photo_url,
                profile_url=profile_url,
                position_title=position_title,
                position_code=position_code,
                status=status,
                keyword_text=keyword_text,
                last_synced_at=datetime.utcnow(),
            )

            if researcher:
                for k, v in values.items():
                    setattr(researcher, k, v)
            else:
                researcher = Researcher(**values)
                session.add(researcher)

            await session.flush()

            # Delete and recreate keywords
            await session.execute(
                text("DELETE FROM researcher_keywords WHERE researcher_id = :rid"),
                {"rid": researcher.id},
            )
            for kw in keywords_list:
                if kw and isinstance(kw, str):
                    session.add(ResearcherKeyword(researcher_id=researcher.id, keyword=kw.strip()[:255]))

            # Delete and recreate affiliations
            # CollabNet uses researcher_researcher_organization_affiliation (current)
            # and researcher_researcher_previous_organization_affiliation (previous)
            await session.execute(
                text("DELETE FROM researcher_affiliations WHERE researcher_id = :rid"),
                {"rid": researcher.id},
            )
            current_affiliations = (
                data.get("researcher_researcher_organization_affiliation")
                or data.get("affiliations")
                or data.get("organizations")
                or []
            )
            for aff in current_affiliations:
                if isinstance(aff, dict):
                    session.add(ResearcherAffiliation(
                        researcher_id=researcher.id,
                        organization_name=aff.get("organization_name") or aff.get("name"),
                        organization_code=aff.get("organization_code") or aff.get("code"),
                        is_current=True,
                    ))
                elif isinstance(aff, str):
                    session.add(ResearcherAffiliation(
                        researcher_id=researcher.id,
                        organization_name=aff,
                        is_current=True,
                    ))

            previous_affiliations = data.get("researcher_researcher_previous_organization_affiliation") or []
            for aff in previous_affiliations:
                if isinstance(aff, dict):
                    session.add(ResearcherAffiliation(
                        researcher_id=researcher.id,
                        organization_name=aff.get("organization_name") or aff.get("name"),
                        organization_code=aff.get("organization_code") or aff.get("code"),
                        is_current=False,
                    ))

            # Delete and recreate education
            # CollabNet uses researcher_researcher_education
            await session.execute(
                text("DELETE FROM researcher_education WHERE researcher_id = :rid"),
                {"rid": researcher.id},
            )
            education_list = (
                data.get("researcher_researcher_education")
                or data.get("education")
                or []
            )
            for edu in education_list:
                if isinstance(edu, dict):
                    session.add(ResearcherEducation(
                        researcher_id=researcher.id,
                        institution=edu.get("organization_name") or edu.get("institution") or edu.get("school"),
                        degree=edu.get("degree"),
                        field_of_study=edu.get("field_of_study") or edu.get("fieldOfStudy") or edu.get("field"),
                    ))

            return researcher
        except Exception as e:
            logger.error(f"Error upserting researcher: {e}", exc_info=True)
            return None

    async def _upsert_publication(self, session: AsyncSession, data: dict) -> Publication | None:
        """Upsert a publication from CollabNet documents API."""
        try:
            collabnet_id = str(data.get("_id") or data.get("id") or "")
            if not collabnet_id:
                return None

            stmt = select(Publication).where(Publication.collabnet_id == collabnet_id)
            result = await session.execute(stmt)
            pub = result.scalar_one_or_none()

            title = data.get("title") or "Untitled"
            if isinstance(title, list):
                title = title[0] if title else "Untitled"
            title = str(title)[:1000]

            # Keywords
            kw = data.get("keywords") or []
            if isinstance(kw, list):
                kw_text = ", ".join(str(k) for k in kw if k)
            else:
                kw_text = str(kw) if kw else None

            values = dict(
                collabnet_id=collabnet_id,
                title=title,
                abstract=data.get("abstract"),
                keywords=kw_text,
                doi=data.get("doi"),
                uri=data.get("uri") or data.get("url"),
                resource_type=data.get("resource_type") or data.get("resourceType") or data.get("type"),
                publication_date=str(data.get("publication_date") or data.get("date") or "")[:50] or None,
                publication_info=data.get("publication_info") or data.get("publicationInfo") or data.get("journal"),
                affiliation=data.get("affiliation") or data.get("department"),
            )

            if pub:
                for k, v in values.items():
                    setattr(pub, k, v)
            else:
                pub = Publication(**values)
                session.add(pub)

            await session.flush()

            # Link authors to researchers
            author_details = data.get("author_details") or data.get("authors") or []
            for author in author_details:
                if isinstance(author, dict):
                    author_id = author.get("primary_id") or author.get("username") or author.get("id")
                    if author_id:
                        # Find researcher by primary_id
                        r_stmt = select(Researcher.id).where(Researcher.primary_id == str(author_id))
                        r_result = await session.execute(r_stmt)
                        r_row = r_result.first()
                        if r_row:
                            # Check if link already exists
                            existing = await session.execute(
                                text("SELECT id FROM researcher_publications WHERE researcher_id = :rid AND publication_id = :pid"),
                                {"rid": r_row[0], "pid": pub.id},
                            )
                            if not existing.first():
                                session.add(ResearcherPublication(researcher_id=r_row[0], publication_id=pub.id))

            return pub
        except Exception as e:
            logger.error(f"Error upserting publication: {e}", exc_info=True)
            return None

    async def _apply_summaries(self, session: AsyncSession, summaries: list[dict]) -> int:
        """Match summaries to researchers and update ai_summary field."""
        matched = 0
        for summary in summaries:
            try:
                # Try to match by primary_id / username first
                target_id = summary.get("primary_id") or summary.get("username") or summary.get("researcher_id")
                researcher = None

                if target_id:
                    stmt = select(Researcher).where(Researcher.primary_id == str(target_id))
                    result = await session.execute(stmt)
                    researcher = result.scalar_one_or_none()

                # Fall back to first_name + last_name matching
                if not researcher:
                    first = (summary.get("first_name") or "").strip()
                    last = (summary.get("last_name") or "").strip()
                    if first and last:
                        stmt = select(Researcher).where(
                            Researcher.first_name == first,
                            Researcher.last_name == last,
                        )
                        result = await session.execute(stmt)
                        researcher = result.scalar_one_or_none()

                # Fall back to full_name matching
                if not researcher:
                    name = summary.get("name") or summary.get("full_name") or summary.get("researcher_name") or ""
                    if not name:
                        first = (summary.get("first_name") or "").strip()
                        last = (summary.get("last_name") or "").strip()
                        if first or last:
                            name = f"{first} {last}".strip()
                    if name:
                        stmt = select(Researcher).where(Researcher.full_name == name)
                        result = await session.execute(stmt)
                        researcher = result.scalar_one_or_none()

                if researcher:
                    # Extract summary text from nested ai_summaries structure
                    raw_summary = ""
                    ai_summaries = summary.get("ai_summaries")
                    if isinstance(ai_summaries, dict):
                        # Concatenate responses from all sections (main_themes, methods, etc.)
                        parts = []
                        for section_key in ("main_themes", "methods", "impacts", "collaborations"):
                            section = ai_summaries.get(section_key)
                            if isinstance(section, dict):
                                resp = section.get("response", "")
                                if resp:
                                    parts.append(_strip_html(resp))
                        raw_summary = "\n\n".join(p for p in parts if p)
                    # Fall back to flat fields
                    if not raw_summary:
                        raw_summary = summary.get("summary") or summary.get("text") or summary.get("content") or ""
                        raw_summary = _strip_html(raw_summary) or ""

                    if raw_summary:
                        researcher.ai_summary = raw_summary
                        matched += 1

            except Exception as e:
                logger.error(f"Error applying summary: {e}")
                continue

        return matched

    async def full_sync(self):
        if self.is_syncing:
            logger.warning("Researcher sync already in progress")
            return

        self.is_syncing = True
        self._cancel_requested = False
        self._task = asyncio.current_task()
        self.sync_stats = {
            "started": datetime.utcnow().isoformat(),
            "type": "researcher_full",
            "phase": "researchers",
            "total": 0,
            "success": 0,
            "errors": 0,
            "researchers_synced": 0,
            "publications_synced": 0,
            "summaries_matched": 0,
            "last_error": None,
        }

        log_id = await self._create_sync_log("researcher_full")
        self._current_log_id = log_id

        try:
            # Phase 1: Fetch and upsert researchers
            logger.info("Researcher sync phase 1: fetching researchers...")
            self.sync_stats["phase"] = "researchers"
            await self._publish_stats()

            researchers_data = await collabnet_client.fetch_all_researchers(
                cancel_check=lambda: self._cancel_requested,
            )

            if self._cancel_requested:
                self.sync_stats["cancelled"] = True
                await self._finish_sync_log(log_id, "cancelled", self.sync_stats)
                return

            self.sync_stats["total"] = len(researchers_data)
            logger.info(f"Fetched {len(researchers_data)} researchers from CollabNet")

            for i, r_data in enumerate(researchers_data):
                if self._cancel_requested:
                    self.sync_stats["cancelled"] = True
                    await self._finish_sync_log(log_id, "cancelled", self.sync_stats)
                    return

                try:
                    async with async_session() as session:
                        async with session.begin():
                            result = await self._upsert_researcher(session, r_data)
                            if result:
                                self.sync_stats["success"] += 1
                                self.sync_stats["researchers_synced"] += 1
                            else:
                                self.sync_stats["errors"] += 1
                except Exception as e:
                    logger.error(f"Error upserting researcher: {e}")
                    self.sync_stats["errors"] += 1
                    self.sync_stats["last_error"] = str(e)[:200]

                if (i + 1) % 100 == 0:
                    await self._publish_stats()

            # Phase 2: Fetch and upsert publications
            logger.info("Researcher sync phase 2: fetching publications...")
            self.sync_stats["phase"] = "publications"
            await self._publish_stats()

            documents_data = await collabnet_client.fetch_all_documents(
                cancel_check=lambda: self._cancel_requested,
            )

            if self._cancel_requested:
                self.sync_stats["cancelled"] = True
                await self._finish_sync_log(log_id, "cancelled", self.sync_stats)
                return

            self.sync_stats["total"] += len(documents_data)
            logger.info(f"Fetched {len(documents_data)} documents from CollabNet")

            for i, doc_data in enumerate(documents_data):
                if self._cancel_requested:
                    self.sync_stats["cancelled"] = True
                    await self._finish_sync_log(log_id, "cancelled", self.sync_stats)
                    return

                try:
                    async with async_session() as session:
                        async with session.begin():
                            result = await self._upsert_publication(session, doc_data)
                            if result:
                                self.sync_stats["success"] += 1
                                self.sync_stats["publications_synced"] += 1
                            else:
                                self.sync_stats["errors"] += 1
                except Exception as e:
                    logger.error(f"Error upserting publication: {e}")
                    self.sync_stats["errors"] += 1
                    self.sync_stats["last_error"] = str(e)[:200]

                if (i + 1) % 500 == 0:
                    await self._publish_stats()

            # Phase 3: Fetch and apply AI summaries
            logger.info("Researcher sync phase 3: fetching AI summaries...")
            self.sync_stats["phase"] = "summaries"
            await self._publish_stats()

            summaries_data = await collabnet_client.fetch_all_summaries(
                cancel_check=lambda: self._cancel_requested,
            )

            if self._cancel_requested:
                self.sync_stats["cancelled"] = True
                await self._finish_sync_log(log_id, "cancelled", self.sync_stats)
                return

            logger.info(f"Fetched {len(summaries_data)} summaries from CollabNet")

            try:
                async with async_session() as session:
                    async with session.begin():
                        matched = await self._apply_summaries(session, summaries_data)
                        self.sync_stats["summaries_matched"] = matched
                        self.sync_stats["success"] += matched
            except Exception as e:
                logger.error(f"Error applying summaries: {e}")
                self.sync_stats["last_error"] = str(e)[:200]

            # Invalidate caches
            await cache_service.invalidate_all()

            # Reset chat schema cache so new tables are discovered
            try:
                from app.services.chat_service import chat_service
                chat_service._schema_cache = None
            except Exception:
                pass

            self.last_sync = datetime.utcnow()
            self.sync_stats["completed"] = self.last_sync.isoformat()
            self.sync_stats["phase"] = "done"
            logger.info(f"Researcher sync completed: {self.sync_stats}")
            await self._finish_sync_log(log_id, "completed", self.sync_stats)

        except asyncio.CancelledError:
            logger.info("Researcher sync cancelled via task cancellation")
            self.sync_stats["cancelled"] = True
            try:
                await self._finish_sync_log(log_id, "cancelled", self.sync_stats, "Cancelled by user")
            except Exception:
                pass
        except Exception as e:
            logger.error(f"Researcher sync failed: {e}", exc_info=True)
            self.sync_stats["error"] = str(e)
            self.sync_stats["last_error"] = str(e)[:200]
            await self._finish_sync_log(log_id, "failed", self.sync_stats, str(e))
        finally:
            self.is_syncing = False
            self._cancel_requested = False
            self._current_log_id = None
            self._task = None
            await self._publish_stats()


researcher_sync_service = ResearcherSyncService()

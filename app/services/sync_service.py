import asyncio
import logging
import re
from datetime import datetime, date

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session
from app.models import (
    Agency, Opportunity, OpportunityApplicantType,
    OpportunityFundingInstrument, OpportunityFundingCategory, OpportunityALN,
)
from app.models.sync_log import SyncLog
from app.services.grants_client import GrantsGovClient
from app.services.cache_service import cache_service

logger = logging.getLogger(__name__)

TEAM_KEYWORDS = re.compile(
    r"\b(team|collaborative|co-pi|multi-pi|co-investigator)\b", re.IGNORECASE
)
MULTI_INST_KEYWORDS = re.compile(
    r"\b(multi-institutional|subaward|consortium|sub-award|subcontract)\b", re.IGNORECASE
)
MULTI_JURIS_KEYWORDS = re.compile(
    r"\b(multi-state|multi-jurisdiction|interstate|inter-state)\b", re.IGNORECASE
)


class SyncService:
    def __init__(self):
        self.client = GrantsGovClient()
        self.is_syncing = False
        self.last_sync: datetime | None = None
        self.sync_stats: dict = {}
        self._cancel_requested = False
        self._current_log_id: int | None = None

    def _parse_date(self, date_str: str | None) -> date | None:
        if not date_str:
            return None
        try:
            # Grants.gov uses MM/DD/YYYY format
            return datetime.strptime(date_str, "%m/%d/%Y").date()
        except (ValueError, TypeError):
            try:
                return datetime.strptime(date_str, "%Y-%m-%d").date()
            except (ValueError, TypeError):
                return None

    def _parse_decimal(self, val) -> float | None:
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def _classify_opportunity(self, description: str | None, num_categories: int) -> dict:
        desc = description or ""
        return {
            "is_multi_disciplinary": num_categories >= 2,
            "is_team_based": bool(TEAM_KEYWORDS.search(desc)),
            "is_multi_institution": bool(MULTI_INST_KEYWORDS.search(desc)),
            "is_multi_jurisdiction": bool(MULTI_JURIS_KEYWORDS.search(desc)),
        }

    async def _upsert_agency(self, session: AsyncSession, agency_code: str, agency_name: str):
        if not agency_code:
            return
        existing = await session.get(Agency, agency_code)
        if not existing:
            session.add(Agency(code=agency_code, name=agency_name or agency_code))

    def _parse_grants_date(self, date_str: str | None) -> date | None:
        """Parse various date formats from Grants.gov API."""
        if not date_str or date_str == "none":
            return None
        # Try MM/DD/YYYY (search results format)
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%Y-%m-%d-%H-%M-%S"):
            try:
                return datetime.strptime(date_str, fmt).date()
            except (ValueError, TypeError):
                continue
        # Try "Mon DD, YYYY HH:MM:SS AM/PM TZ" format from synopsis
        try:
            # Strip timezone abbreviation
            parts = date_str.rsplit(" ", 1)
            if len(parts) == 2:
                return datetime.strptime(parts[0], "%b %d, %Y %I:%M:%S %p").date()
        except (ValueError, TypeError):
            pass
        return None

    async def _create_sync_log(self, sync_type: str) -> int:
        """Create a sync_log row and return its id."""
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
        """Update sync_log row on completion."""
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

    def cancel_sync(self):
        """Request cancellation of the running sync."""
        if self.is_syncing:
            self._cancel_requested = True
            return True
        return False

    async def _upsert_opportunity(self, session: AsyncSession, detail: dict) -> Opportunity | None:
        try:
            # fetchOpportunity response structure - data IS the opportunity
            opp_id = detail.get("id")
            if not opp_id:
                logger.warning("No opportunity ID found in detail response")
                return None

            opp_id = int(opp_id)

            # Agency info from agencyDetails
            agency_details = detail.get("agencyDetails", {}) or {}
            agency_code = detail.get("owningAgencyCode") or agency_details.get("agencyCode", "")
            agency_name = agency_details.get("agencyName", "")
            if agency_code:
                await self._upsert_agency(session, agency_code, agency_name)

            # Also upsert top-level agency if different
            top_agency = detail.get("topAgencyDetails", {}) or {}
            top_code = top_agency.get("agencyCode", "")
            if top_code and top_code != agency_code:
                await self._upsert_agency(session, top_code, top_agency.get("agencyName", ""))
                # Set parent relationship
                if agency_code:
                    existing = await session.get(Agency, agency_code)
                    if existing and not existing.parent_agency_code:
                        existing.parent_agency_code = top_code

            # Look for existing
            stmt = select(Opportunity).where(Opportunity.opportunity_id == opp_id)
            result = await session.execute(stmt)
            opp = result.scalar_one_or_none()

            synopsis = detail.get("synopsis", {}) or {}
            description = synopsis.get("synopsisDesc", "") or ""

            # Funding categories from synopsis for classification
            categories = synopsis.get("fundingActivityCategories", []) or []
            classification = self._classify_opportunity(description, len(categories))

            # Contact info from synopsis
            contact_name = synopsis.get("agencyContactName")
            contact_email = synopsis.get("agencyContactEmail")
            contact_phone = synopsis.get("agencyContactPhone")

            # Status: 'ost' field contains "POSTED", "FORECASTED", etc.
            status_val = (detail.get("ost") or "posted").lower()

            title = detail.get("opportunityTitle") or "Untitled"

            # Category
            opp_category = detail.get("opportunityCategory", {}) or {}

            # Dates: posting from synopsis, close from responseDateDesc or search hit
            posting_date = self._parse_grants_date(
                synopsis.get("postingDateStr") or synopsis.get("postingDate")
            )
            close_date_desc = detail.get("originalDueDateDesc") or synopsis.get("responseDateDesc")
            # closeDate from search results is MM/DD/YYYY format
            close_date = self._parse_date(detail.get("_search_close_date")) or self._parse_date(close_date_desc)
            archive_date = self._parse_grants_date(synopsis.get("archiveDateStr"))

            # Award info from synopsis - these come as strings
            award_ceiling_raw = synopsis.get("awardCeiling")
            if award_ceiling_raw == "none":
                award_ceiling_raw = None
            award_floor_raw = synopsis.get("awardFloor")
            if award_floor_raw == "none":
                award_floor_raw = None

            # Funding instruments from synopsis
            instruments = synopsis.get("fundingInstruments", []) or []
            fi_desc = ", ".join(fi.get("description", "") for fi in instruments) if instruments else None

            values = dict(
                opportunity_id=opp_id,
                opportunity_number=detail.get("opportunityNumber"),
                title=title,
                status=status_val,
                category=opp_category.get("category"),
                category_explanation=opp_category.get("description"),
                agency_code=agency_code or None,
                posting_date=posting_date,
                close_date=close_date,
                close_date_description=close_date_desc,
                archive_date=archive_date,
                award_ceiling=self._parse_decimal(award_ceiling_raw),
                award_floor=self._parse_decimal(award_floor_raw),
                estimated_total_funding=self._parse_decimal(synopsis.get("estimatedFunding")),
                expected_number_of_awards=self._parse_decimal(synopsis.get("numberOfAwards")),
                cost_sharing=synopsis.get("costSharing"),
                synopsis_description=description[:65000] if description else None,
                contact_name=contact_name,
                contact_email=contact_email,
                contact_phone=contact_phone,
                funding_instrument_type=instruments[0].get("id") if instruments else None,
                funding_instrument_description=fi_desc,
                grants_gov_url=f"https://www.grants.gov/search-results-detail/{opp_id}",
                last_synced_at=datetime.utcnow(),
                **classification,
            )

            if opp:
                for k, v in values.items():
                    setattr(opp, k, v)
            else:
                opp = Opportunity(**values)
                session.add(opp)

            await session.flush()

            # Upsert association tables - delete and recreate
            for cls in [OpportunityApplicantType, OpportunityFundingInstrument, OpportunityFundingCategory, OpportunityALN]:
                await session.execute(
                    text(f"DELETE FROM {cls.__tablename__} WHERE opportunity_id = :oid"),
                    {"oid": opp.id},
                )

            # Applicant types from synopsis
            for at in (synopsis.get("applicantTypes", []) or []):
                session.add(OpportunityApplicantType(
                    opportunity_id=opp.id,
                    type_code=str(at.get("id", "")),
                    type_name=at.get("description", ""),
                ))

            # Funding instruments from synopsis
            for fi in instruments:
                session.add(OpportunityFundingInstrument(
                    opportunity_id=opp.id,
                    instrument_code=str(fi.get("id", "")),
                    instrument_name=fi.get("description", ""),
                ))

            # Funding categories from synopsis
            for fc in categories:
                session.add(OpportunityFundingCategory(
                    opportunity_id=opp.id,
                    category_code=str(fc.get("id", "")),
                    category_name=fc.get("description", ""),
                ))

            # ALNs/CFDAs from top-level
            for aln in (detail.get("cfdas", []) or []):
                aln_num = str(aln.get("cfdaNumber", ""))
                session.add(OpportunityALN(
                    opportunity_id=opp.id,
                    aln_number=aln_num,
                    program_title=aln.get("programTitle"),
                ))

            return opp
        except Exception as e:
            logger.error(f"Error upserting opportunity: {e}", exc_info=True)
            return None

    async def full_sync(self):
        if self.is_syncing:
            logger.warning("Sync already in progress")
            return

        self.is_syncing = True
        self._cancel_requested = False
        self.sync_stats = {
            "started": datetime.utcnow().isoformat(),
            "type": "full",
            "total": 0,
            "success": 0,
            "errors": 0,
            "current_batch": 0,
            "total_batches": 0,
            "last_error": None,
            "errors_list": [],
        }

        log_id = await self._create_sync_log("full")
        self._current_log_id = log_id

        try:
            logger.info("Starting full sync from Grants.gov...")
            items = await self.client.fetch_all_opportunities()
            self.sync_stats["total"] = len(items)
            logger.info(f"Found {len(items)} opportunities to sync")

            # Build a map of search hit close dates
            close_dates = {}
            for item in items:
                oid = item.get("id")
                cd = item.get("closeDate")
                if oid and cd:
                    close_dates[int(oid)] = cd

            batch_size = 50
            total_batches = (len(items) + batch_size - 1) // batch_size
            self.sync_stats["total_batches"] = total_batches

            for i in range(0, len(items), batch_size):
                if self._cancel_requested:
                    logger.info("Sync cancelled by user")
                    self.sync_stats["cancelled"] = True
                    await self._finish_sync_log(log_id, "cancelled", self.sync_stats)
                    return

                batch = items[i:i + batch_size]
                self.sync_stats["current_batch"] = i // batch_size + 1

                # Fetch details concurrently (bounded by semaphore)
                detail_tasks = []
                for item in batch:
                    opp_id = item.get("id")
                    if opp_id:
                        detail_tasks.append(
                            self._fetch_and_upsert(int(opp_id), close_dates.get(int(opp_id)))
                        )

                await asyncio.gather(*detail_tasks, return_exceptions=True)
                logger.info(f"Synced batch {i // batch_size + 1}/{total_batches}, progress: {min(i + batch_size, len(items))}/{len(items)}")

            await cache_service.invalidate_all()
            self.last_sync = datetime.utcnow()
            self.sync_stats["completed"] = self.last_sync.isoformat()
            logger.info(f"Full sync completed: {self.sync_stats}")
            await self._finish_sync_log(log_id, "completed", self.sync_stats)

        except Exception as e:
            logger.error(f"Full sync failed: {e}", exc_info=True)
            self.sync_stats["error"] = str(e)
            self.sync_stats["last_error"] = str(e)
            await self._finish_sync_log(log_id, "failed", self.sync_stats, str(e))
        finally:
            self.is_syncing = False
            self._cancel_requested = False
            self._current_log_id = None

    async def _fetch_and_upsert(self, opp_id: int, close_date_str: str | None = None):
        try:
            detail = await self.client.fetch_opportunity(opp_id)
            # Inject closeDate from search results if available
            if close_date_str:
                detail["_search_close_date"] = close_date_str
            async with async_session() as session:
                async with session.begin():
                    result = await self._upsert_opportunity(session, detail)
                    if result:
                        self.sync_stats["success"] = self.sync_stats.get("success", 0) + 1
                    else:
                        self.sync_stats["errors"] = self.sync_stats.get("errors", 0) + 1
                        self._add_error(opp_id, "Upsert returned None")
        except Exception as e:
            logger.error(f"Error fetching/upserting opportunity {opp_id}: {e}")
            self.sync_stats["errors"] = self.sync_stats.get("errors", 0) + 1
            self._add_error(opp_id, str(e))

    def _add_error(self, opp_id: int, message: str):
        errors_list = self.sync_stats.get("errors_list", [])
        errors_list.append({"opp_id": opp_id, "message": message[:200]})
        if len(errors_list) > 20:
            errors_list = errors_list[-20:]
        self.sync_stats["errors_list"] = errors_list
        self.sync_stats["last_error"] = f"Opp {opp_id}: {message[:200]}"

    async def incremental_sync(self):
        """Fetch only recently changed opportunities."""
        if self.is_syncing:
            logger.warning("Sync already in progress")
            return

        self.is_syncing = True
        self._cancel_requested = False
        self.sync_stats = {
            "started": datetime.utcnow().isoformat(),
            "type": "incremental",
            "total": 0,
            "success": 0,
            "errors": 0,
            "current_batch": 0,
            "total_batches": 10,
            "last_error": None,
            "errors_list": [],
        }

        log_id = await self._create_sync_log("incremental")
        self._current_log_id = log_id

        try:
            logger.info("Starting incremental sync...")
            # Fetch first few pages of recently posted/forecasted
            for page in range(1, 11):  # First 10 pages = 250 opportunities
                if self._cancel_requested:
                    logger.info("Sync cancelled by user")
                    self.sync_stats["cancelled"] = True
                    await self._finish_sync_log(log_id, "cancelled", self.sync_stats)
                    return

                self.sync_stats["current_batch"] = page

                result = await self.client.search(page_number=page, rows_per_page=25)
                items = result.get("oppHits", [])
                if not items:
                    break

                self.sync_stats["total"] += len(items)

                for item in items:
                    opp_id = item.get("id")
                    if opp_id:
                        await self._fetch_and_upsert(int(opp_id), item.get("closeDate"))

            # Mark past-deadline opportunities as closed
            async with async_session() as session:
                async with session.begin():
                    await session.execute(
                        text(
                            "UPDATE opportunities SET status = 'closed' "
                            "WHERE status = 'posted' AND close_date < CURDATE()"
                        )
                    )

            await cache_service.invalidate_all()
            self.last_sync = datetime.utcnow()
            self.sync_stats["completed"] = self.last_sync.isoformat()
            logger.info(f"Incremental sync completed: {self.sync_stats}")
            await self._finish_sync_log(log_id, "completed", self.sync_stats)

        except Exception as e:
            logger.error(f"Incremental sync failed: {e}", exc_info=True)
            self.sync_stats["error"] = str(e)
            self.sync_stats["last_error"] = str(e)
            await self._finish_sync_log(log_id, "failed", self.sync_stats, str(e))
        finally:
            self.is_syncing = False
            self._cancel_requested = False
            self._current_log_id = None


sync_service = SyncService()

import asyncio
import json
import logging
import os
from datetime import datetime

from app.services.cache_service import cache_service

logger = logging.getLogger(__name__)

REDIS_PIPELINE_KEY = "pf:pipeline_state"
REDIS_PIPELINE_TTL = 3600


def _empty_phases() -> list[dict]:
    return [
        {"phase": 1, "name": "Discovery", "status": "idle", "total": 0, "processed": 0, "errors": 0, "detail": "", "error_log": []},
        {"phase": 2, "name": "Download", "status": "idle", "total": 0, "processed": 0, "errors": 0, "detail": "", "error_log": []},
        {"phase": 3, "name": "Retrieve", "status": "idle", "total": 0, "processed": 0, "errors": 0, "detail": "", "error_log": []},
        {"phase": 4, "name": "Extract", "status": "idle", "total": 0, "processed": 0, "errors": 0, "detail": "", "error_log": []},
        {"phase": 5, "name": "Classify", "status": "idle", "total": 0, "processed": 0, "errors": 0, "detail": "", "error_log": []},
        {"phase": 6, "name": "Embed", "status": "idle", "total": 0, "processed": 0, "errors": 0, "detail": "", "error_log": []},
    ]


def _log_error(phase: dict, msg: str):
    """Append an error message to a phase's error_log (max 50 entries)."""
    phase["error_log"].append(msg)
    if len(phase["error_log"]) > 50:
        phase["error_log"] = phase["error_log"][-50:]


class PipelineService:
    def __init__(self):
        self.is_running = False
        self._cancel_requested = False
        self._task: asyncio.Task | None = None
        self.state: dict = {
            "is_running": False,
            "started_at": None,
            "current_phase": None,
            "config": {"types": []},
            "phases": _empty_phases(),
        }

    async def clear_stale_state(self):
        """Clear any stale 'running' pipeline state in Redis (call on startup)."""
        try:
            shared = await cache_service.get(REDIS_PIPELINE_KEY)
            if shared and shared.get("is_running"):
                logger.info("Clearing stale pipeline state from previous run")
                shared["is_running"] = False
                shared["current_phase"] = None
                for p in shared.get("phases", []):
                    if p.get("status") == "running":
                        p["status"] = "failed"
                        p["detail"] = "Interrupted (server restart)"
                await cache_service.set(REDIS_PIPELINE_KEY, shared, REDIS_PIPELINE_TTL)
        except Exception:
            pass

    async def _publish_state(self):
        try:
            await cache_service.set(REDIS_PIPELINE_KEY, self.state, REDIS_PIPELINE_TTL)
        except Exception:
            pass

    async def get_status(self) -> dict:
        """Read pipeline state from Redis (cross-worker safe)."""
        try:
            shared = await cache_service.get(REDIS_PIPELINE_KEY)
            if shared:
                # If Redis says running but this worker isn't running anything,
                # it's stale from a previous container — don't lie to the UI
                if shared.get("is_running") and not self.is_running:
                    shared["_possibly_stale"] = True
                return shared
        except Exception:
            pass
        return self.state

    async def start(self, types: list[str]):
        if self.is_running:
            raise RuntimeError("Pipeline already running")

        self.is_running = True
        self._cancel_requested = False
        self.state = {
            "is_running": True,
            "started_at": datetime.utcnow().isoformat(),
            "current_phase": 1,
            "config": {"types": types},
            "phases": _empty_phases(),
        }
        # Mark phases as pending
        for p in self.state["phases"]:
            p["status"] = "pending"
        await self._publish_state()

        # Nuke all existing opportunities so we get a clean sync
        await self._nuke_all_opportunities()

        self._task = asyncio.create_task(self._run(types))

    async def _nuke_all_opportunities(self):
        """Delete ALL opportunities, documents, chunks, and related data for a clean re-sync."""
        import shutil
        from app.database import async_session
        from app.config import settings as app_settings
        from sqlalchemy import text

        logger.info("Nuking all opportunities and documents for clean re-sync...")

        # Clear ChromaDB collection
        try:
            import chromadb
            chroma_client = chromadb.HttpClient(
                host=app_settings.CHROMADB_HOST,
                port=app_settings.CHROMADB_PORT,
            )
            try:
                chroma_client.delete_collection("opportunity_documents")
                logger.info("Cleared ChromaDB collection")
            except Exception:
                pass  # Collection may not exist yet
        except Exception as e:
            logger.warning(f"Could not clear ChromaDB: {e}")

        # Delete all DB records (CASCADE handles child tables)
        async with async_session() as session:
            async with session.begin():
                await session.execute(text("DELETE FROM document_chunks"))
                await session.execute(text("DELETE FROM opportunity_documents"))
                await session.execute(text("DELETE FROM opportunity_funding_categories"))
                await session.execute(text("DELETE FROM opportunity_applicant_types"))
                await session.execute(text("DELETE FROM opportunity_funding_instruments"))
                await session.execute(text("DELETE FROM opportunity_alns"))
                await session.execute(text("DELETE FROM opportunities"))
        logger.info("Deleted all opportunity data from database")

        # Wipe downloaded document files
        doc_path = app_settings.DOCUMENT_STORAGE_PATH
        if os.path.exists(doc_path):
            shutil.rmtree(doc_path, ignore_errors=True)
            os.makedirs(doc_path, exist_ok=True)
            logger.info(f"Cleared document storage at {doc_path}")

        # Invalidate all caches
        from app.services.cache_service import cache_service
        await cache_service.invalidate_all()
        logger.info("Nuke complete — ready for fresh sync")

    async def cancel(self):
        if self.is_running:
            self._cancel_requested = True
            # Also cancel the underlying services
            from app.services.sync_service import sync_service
            from app.services.document_service import document_service
            sync_service.cancel_sync()
            document_service._cancel_requested = True
            if self._task and not self._task.done():
                self._task.cancel()
        else:
            # Nothing running on this worker — clear stale Redis state
            try:
                shared = await cache_service.get(REDIS_PIPELINE_KEY)
                if shared and shared.get("is_running"):
                    shared["is_running"] = False
                    shared["current_phase"] = None
                    for p in shared.get("phases", []):
                        if p.get("status") == "running":
                            p["status"] = "failed"
                            p["detail"] = "Interrupted (server restart)"
                    await cache_service.set(REDIS_PIPELINE_KEY, shared, REDIS_PIPELINE_TTL)
            except Exception:
                pass

    def _phase(self, idx: int) -> dict:
        return self.state["phases"][idx]

    async def _run(self, types: list[str]):
        try:
            await self._phase_1_discovery(types)
            if self._cancel_requested:
                return
            await self._phase_2_download()
            if self._cancel_requested:
                return
            await self._phase_3_retrieve()
            if self._cancel_requested:
                return
            await self._phase_4_extract()
            if self._cancel_requested:
                return
            await self._phase_5_classify()
            if self._cancel_requested:
                return
            await self._phase_6_embed()
        except asyncio.CancelledError:
            logger.info("Pipeline cancelled")
            for p in self.state["phases"]:
                if p["status"] == "running":
                    p["status"] = "failed"
                    p["detail"] = "Cancelled"
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            for p in self.state["phases"]:
                if p["status"] == "running":
                    p["status"] = "failed"
                    p["detail"] = f"Fatal error: {str(e)[:200]}"
                    _log_error(p, f"Fatal: {str(e)[:500]}")
        finally:
            self.is_running = False
            self._cancel_requested = False
            self._task = None
            self.state["is_running"] = False
            self.state["current_phase"] = None
            await self._publish_state()

    async def _phase_1_discovery(self, types: list[str]):
        """Phase 1: Sync opportunities from Grants.gov."""
        phase = self._phase(0)
        phase["status"] = "running"
        self.state["current_phase"] = 1
        await self._publish_state()

        from app.services.sync_service import sync_service

        # Hook into sync_service progress
        original_publish = sync_service._publish_stats

        async def _progress_hook():
            await original_publish()
            stats = sync_service.sync_stats
            if stats.get("phase") == "listing":
                phase["detail"] = f"Listing: {stats.get('listing_fetched', 0)} found"
                phase["total"] = stats.get("listing_estimated", 0)
                phase["processed"] = stats.get("listing_fetched", 0)
            elif stats.get("phase") == "fetching":
                phase["detail"] = f"Fetching batch {stats.get('current_batch', 0)}/{stats.get('total_batches', 0)}"
                phase["total"] = stats.get("total", 0)
                phase["processed"] = stats.get("success", 0) + stats.get("skipped", 0)
            phase["errors"] = stats.get("errors", 0)
            await self._publish_state()

        sync_service._publish_stats = _progress_hook
        try:
            await sync_service.full_sync(opp_types=types)
        finally:
            sync_service._publish_stats = original_publish

        if self._cancel_requested:
            phase["status"] = "failed"
            phase["detail"] = "Cancelled"
        else:
            phase["status"] = "completed"
            stats = sync_service.sync_stats
            phase["processed"] = stats.get("success", 0)
            phase["total"] = stats.get("total", 0)
            phase["errors"] = stats.get("errors", 0)
        await self._publish_state()

    async def _phase_2_download(self):
        """Phase 2: Download pending documents from Grants.gov."""
        phase = self._phase(1)
        phase["status"] = "running"
        self.state["current_phase"] = 2
        await self._publish_state()

        from app.database import async_session
        from app.models.document import OpportunityDocument
        from app.models.opportunity import Opportunity
        from app.services.document_service import document_service
        from app.services.settings_service import settings_service
        from sqlalchemy import select, or_
        from datetime import date

        async with async_session() as session:
            ocr_settings = await settings_service.get_ocr_settings(session)
            today = date.today()
            stmt = (
                select(OpportunityDocument)
                .join(Opportunity, OpportunityDocument.opportunity_id == Opportunity.id)
                .where(
                    Opportunity.status != "archived",
                    or_(Opportunity.close_date >= today, Opportunity.close_date.is_(None)),
                    OpportunityDocument.download_status == "pending",
                )
            )
            result = await session.execute(stmt)
            docs = list(result.scalars().all())

        phase["total"] = len(docs)
        await self._publish_state()

        num_workers = ocr_settings.get("doc_workers", 4)
        try:
            num_workers = int(num_workers)
        except (ValueError, TypeError):
            num_workers = 4
        semaphore = asyncio.Semaphore(num_workers)
        downloaded = 0
        errors = 0

        async def _dl_one(doc_ref):
            nonlocal downloaded, errors
            if self._cancel_requested:
                return
            async with semaphore:
                if self._cancel_requested:
                    return
                try:
                    async with async_session() as sess:
                        from sqlalchemy import select as sel
                        doc = (await sess.execute(
                            sel(OpportunityDocument).where(OpportunityDocument.id == doc_ref.id)
                        )).scalar_one_or_none()
                        if doc and doc.download_status == "pending":
                            await document_service._download_document(doc, sess)
                            await sess.commit()
                            if doc.download_status == "downloaded":
                                downloaded += 1
                            else:
                                errors += 1
                except Exception as e:
                    err_msg = f"Doc {doc_ref.id} ({getattr(doc_ref, 'file_name', '?')}): {str(e)[:200]}"
                    logger.error(f"Download error: {err_msg}")
                    _log_error(phase, err_msg)
                    errors += 1
                phase["processed"] = downloaded
                phase["errors"] = errors
                phase["detail"] = f"Downloaded {downloaded}/{phase['total']}, {errors} errors"
                if (downloaded + errors) % 5 == 0:
                    await self._publish_state()

        batch_size = max(num_workers * 3, 10)
        for i in range(0, len(docs), batch_size):
            if self._cancel_requested:
                break
            batch = docs[i:i + batch_size]
            await asyncio.gather(*[_dl_one(d) for d in batch])

        phase["status"] = "failed" if self._cancel_requested else "completed"
        if self._cancel_requested:
            phase["detail"] = "Cancelled"
        else:
            phase["detail"] = f"{downloaded} downloaded, {errors} errors"
        await self._publish_state()

    async def _phase_3_retrieve(self):
        """Phase 3: Retrieve linked documents (URL extraction + web search)."""
        phase = self._phase(2)
        phase["status"] = "running"
        self.state["current_phase"] = 3
        await self._publish_state()

        from app.services.document_service import document_service

        # --- Sub-phase 1: Link extraction ---
        document_service.is_processing = True
        document_service._cancel_requested = False
        document_service.processing_stats = {
            "started": datetime.utcnow().isoformat(),
            "total": 0, "scanned": 0, "downloaded": 0, "errors": 0,
            "phase": "extracting links",
        }

        link_docs = 0
        try:
            phase["detail"] = "Extracting links..."
            await self._publish_state()
            await document_service._inline_link_extraction()
            link_docs = document_service.processing_stats.get("downloaded", 0)
            link_scanned = document_service.processing_stats.get("scanned", 0)
            link_total = document_service.processing_stats.get("total", 0)
            phase["processed"] = link_docs
            phase["total"] = link_total
            phase["errors"] = document_service.processing_stats.get("errors", 0)
            phase["detail"] = f"Extracting links: {link_scanned}/{link_total} scanned, {link_docs} docs found"
            await self._publish_state()
        except Exception as e:
            logger.error(f"Phase 3 link extraction failed: {e}", exc_info=True)
            _log_error(phase, f"Link extraction: {str(e)[:300]}")
            phase["errors"] += 1
        finally:
            document_service.is_processing = False

        # --- Sub-phase 2: Synopsis doc creation ---
        # (already called inside _inline_link_extraction, but report progress)
        synopsis_created = document_service.processing_stats.get("downloaded", 0) - link_docs
        if synopsis_created > 0:
            phase["detail"] = f"Creating synopsis docs: {synopsis_created} created"
            await self._publish_state()

        # --- Sub-phase 3: Web search for solicitations ---
        if not self._cancel_requested:
            phase["detail"] = "Searching web for solicitations..."
            await self._publish_state()
            try:
                document_service.is_processing = True
                document_service._cancel_requested = False
                document_service.processing_stats = {
                    "started": datetime.utcnow().isoformat(),
                    "total": 0, "scanned": 0, "downloaded": 0, "errors": 0,
                    "phase": "web search",
                }
                from app.config import settings as app_settings
                if app_settings.BRAVE_API_KEY:
                    from app.database import async_session
                    from app.models.document import OpportunityDocument
                    from app.models.opportunity import Opportunity
                    from sqlalchemy import select, or_
                    from sqlalchemy.orm import aliased
                    from datetime import date
                    import httpx

                    async with async_session() as session:
                        today = date.today()
                        SolDoc = aliased(OpportunityDocument)
                        has_sol = (
                            select(SolDoc.opportunity_id)
                            .where(SolDoc.doc_category == "solicitation")
                            .correlate(Opportunity)
                        )
                        stmt = (
                            select(Opportunity)
                            .where(
                                Opportunity.status.notin_(["archived", "closed"]),
                                or_(Opportunity.close_date >= today, Opportunity.close_date.is_(None)),
                                ~Opportunity.id.in_(has_sol),
                            )
                        )
                        result = await session.execute(stmt)
                        all_opps = list(result.scalars().all())

                    total_opps = len(all_opps)
                    web_found = 0
                    web_searched = 0
                    phase["detail"] = f"Web search: 0/{total_opps} searched, 0 PDFs found"
                    await self._publish_state()

                    search_semaphore = asyncio.Semaphore(4)

                    async def _search_one(opp):
                        nonlocal web_found, web_searched
                        if self._cancel_requested:
                            return
                        async with search_semaphore:
                            if self._cancel_requested:
                                return
                            try:
                                async with httpx.AsyncClient(
                                    follow_redirects=True, timeout=30.0,
                                    headers={"User-Agent": "ProposalForge/1.0"},
                                ) as client:
                                    async with async_session() as sess:
                                        found = await document_service._web_search_for_opportunity(opp, sess, client)
                                        if found > 0:
                                            await sess.commit()
                                            web_found += found
                                            phase["processed"] = link_docs + web_found
                            except Exception as e:
                                err_msg = f"Web search opp {getattr(opp, 'opportunity_id', '?')}: {str(e)[:150]}"
                                logger.warning(err_msg)
                                _log_error(phase, err_msg)
                                phase["errors"] += 1
                            web_searched += 1
                            if web_searched % 10 == 0:
                                phase["detail"] = f"Web search: {web_searched}/{total_opps} searched, {web_found} PDFs found"
                                await self._publish_state()

                    batch_size = 50
                    for batch_start in range(0, total_opps, batch_size):
                        if self._cancel_requested:
                            break
                        batch = all_opps[batch_start:batch_start + batch_size]
                        await asyncio.gather(*[_search_one(opp) for opp in batch])
                        phase["detail"] = f"Web search: {web_searched}/{total_opps} searched, {web_found} PDFs found"
                        await self._publish_state()
            except Exception as e:
                logger.error(f"Phase 3 web search failed: {e}", exc_info=True)
            finally:
                document_service.is_processing = False

        phase["status"] = "failed" if self._cancel_requested else "completed"
        if self._cancel_requested:
            phase["detail"] = "Cancelled"
        await self._publish_state()

    async def _phase_4_extract(self):
        """Phase 4: OCR / text extraction on downloaded documents."""
        phase = self._phase(3)
        phase["status"] = "running"
        self.state["current_phase"] = 4
        await self._publish_state()

        from app.database import async_session
        from app.models.document import OpportunityDocument
        from app.models.opportunity import Opportunity
        from app.services.document_service import document_service
        from app.services.settings_service import settings_service
        from sqlalchemy import select, or_
        from datetime import date
        import httpx

        async with async_session() as session:
            ocr_settings = await settings_service.get_ocr_settings(session)
            today = date.today()
            stmt = (
                select(OpportunityDocument)
                .join(Opportunity, OpportunityDocument.opportunity_id == Opportunity.id)
                .where(
                    Opportunity.status != "archived",
                    or_(Opportunity.close_date >= today, Opportunity.close_date.is_(None)),
                    OpportunityDocument.download_status == "downloaded",
                    OpportunityDocument.ocr_status == "pending",
                )
            )
            result = await session.execute(stmt)
            docs = list(result.scalars().all())

        phase["total"] = len(docs)
        await self._publish_state()

        # Process docs ONE AT A TIME to isolate crashes (pymupdf can segfault on corrupt PDFs)
        extracted = 0
        errors = 0

        ocr_client = httpx.AsyncClient(timeout=300.0)

        try:
            for i, doc_ref in enumerate(docs):
                if self._cancel_requested:
                    break
                doc_name = getattr(doc_ref, 'file_name', None) or f"doc-{doc_ref.id}"
                phase["detail"] = f"Extracting {i+1}/{len(docs)}: {doc_name[:60]}"
                if i % 5 == 0:
                    await self._publish_state()

                try:
                    async with async_session() as sess:
                        from sqlalchemy import select as sel
                        doc = (await sess.execute(
                            sel(OpportunityDocument).where(OpportunityDocument.id == doc_ref.id)
                        )).scalar_one_or_none()
                        if doc and doc.ocr_status == "pending":
                            await document_service._ocr_document(doc, ocr_settings, sess, ocr_client=ocr_client)
                            # Skip classification/embedding for unsupported formats
                            if doc.ocr_status == "skipped":
                                if doc.classify_status == "pending":
                                    doc.classify_status = "skipped"
                                if doc.embed_status == "pending":
                                    doc.embed_status = "skipped"
                            await sess.commit()
                            if doc.ocr_status in ("completed", "skipped"):
                                extracted += 1
                                logger.info(f"Extracted doc {doc.id} ({doc_name}): {doc.ocr_status}, {doc.extracted_text_length or 0} chars")
                            else:
                                errors += 1
                                err_msg = f"Doc {doc.id} ({doc_name}): status={doc.ocr_status}, {doc.error_message or 'unknown'}"
                                logger.warning(f"Extract failed: {err_msg}")
                                _log_error(phase, err_msg)
                        else:
                            extracted += 1  # already processed or missing
                except Exception as e:
                    errors += 1
                    err_msg = f"Doc {doc_ref.id} ({doc_name}): {str(e)[:300]}"
                    logger.error(f"Extract exception: {err_msg}", exc_info=True)
                    _log_error(phase, err_msg)
                    # Mark as failed in DB so we don't retry forever
                    try:
                        async with async_session() as sess:
                            from sqlalchemy import select as sel
                            doc = (await sess.execute(
                                sel(OpportunityDocument).where(OpportunityDocument.id == doc_ref.id)
                            )).scalar_one_or_none()
                            if doc:
                                doc.ocr_status = "failed"
                                doc.error_message = f"Exception: {str(e)[:500]}"
                                await sess.commit()
                    except Exception:
                        pass  # best effort

                phase["processed"] = extracted
                phase["errors"] = errors
        finally:
            await ocr_client.aclose()

        phase["status"] = "failed" if self._cancel_requested else "completed"
        if self._cancel_requested:
            phase["detail"] = "Cancelled"
        else:
            phase["detail"] = f"{extracted} extracted, {errors} errors"
        await self._publish_state()

    async def _phase_5_classify(self):
        """Phase 5: Classify documents (heuristic + LLM fallback)."""
        phase = self._phase(4)
        phase["status"] = "running"
        self.state["current_phase"] = 5
        await self._publish_state()

        from app.database import async_session
        from app.models.document import OpportunityDocument
        from app.models.opportunity import Opportunity
        from app.services.document_service import document_service
        from app.services.settings_service import settings_service
        from sqlalchemy import select, or_
        from datetime import date

        async with async_session() as session:
            llm_settings = await settings_service.get_llm_settings(session)
            today = date.today()
            stmt = (
                select(OpportunityDocument)
                .join(Opportunity, OpportunityDocument.opportunity_id == Opportunity.id)
                .where(
                    Opportunity.status != "archived",
                    or_(Opportunity.close_date >= today, Opportunity.close_date.is_(None)),
                    OpportunityDocument.ocr_status == "completed",
                    OpportunityDocument.classify_status == "pending",
                )
            )
            result = await session.execute(stmt)
            docs = list(result.scalars().all())

        phase["total"] = len(docs)
        await self._publish_state()

        from openai import AsyncOpenAI
        llm_base = llm_settings.get("base_url", "")
        llm_key = llm_settings.get("api_key", "")
        llm_client = AsyncOpenAI(base_url=llm_base, api_key=llm_key or "not-needed") if llm_base else None

        classified = 0
        errors = 0
        semaphore = asyncio.Semaphore(4)

        async def _classify_one(doc_ref):
            nonlocal classified, errors
            if self._cancel_requested:
                return
            async with semaphore:
                if self._cancel_requested:
                    return
                try:
                    async with async_session() as sess:
                        from sqlalchemy import select as sel
                        doc = (await sess.execute(
                            sel(OpportunityDocument).where(OpportunityDocument.id == doc_ref.id)
                        )).scalar_one_or_none()
                        if doc and doc.classify_status == "pending":
                            await document_service._classify_document(
                                doc, llm_settings, sess, llm_client=llm_client
                            )
                            await sess.commit()
                            classified += 1
                except Exception as e:
                    err_msg = f"Doc {doc_ref.id}: {str(e)[:200]}"
                    logger.error(f"Classify error: {err_msg}")
                    _log_error(phase, err_msg)
                    errors += 1
                phase["processed"] = classified
                phase["errors"] = errors
                phase["detail"] = f"Classified {classified}/{phase['total']}, {errors} errors"
                if classified % 10 == 0:
                    await self._publish_state()

        batch_size = 20
        for i in range(0, len(docs), batch_size):
            if self._cancel_requested:
                break
            batch = docs[i:i + batch_size]
            await asyncio.gather(*[_classify_one(d) for d in batch])

        phase["status"] = "failed" if self._cancel_requested else "completed"
        if self._cancel_requested:
            phase["detail"] = "Cancelled"
        else:
            phase["detail"] = f"{classified} classified, {errors} errors"
        await self._publish_state()

    async def _phase_6_embed(self):
        """Phase 6: Embed solicitation documents (RFP/RFA/NOFO/FOA) into ChromaDB."""
        phase = self._phase(5)
        phase["status"] = "running"
        self.state["current_phase"] = 6
        await self._publish_state()

        from app.database import async_session
        from app.models.document import OpportunityDocument, DocumentChunk
        from app.models.opportunity import Opportunity
        from app.services.document_service import document_service
        from app.services.settings_service import settings_service
        from app.config import settings as app_settings
        from sqlalchemy import select, or_
        from datetime import date

        async with async_session() as session:
            embed_settings = await settings_service.get_embedding_settings(session)
            ocr_settings = await settings_service.get_ocr_settings(session)

        base_url = embed_settings.get("base_url", "")
        model = embed_settings.get("model", "")
        api_key = embed_settings.get("api_key", "")

        if not base_url or not model:
            phase["status"] = "completed"
            phase["detail"] = "Embedding endpoint not configured, skipped"
            await self._publish_state()
            return

        async with async_session() as session:
            today = date.today()
            # Only embed solicitation docs (RFP/RFA/NOFO/FOA) that have text and aren't yet embedded
            stmt = (
                select(OpportunityDocument)
                .join(Opportunity, OpportunityDocument.opportunity_id == Opportunity.id)
                .where(
                    Opportunity.status != "archived",
                    or_(Opportunity.close_date >= today, Opportunity.close_date.is_(None)),
                    OpportunityDocument.ocr_status == "completed",
                    OpportunityDocument.classify_status == "completed",
                    OpportunityDocument.doc_category == "solicitation",
                    OpportunityDocument.embed_status.in_(["pending", "failed"]),
                )
            )
            result = await session.execute(stmt)
            docs = list(result.scalars().all())

        phase["total"] = len(docs)
        phase["detail"] = f"{len(docs)} solicitation docs to embed"
        await self._publish_state()

        if not docs:
            phase["status"] = "completed"
            phase["detail"] = "No solicitation documents to embed"
            await self._publish_state()
            return

        from openai import AsyncOpenAI
        import chromadb

        embed_client = AsyncOpenAI(base_url=base_url, api_key=api_key or "not-needed")

        chroma_client = chromadb.HttpClient(
            host=app_settings.CHROMADB_HOST,
            port=app_settings.CHROMADB_PORT,
        )
        chroma_collection = chroma_client.get_or_create_collection(
            name="opportunity_documents",
            metadata={"hnsw:space": "cosine"},
        )

        embedded = 0
        errors = 0
        semaphore = asyncio.Semaphore(2)  # Low concurrency to avoid DB deadlocks

        async def _embed_one(doc_ref):
            nonlocal embedded, errors
            if self._cancel_requested:
                return
            async with semaphore:
                if self._cancel_requested:
                    return
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        async with async_session() as sess:
                            from sqlalchemy import select as sel
                            doc = (await sess.execute(
                                sel(OpportunityDocument).where(OpportunityDocument.id == doc_ref.id)
                            )).scalar_one_or_none()
                            if not doc or doc.doc_category != "solicitation":
                                return
                            if doc.embed_status not in ("pending", "failed"):
                                return

                            await document_service._embed_document(
                                doc, embed_settings, ocr_settings, sess,
                                embed_client=embed_client,
                                chroma_collection=chroma_collection,
                            )
                            await sess.commit()
                            if doc.embed_status == "completed":
                                embedded += 1
                            else:
                                errors += 1
                            break  # Success, exit retry loop
                    except Exception as e:
                        if "Deadlock" in str(e) and attempt < max_retries - 1:
                            logger.warning(f"Deadlock on doc {doc_ref.id}, retry {attempt + 1}/{max_retries}")
                            await asyncio.sleep(1 + attempt)
                            continue
                        err_msg = f"Doc {doc_ref.id}: {str(e)[:200]}"
                        logger.error(f"Embed error: {err_msg}")
                        _log_error(phase, err_msg)
                        errors += 1
                        break
                phase["processed"] = embedded
                phase["errors"] = errors
                if embedded % 5 == 0:
                    await self._publish_state()

        batch_size = 10
        for i in range(0, len(docs), batch_size):
            if self._cancel_requested:
                break
            batch = docs[i:i + batch_size]
            await asyncio.gather(*[_embed_one(d) for d in batch])

        phase["status"] = "failed" if self._cancel_requested else "completed"
        if self._cancel_requested:
            phase["detail"] = "Cancelled"
        else:
            phase["detail"] = f"{embedded} solicitation docs embedded"
        await self._publish_state()


pipeline_service = PipelineService()

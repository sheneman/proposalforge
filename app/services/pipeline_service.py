import asyncio
import json
import logging
from datetime import datetime

from app.services.cache_service import cache_service

logger = logging.getLogger(__name__)

REDIS_PIPELINE_KEY = "pf:pipeline_state"
REDIS_PIPELINE_TTL = 3600


def _empty_phases() -> list[dict]:
    return [
        {"phase": 1, "name": "Discovery", "status": "idle", "total": 0, "processed": 0, "errors": 0, "detail": ""},
        {"phase": 2, "name": "Download", "status": "idle", "total": 0, "processed": 0, "errors": 0, "detail": ""},
        {"phase": 3, "name": "Retrieve", "status": "idle", "total": 0, "processed": 0, "errors": 0, "detail": ""},
        {"phase": 4, "name": "Extract", "status": "idle", "total": 0, "processed": 0, "errors": 0, "detail": ""},
        {"phase": 5, "name": "Classify", "status": "idle", "total": 0, "processed": 0, "errors": 0, "detail": ""},
        {"phase": 6, "name": "Embed", "status": "idle", "total": 0, "processed": 0, "errors": 0, "detail": ""},
    ]


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

        self._task = asyncio.create_task(self._run(types))

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
                    p["detail"] = str(e)[:200]
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
                    logger.error(f"Download error for doc {doc_ref.id}: {e}")
                    errors += 1
                phase["processed"] = downloaded
                phase["errors"] = errors
                if downloaded % 10 == 0:
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
        await self._publish_state()

    async def _phase_3_retrieve(self):
        """Phase 3: Retrieve linked documents (URL extraction + web search)."""
        phase = self._phase(2)
        phase["status"] = "running"
        self.state["current_phase"] = 3
        await self._publish_state()

        from app.services.document_service import document_service

        # Use inline link extraction + synopsis doc creation
        document_service.is_processing = True
        document_service._cancel_requested = False
        document_service.processing_stats = {
            "started": datetime.utcnow().isoformat(),
            "total": 0, "scanned": 0, "downloaded": 0, "errors": 0,
            "phase": "extracting links",
        }

        try:
            await document_service._inline_link_extraction()
            phase["processed"] = document_service.processing_stats.get("downloaded", 0)
            phase["total"] = document_service.processing_stats.get("total", 0)
            phase["errors"] = document_service.processing_stats.get("errors", 0)
        except Exception as e:
            logger.error(f"Phase 3 link extraction failed: {e}", exc_info=True)
            phase["errors"] += 1
        finally:
            document_service.is_processing = False

        if not self._cancel_requested:
            # Web search for solicitations
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
                # Call search_for_solicitations internals without chaining
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
                                Opportunity.status != "archived",
                                or_(Opportunity.close_date >= today, Opportunity.close_date.is_(None)),
                                ~Opportunity.id.in_(has_sol),
                            )
                            .limit(200)
                        )
                        result = await session.execute(stmt)
                        opps = result.scalars().all()

                        phase["detail"] = f"Web search: {len(opps)} opportunities"
                        await self._publish_state()

                        async with httpx.AsyncClient(
                            follow_redirects=True, timeout=30.0,
                            headers={"User-Agent": "ProposalForge/1.0"},
                        ) as client:
                            for i, opp in enumerate(opps):
                                if self._cancel_requested:
                                    break
                                try:
                                    found = await document_service._web_search_for_opportunity(opp, session, client)
                                    if found > 0:
                                        await session.commit()
                                        phase["processed"] += found
                                except Exception:
                                    phase["errors"] += 1
                                    await session.rollback()
                                if i % 5 == 4:
                                    await self._publish_state()
                                await asyncio.sleep(1)
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

        num_workers = ocr_settings.get("doc_workers", 4)
        try:
            num_workers = int(num_workers)
        except (ValueError, TypeError):
            num_workers = 4
        semaphore = asyncio.Semaphore(num_workers)
        extracted = 0
        errors = 0

        ocr_client = httpx.AsyncClient(timeout=300.0)

        async def _ocr_one(doc_ref):
            nonlocal extracted, errors
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
                            else:
                                errors += 1
                except Exception as e:
                    logger.error(f"OCR error for doc {doc_ref.id}: {e}")
                    errors += 1
                phase["processed"] = extracted
                phase["errors"] = errors
                if extracted % 10 == 0:
                    await self._publish_state()

        try:
            batch_size = max(num_workers * 3, 10)
            for i in range(0, len(docs), batch_size):
                if self._cancel_requested:
                    break
                batch = docs[i:i + batch_size]
                await asyncio.gather(*[_ocr_one(d) for d in batch])
        finally:
            await ocr_client.aclose()

        phase["status"] = "failed" if self._cancel_requested else "completed"
        if self._cancel_requested:
            phase["detail"] = "Cancelled"
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
                    logger.error(f"Classify error for doc {doc_ref.id}: {e}")
                    errors += 1
                phase["processed"] = classified
                phase["errors"] = errors
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
        semaphore = asyncio.Semaphore(4)

        async def _embed_one(doc_ref):
            nonlocal embedded, errors
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
                except Exception as e:
                    logger.error(f"Embed error for doc {doc_ref.id}: {e}")
                    errors += 1
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

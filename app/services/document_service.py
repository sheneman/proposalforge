import asyncio
import logging
import os
import re
import uuid
from datetime import datetime

import httpx
from sqlalchemy import select, or_, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import async_session
from app.models.document import OpportunityDocument, DocumentChunk
from app.services.grants_client import GrantsGovClient
from app.services.settings_service import settings_service
from app.services.cache_service import cache_service

logger = logging.getLogger(__name__)

REDIS_DOC_SYNC_KEY = "pf:doc_sync_stats"
REDIS_DOC_PROCESSING_FLAG = "pf:doc_processing"
REDIS_DOC_COMPLETED_KEY = "pf:doc_completed"


class DocumentService:
    def __init__(self):
        self.is_processing = False
        self._cancel_requested = False
        self.processing_stats: dict = {}
        self._grants_client = GrantsGovClient()

    async def extract_attachment_metadata(
        self, session: AsyncSession, opp, detail: dict
    ) -> int:
        """Parse attachment metadata from fetchOpportunity response and upsert rows.

        Returns the number of new documents created.
        """
        folders = detail.get("synopsisAttachmentFolders") or []
        created = 0

        for folder in folders:
            folder_name = folder.get("folderName", "")
            attachments = folder.get("synopsisAttachments") or []

            for att in attachments:
                att_id = str(att.get("id", ""))
                if not att_id:
                    continue

                # Check if already exists
                stmt = select(OpportunityDocument).where(
                    OpportunityDocument.attachment_id == att_id
                )
                result = await session.execute(stmt)
                existing = result.scalar_one_or_none()

                if existing:
                    # Update metadata if changed
                    existing.file_name = att.get("fileName", existing.file_name)
                    existing.mime_type = att.get("mimeType", existing.mime_type)
                    existing.file_size = att.get("fileLobSize", existing.file_size)
                    existing.file_description = att.get("fileDescription", existing.file_description)
                    existing.folder_name = folder_name
                    continue

                doc = OpportunityDocument(
                    opportunity_id=opp.id,
                    attachment_id=att_id,
                    file_name=att.get("fileName", "unknown"),
                    mime_type=att.get("mimeType"),
                    file_size=att.get("fileLobSize"),
                    file_description=att.get("fileDescription"),
                    folder_name=folder_name,
                    download_status="pending",
                    ocr_status="pending",
                    embed_status="pending",
                )
                session.add(doc)
                created += 1

        if created > 0:
            await session.flush()

        return created

    async def process_pending_documents(self):
        """Batch orchestrator: download, OCR, chunk, embed all pending documents."""
        if self.is_processing:
            logger.warning("Document processing already in progress")
            return

        self.is_processing = True
        self._cancel_requested = False
        self.processing_stats = {
            "started": datetime.utcnow().isoformat(),
            "total": 0,
            "downloaded": 0,
            "ocr_completed": 0,
            "embedded": 0,
            "errors": 0,
            "phase": "starting",
        }
        await self._publish_stats()

        try:
            async with async_session() as session:
                # Get OCR settings once
                ocr_settings = await settings_service.get_ocr_settings(session)
                embed_settings = await settings_service.get_embedding_settings(session)

                # Reset all failed docs back to pending and clear errors
                await session.execute(
                    text("UPDATE opportunity_documents SET download_status = 'pending', error_message = NULL WHERE download_status = 'failed'")
                )
                await session.execute(
                    text("UPDATE opportunity_documents SET ocr_status = 'pending', error_message = NULL WHERE ocr_status = 'failed'")
                )
                await session.execute(
                    text("UPDATE opportunity_documents SET embed_status = 'pending', error_message = NULL WHERE embed_status = 'failed'")
                )
                await session.commit()
                logger.info("Reset failed documents to pending")

                # Query all docs with any pending status
                stmt = select(OpportunityDocument).where(
                    or_(
                        OpportunityDocument.download_status == "pending",
                        OpportunityDocument.ocr_status == "pending",
                        OpportunityDocument.embed_status == "pending",
                    )
                )
                result = await session.execute(stmt)
                docs = list(result.scalars().all())

            num_workers = ocr_settings.get("doc_workers", 4)
            self.processing_stats["total"] = len(docs)
            self.processing_stats["phase"] = "processing"
            await self._publish_stats()

            logger.info(f"Processing {len(docs)} pending documents with {num_workers} workers")

            semaphore = asyncio.Semaphore(num_workers)

            async def _process_one(doc_ref):
                if self._cancel_requested:
                    return
                async with semaphore:
                    if self._cancel_requested:
                        return
                    try:
                        async with async_session() as session:
                            stmt = select(OpportunityDocument).where(
                                OpportunityDocument.id == doc_ref.id
                            )
                            result = await session.execute(stmt)
                            doc = result.scalar_one_or_none()
                            if not doc:
                                return

                            if doc.download_status == "pending":
                                await self._download_document(doc, session)

                            if doc.download_status == "downloaded" and doc.ocr_status == "pending":
                                await self._ocr_document(doc, ocr_settings, session)

                            if doc.ocr_status == "completed" and doc.embed_status == "pending":
                                await self._embed_document(doc, embed_settings, ocr_settings, session)

                            await session.commit()
                    except Exception as e:
                        logger.error(f"Error processing document {doc_ref.id}: {e}", exc_info=True)
                        self.processing_stats["errors"] += 1
                        try:
                            async with async_session() as err_session:
                                stmt = select(OpportunityDocument).where(
                                    OpportunityDocument.id == doc_ref.id
                                )
                                result = await err_session.execute(stmt)
                                err_doc = result.scalar_one_or_none()
                                if err_doc:
                                    err_doc.error_message = str(e)[:2000]
                                    await err_session.commit()
                        except Exception:
                            pass

                    await self._publish_stats()

            # Process in batches to allow cancel checks and stats updates
            batch_size = max(num_workers * 3, 10)
            for i in range(0, len(docs), batch_size):
                if self._cancel_requested:
                    logger.info("Document processing cancelled by user")
                    self.processing_stats["cancelled"] = True
                    break

                batch = docs[i:i + batch_size]
                await asyncio.gather(*[_process_one(d) for d in batch])

            self.processing_stats["completed"] = datetime.utcnow().isoformat()
            self.processing_stats["phase"] = "completed"

        except Exception as e:
            logger.error(f"Document processing failed: {e}", exc_info=True)
            self.processing_stats["error"] = str(e)[:500]
        finally:
            self.is_processing = False
            await self._publish_stats()

    async def _download_document(self, doc: OpportunityDocument, session: AsyncSession):
        """Download a single document from Grants.gov."""
        safe_name = re.sub(r'[^\w\-.]', '_', doc.file_name)
        dest_path = os.path.join(
            settings.DOCUMENT_STORAGE_PATH,
            str(doc.opportunity_id),
            f"{doc.attachment_id}_{safe_name}",
        )

        success = await self._grants_client.download_attachment(doc.attachment_id, dest_path)

        if success:
            doc.local_path = dest_path
            doc.download_status = "downloaded"
            self.processing_stats["downloaded"] = self.processing_stats.get("downloaded", 0) + 1
        else:
            doc.download_status = "failed"
            doc.error_message = "Download failed after retries"
            self.processing_stats["errors"] = self.processing_stats.get("errors", 0) + 1

    async def _ocr_document(self, doc: OpportunityDocument, ocr_settings: dict, session: AsyncSession):
        """OCR a downloaded document to extract text."""
        if not doc.local_path or not os.path.exists(doc.local_path):
            doc.ocr_status = "failed"
            doc.error_message = "Local file not found for OCR"
            return

        # Skip non-PDF files for OCR
        mime = (doc.mime_type or "").lower()
        name = (doc.file_name or "").lower()
        if not (mime == "application/pdf" or name.endswith(".pdf")):
            doc.ocr_status = "skipped"
            return

        method = ocr_settings.get("method", "dotsocr")

        try:
            if method == "dotsocr":
                extracted_text = await self._ocr_dotsocr(doc.local_path, ocr_settings)
            elif method == "pymupdf":
                extracted_text = await self._ocr_pymupdf(doc.local_path)
            else:
                doc.ocr_status = "failed"
                doc.error_message = f"Unknown OCR method: {method}"
                return

            if extracted_text:
                doc.ocr_status = "completed"
                doc.extracted_text_length = len(extracted_text)
                # Store text temporarily in error_message field? No, store in chunks later.
                # We'll pass it through by writing a temp file
                text_path = doc.local_path + ".txt"
                with open(text_path, "w", encoding="utf-8") as f:
                    f.write(extracted_text)
                self.processing_stats["ocr_completed"] = self.processing_stats.get("ocr_completed", 0) + 1
            else:
                doc.ocr_status = "failed"
                doc.error_message = "OCR returned empty text"

        except Exception as e:
            doc.ocr_status = "failed"
            doc.error_message = f"OCR error: {str(e)[:500]}"
            logger.error(f"OCR failed for document {doc.id}: {e}", exc_info=True)

    async def _ocr_dotsocr(self, file_path: str, ocr_settings: dict) -> str | None:
        """Send PDF to dotsocr endpoint, receive markdown."""
        endpoint = ocr_settings.get("endpoint_url", settings.OCR_ENDPOINT_URL)

        async with httpx.AsyncClient(timeout=300.0) as client:
            with open(file_path, "rb") as f:
                files = {"file": (os.path.basename(file_path), f, "application/pdf")}
                response = await client.post(endpoint, files=files)

            response.raise_for_status()

            # dotsocr returns markdown text
            content_type = response.headers.get("content-type", "")
            if "json" in content_type:
                data = response.json()
                return data.get("text") or data.get("markdown") or data.get("content", "")
            else:
                return response.text

    async def _ocr_pymupdf(self, file_path: str) -> str | None:
        """Extract text from PDF using PyMuPDF (local, no network)."""
        loop = asyncio.get_event_loop()

        def _extract():
            import pymupdf
            doc = pymupdf.open(file_path)
            pages = []
            for page in doc:
                pages.append(page.get_text())
            doc.close()
            return "\n\n".join(pages)

        return await loop.run_in_executor(None, _extract)

    async def _embed_document(self, doc: OpportunityDocument, embed_settings: dict, ocr_settings: dict, session: AsyncSession):
        """Chunk extracted text, generate embeddings, store in ChromaDB."""
        text_path = doc.local_path + ".txt"
        if not os.path.exists(text_path):
            doc.embed_status = "failed"
            doc.error_message = "Extracted text file not found"
            return

        with open(text_path, "r", encoding="utf-8") as f:
            full_text = f.read()

        if not full_text.strip():
            doc.embed_status = "failed"
            doc.error_message = "Extracted text is empty"
            return

        base_url = embed_settings.get("base_url", "")
        model = embed_settings.get("model", "")
        api_key = embed_settings.get("api_key", "")

        if not base_url or not model:
            doc.embed_status = "failed"
            doc.error_message = "Embedding endpoint not configured"
            return

        try:
            # Chunk the text using token-based sizing
            chunk_size = ocr_settings.get("chunk_size_tokens", 1000)
            chunk_overlap = ocr_settings.get("chunk_overlap_tokens", 200)
            chunks = self._chunk_text(full_text, chunk_size=chunk_size, overlap=chunk_overlap)

            # Delete existing chunks for this document
            await session.execute(
                text("DELETE FROM document_chunks WHERE document_id = :doc_id"),
                {"doc_id": doc.id},
            )

            # Generate embeddings in batches
            from openai import AsyncOpenAI
            embed_client = AsyncOpenAI(base_url=base_url, api_key=api_key or "not-needed")

            all_embeddings = []
            batch_size = 20
            for i in range(0, len(chunks), batch_size):
                batch_texts = [c["text"] for c in chunks[i:i + batch_size]]
                response = await embed_client.embeddings.create(
                    model=model,
                    input=batch_texts,
                    timeout=60,
                )
                all_embeddings.extend([d.embedding for d in response.data])

            # Store in ChromaDB
            import chromadb
            chroma_client = chromadb.HttpClient(
                host=settings.CHROMADB_HOST,
                port=settings.CHROMADB_PORT,
            )
            collection = chroma_client.get_or_create_collection(
                name="opportunity_documents",
                metadata={"hnsw:space": "cosine"},
            )

            chroma_ids = []
            chroma_embeddings = []
            chroma_documents = []
            chroma_metadatas = []

            chunk_rows = []
            for idx, chunk_data in enumerate(chunks):
                chroma_id = f"doc_{doc.id}_chunk_{idx}_{uuid.uuid4().hex[:8]}"
                chroma_ids.append(chroma_id)
                chroma_embeddings.append(all_embeddings[idx])
                chroma_documents.append(chunk_data["text"])
                chroma_metadatas.append({
                    "document_id": doc.id,
                    "opportunity_id": doc.opportunity_id,
                    "attachment_id": doc.attachment_id,
                    "chunk_index": idx,
                    "file_name": doc.file_name,
                })

                chunk_row = DocumentChunk(
                    document_id=doc.id,
                    chunk_index=idx,
                    chunk_text=chunk_data["text"],
                    char_offset=chunk_data["offset"],
                    char_length=chunk_data["length"],
                    chroma_id=chroma_id,
                )
                chunk_rows.append(chunk_row)

            # Upsert to ChromaDB in batches
            for i in range(0, len(chroma_ids), 100):
                collection.upsert(
                    ids=chroma_ids[i:i + 100],
                    embeddings=chroma_embeddings[i:i + 100],
                    documents=chroma_documents[i:i + 100],
                    metadatas=chroma_metadatas[i:i + 100],
                )

            # Save chunk rows to MariaDB
            for row in chunk_rows:
                session.add(row)
            await session.flush()

            doc.embed_status = "completed"
            doc.chunk_count = len(chunks)
            self.processing_stats["embedded"] = self.processing_stats.get("embedded", 0) + 1

        except Exception as e:
            doc.embed_status = "failed"
            doc.error_message = f"Embedding error: {str(e)[:500]}"
            logger.error(f"Embedding failed for document {doc.id}: {e}", exc_info=True)

    def _chunk_text(
        self, text_content: str, chunk_size: int = 1000, overlap: int = 200
    ) -> list[dict]:
        """Split text at paragraph/sentence boundaries using token counts.

        Args:
            text_content: Full text to chunk.
            chunk_size: Target chunk size in tokens.
            overlap: Overlap between chunks in tokens.

        Returns list of {text, offset, length}.
        """
        if not text_content:
            return []

        import tiktoken
        try:
            enc = tiktoken.get_encoding("cl100k_base")
        except Exception:
            enc = tiktoken.get_encoding("gpt2")

        def _token_len(s: str) -> int:
            return len(enc.encode(s, disallowed_special=()))

        # Split into paragraphs
        paragraphs = re.split(r'\n\s*\n', text_content)

        chunks = []
        current_paras: list[str] = []
        current_tokens = 0
        current_offset = 0
        pos = 0

        for para in paragraphs:
            para = para.strip()
            if not para:
                pos += 2
                continue

            para_start = text_content.find(para, pos)
            if para_start == -1:
                para_start = pos
            pos = para_start + len(para)

            para_tokens = _token_len(para)

            if current_tokens + para_tokens > chunk_size and current_paras:
                chunk_text = "\n\n".join(current_paras).strip()
                chunks.append({
                    "text": chunk_text,
                    "offset": current_offset,
                    "length": len(chunk_text),
                })

                # Build overlap from trailing paragraphs
                if overlap > 0:
                    overlap_paras: list[str] = []
                    overlap_tokens = 0
                    for p in reversed(current_paras):
                        p_tok = _token_len(p)
                        if overlap_tokens + p_tok > overlap and overlap_paras:
                            break
                        overlap_paras.insert(0, p)
                        overlap_tokens += p_tok
                    current_paras = overlap_paras + [para]
                    current_tokens = overlap_tokens + para_tokens
                    # Approximate offset from overlap start
                    overlap_text = "\n\n".join(overlap_paras)
                    current_offset = para_start - len(overlap_text) - 2 if overlap_text else para_start
                else:
                    current_paras = [para]
                    current_tokens = para_tokens
                    current_offset = para_start
            else:
                if not current_paras:
                    current_offset = para_start
                current_paras.append(para)
                current_tokens += para_tokens

        # Final chunk
        if current_paras:
            chunk_text = "\n\n".join(current_paras).strip()
            if chunk_text:
                chunks.append({
                    "text": chunk_text,
                    "offset": current_offset,
                    "length": len(chunk_text),
                })

        return chunks

    async def semantic_search(
        self,
        query: str,
        n_results: int = 10,
        opportunity_id: int | None = None,
    ) -> list[dict]:
        """Search ChromaDB for relevant document chunks.

        Returns list of {chunk_text, document_id, opportunity_id, file_name, score}.
        """
        async with async_session() as session:
            embed_settings = await settings_service.get_embedding_settings(session)

        base_url = embed_settings.get("base_url", "")
        model = embed_settings.get("model", "")
        api_key = embed_settings.get("api_key", "")

        if not base_url or not model:
            return []

        try:
            from openai import AsyncOpenAI
            embed_client = AsyncOpenAI(base_url=base_url, api_key=api_key or "not-needed")
            response = await embed_client.embeddings.create(
                model=model,
                input=query,
                timeout=30,
            )
            query_embedding = response.data[0].embedding

            import chromadb
            chroma_client = chromadb.HttpClient(
                host=settings.CHROMADB_HOST,
                port=settings.CHROMADB_PORT,
            )
            collection = chroma_client.get_or_create_collection(
                name="opportunity_documents",
                metadata={"hnsw:space": "cosine"},
            )

            where_filter = None
            if opportunity_id is not None:
                where_filter = {"opportunity_id": opportunity_id}

            results = collection.query(
                query_embeddings=[query_embedding],
                n_results=n_results,
                where=where_filter,
                include=["documents", "metadatas", "distances"],
            )

            items = []
            if results and results.get("documents"):
                docs = results["documents"][0]
                metas = results["metadatas"][0]
                distances = results["distances"][0]

                for doc_text, meta, dist in zip(docs, metas, distances):
                    items.append({
                        "chunk_text": doc_text,
                        "document_id": meta.get("document_id"),
                        "opportunity_id": meta.get("opportunity_id"),
                        "file_name": meta.get("file_name"),
                        "score": 1.0 - dist,  # cosine distance to similarity
                    })

            return items

        except Exception as e:
            logger.error(f"Semantic search failed: {e}", exc_info=True)
            return []

    def cancel_processing(self):
        """Request cancellation of document processing."""
        self._cancel_requested = True

    async def get_processing_status(self) -> dict:
        """Get current processing status. Redis is the single source of truth."""
        # Check the simple atomic flag first — most reliable
        is_active = False
        flag = None
        try:
            flag = await cache_service._redis.get(REDIS_DOC_PROCESSING_FLAG)
            is_active = flag == b"1" or flag == "1"
        except Exception as e:
            logger.warning(f"Redis flag check failed: {e}, falling back to local state")
            is_active = self.is_processing

        # Get detailed stats from Redis (or local fallback)
        shared = await self._get_shared_stats()
        stats = shared if shared else self.processing_stats

        # If idle, ensure we have the completion timestamp from persistent key
        if not is_active and not stats.get("completed"):
            try:
                completed = await cache_service._redis.get(REDIS_DOC_COMPLETED_KEY)
                if completed:
                    stats = {**stats, "completed": completed}
            except Exception:
                pass

        return {"is_processing": is_active, "stats": stats}

    async def get_document_counts(self) -> dict:
        """Get aggregate counts for the admin dashboard."""
        async with async_session() as session:
            total = await session.execute(
                select(func.count(OpportunityDocument.id))
            )
            downloaded = await session.execute(
                select(func.count(OpportunityDocument.id)).where(
                    OpportunityDocument.download_status == "downloaded"
                )
            )
            ocr_completed = await session.execute(
                select(func.count(OpportunityDocument.id)).where(
                    OpportunityDocument.ocr_status == "completed"
                )
            )
            embedded = await session.execute(
                select(func.count(OpportunityDocument.id)).where(
                    OpportunityDocument.embed_status == "completed"
                )
            )
            errors = await session.execute(
                select(func.count(OpportunityDocument.id)).where(
                    or_(
                        OpportunityDocument.download_status == "failed",
                        OpportunityDocument.ocr_status == "failed",
                        OpportunityDocument.embed_status == "failed",
                    )
                )
            )
            pending = await session.execute(
                select(func.count(OpportunityDocument.id)).where(
                    or_(
                        OpportunityDocument.download_status == "pending",
                        OpportunityDocument.ocr_status == "pending",
                        OpportunityDocument.embed_status == "pending",
                    )
                )
            )

            return {
                "total": total.scalar() or 0,
                "downloaded": downloaded.scalar() or 0,
                "ocr_completed": ocr_completed.scalar() or 0,
                "embedded": embedded.scalar() or 0,
                "errors": errors.scalar() or 0,
                "pending": pending.scalar() or 0,
            }

    async def get_recent_errors(self, limit: int = 20) -> list[dict]:
        """Get recent document processing errors for the admin UI."""
        async with async_session() as session:
            stmt = (
                select(OpportunityDocument)
                .where(OpportunityDocument.error_message.is_not(None))
                .order_by(OpportunityDocument.updated_at.desc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            docs = result.scalars().all()

            return [
                {
                    "id": doc.id,
                    "file_name": doc.file_name,
                    "attachment_id": doc.attachment_id,
                    "download_status": doc.download_status,
                    "ocr_status": doc.ocr_status,
                    "embed_status": doc.embed_status,
                    "error_message": doc.error_message,
                    "updated_at": doc.updated_at.isoformat() if doc.updated_at else "",
                }
                for doc in docs
            ]

    async def _publish_stats(self):
        """Publish processing stats to Redis for cross-worker visibility."""
        try:
            import json
            payload = {**self.processing_stats, "is_processing": self.is_processing}
            pipe = cache_service._redis.pipeline()
            pipe.set(REDIS_DOC_SYNC_KEY, json.dumps(payload), ex=300)
            # Separate atomic flag — simple string, no JSON
            if self.is_processing:
                pipe.set(REDIS_DOC_PROCESSING_FLAG, "1", ex=300)
            else:
                pipe.delete(REDIS_DOC_PROCESSING_FLAG)
                # Store completion time persistently (24h TTL)
                if payload.get("completed"):
                    pipe.set(REDIS_DOC_COMPLETED_KEY, payload["completed"], ex=86400)
            await pipe.execute()
        except Exception:
            pass

    async def _get_shared_stats(self) -> dict | None:
        """Read processing stats from Redis."""
        try:
            import json
            data = await cache_service._redis.get(REDIS_DOC_SYNC_KEY)
            if data:
                return json.loads(data)
        except Exception:
            pass
        return None


document_service = DocumentService()

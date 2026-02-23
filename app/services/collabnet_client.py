import asyncio
import logging
from typing import Any

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_BACKOFF = [2, 5, 15]
PAGE_SIZE = 1000


class CollabNetClient:
    def __init__(self, delay: float = 0.1, max_concurrent: int = 5):
        self._delay = delay
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._last_request_time = 0.0

    @property
    def _base_url(self) -> str:
        return settings.COLLABNET_API_URL.rstrip("/")

    @property
    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if settings.COLLABNET_API_KEY:
            headers["X-API-Key"] = settings.COLLABNET_API_KEY
        return headers

    async def _throttle(self):
        now = asyncio.get_event_loop().time()
        elapsed = now - self._last_request_time
        if elapsed < self._delay:
            await asyncio.sleep(self._delay - elapsed)
        self._last_request_time = asyncio.get_event_loop().time()

    async def _get_with_retry(self, path: str, params: dict | None = None) -> Any:
        url = f"{self._base_url}{path}"
        for attempt in range(MAX_RETRIES):
            await self._throttle()
            try:
                async with self._semaphore:
                    async with httpx.AsyncClient(timeout=30.0) as client:
                        response = await client.get(url, params=params, headers=self._headers)

                        if response.status_code == 429:
                            wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                            logger.warning(f"Rate limited (429), waiting {wait}s (attempt {attempt + 1})")
                            await asyncio.sleep(wait)
                            continue

                        response.raise_for_status()
                        return response.json()

            except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError) as e:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                logger.warning(f"Connection error: {e}, retrying in {wait}s (attempt {attempt + 1})")
                await asyncio.sleep(wait)
                continue

        raise RuntimeError(f"Failed after {MAX_RETRIES} retries for {url}")

    async def _paginate(self, path: str, cancel_check=None) -> list[dict]:
        """Paginate through all results for an endpoint."""
        all_items = []
        skip = 0
        while True:
            if cancel_check and cancel_check():
                return all_items

            data = await self._get_with_retry(path, params={"skip": skip, "limit": PAGE_SIZE})

            # Response may be a list directly or wrapped in a key
            items = data if isinstance(data, list) else data.get("items", data.get("data", []))
            if not isinstance(items, list):
                break

            if not items:
                break

            all_items.extend(items)
            logger.info(f"Fetched {len(items)} items from {path} (skip={skip}, total={len(all_items)})")

            if len(items) < PAGE_SIZE:
                break

            skip += len(items)

        return all_items

    async def get_stats(self) -> dict:
        return await self._get_with_retry("/stats")

    async def fetch_all_researchers(self, cancel_check=None) -> list[dict]:
        return await self._paginate("/researchers", cancel_check=cancel_check)

    async def fetch_all_documents(self, cancel_check=None) -> list[dict]:
        return await self._paginate("/documents", cancel_check=cancel_check)

    async def fetch_all_summaries(self, cancel_check=None) -> list[dict]:
        return await self._paginate("/summaries", cancel_check=cancel_check)

    async def fetch_researcher(self, researcher_id: str) -> dict:
        return await self._get_with_retry(f"/researchers/{researcher_id}")

    async def fetch_document(self, document_id: str) -> dict:
        return await self._get_with_retry(f"/documents/{document_id}")


collabnet_client = CollabNetClient()

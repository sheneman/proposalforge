import asyncio
import logging
from typing import Any

import httpx

from app.config import settings
from app.services.retry import MAX_RETRIES, RETRY_BACKOFF

logger = logging.getLogger(__name__)
PAGE_SIZE = 100  # Esploro max per page


class VersoClient:
    """Client for the VERSO/Esploro API (Ex Libris)."""

    def __init__(self, delay: float = 0.025, max_concurrent: int = 3):
        # 0.025s delay = ~40 req/s, well under the 50/s institution-wide limit
        self._delay = delay
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._last_request_time = 0.0

    @property
    def _base_url(self) -> str:
        return settings.VERSO_API_URL.rstrip("/")

    @property
    def _api_key(self) -> str:
        return settings.VERSO_API_KEY

    def _params(self, extra: dict | None = None) -> dict:
        params = {"apikey": self._api_key}
        if extra:
            params.update(extra)
        return params

    @property
    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Authorization": f"apikey {self._api_key}",
        }

    async def _throttle(self):
        now = asyncio.get_event_loop().time()
        elapsed = now - self._last_request_time
        if elapsed < self._delay:
            await asyncio.sleep(self._delay - elapsed)
        self._last_request_time = asyncio.get_event_loop().time()

    async def _get_with_retry(self, path: str, params: dict | None = None) -> Any:
        url = f"{self._base_url}{path}"
        merged_params = self._params(params)

        for attempt in range(MAX_RETRIES):
            await self._throttle()
            try:
                async with self._semaphore:
                    async with httpx.AsyncClient(timeout=30.0) as client:
                        response = await client.get(url, params=merged_params, headers=self._headers)

                        if response.status_code == 429:
                            # Check X-Exl-Api-Remaining header
                            wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                            logger.warning(f"VERSO rate limited (429), waiting {wait}s (attempt {attempt + 1})")
                            await asyncio.sleep(wait)
                            continue

                        response.raise_for_status()
                        return response.json()

            except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError) as e:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                logger.warning(f"VERSO connection error: {e}, retrying in {wait}s (attempt {attempt + 1})")
                await asyncio.sleep(wait)
                continue

        raise RuntimeError(f"VERSO: failed after {MAX_RETRIES} retries for {url}")

    async def _paginate(self, path: str, params: dict | None = None, cancel_check=None) -> list[dict]:
        """Paginate through all results using offset/limit."""
        all_items = []
        offset = 0
        base_params = dict(params or {})

        while True:
            if cancel_check and cancel_check():
                return all_items

            page_params = {**base_params, "offset": offset, "limit": PAGE_SIZE}
            data = await self._get_with_retry(path, params=page_params)

            # Esploro wraps results in various keys
            items = []
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                # Try common Esploro response wrappers
                for key in ("grant", "grants", "project", "projects",
                            "activity", "activities", "asset", "assets",
                            "researcher", "researchers", "items", "data"):
                    if key in data and isinstance(data[key], list):
                        items = data[key]
                        break
                # Single item wrapped in a key
                if not items and "total_record_count" not in data:
                    # Might be a single-item response
                    items = [data]

            if not items:
                break

            all_items.extend(items)
            logger.info(f"VERSO: fetched {len(items)} items from {path} (offset={offset}, total={len(all_items)})")

            if len(items) < PAGE_SIZE:
                break

            offset += len(items)

        return all_items

    async def fetch_researcher(self, primary_id: str) -> dict:
        """Fetch a single researcher's full record from VERSO."""
        return await self._get_with_retry(
            f"/researchers/{primary_id}",
            params={"user_id_type": "all_unique", "view": "full"},
        )

    async def fetch_researcher_assets(self, primary_id: str, cancel_check=None) -> list[dict]:
        """Fetch all assets (publications, etc.) for a researcher."""
        return await self._paginate(
            "/assets",
            params={"user_primary_id": primary_id},
            cancel_check=cancel_check,
        )

    async def fetch_grants(self, cancel_check=None) -> list[dict]:
        """Fetch all grants from VERSO."""
        return await self._paginate("/grants", cancel_check=cancel_check)

    async def fetch_projects(self, cancel_check=None) -> list[dict]:
        """Fetch all projects from VERSO."""
        return await self._paginate("/projects", cancel_check=cancel_check)

    async def fetch_activities(self, cancel_check=None) -> list[dict]:
        """Fetch all activities from VERSO."""
        return await self._paginate("/activities", cancel_check=cancel_check)


verso_client = VersoClient()

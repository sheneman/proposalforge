import asyncio
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

SEARCH_URL = "https://api.grants.gov/v1/api/search2"
FETCH_URL = "https://api.grants.gov/v1/api/fetchOpportunity"

MAX_RETRIES = 3
RETRY_BACKOFF = [2, 5, 15]  # seconds


class GrantsGovClient:
    def __init__(self, delay: float = 1.0, max_concurrent: int = 3):
        self._delay = delay
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._last_request_time = 0.0

    async def _throttle(self):
        now = asyncio.get_event_loop().time()
        elapsed = now - self._last_request_time
        if elapsed < self._delay:
            await asyncio.sleep(self._delay - elapsed)
        self._last_request_time = asyncio.get_event_loop().time()

    async def _request_with_retry(self, url: str, payload: dict) -> dict[str, Any]:
        """Make a POST request with retry logic for transient errors."""
        for attempt in range(MAX_RETRIES):
            await self._throttle()
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.post(url, json=payload)

                    if response.status_code == 429:
                        wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                        logger.warning(f"Rate limited (429), waiting {wait}s (attempt {attempt + 1})")
                        await asyncio.sleep(wait)
                        continue

                    response.raise_for_status()
                    result = response.json()

                    # API wraps response in 'data' key
                    data = result.get("data", result)

                    # On transient errors, 'data' is a string like "read ECONNRESET"
                    if isinstance(data, str):
                        wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                        logger.warning(f"API returned error string: '{data}', retrying in {wait}s (attempt {attempt + 1})")
                        await asyncio.sleep(wait)
                        continue

                    return data

            except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError) as e:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                logger.warning(f"Connection error: {e}, retrying in {wait}s (attempt {attempt + 1})")
                await asyncio.sleep(wait)
                continue

        raise RuntimeError(f"Failed after {MAX_RETRIES} retries for {url}")

    async def search(
        self,
        keyword: str = "",
        opp_status: str = "posted",
        page_number: int = 1,
        rows_per_page: int = 25,
    ) -> dict[str, Any]:
        payload = {
            "keyword": keyword,
            "oppStatuses": opp_status,
            "paginationModel": {
                "sortBy": "openDate",
                "sortOrder": "desc",
                "pageNumber": page_number,
                "rowsPerPage": rows_per_page,
            },
        }
        return await self._request_with_retry(SEARCH_URL, payload)

    async def fetch_opportunity(self, opportunity_id: int) -> dict[str, Any]:
        async with self._semaphore:
            return await self._request_with_retry(
                FETCH_URL,
                {"opportunityId": opportunity_id},
            )

    async def fetch_all_opportunities(
        self,
        opp_statuses: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Paginate through all search results for each status."""
        if opp_statuses is None:
            opp_statuses = ["posted", "forecasted"]

        all_items = []

        for status in opp_statuses:
            page = 1
            while True:
                logger.info(f"Fetching {status} opportunities, page {page}...")
                try:
                    result = await self.search(
                        opp_status=status,
                        page_number=page,
                        rows_per_page=25,
                    )
                except RuntimeError as e:
                    logger.error(f"Failed to fetch page {page} for {status}: {e}")
                    break

                items = result.get("oppHits", [])
                if not items:
                    break

                all_items.extend(items)
                total_hits = result.get("hitCount", 0)
                logger.info(f"Page {page}: got {len(items)} items, total {status}={total_hits}")

                if page * 25 >= total_hits:
                    break

                page += 1

        return all_items

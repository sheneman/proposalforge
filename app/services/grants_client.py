import asyncio
import logging
from typing import Any, Callable

import httpx

from app.services.retry import MAX_RETRIES, RETRY_BACKOFF

logger = logging.getLogger(__name__)

SEARCH_URL = "https://api.grants.gov/v1/api/search2"
FETCH_URL = "https://api.grants.gov/v1/api/fetchOpportunity"
ATTACHMENT_DOWNLOAD_URL = "https://apply07.grants.gov/grantsws/rest/opportunity/att/download"

PAGE_SIZE = 25  # Grants.gov API caps at 25 per request


class GrantsGovClient:
    def __init__(self, delay: float = 0.25, max_concurrent: int = 10):
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
        start_record: int = 0,
        rows: int = PAGE_SIZE,
    ) -> dict[str, Any]:
        payload = {
            "keyword": keyword,
            "oppStatuses": opp_status,
            "rows": rows,
            "startRecordNum": start_record,
        }
        return await self._request_with_retry(SEARCH_URL, payload)

    async def fetch_opportunity(self, opportunity_id: int) -> dict[str, Any]:
        async with self._semaphore:
            return await self._request_with_retry(
                FETCH_URL,
                {"opportunityId": opportunity_id},
            )

    async def download_attachment(self, attachment_id: str, dest_path: str) -> bool:
        """Download an attachment file from Grants.gov.

        Args:
            attachment_id: The attachment ID from synopsisAttachmentFolders.
            dest_path: Local file path to save to.

        Returns:
            True if download succeeded, False otherwise.
        """
        import os
        url = f"{ATTACHMENT_DOWNLOAD_URL}/{attachment_id}"

        for attempt in range(MAX_RETRIES):
            async with self._semaphore:
                await self._throttle()
                try:
                    async with httpx.AsyncClient(timeout=120.0) as client:
                        async with client.stream("GET", url) as response:
                            if response.status_code == 429:
                                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                                logger.warning(f"Rate limited downloading {attachment_id}, waiting {wait}s")
                                await asyncio.sleep(wait)
                                continue

                            response.raise_for_status()

                            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                            with open(dest_path, "wb") as f:
                                async for chunk in response.aiter_bytes(chunk_size=8192):
                                    f.write(chunk)

                    logger.info(f"Downloaded attachment {attachment_id} to {dest_path}")
                    return True

                except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError) as e:
                    wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                    logger.warning(f"Download error for {attachment_id}: {e}, retrying in {wait}s")
                    await asyncio.sleep(wait)
                    continue
                except Exception as e:
                    logger.error(f"Failed to download attachment {attachment_id}: {e}")
                    return False

        logger.error(f"Failed to download attachment {attachment_id} after {MAX_RETRIES} retries")
        return False

    async def fetch_all_opportunities(
        self,
        opp_statuses: list[str] | None = None,
        progress_callback: Callable[[str, int, int], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
    ) -> list[dict[str, Any]]:
        """Paginate through all search results for each status.

        Args:
            opp_statuses: List of statuses to fetch.
            progress_callback: Called with (status, items_so_far, estimated_total) after each page.
            cancel_check: Called before each page; return True to abort.
        """
        if opp_statuses is None:
            opp_statuses = ["posted", "forecasted", "closed", "archived"]

        all_items = []
        estimated_total = 0

        for status in opp_statuses:
            if cancel_check and cancel_check():
                return all_items

            offset = 0
            while True:
                if cancel_check and cancel_check():
                    return all_items

                logger.info(f"Fetching {status} opportunities, offset {offset}...")
                try:
                    result = await self.search(
                        opp_status=status,
                        start_record=offset,
                        rows=PAGE_SIZE,
                    )
                except RuntimeError as e:
                    logger.error(f"Failed to fetch {status} at offset {offset}: {e}")
                    break

                items = result.get("oppHits", [])
                if not items:
                    break

                all_items.extend(items)
                total_hits = result.get("hitCount", 0)
                if offset == 0:
                    estimated_total += total_hits

                logger.info(f"Offset {offset}: got {len(items)} items, total {status}={total_hits}, cumulative={len(all_items)}")

                if progress_callback:
                    progress_callback(status, len(all_items), estimated_total)

                offset += len(items)
                if offset >= total_hits:
                    break

        return all_items

import asyncio
import logging

MAX_RETRIES = 5
RETRY_BACKOFF = [1, 2, 5, 10, 20]  # seconds


async def retry_async(coro_factory, logger: logging.Logger, description="operation", retryable=None):
    """Retry an async operation up to MAX_RETRIES times with backoff.

    coro_factory: callable that returns a new awaitable each call
    retryable: optional callable(exception) -> bool, defaults to all exceptions
    """
    for attempt in range(MAX_RETRIES):
        try:
            return await coro_factory()
        except Exception as e:
            if retryable and not retryable(e):
                raise
            if attempt == MAX_RETRIES - 1:
                raise
            wait = RETRY_BACKOFF[attempt]
            logger.warning(f"{description} failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}, retrying in {wait}s")
            await asyncio.sleep(wait)

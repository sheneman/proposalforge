import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings as app_settings
from app.models.site_setting import SiteSetting

logger = logging.getLogger(__name__)

# Keys used for LLM configuration
LLM_BASE_URL_KEY = "llm_base_url"
LLM_MODEL_KEY = "llm_model"
LLM_API_KEY_KEY = "llm_api_key"

# Keys for Embedding configuration
EMBED_BASE_URL_KEY = "embed_base_url"
EMBED_MODEL_KEY = "embed_model"
EMBED_API_KEY_KEY = "embed_api_key"

# Keys for Re-ranker configuration
RERANKER_BASE_URL_KEY = "reranker_base_url"
RERANKER_MODEL_KEY = "reranker_model"
RERANKER_API_KEY_KEY = "reranker_api_key"

# Timezone
TIMEZONE_KEY = "app_timezone"
DEFAULT_TIMEZONE = "US/Pacific"

TIMEZONE_CHOICES = [
    "UTC",
    "US/Eastern",
    "US/Central",
    "US/Mountain",
    "US/Pacific",
    "US/Alaska",
    "US/Hawaii",
]

# Keys for OCR / Document Processing configuration
OCR_METHOD_KEY = "ocr_method"
OCR_ENDPOINT_URL_KEY = "ocr_endpoint_url"
DOC_WORKERS_KEY = "doc_workers"
CHUNK_SIZE_TOKENS_KEY = "chunk_size_tokens"
CHUNK_OVERLAP_TOKENS_KEY = "chunk_overlap_tokens"

# Per-source scheduler keys
GRANTS_SCHEDULER_ENABLED_KEY = "grants_scheduler_enabled"
GRANTS_SCHEDULER_INTERVAL_KEY = "grants_scheduler_interval_hours"
COLLABNET_SCHEDULER_ENABLED_KEY = "collabnet_scheduler_enabled"
COLLABNET_SCHEDULER_DAY_KEY = "collabnet_scheduler_day"
COLLABNET_SCHEDULER_HOUR_KEY = "collabnet_scheduler_hour"
COLLABNET_SCHEDULER_MINUTE_KEY = "collabnet_scheduler_minute"


class SettingsService:

    async def get(self, session: AsyncSession, key: str, default: str | None = None) -> str | None:
        """Get a setting value by key, falling back to default."""
        stmt = select(SiteSetting.value).where(SiteSetting.key == key)
        result = await session.execute(stmt)
        value = result.scalar_one_or_none()
        return value if value is not None else default

    async def set(self, session: AsyncSession, key: str, value: str | None) -> None:
        """Set a setting value, creating or updating as needed."""
        stmt = select(SiteSetting).where(SiteSetting.key == key)
        result = await session.execute(stmt)
        existing = result.scalar_one_or_none()

        if existing:
            existing.value = value
        else:
            session.add(SiteSetting(key=key, value=value))

        await session.commit()

    # --- LLM Settings ---

    async def get_llm_settings(self, session: AsyncSession) -> dict[str, str]:
        """Get all LLM settings, falling back to config.py defaults.

        DB value takes precedence if non-empty, otherwise config.py default.
        """
        base_url = await self.get(session, LLM_BASE_URL_KEY)
        model = await self.get(session, LLM_MODEL_KEY)
        api_key = await self.get(session, LLM_API_KEY_KEY)
        return {
            "base_url": base_url or app_settings.LLM_BASE_URL,
            "model": model or app_settings.LLM_MODEL,
            "api_key": api_key or app_settings.LLM_API_KEY,
        }

    async def save_llm_settings(
        self,
        session: AsyncSession,
        base_url: str = "",
        model: str = "",
        api_key: str = "",
    ) -> None:
        """Save all LLM settings to the database."""
        await self.set(session, LLM_BASE_URL_KEY, base_url)
        await self.set(session, LLM_MODEL_KEY, model)
        await self.set(session, LLM_API_KEY_KEY, api_key)

    # --- Embedding Settings ---

    async def get_embedding_settings(self, session: AsyncSession) -> dict[str, str]:
        """Get embedding endpoint settings."""
        base_url = await self.get(session, EMBED_BASE_URL_KEY)
        model = await self.get(session, EMBED_MODEL_KEY)
        api_key = await self.get(session, EMBED_API_KEY_KEY)
        return {
            "base_url": base_url or "",
            "model": model or "",
            "api_key": api_key or "",
        }

    async def save_embedding_settings(
        self,
        session: AsyncSession,
        base_url: str = "",
        model: str = "",
        api_key: str = "",
    ) -> None:
        """Save embedding endpoint settings to the database."""
        await self.set(session, EMBED_BASE_URL_KEY, base_url)
        await self.set(session, EMBED_MODEL_KEY, model)
        await self.set(session, EMBED_API_KEY_KEY, api_key)

    # --- Re-ranker Settings ---

    async def get_reranker_settings(self, session: AsyncSession) -> dict[str, str]:
        """Get re-ranker endpoint settings."""
        base_url = await self.get(session, RERANKER_BASE_URL_KEY)
        model = await self.get(session, RERANKER_MODEL_KEY)
        api_key = await self.get(session, RERANKER_API_KEY_KEY)
        return {
            "base_url": base_url or "",
            "model": model or "",
            "api_key": api_key or "",
        }

    async def save_reranker_settings(
        self,
        session: AsyncSession,
        base_url: str = "",
        model: str = "",
        api_key: str = "",
    ) -> None:
        """Save re-ranker endpoint settings to the database."""
        await self.set(session, RERANKER_BASE_URL_KEY, base_url)
        await self.set(session, RERANKER_MODEL_KEY, model)
        await self.set(session, RERANKER_API_KEY_KEY, api_key)

    # --- Timezone ---

    async def get_timezone(self, session: AsyncSession) -> str:
        """Get the display timezone setting."""
        tz = await self.get(session, TIMEZONE_KEY)
        return tz or DEFAULT_TIMEZONE

    async def save_timezone(self, session: AsyncSession, timezone: str) -> None:
        """Save the display timezone setting."""
        if timezone in TIMEZONE_CHOICES:
            await self.set(session, TIMEZONE_KEY, timezone)

    # --- OCR Settings ---

    async def get_ocr_settings(self, session: AsyncSession) -> dict[str, Any]:
        """Get OCR / document processing settings, falling back to config.py defaults."""
        method = await self.get(session, OCR_METHOD_KEY)
        endpoint_url = await self.get(session, OCR_ENDPOINT_URL_KEY)
        workers = await self.get(session, DOC_WORKERS_KEY)
        chunk_size = await self.get(session, CHUNK_SIZE_TOKENS_KEY)
        chunk_overlap = await self.get(session, CHUNK_OVERLAP_TOKENS_KEY)
        return {
            "method": method or app_settings.OCR_METHOD,
            "endpoint_url": endpoint_url or app_settings.OCR_ENDPOINT_URL,
            "doc_workers": int(workers) if workers else 4,
            "chunk_size_tokens": int(chunk_size) if chunk_size else 1000,
            "chunk_overlap_tokens": int(chunk_overlap) if chunk_overlap else 200,
        }

    async def save_ocr_settings(
        self,
        session: AsyncSession,
        method: str = "",
        endpoint_url: str = "",
        doc_workers: int | None = None,
        chunk_size_tokens: int | None = None,
        chunk_overlap_tokens: int | None = None,
    ) -> None:
        """Save OCR / document processing settings to the database."""
        await self.set(session, OCR_METHOD_KEY, method)
        await self.set(session, OCR_ENDPOINT_URL_KEY, endpoint_url)
        if doc_workers is not None:
            await self.set(session, DOC_WORKERS_KEY, str(max(1, min(doc_workers, 16))))
        if chunk_size_tokens is not None:
            await self.set(session, CHUNK_SIZE_TOKENS_KEY, str(max(100, chunk_size_tokens)))
        if chunk_overlap_tokens is not None:
            await self.set(session, CHUNK_OVERLAP_TOKENS_KEY, str(max(0, chunk_overlap_tokens)))

    # --- Per-source Scheduler Settings ---

    async def get_grants_scheduler_settings(self, session: AsyncSession) -> dict[str, Any]:
        """Get Grants.gov scheduler settings."""
        enabled = await self.get(session, GRANTS_SCHEDULER_ENABLED_KEY)
        interval = await self.get(session, GRANTS_SCHEDULER_INTERVAL_KEY)
        return {
            "enabled": enabled != "false",  # default True
            "interval_hours": int(interval) if interval else app_settings.SYNC_INTERVAL_HOURS,
        }

    async def save_grants_scheduler_settings(
        self,
        session: AsyncSession,
        enabled: bool | None = None,
        interval_hours: int | None = None,
    ) -> None:
        """Save Grants.gov scheduler settings."""
        if enabled is not None:
            await self.set(session, GRANTS_SCHEDULER_ENABLED_KEY, "true" if enabled else "false")
        if interval_hours is not None:
            await self.set(session, GRANTS_SCHEDULER_INTERVAL_KEY, str(interval_hours))

    async def get_collabnet_scheduler_settings(self, session: AsyncSession) -> dict[str, Any]:
        """Get CollabNet scheduler settings."""
        enabled = await self.get(session, COLLABNET_SCHEDULER_ENABLED_KEY)
        day = await self.get(session, COLLABNET_SCHEDULER_DAY_KEY)
        hour = await self.get(session, COLLABNET_SCHEDULER_HOUR_KEY)
        minute = await self.get(session, COLLABNET_SCHEDULER_MINUTE_KEY)
        return {
            "enabled": enabled != "false",  # default True
            "day": day or "fri",
            "hour": int(hour) if hour else 1,
            "minute": int(minute) if minute else 0,
        }

    async def save_collabnet_scheduler_settings(
        self,
        session: AsyncSession,
        enabled: bool | None = None,
        day: str | None = None,
        hour: int | None = None,
        minute: int | None = None,
    ) -> None:
        """Save CollabNet scheduler settings."""
        if enabled is not None:
            await self.set(session, COLLABNET_SCHEDULER_ENABLED_KEY, "true" if enabled else "false")
        if day is not None:
            await self.set(session, COLLABNET_SCHEDULER_DAY_KEY, day)
        if hour is not None:
            await self.set(session, COLLABNET_SCHEDULER_HOUR_KEY, str(hour))
        if minute is not None:
            await self.set(session, COLLABNET_SCHEDULER_MINUTE_KEY, str(minute))


settings_service = SettingsService()

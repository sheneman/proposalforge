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


settings_service = SettingsService()

import json
import logging
from typing import Any

import redis.asyncio as redis

from app.config import settings

logger = logging.getLogger(__name__)

FACET_TTL = 300  # 5 minutes
AGENCY_LIST_TTL = 3600  # 1 hour
STATS_TTL = 300  # 5 minutes


class CacheService:
    def __init__(self):
        self._redis: redis.Redis | None = None

    async def connect(self):
        self._redis = redis.from_url(settings.REDIS_URL, decode_responses=True)

    async def close(self):
        if self._redis:
            await self._redis.close()

    async def get(self, key: str) -> Any | None:
        if not self._redis:
            return None
        try:
            data = await self._redis.get(key)
            if data:
                return json.loads(data)
        except Exception as e:
            logger.warning(f"Cache get error for {key}: {e}")
        return None

    async def set(self, key: str, value: Any, ttl: int = FACET_TTL):
        if not self._redis:
            return
        try:
            await self._redis.set(key, json.dumps(value, default=str), ex=ttl)
        except Exception as e:
            logger.warning(f"Cache set error for {key}: {e}")

    async def delete_pattern(self, pattern: str):
        if not self._redis:
            return
        try:
            keys = []
            async for key in self._redis.scan_iter(match=pattern):
                keys.append(key)
            if keys:
                await self._redis.delete(*keys)
        except Exception as e:
            logger.warning(f"Cache delete error for {pattern}: {e}")

    async def invalidate_all(self):
        await self.delete_pattern("pf:*")


cache_service = CacheService()

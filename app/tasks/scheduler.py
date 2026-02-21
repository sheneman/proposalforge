import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.config import settings
from app.services.sync_service import sync_service

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


def setup_scheduler():
    scheduler.add_job(
        sync_service.incremental_sync,
        "interval",
        hours=settings.SYNC_INTERVAL_HOURS,
        id="incremental_sync",
        name="Incremental sync from Grants.gov",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(f"Scheduler started: incremental sync every {settings.SYNC_INTERVAL_HOURS} hours")

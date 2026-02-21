import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.config import settings
from app.services.sync_service import sync_service

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()

_enabled = True
_interval_hours = settings.SYNC_INTERVAL_HOURS


def setup_scheduler():
    global _interval_hours
    _interval_hours = settings.SYNC_INTERVAL_HOURS
    scheduler.add_job(
        sync_service.incremental_sync,
        "interval",
        hours=_interval_hours,
        id="incremental_sync",
        name="Incremental sync from Grants.gov",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(f"Scheduler started: incremental sync every {_interval_hours} hours")


def is_enabled() -> bool:
    return _enabled


def get_interval_hours() -> int:
    return _interval_hours


def get_next_run_time():
    job = scheduler.get_job("incremental_sync")
    if job:
        return job.next_run_time
    return None


def toggle_scheduler() -> bool:
    """Toggle scheduler on/off. Returns new enabled state."""
    global _enabled
    job = scheduler.get_job("incremental_sync")
    if not job:
        return False

    if _enabled:
        job.pause()
        _enabled = False
        logger.info("Scheduler paused")
    else:
        job.resume()
        _enabled = True
        logger.info("Scheduler resumed")
    return _enabled


def update_interval(hours: int):
    """Update sync interval. Reschedules the job."""
    global _interval_hours
    _interval_hours = hours
    scheduler.reschedule_job(
        "incremental_sync",
        trigger="interval",
        hours=hours,
    )
    logger.info(f"Scheduler interval updated to {hours} hours")

import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.config import settings
from app.services.sync_service import sync_service
from app.services.researcher_sync_service import researcher_sync_service

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()

# Per-source state
_grants_enabled = True
_grants_interval_hours = settings.SYNC_INTERVAL_HOURS

_collabnet_enabled = True
_collabnet_day = "fri"
_collabnet_hour = 1
_collabnet_minute = 0

DAY_CHOICES = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


def setup_scheduler():
    """Initial setup with config defaults (called if DB not available)."""
    global _grants_interval_hours
    _grants_interval_hours = settings.SYNC_INTERVAL_HOURS
    _add_jobs()
    scheduler.start()
    logger.info(
        f"Scheduler started: grants every {_grants_interval_hours}h, "
        f"collabnet {_collabnet_day} {_collabnet_hour:02d}:{_collabnet_minute:02d} UTC"
    )


async def setup_scheduler_from_db():
    """Load persisted settings from DB and start scheduler."""
    global _grants_enabled, _grants_interval_hours
    global _collabnet_enabled, _collabnet_day, _collabnet_hour, _collabnet_minute

    try:
        from app.database import async_session
        from app.services.settings_service import settings_service

        async with async_session() as session:
            grants = await settings_service.get_grants_scheduler_settings(session)
            _grants_enabled = grants["enabled"]
            _grants_interval_hours = grants["interval_hours"]

            collabnet = await settings_service.get_collabnet_scheduler_settings(session)
            _collabnet_enabled = collabnet["enabled"]
            _collabnet_day = collabnet["day"]
            _collabnet_hour = collabnet["hour"]
            _collabnet_minute = collabnet["minute"]

        logger.info(
            f"Scheduler settings loaded from DB: grants every {_grants_interval_hours}h "
            f"({'enabled' if _grants_enabled else 'paused'}), "
            f"collabnet {_collabnet_day} {_collabnet_hour:02d}:{_collabnet_minute:02d} "
            f"({'enabled' if _collabnet_enabled else 'paused'})"
        )
    except Exception as e:
        logger.warning(f"Could not load scheduler settings from DB, using defaults: {e}")

    _add_jobs()
    scheduler.start()

    # Apply paused state after start
    if not _grants_enabled:
        job = scheduler.get_job("incremental_sync")
        if job:
            job.pause()
    if not _collabnet_enabled:
        job = scheduler.get_job("researcher_sync")
        if job:
            job.pause()


def _add_jobs():
    """Add scheduler jobs with current settings."""
    scheduler.add_job(
        sync_service.incremental_sync,
        "interval",
        hours=_grants_interval_hours,
        id="incremental_sync",
        name="Incremental sync from Grants.gov",
        replace_existing=True,
    )
    scheduler.add_job(
        researcher_sync_service.full_sync,
        "cron",
        day_of_week=_collabnet_day,
        hour=_collabnet_hour,
        minute=_collabnet_minute,
        id="researcher_sync",
        name="Weekly researcher sync from CollabNet",
        replace_existing=True,
    )


# --- Grants.gov scheduler ---

def is_grants_enabled() -> bool:
    return _grants_enabled


def get_grants_interval_hours() -> int:
    return _grants_interval_hours


def get_next_run_time(job_id: str = "incremental_sync"):
    job = scheduler.get_job(job_id)
    if job:
        return job.next_run_time
    return None


def toggle_grants_scheduler() -> bool:
    """Toggle Grants.gov scheduler on/off. Returns new enabled state."""
    global _grants_enabled
    job = scheduler.get_job("incremental_sync")
    if not job:
        return False

    if _grants_enabled:
        job.pause()
        _grants_enabled = False
        logger.info("Grants.gov scheduler paused")
    else:
        job.resume()
        _grants_enabled = True
        logger.info("Grants.gov scheduler resumed")
    return _grants_enabled


def update_grants_interval(hours: int):
    """Update Grants.gov sync interval. Reschedules the job."""
    global _grants_interval_hours
    _grants_interval_hours = hours
    scheduler.reschedule_job(
        "incremental_sync",
        trigger="interval",
        hours=hours,
    )
    logger.info(f"Grants.gov scheduler interval updated to {hours} hours")


# --- CollabNet scheduler ---

def is_collabnet_enabled() -> bool:
    return _collabnet_enabled


def get_collabnet_schedule() -> dict:
    return {
        "day": _collabnet_day,
        "hour": _collabnet_hour,
        "minute": _collabnet_minute,
    }


def toggle_collabnet_scheduler() -> bool:
    """Toggle CollabNet scheduler on/off. Returns new enabled state."""
    global _collabnet_enabled
    job = scheduler.get_job("researcher_sync")
    if not job:
        return False

    if _collabnet_enabled:
        job.pause()
        _collabnet_enabled = False
        logger.info("CollabNet scheduler paused")
    else:
        job.resume()
        _collabnet_enabled = True
        logger.info("CollabNet scheduler resumed")
    return _collabnet_enabled


def update_collabnet_schedule(day: str, hour: int, minute: int):
    """Update CollabNet sync schedule (cron). Reschedules the job."""
    global _collabnet_day, _collabnet_hour, _collabnet_minute
    _collabnet_day = day
    _collabnet_hour = hour
    _collabnet_minute = minute
    scheduler.reschedule_job(
        "researcher_sync",
        trigger="cron",
        day_of_week=day,
        hour=hour,
        minute=minute,
    )
    logger.info(f"CollabNet scheduler updated to {day} {hour:02d}:{minute:02d} UTC")


# --- Backwards compatibility ---

def is_enabled() -> bool:
    return _grants_enabled


def get_interval_hours() -> int:
    return _grants_interval_hours


def toggle_scheduler() -> bool:
    return toggle_grants_scheduler()


def update_interval(hours: int):
    update_grants_interval(hours)

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from app.config import settings
from app.database import engine, Base
from app.services.cache_service import cache_service
from app.services.sync_service import sync_service
from app.services.researcher_sync_service import researcher_sync_service
from app.tasks.scheduler import setup_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting ProposalForge...")

    # Connect to Redis
    await cache_service.connect()

    # Create tables directly via SQLAlchemy
    import app.models  # noqa: F401 - ensure all models are imported
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables created")

    # Clean up any orphaned "running" sync logs from prior crashes/restarts
    await sync_service._mark_stale_syncs()

    # Start scheduler and optional initial sync only in one worker.
    # Uvicorn workers each run lifespan; use a Redis lock to ensure only one proceeds.
    _is_primary = False
    try:
        _is_primary = await cache_service.acquire_primary_lock()
    except Exception:
        logger.warning("Could not acquire primary lock, assuming single worker")
        _is_primary = True

    if _is_primary:
        setup_scheduler()
        if settings.SYNC_ON_STARTUP:
            asyncio.create_task(sync_service.full_sync())
        if settings.RESEARCHER_SYNC_ON_STARTUP:
            asyncio.create_task(researcher_sync_service.full_sync())

    yield

    # Shutdown
    from app.tasks.scheduler import scheduler
    scheduler.shutdown(wait=False)
    await cache_service.close()
    await engine.dispose()
    logger.info("ProposalForge shutdown complete")


app = FastAPI(
    title="ProposalForge",
    description="Federal grant opportunity search and discovery",
    version="1.0.0",
    lifespan=lifespan,
)

# Global exception handler — silent single retry, then show error
@app.exception_handler(500)
async def internal_error_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled error on {request.url.path}: {exc}", exc_info=True)
    # Only auto-retry once to avoid infinite reload loops
    if request.query_params.get("_retry"):
        return HTMLResponse(
            content=(
                '<!DOCTYPE html><html><head><meta charset="utf-8">'
                "<title>ProposalForge</title>"
                "</head><body style=\"font-family:system-ui;text-align:center;padding:60px\">"
                "<h2>Something went wrong</h2>"
                "<p>Please try refreshing the page.</p>"
                "</body></html>"
            ),
            status_code=500,
        )
    # Build retry URL with _retry param
    retry_url = str(request.url)
    retry_url += "&_retry=1" if "?" in retry_url else "?_retry=1"
    return HTMLResponse(
        content=(
            '<!DOCTYPE html><html><head><meta charset="utf-8">'
            f'<script>window.location.replace("{retry_url}");</script>'
            "</head><body></body></html>"
        ),
        status_code=500,
    )

# Session middleware for admin auth
app.add_middleware(SessionMiddleware, secret_key=settings.SECRET_KEY)

# Mount static files
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Include routers
from app.api.pages import router as pages_router
from app.api.search import router as search_router
from app.api.opportunities import router as opportunities_router
from app.api.sync import router as sync_router
from app.api.admin import router as admin_router
from app.api.analytics import router as analytics_router
from app.api.researchers import router as researchers_router
from app.api.matches import router as matches_router

app.include_router(pages_router)
app.include_router(search_router)
app.include_router(opportunities_router)
app.include_router(sync_router)
app.include_router(admin_router)
app.include_router(analytics_router)
app.include_router(researchers_router)
app.include_router(matches_router)

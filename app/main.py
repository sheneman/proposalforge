import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.database import engine, Base
from app.services.cache_service import cache_service
from app.services.sync_service import sync_service
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

    # Start scheduler
    setup_scheduler()

    # Optional initial sync
    if settings.SYNC_ON_STARTUP:
        asyncio.create_task(sync_service.full_sync())

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

# Mount static files
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Include routers
from app.api.pages import router as pages_router
from app.api.search import router as search_router
from app.api.opportunities import router as opportunities_router
from app.api.sync import router as sync_router
from app.api.admin import router as admin_router

app.include_router(pages_router)
app.include_router(search_router)
app.include_router(opportunities_router)
app.include_router(sync_router)
app.include_router(admin_router)

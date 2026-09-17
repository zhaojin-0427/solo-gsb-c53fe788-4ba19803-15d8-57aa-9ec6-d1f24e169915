"""FastAPI application entry point."""
import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from .config import get_settings
from .database import close_pool, init_pool
from .reaper import reaper_loop
from .routers import admin, quota

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    app.state.settings = settings
    db_pool = await init_pool(
        settings.database_url,
        min_size=settings.db_min_size,
        max_size=settings.db_max_size,
        timeout=settings.db_connect_timeout,
    )
    reaper_task = asyncio.create_task(
        reaper_loop(settings.reaper_interval_seconds,
                    settings.reaper_batch_size),
        name="quota-reaper",
    )
    try:
        yield
    finally:
        reaper_task.cancel()
        try:
            await reaper_task
        except asyncio.CancelledError:
            pass
        await close_pool()


app = FastAPI(
    title="Distributed Quota Decision API",
    version="1.0.0",
    description="Three-level (tenant / subject / action) token-bucket "
                "quota reservations with idempotent reserve/commit/cancel.",
    lifespan=lifespan,
)


@app.middleware("http")
async def inject_pool(request: Request, call_next):
    # The pool is a module-level singleton in app.database.
    from .database import pool as get_pool
    request.state.pool = get_pool()
    return await call_next(request)


app.include_router(quota.router)
app.include_router(admin.router)


@app.get("/health", tags=["meta"])
async def health():
    return {"status": "ok"}

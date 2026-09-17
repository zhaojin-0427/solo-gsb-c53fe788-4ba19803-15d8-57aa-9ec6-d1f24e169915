"""Pytest fixtures: embedded PostgreSQL + ASGI test client."""
import asyncio
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from pgserver import PostgresServer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import get_settings  # noqa: E402
from app.database import close_pool, init_pool  # noqa: E402
from app.main import app  # noqa: E402


@pytest.fixture(scope="session")
def event_loop():
    """Single loop for the whole session so the pool and pgserver share it."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def pg_dsn():
    data_dir = Path(tempfile.mkdtemp(prefix="quota-pg-"))
    server = PostgresServer(data_dir)
    dsn = server.get_uri("postgres")
    yield dsn
    try:
        server.cleanup()
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)


@pytest_asyncio.fixture(scope="session", loop_scope="session", autouse=True)
async def pool_ready(pg_dsn):
    get_settings.cache_clear()
    os.environ["QUOTA_DATABASE_URL"] = pg_dsn
    os.environ["QUOTA_DEFAULT_CAPACITY"] = "100"
    os.environ["QUOTA_DEFAULT_REFILL_RATE"] = "10"
    os.environ["QUOTA_RESERVATION_TTL_SECONDS"] = "2"
    os.environ["QUOTA_REAPER_INTERVAL_SECONDS"] = "0.2"
    get_settings.cache_clear()

    s = get_settings()
    await init_pool(s.database_url, min_size=2, max_size=20, timeout=30)
    yield
    await close_pool()


@pytest_asyncio.fixture(loop_scope="session")
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport,
                           base_url="http://test") as ac:
        yield ac


@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def clean_db(pool_ready):
    from app.database import pool
    async with pool().acquire() as conn:
        await conn.execute("TRUNCATE quota_reservations, quota_buckets")
    yield

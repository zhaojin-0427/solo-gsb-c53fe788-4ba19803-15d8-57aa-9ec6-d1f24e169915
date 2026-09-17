"""Background loop that cancels expired, uncommitted reservations."""
import asyncio
import logging

from .database import pool

logger = logging.getLogger("quota.reaper")


async def reaper_loop(interval: float, batch_size: int) -> None:
    while True:
        try:
            count = await pool().fetchval(
                "SELECT quota_recycle_expired($1)", batch_size)
            if count:
                logger.info("recycled %d expired reservation(s)", count)
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - keep the loop alive
            logger.exception("reaper iteration failed")
        await asyncio.sleep(interval)

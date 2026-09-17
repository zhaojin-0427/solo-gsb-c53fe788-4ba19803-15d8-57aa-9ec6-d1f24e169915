"""Helpers shared by the HTTP routers: fingerprinting and DB calls."""
import json
from typing import Any

from asyncpg import Pool

from .config import get_settings


def fingerprint(body: dict[str, Any]) -> str:
    """Canonical JSON of all request parameters (excluding request_id).

    A retry with the same request_id but different tenant/subject/action/
    cost is reported as 409 conflict.
    """
    key = {k: body[k] for k in ("tenant", "subject", "action", "cost")}
    return json.dumps(key, sort_keys=True, separators=(",", ":"))


async def db_reserve(pool: Pool, body: dict[str, Any]) -> tuple[dict, bool]:
    s = get_settings()
    row = await pool.fetchrow(
        """
        SELECT result, is_new FROM quota_reserve(
            $1,$2,$3,$4,$5::numeric,$6::jsonb,$7,$8::numeric,$9::numeric)
        """,
        body["request_id"], body["tenant"], body["subject"], body["action"],
        str(body["cost"]), fingerprint(body),
        s.reservation_ttl_seconds,
        str(s.default_capacity), str(s.default_refill_rate),
    )
    return json.loads(row["result"]), row["is_new"]


async def db_commit(pool: Pool, body: dict[str, Any]) -> dict:
    row = await pool.fetchrow(
        "SELECT quota_commit($1, $2::jsonb) AS result",
        body["request_id"], fingerprint(body),
    )
    return json.loads(row["result"])


async def db_cancel(pool: Pool, body: dict[str, Any]) -> dict:
    row = await pool.fetchrow(
        "SELECT quota_cancel($1, $2::jsonb) AS result",
        body["request_id"], fingerprint(body),
    )
    return json.loads(row["result"])

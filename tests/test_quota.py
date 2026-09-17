"""End-to-end tests for the quota decision API."""
import asyncio
import uuid

import pytest

from app.database import pool

pytestmark = pytest.mark.asyncio(loop_scope="session")


def req(cost=10.0, **over):
    body = {"tenant": "t1", "subject": "u1", "action": "read",
            "cost": cost, "request_id": str(uuid.uuid4())}
    body.update(over)
    return body


async def bucket_tokens(tenant="t1", subject="u1", action="read"):
    async with pool().acquire() as conn:
        rows = {
            r["level"]: float(r["tokens"])
            for r in await conn.fetch(
                "SELECT level, tokens FROM quota_buckets WHERE tenant=$1",
                tenant)
        }
    return rows


# --- basic grant / deny ----------------------------------------------------

async def test_reserve_grants_and_deducts_three_levels(client):
    await set_policy(client, "tenant", 100, 0)
    await set_policy(client, "subject", 100, 0)
    await set_policy(client, "action", 100, 0)
    r = await client.post("/v1/reserve", json=req(cost=10))
    assert r.status_code == 200
    data = r.json()
    assert data["outcome"] == "granted"
    assert len(data["available"]) == 3

    tokens = await bucket_tokens()
    assert tokens == {"tenant": 90.0, "subject": 90.0, "action": 90.0}


async def test_denied_lists_every_deficient_level_and_max_retry(client):
    # Only the action bucket is constrained; tenant/subject are generous.
    await set_policy(client, "tenant", 10000, 1000)
    await set_policy(client, "subject", 10000, 1000)
    await set_policy(client, "action", 60, 1)
    # Drain the action bucket so the next request is denied immediately.
    await client.post("/v1/reserve", json=req(cost=55))
    r = await client.post("/v1/reserve", json=req(cost=50))
    assert r.status_code == 429
    data = r.json()
    assert data["outcome"] == "denied"
    assert [lv["level"] for lv in data["levels"]] == ["action"]
    level = data["levels"][0]
    assert level["required"] == 50
    assert level["retry_after_seconds"] == data["retry_after_seconds"]
    assert 44 <= level["retry_after_seconds"] <= 46

    # No deduction on denial.
    tokens = await bucket_tokens()
    assert abs(tokens["action"] - 5.0) < 0.01


async def set_policy(client, level, capacity, refill_rate,
                     tenant="t1", subject="u1", action="read"):
    q = f"tenant={tenant}"
    if level in ("subject", "action"):
        q += f"&subject={subject}"
    if level == "action":
        q += f"&action={action}"
    r = await client.put(f"/admin/policies/{level}?{q}",
                         json={"capacity": capacity,
                               "refill_rate": refill_rate})
    assert r.status_code == 200, r.text


async def test_denied_reports_all_three_short_levels(client):
    await set_policy(client, "tenant", 30, 0)
    await set_policy(client, "subject", 30, 0)
    await set_policy(client, "action", 30, 0)
    body = req(cost=50)
    r = await client.post("/v1/reserve", json=body)
    assert r.status_code == 429
    levels = {lv["level"]: lv for lv in r.json()["levels"]}
    assert set(levels) == {"tenant", "subject", "action"}
    # refill_rate 0 everywhere -> never enough
    assert all(lv["retry_after_seconds"] is None for lv in levels.values())
    assert r.json()["retry_after_seconds"] is None


async def test_cost_above_capacity_is_not_retryable(client):
    await client.put("/admin/policies/action?tenant=t1&subject=u1&action=read",
                     json={"capacity": 5, "refill_rate": 100})
    r = await client.post("/v1/reserve", json=req(cost=50))
    action = [lv for lv in r.json()["levels"] if lv["level"] == "action"][0]
    assert action["retryable"] is False
    assert action["retry_after_seconds"] is None
    assert r.json()["retry_after_seconds"] is None


# --- idempotency -----------------------------------------------------------

async def test_same_request_id_replays_grant(client):
    await set_policy(client, "tenant", 100, 0)
    await set_policy(client, "subject", 100, 0)
    await set_policy(client, "action", 100, 0)
    body = req(cost=10)
    r1 = await client.post("/v1/reserve", json=body)
    r2 = await client.post("/v1/reserve", json=body)
    assert r1.status_code == r2.status_code == 200
    assert r1.json()["expires_at"] == r2.json()["expires_at"]
    assert r2.json()["replayed"] is True
    # Deducted only once.
    tokens = await bucket_tokens()
    assert tokens["action"] == 90.0


async def test_same_request_id_replays_denial(client):
    await client.put("/admin/policies/action?tenant=t1&subject=u1&action=read",
                     json={"capacity": 5, "refill_rate": 0})
    body = req(cost=50)
    r1 = await client.post("/v1/reserve", json=body)
    r2 = await client.post("/v1/reserve", json=body)
    assert r1.status_code == r2.status_code == 429
    assert r2.json()["replayed"] is True
    assert r1.json()["retry_after_seconds"] == r2.json()["retry_after_seconds"]


async def test_changed_parameters_conflict(client):
    body = req(cost=10)
    await client.post("/v1/reserve", json=body)
    body["cost"] = 20
    r = await client.post("/v1/reserve", json=body)
    assert r.status_code == 409


# --- commit / cancel -------------------------------------------------------

async def test_commit_deducts_nothing_and_is_idempotent(client):
    body = req(cost=10)
    await client.post("/v1/reserve", json=body)
    c1 = await client.post("/v1/commit", json=body)
    c2 = await client.post("/v1/commit", json=body)
    assert c1.status_code == 200 and c1.json()["replayed"] is False
    assert c2.status_code == 200 and c2.json()["replayed"] is True

    tokens = await bucket_tokens()
    assert tokens["action"] == 90.0  # still only the reserve deduction

    # Cancel after commit is rejected and refunds nothing.
    x = await client.post("/v1/cancel", json=body)
    assert x.status_code == 409
    tokens2 = await bucket_tokens()
    assert tokens2 == tokens


async def test_cancel_refunds_once_and_is_idempotent(client):
    await set_policy(client, "tenant", 100, 0)
    await set_policy(client, "subject", 100, 0)
    await set_policy(client, "action", 100, 0)
    body = req(cost=10)
    await client.post("/v1/reserve", json=body)
    x1 = await client.post("/v1/cancel", json=body)
    x2 = await client.post("/v1/cancel", json=body)
    assert x1.json() == {"outcome": "cancelled", "refunded": True,
                         "replayed": False}
    assert x2.status_code == 200
    assert x2.json()["refunded"] is False and x2.json()["replayed"] is True

    tokens = await bucket_tokens()
    assert tokens == {"tenant": 100.0, "subject": 100.0, "action": 100.0}

    # Commit after cancel is rejected.
    assert (await client.post("/v1/commit", json=body)).status_code == 409


async def test_unknown_and_fingerprint_mismatch(client):
    body = req()
    assert (await client.post("/v1/commit", json=body)).status_code == 404
    assert (await client.post("/v1/cancel", json=body)).status_code == 404

    await client.post("/v1/reserve", json=body)
    changed = {**body, "cost": 1}
    assert (await client.post("/v1/commit", json=changed)).status_code == 409
    assert (await client.post("/v1/cancel", json=changed)).status_code == 409


# --- concurrency: no oversell, no double refund ----------------------------

async def test_concurrent_reserves_never_oversell(client):
    await set_policy(client, "tenant", 100, 0)
    await set_policy(client, "subject", 100, 0)
    await set_policy(client, "action", 100, 0)
    n = 30
    bodies = [req(cost=10) for _ in range(n)]  # capacity 100 -> 10 fit

    results = await asyncio.gather(*[
        client.post("/v1/reserve", json=b) for b in bodies
    ])
    granted = [r for r in results if r.status_code == 200]
    denied = [r for r in results if r.status_code == 429]
    assert len(granted) == 10
    assert len(denied) == 20

    tokens = await bucket_tokens()
    assert tokens == {"tenant": 0.0, "subject": 0.0, "action": 0.0}


async def test_concurrent_reserves_same_request_id_single_grant(client):
    body = req(cost=10)
    results = await asyncio.gather(*[
        client.post("/v1/reserve", json=body) for _ in range(20)
    ])
    assert sum(r.status_code == 200 for r in results) == 20
    assert sum(r.json().get("replayed") for r in results) == 19
    tokens = await bucket_tokens()
    assert tokens["action"] == 90.0


async def test_duplicate_submit_after_grant_safe(client):
    await set_policy(client, "tenant", 100, 0)
    await set_policy(client, "subject", 100, 0)
    await set_policy(client, "action", 100, 0)
    body = req(cost=10)
    await client.post("/v1/reserve", json=body)
    results = await asyncio.gather(*[
        client.post("/v1/commit", json=body) for _ in range(10)
    ])
    assert all(r.status_code == 200 for r in results)
    assert sum(r.json()["replayed"] for r in results) == 9
    tokens = await bucket_tokens()
    assert tokens["action"] == 90.0


async def test_cancel_vs_expiry_recycle_refunds_once(client):
    # TTL is 2s in the test configuration.
    await set_policy(client, "tenant", 100, 0)
    await set_policy(client, "subject", 100, 0)
    await set_policy(client, "action", 100, 0)
    body = req(cost=10)
    await client.post("/v1/reserve", json=body)

    await asyncio.sleep(2.2)

    # Fire cancel and two reaper cycles concurrently.
    cancel_res, r1, r2 = await asyncio.gather(
        client.post("/v1/cancel", json=body),
        client.post("/admin/recycle"),
        client.post("/admin/recycle"),
    )
    assert cancel_res.status_code == 200
    # Exactly one refund regardless of whether cancel or reaper won.
    reaped = r1.json()["expired"] + r2.json()["expired"]
    refunded = int(cancel_res.json()["refunded"]) + reaped
    assert refunded == 1
    if reaped:
        assert cancel_res.json()["outcome"] == "expired"
    else:
        assert cancel_res.json()["outcome"] == "cancelled"

    tokens = await bucket_tokens()
    assert tokens == {"tenant": 100.0, "subject": 100.0, "action": 100.0}

    # A late cancel replays and never refunds again.
    again = await client.post("/v1/cancel", json=body)
    assert again.status_code == 200 and again.json()["refunded"] is False
    tokens2 = await bucket_tokens()
    assert tokens2 == tokens


async def test_expiry_frees_capacity_for_new_request(client):
    await set_policy(client, "tenant", 100, 0)
    await set_policy(client, "subject", 100, 0)
    await set_policy(client, "action", 100, 0)
    body = req(cost=90)
    assert (await client.post("/v1/reserve", json=body)).status_code == 200
    assert (await client.post("/v1/reserve",
            json=req(cost=20))).status_code == 429

    await asyncio.sleep(2.2)
    await client.post("/admin/recycle")

    assert (await client.post("/v1/reserve",
            json=req(cost=20))).status_code == 200


# --- policy isolation ------------------------------------------------------

async def test_policy_update_does_not_change_existing_reservation(client):
    body = req(cost=10)
    r = await client.post("/v1/reserve", json=body)
    expires_before = r.json()["expires_at"]

    await client.put("/admin/policies/action?tenant=t1&subject=u1&action=read",
                     json={"capacity": 1, "refill_rate": 0})

    # Existing grant is untouched and can still be committed.
    assert (await client.post("/v1/commit", json=body)).status_code == 200
    assert r.json()["expires_at"] == expires_before

    # New applications see the new policy.
    assert (await client.post("/v1/reserve",
            json=req(cost=5))).status_code == 429

"""/admin endpoints: policies, bucket state, reservations, reaper."""
from fastapi import APIRouter, HTTPException, Query, Request

from ..config import get_settings
from ..schemas import PolicyRequest

router = APIRouter(prefix="/admin", tags=["admin"])


def _bucket_key(level: str, tenant: str, subject: str, action: str):
    if level == "tenant":
        return tenant, "", ""
    if level == "subject":
        return tenant, subject, ""
    return tenant, subject, action


def _bucket_out(row) -> dict:
    return {
        "tenant": row["tenant"],
        "subject": row["subject"] or None,
        "action": row["action"] or None,
        "level": row["level"],
        "capacity": float(row["capacity"]),
        "refill_rate": float(row["refill_rate"]),
        "tokens": float(row["tokens"]),
        "updated_at": row["updated_at"].isoformat(),
    }


@router.put("/policies/{level}")
async def upsert_policy(level: str, payload: PolicyRequest, request: Request,
                        tenant: str = Query(..., min_length=1),
                        subject: str = Query(""),
                        action: str = Query("")):
    if level not in ("tenant", "subject", "action"):
        raise HTTPException(status_code=422,
                            detail="level must be tenant|subject|action")
    if level == "subject" and not subject:
        raise HTTPException(status_code=422,
                            detail="subject is required for subject level")
    if level == "action" and (not subject or not action):
        raise HTTPException(status_code=422,
                            detail="subject and action are required "
                                   "for action level")

    t, s, a = _bucket_key(level, tenant, subject, action)
    await request.state.pool.execute(
        "SELECT quota_upsert_policy($1,$2,$3,$4,$5::numeric,$6::numeric)",
        t, s, a, level, str(payload.capacity), str(payload.refill_rate),
    )
    row = await request.state.pool.fetchrow(
        "SELECT * FROM quota_buckets WHERE tenant=$1 AND subject=$2 "
        "AND action=$3", t, s, a)
    return _bucket_out(row)


@router.get("/policies/{level}")
async def get_policy(level: str, request: Request,
                     tenant: str = Query(...),
                     subject: str = Query(""),
                     action: str = Query("")):
    if level not in ("tenant", "subject", "action"):
        raise HTTPException(status_code=422, detail="unknown level")
    t, s, a = _bucket_key(level, tenant, subject, action)
    row = await request.state.pool.fetchrow(
        "SELECT * FROM quota_buckets WHERE tenant=$1 AND subject=$2 "
        "AND action=$3", t, s, a)
    if row is None:
        raise HTTPException(status_code=404, detail="policy not found")
    return _bucket_out(row)


@router.get("/buckets")
async def list_buckets(request: Request, tenant: str = Query(...)):
    rows = await request.state.pool.fetch(
        "SELECT * FROM quota_buckets WHERE tenant=$1 "
        "ORDER BY subject, action", tenant)
    return [_bucket_out(r) for r in rows]


@router.get("/reservations/{request_id}")
async def get_reservation(request_id: str, request: Request):
    row = await request.state.pool.fetchrow(
        "SELECT * FROM quota_reservations WHERE request_id=$1", request_id)
    if row is None:
        raise HTTPException(status_code=404, detail="reservation not found")
    return {
        "request_id": row["request_id"],
        "tenant": row["tenant"],
        "subject": row["subject"],
        "action": row["action"],
        "cost": float(row["cost"]),
        "status": row["status"],
        "expires_at": row["expires_at"].isoformat()
            if row["expires_at"] else None,
        "created_at": row["created_at"].isoformat(),
        "updated_at": row["updated_at"].isoformat(),
    }


@router.post("/recycle", status_code=200)
async def run_recycle(request: Request):
    """Synchronously expire overdue grants (also runs automatically)."""
    count = await request.state.pool.fetchval(
        "SELECT quota_recycle_expired($1)",
        get_settings().reaper_batch_size)
    return {"expired": count}

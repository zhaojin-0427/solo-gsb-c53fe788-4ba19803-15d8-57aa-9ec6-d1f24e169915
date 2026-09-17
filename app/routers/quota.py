"""/v1/reserve, /v1/commit, /v1/cancel endpoints."""
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from .. import service
from ..schemas import (CancelRequest, CommitRequest, ReserveRequest)

router = APIRouter(prefix="/v1", tags=["quota"])


@router.post("/reserve", status_code=200)
async def reserve(payload: ReserveRequest, request: Request):
    body = payload.model_dump()
    result, is_new = await service.db_reserve(request.state.pool, body)
    outcome = result["outcome"]

    if outcome == "conflict":
        raise HTTPException(
            status_code=409,
            detail={"error": "request_id_conflict",
                    "message": "request_id was already used with different "
                               "parameters"},
        )

    # Stored decisions carry replayed=true; the first call inserts the row.
    result["replayed"] = not is_new
    return JSONResponse(result,
                        status_code=200 if outcome == "granted" else 429)


@router.post("/commit")
async def commit(payload: CommitRequest, request: Request):
    body = payload.model_dump()
    result = await service.db_commit(request.state.pool, body)
    outcome = result["outcome"]

    if outcome == "not_found":
        raise HTTPException(status_code=404,
                            detail="unknown request_id")
    if outcome == "conflict":
        raise HTTPException(status_code=409,
                            detail="request parameters do not match the "
                                   "original reservation")
    if outcome != "committed":
        # committed/cancelled/expired reservations cannot be committed.
        raise HTTPException(
            status_code=409,
            detail={"error": f"reservation_{outcome}",
                    "message": f"reservation is already {outcome}"},
        )

    return {"outcome": "committed", "replayed": result.get("replayed", False)}


@router.post("/cancel")
async def cancel(payload: CancelRequest, request: Request):
    body = payload.model_dump()
    result = await service.db_cancel(request.state.pool, body)
    outcome = result["outcome"]

    if outcome == "not_found":
        raise HTTPException(status_code=404, detail="unknown request_id")
    if outcome == "conflict":
        raise HTTPException(status_code=409,
                            detail="request parameters do not match the "
                                   "original reservation")
    if outcome not in ("cancelled", "expired"):
        # committed / denied -> illegal transition; no tokens are returned.
        raise HTTPException(
            status_code=409,
            detail={"error": f"reservation_{outcome}",
                    "message": f"reservation is already {outcome}"},
        )

    return {"outcome": outcome,
            "refunded": result.get("refunded", False),
            "replayed": result.get("replayed", False)}

"""Pydantic request models for the quota API.

Responses are returned as plain JSON (the reserve outcome shape varies),
so only request bodies are modelled here.
"""
from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ReserveRequest(StrictModel):
    tenant: str = Field(min_length=1)
    subject: str = Field(min_length=1)
    action: str = Field(min_length=1)
    cost: float = Field(gt=0)
    request_id: str = Field(min_length=1)


class CommitRequest(ReserveRequest):
    """Commit reuses the full reserve fingerprint."""


class CancelRequest(ReserveRequest):
    pass


class PolicyRequest(StrictModel):
    capacity: float = Field(gt=0)
    refill_rate: float = Field(ge=0)

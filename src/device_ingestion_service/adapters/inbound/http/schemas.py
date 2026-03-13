from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class IngestionRequestBody(BaseModel):
    organization_id: str = Field(min_length=1)
    site_id: str = Field(min_length=1)
    protocol: str = Field(min_length=1)
    topic: str = Field(min_length=1)
    payload: dict[str, Any]


class IngestItemResponse(BaseModel):
    status: Literal["accepted", "duplicate", "dead_letter"]
    protocol: str
    point_key: str | None = None
    telemetry_id: str | None = None
    duplicate_of: str | None = None
    outbox_event_id: str | None = None
    dead_letter_id: str | None = None
    reason: str | None = None


class IngestSummaryResponse(BaseModel):
    accepted: int
    duplicate: int
    dead_letter: int


class IngestionResponse(BaseModel):
    summary: IngestSummaryResponse
    items: list[IngestItemResponse]


class DeadLetterResponse(BaseModel):
    dead_letter_id: str
    organization_id: str
    site_id: str
    protocol: str
    topic: str
    reason: str
    payload: dict[str, Any]
    received_at: datetime


class HealthResponse(BaseModel):
    status: str
    service: str
    env: str
    persistence_backend: str


class MetricsResponse(BaseModel):
    status: dict[str, int]
    protocol: dict[str, int]

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any


class IngestionStatus(StrEnum):
    accepted = "accepted"
    duplicate = "duplicate"
    dead_letter = "dead_letter"


@dataclass(frozen=True)
class ParsedMeasurement:
    source: str
    message_id: str
    device_id: str
    point_key: str
    value: float | int | bool | str
    observed_at: datetime
    unit: str | None


@dataclass(frozen=True)
class TelemetryEvent:
    telemetry_id: str
    organization_id: str
    site_id: str
    source: str
    message_id: str
    device_id: str
    point_key: str
    value: float | int | bool | str
    observed_at: datetime
    unit: str | None
    payload_hash: str
    received_at: datetime


@dataclass(frozen=True)
class OutboxEvent:
    event_id: str
    aggregate_type: str
    aggregate_id: str
    event_type: str
    payload: dict[str, Any]
    created_at: datetime


@dataclass(frozen=True)
class DeadLetterEvent:
    dead_letter_id: str
    organization_id: str
    site_id: str
    protocol: str
    topic: str
    reason: str
    payload: dict[str, Any]
    received_at: datetime


@dataclass(frozen=True)
class DeadLetterRecord:
    dead_letter_id: str
    organization_id: str
    site_id: str
    protocol: str
    topic: str
    reason: str
    payload: dict[str, Any]
    received_at: datetime


@dataclass(frozen=True)
class IngestResultItem:
    status: IngestionStatus
    protocol: str
    point_key: str | None
    telemetry_id: str | None
    duplicate_of: str | None
    outbox_event_id: str | None
    dead_letter_id: str | None
    reason: str | None


@dataclass(frozen=True)
class IngestBatchResult:
    items: list[IngestResultItem]

    @property
    def accepted_count(self) -> int:
        return sum(1 for item in self.items if item.status == IngestionStatus.accepted)

    @property
    def duplicate_count(self) -> int:
        return sum(1 for item in self.items if item.status == IngestionStatus.duplicate)

    @property
    def dead_letter_count(self) -> int:
        return sum(1 for item in self.items if item.status == IngestionStatus.dead_letter)

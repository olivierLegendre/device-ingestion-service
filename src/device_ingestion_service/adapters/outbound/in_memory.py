from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime

from device_ingestion_service.application.uow import UnitOfWork
from device_ingestion_service.domain.entities import (
    DeadLetterEvent,
    DeadLetterRecord,
    OutboxEvent,
    TelemetryEvent,
)


@dataclass
class _InMemoryStore:
    telemetry: dict[str, TelemetryEvent] = field(default_factory=dict)
    telemetry_index: list[tuple[str, str, datetime, str]] = field(default_factory=list)
    outbox: dict[str, OutboxEvent] = field(default_factory=dict)
    dead_letters: dict[str, DeadLetterEvent] = field(default_factory=dict)


class InMemoryTelemetryRepository:
    def __init__(self, store: _InMemoryStore) -> None:
        self._store = store

    def find_recent_duplicate(
        self, *, site_id: str, payload_hash: str, since: datetime
    ) -> str | None:
        for indexed_site, indexed_hash, created_at, telemetry_id in reversed(
            self._store.telemetry_index
        ):
            if indexed_site != site_id:
                continue
            if indexed_hash != payload_hash:
                continue
            if created_at >= since:
                return telemetry_id
        return None

    def add(self, item: TelemetryEvent) -> TelemetryEvent:
        self._store.telemetry[item.telemetry_id] = item
        self._store.telemetry_index.append(
            (item.site_id, item.payload_hash, item.received_at, item.telemetry_id)
        )
        return item


class InMemoryOutboxRepository:
    def __init__(self, store: _InMemoryStore) -> None:
        self._store = store

    def add(self, item: OutboxEvent) -> OutboxEvent:
        self._store.outbox[item.event_id] = item
        return item


class InMemoryDeadLetterRepository:
    def __init__(self, store: _InMemoryStore) -> None:
        self._store = store

    def add(self, item: DeadLetterEvent) -> DeadLetterEvent:
        self._store.dead_letters[item.dead_letter_id] = item
        return item

    def list_recent(
        self,
        *,
        organization_id: str,
        site_id: str,
        limit: int,
    ) -> Sequence[DeadLetterRecord]:
        rows = [
            row
            for row in self._store.dead_letters.values()
            if row.organization_id == organization_id and row.site_id == site_id
        ]
        rows.sort(key=lambda row: row.received_at, reverse=True)
        limited = rows[:limit]
        return [
            DeadLetterRecord(
                dead_letter_id=row.dead_letter_id,
                organization_id=row.organization_id,
                site_id=row.site_id,
                protocol=row.protocol,
                topic=row.topic,
                reason=row.reason,
                payload=row.payload,
                received_at=row.received_at,
            )
            for row in limited
        ]


class InMemoryUnitOfWork(UnitOfWork):
    def __init__(self, store: _InMemoryStore | None = None) -> None:
        self._store = store or _InMemoryStore()

    def __enter__(self) -> "InMemoryUnitOfWork":
        self.telemetry = InMemoryTelemetryRepository(self._store)
        self.outbox = InMemoryOutboxRepository(self._store)
        self.dead_letters = InMemoryDeadLetterRepository(self._store)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type:
            self.rollback()

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

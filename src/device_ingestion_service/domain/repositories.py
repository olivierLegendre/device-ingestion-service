from collections.abc import Sequence
from datetime import datetime
from typing import Protocol

from device_ingestion_service.domain.entities import (
    DeadLetterEvent,
    DeadLetterRecord,
    OutboxEvent,
    TelemetryEvent,
)


class TelemetryRepository(Protocol):
    def find_recent_duplicate(
        self,
        *,
        site_id: str,
        payload_hash: str,
        since: datetime,
    ) -> str | None: ...

    def add(self, item: TelemetryEvent) -> TelemetryEvent: ...


class OutboxRepository(Protocol):
    def add(self, item: OutboxEvent) -> OutboxEvent: ...


class DeadLetterRepository(Protocol):
    def add(self, item: DeadLetterEvent) -> DeadLetterEvent: ...

    def list_recent(
        self,
        *,
        organization_id: str,
        site_id: str,
        limit: int,
    ) -> Sequence[DeadLetterRecord]: ...

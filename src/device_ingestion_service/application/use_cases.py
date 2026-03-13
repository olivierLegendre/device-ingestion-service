import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import Lock
from uuid import uuid4

from device_ingestion_service.adapters.inbound.mqtt.protocols import (
    ProtocolEnvelope,
    ProtocolParser,
)
from device_ingestion_service.application.uow import UnitOfWork
from device_ingestion_service.domain.entities import (
    DeadLetterEvent,
    DeadLetterRecord,
    IngestBatchResult,
    IngestionStatus,
    IngestResultItem,
    OutboxEvent,
    TelemetryEvent,
)
from device_ingestion_service.domain.errors import (
    IngestionError,
    InvalidPayloadError,
    UnprocessablePayloadError,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IngestionRequest:
    organization_id: str
    site_id: str
    protocol: str
    topic: str
    payload: dict[str, object]
    received_at: datetime | None = None


class IngestionMetrics:
    def __init__(self) -> None:
        self._lock = Lock()
        self._status_totals = {
            IngestionStatus.accepted.value: 0,
            IngestionStatus.duplicate.value: 0,
            IngestionStatus.dead_letter.value: 0,
        }
        self._protocol_totals: dict[str, int] = {}

    def record(self, protocol: str, status: IngestionStatus) -> None:
        with self._lock:
            self._status_totals[status.value] = self._status_totals.get(status.value, 0) + 1
            self._protocol_totals[protocol] = self._protocol_totals.get(protocol, 0) + 1

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return {
                "status": dict(self._status_totals),
                "protocol": dict(self._protocol_totals),
            }


class DeviceIngestionUseCases:
    def __init__(
        self,
        uow: UnitOfWork,
        parsers: dict[str, ProtocolParser],
        dedup_window_seconds: int,
        metrics: IngestionMetrics,
    ) -> None:
        self._uow = uow
        self._parsers = parsers
        self._dedup_window = timedelta(seconds=dedup_window_seconds)
        self._metrics = metrics

    def ingest(self, request: IngestionRequest) -> IngestBatchResult:
        protocol_key = request.protocol.strip().lower()
        parser = self._parsers.get(protocol_key)
        if parser is None:
            return self._dead_letter(
                request=request,
                protocol=protocol_key,
                reason=f"unsupported_protocol:{protocol_key}",
            )

        envelope = ProtocolEnvelope(topic=request.topic, payload=request.payload)
        received_at = request.received_at or datetime.now(tz=UTC)

        try:
            measurements = parser.parse(envelope)
        except (InvalidPayloadError, UnprocessablePayloadError) as exc:
            return self._dead_letter(
                request=request,
                protocol=protocol_key,
                reason=exc.__class__.__name__.replace("Error", "").lower(),
            )
        except IngestionError as exc:
            return self._dead_letter(
                request=request,
                protocol=protocol_key,
                reason=f"ingestion_error:{exc}",
            )

        items: list[IngestResultItem] = []
        dedup_since = received_at - self._dedup_window

        with self._uow as uow:
            for measurement in measurements:
                payload_hash = _payload_hash(
                    protocol=protocol_key,
                    topic=request.topic,
                    organization_id=request.organization_id,
                    site_id=request.site_id,
                    device_id=measurement.device_id,
                    message_id=measurement.message_id,
                    point_key=measurement.point_key,
                    value=measurement.value,
                    observed_at=measurement.observed_at,
                    unit=measurement.unit,
                )

                duplicate_of = uow.telemetry.find_recent_duplicate(
                    site_id=request.site_id,
                    payload_hash=payload_hash,
                    since=dedup_since,
                )
                if duplicate_of is not None:
                    item = IngestResultItem(
                        status=IngestionStatus.duplicate,
                        protocol=protocol_key,
                        point_key=measurement.point_key,
                        telemetry_id=None,
                        duplicate_of=duplicate_of,
                        outbox_event_id=None,
                        dead_letter_id=None,
                        reason="duplicate_within_window",
                    )
                    items.append(item)
                    self._metrics.record(protocol_key, item.status)
                    continue

                telemetry = TelemetryEvent(
                    telemetry_id=str(uuid4()),
                    organization_id=request.organization_id,
                    site_id=request.site_id,
                    source=measurement.source,
                    message_id=measurement.message_id,
                    device_id=measurement.device_id,
                    point_key=measurement.point_key,
                    value=measurement.value,
                    observed_at=measurement.observed_at,
                    unit=measurement.unit,
                    payload_hash=payload_hash,
                    received_at=received_at,
                )
                stored = uow.telemetry.add(telemetry)

                outbox = OutboxEvent(
                    event_id=str(uuid4()),
                    aggregate_type="telemetry_event",
                    aggregate_id=stored.telemetry_id,
                    event_type="telemetry.ingested.v1",
                    payload={
                        "telemetry_id": stored.telemetry_id,
                        "organization_id": stored.organization_id,
                        "site_id": stored.site_id,
                        "source": stored.source,
                        "device_id": stored.device_id,
                        "point_key": stored.point_key,
                        "value": stored.value,
                        "unit": stored.unit,
                        "observed_at": stored.observed_at.isoformat(),
                    },
                    created_at=received_at,
                )
                persisted_outbox = uow.outbox.add(outbox)

                item = IngestResultItem(
                    status=IngestionStatus.accepted,
                    protocol=protocol_key,
                    point_key=measurement.point_key,
                    telemetry_id=stored.telemetry_id,
                    duplicate_of=None,
                    outbox_event_id=persisted_outbox.event_id,
                    dead_letter_id=None,
                    reason=None,
                )
                items.append(item)
                self._metrics.record(protocol_key, item.status)

            uow.commit()

        logger.info(
            "ingestion_batch_processed",
            extra={
                "protocol": protocol_key,
                "site_id": request.site_id,
                "organization_id": request.organization_id,
                "accepted": sum(1 for item in items if item.status == IngestionStatus.accepted),
                "duplicates": sum(1 for item in items if item.status == IngestionStatus.duplicate),
            },
        )
        return IngestBatchResult(items=items)

    def list_dead_letters(
        self,
        *,
        organization_id: str,
        site_id: str,
        limit: int,
    ) -> list[DeadLetterRecord]:
        with self._uow as uow:
            rows = uow.dead_letters.list_recent(
                organization_id=organization_id,
                site_id=site_id,
                limit=limit,
            )
            return list(rows)

    def metrics_snapshot(self) -> dict[str, object]:
        return self._metrics.snapshot()

    def _dead_letter(
        self,
        *,
        request: IngestionRequest,
        protocol: str,
        reason: str,
    ) -> IngestBatchResult:
        received_at = request.received_at or datetime.now(tz=UTC)
        dead_letter = DeadLetterEvent(
            dead_letter_id=str(uuid4()),
            organization_id=request.organization_id,
            site_id=request.site_id,
            protocol=protocol,
            topic=request.topic,
            reason=reason,
            payload=request.payload,
            received_at=received_at,
        )

        with self._uow as uow:
            persisted = uow.dead_letters.add(dead_letter)
            uow.commit()

        item = IngestResultItem(
            status=IngestionStatus.dead_letter,
            protocol=protocol,
            point_key=None,
            telemetry_id=None,
            duplicate_of=None,
            outbox_event_id=None,
            dead_letter_id=persisted.dead_letter_id,
            reason=reason,
        )
        self._metrics.record(protocol, item.status)
        logger.warning(
            "ingestion_dead_letter",
            extra={
                "protocol": protocol,
                "site_id": request.site_id,
                "organization_id": request.organization_id,
                "reason": reason,
                "dead_letter_id": persisted.dead_letter_id,
            },
        )
        return IngestBatchResult(items=[item])


def _payload_hash(
    *,
    protocol: str,
    topic: str,
    organization_id: str,
    site_id: str,
    device_id: str,
    message_id: str,
    point_key: str,
    value: float | int | bool | str,
    observed_at: datetime,
    unit: str | None,
) -> str:
    canonical = {
        "protocol": protocol,
        "topic": topic,
        "organization_id": organization_id,
        "site_id": site_id,
        "device_id": device_id,
        "message_id": message_id,
        "point_key": point_key,
        "value": value,
        "observed_at": observed_at.astimezone(UTC).isoformat(),
        "unit": unit,
    }
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

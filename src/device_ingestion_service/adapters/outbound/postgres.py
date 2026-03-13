from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from psycopg import connect
from psycopg.connection import Connection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from device_ingestion_service.application.uow import UnitOfWork
from device_ingestion_service.domain.entities import (
    DeadLetterEvent,
    DeadLetterRecord,
    OutboxEvent,
    TelemetryEvent,
)


def _sql_path() -> Path:
    return Path(__file__).resolve().parents[4] / "scripts" / "init_postgres.sql"


def ensure_schema(dsn: str) -> None:
    sql = _sql_path().read_text(encoding="utf-8")
    with connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)


class PostgresTelemetryRepository:
    def __init__(self, conn: Connection) -> None:
        self._conn = conn

    def find_recent_duplicate(
        self, *, site_id: str, payload_hash: str, since: datetime
    ) -> str | None:
        with self._conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT telemetry_id
                FROM telemetry_events
                WHERE site_id = %s
                  AND payload_hash = %s
                  AND created_at >= %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (site_id, payload_hash, since),
            )
            row = cur.fetchone()
        return None if row is None else str(row["telemetry_id"])

    def add(self, item: TelemetryEvent) -> TelemetryEvent:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO telemetry_events (
                    telemetry_id,
                    organization_id,
                    site_id,
                    source,
                    message_id,
                    device_id,
                    point_key,
                    value_json,
                    unit,
                    observed_at,
                    payload_hash,
                    received_at,
                    created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                """,
                (
                    item.telemetry_id,
                    item.organization_id,
                    item.site_id,
                    item.source,
                    item.message_id,
                    item.device_id,
                    item.point_key,
                    Jsonb(item.value),
                    item.unit,
                    item.observed_at,
                    item.payload_hash,
                    item.received_at,
                ),
            )
        return item


class PostgresOutboxRepository:
    def __init__(self, conn: Connection) -> None:
        self._conn = conn

    def add(self, item: OutboxEvent) -> OutboxEvent:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO outbox_events (
                    event_id,
                    aggregate_type,
                    aggregate_id,
                    event_type,
                    payload_json,
                    created_at,
                    published_at
                ) VALUES (%s, %s, %s, %s, %s, %s, NULL)
                """,
                (
                    item.event_id,
                    item.aggregate_type,
                    item.aggregate_id,
                    item.event_type,
                    Jsonb(item.payload),
                    item.created_at,
                ),
            )
        return item


class PostgresDeadLetterRepository:
    def __init__(self, conn: Connection) -> None:
        self._conn = conn

    def add(self, item: DeadLetterEvent) -> DeadLetterEvent:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dead_letters (
                    dead_letter_id,
                    organization_id,
                    site_id,
                    protocol,
                    topic,
                    reason,
                    payload_json,
                    received_at,
                    created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
                """,
                (
                    item.dead_letter_id,
                    item.organization_id,
                    item.site_id,
                    item.protocol,
                    item.topic,
                    item.reason,
                    Jsonb(item.payload),
                    item.received_at,
                ),
            )
        return item

    def list_recent(
        self,
        *,
        organization_id: str,
        site_id: str,
        limit: int,
    ) -> Sequence[DeadLetterRecord]:
        with self._conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    dead_letter_id,
                    organization_id,
                    site_id,
                    protocol,
                    topic,
                    reason,
                    payload_json,
                    received_at
                FROM dead_letters
                WHERE organization_id = %s
                  AND site_id = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (organization_id, site_id, limit),
            )
            rows = cur.fetchall()

        return [
            DeadLetterRecord(
                dead_letter_id=row["dead_letter_id"],
                organization_id=row["organization_id"],
                site_id=row["site_id"],
                protocol=row["protocol"],
                topic=row["topic"],
                reason=row["reason"],
                payload=row["payload_json"] or {},
                received_at=row["received_at"],
            )
            for row in rows
        ]


class PostgresUnitOfWork(UnitOfWork):
    def __init__(self, dsn: str, auto_init_schema: bool = True) -> None:
        self._dsn = dsn
        self._auto_init_schema = auto_init_schema
        self._schema_initialized = False
        self._conn: Connection | None = None

    def __enter__(self) -> "PostgresUnitOfWork":
        if self._auto_init_schema and not self._schema_initialized:
            ensure_schema(self._dsn)
            self._schema_initialized = True

        self._conn = connect(self._dsn)
        self.telemetry = PostgresTelemetryRepository(self._conn)
        self.outbox = PostgresOutboxRepository(self._conn)
        self.dead_letters = PostgresDeadLetterRepository(self._conn)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._conn is None:
            return
        if exc_type:
            self.rollback()
        self._conn.close()
        self._conn = None

    def commit(self) -> None:
        if self._conn is not None:
            self._conn.commit()

    def rollback(self) -> None:
        if self._conn is not None:
            self._conn.rollback()

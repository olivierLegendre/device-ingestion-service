# device-ingestion-service

Wave 3 extraction service for MQTT ingestion, normalization, deduplication, and outbox publishing.

## Scope (Wave 3)

- Protocol adapters for current sources (`zigbee2mqtt`, `lorawan`).
- Deterministic idempotency and duplicate suppression.
- Structured logging and ingest metrics.
- Dead-letter handling for invalid/unprocessable messages.
- PostgreSQL-backed runtime with in-memory mode for tests/tooling.

## Architecture

```text
src/device_ingestion_service/
  domain/                  # entities + domain constraints
  application/             # use-cases + unit of work boundary + metrics
  adapters/
    inbound/http/          # FastAPI endpoints
    inbound/mqtt/          # protocol parsers + worker
    outbound/              # postgres and in-memory persistence
```

## Setup

```bash
PYTHON_BIN=python3.12 ./scripts/setup_dev.sh
source .venv/bin/activate
```

## Run API

```bash
export DEVICE_INGESTION_POSTGRES_DSN='postgresql://svc_device_ingestion_app:dev_device_ingestion_app@localhost:55440/device_ingestion'
uvicorn device_ingestion_service.main:app --reload
```

Schema migration is handled with Alembic (`alembic/` + `scripts/migrate_postgres.sh`).
`scripts/init_postgres.sql` remains as a bootstrap fallback, but production path should use Alembic upgrades.

Apply migrations (migrator role):

```bash
export DEVICE_INGESTION_MIGRATOR_DSN='postgresql://svc_device_ingestion_migrator:dev_device_ingestion_migrator@localhost:55440/device_ingestion'
./scripts/migrate_postgres.sh upgrade head
```

## Run MQTT Worker

```bash
python scripts/run_worker.py
```

## API Endpoints

- `POST /api/v1/ingestion/events`
- `GET /api/v1/ingestion/dead-letters`
- `GET /api/v1/ingestion/metrics`
- `GET /healthz`

## Test

Unit and contract checks:

```bash
python scripts/export_openapi.py
ruff check .
mypy src
pytest -m "not postgres_integration"
```

PostgreSQL integration tests:

```bash
./scripts/run_postgres_integration_tests.sh
```

Shared Postgres dependency:

- The integration script uses `platform-foundation` shared cluster provisioning (no local per-service Postgres container).
- Optional env override: `POSTGRES_SHARED_ENV_FILE=/path/to/postgres-shared.env`.
- Integration flow now applies Alembic migrations with the migrator role, then runs tests with the app role DSN.

Golden compatibility fixture regression:

- Fixture file: `tests/fixtures/poc_ingestion_golden.json`
- Coverage: deterministic dedup, accepted path, dead-letter path.

## Deep Dive Documentation

Architecture choices and rationale are documented in `docs/architecture-deep-dive.md`.

## Contract

The OpenAPI contract artifact is stored at `contracts/openapi-v1.json`.
Refresh it with:

```bash
python scripts/export_openapi.py
```

## Operations Runbook

- `docs/runbooks/incident-rollback-recovery.md`

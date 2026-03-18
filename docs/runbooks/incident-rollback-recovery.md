# Incident / Rollback / Recovery Runbook

## Scope

Service: `device-ingestion-service`
Critical path: MQTT ingest, normalization, persistence, dead-letter routing.

## Incident Response

1. Confirm tenant/site and topic scope impacted.
2. Capture sample payloads, parser errors, and dead-letter growth.
3. If data integrity is at risk, pause ingest consumers before rollback.

## Rollback

1. Re-deploy last known good release artifact for `device-ingestion-service`.
2. Restart ingest worker process after API is healthy.
3. Keep worker paused until validation checks pass.

## Recovery Validation

```bash
source .venv/bin/activate
python scripts/export_openapi.py
ruff check .
mypy src
pytest -m "not postgres_integration"
./scripts/run_postgres_integration_tests.sh
python scripts/run_worker.py
```

## Post-Incident

1. Record root cause category (parser, mapping, storage, queue).
2. Add fixture/test for failing payload shape.
3. Update cutover notes if the issue came from legacy Node-RED path equivalence.

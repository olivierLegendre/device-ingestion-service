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
2. If incident root cause includes schema drift, rollback one migration step with migrator role:

```bash
export DEVICE_INGESTION_MIGRATOR_DSN='postgresql://svc_device_ingestion_migrator:***@<host>:<port>/device_ingestion'
./scripts/migrate_postgres.sh downgrade -1
```

3. Restart ingest worker process after API is healthy.
4. Keep worker paused until validation checks pass.

## Recovery Validation

```bash
source .venv/bin/activate
python scripts/export_openapi.py
ruff check .
mypy src
pytest -m "not postgres_integration"
./scripts/run_postgres_integration_tests.sh
./scripts/migrate_postgres.sh current
python scripts/run_worker.py
```

## Post-Incident

1. Record root cause category (parser, mapping, storage, queue).
2. Add fixture/test for failing payload shape.
3. Update cutover notes if the issue came from legacy Node-RED path equivalence.

## Wave 8 Hardening And Namespace Migration Notes

1. If release is blocked by vulnerability gate, capture the exact HIGH/CRITICAL finding list and either:
- patch and rebuild immediately; or
- apply documented risk acceptance exception before re-run.
2. If keyless OIDC signing/verification fails, treat this as release-blocking identity drift.
3. If namespace migration issues occur (`ghcr.io/ramery/...`), rollback by pinning the last known good immutable tag in deployment manifest and rerun pullability checks.
4. Always attach evidence artifacts (scan output, signature verify output, pullability check result) to incident record.

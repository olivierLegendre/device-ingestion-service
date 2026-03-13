#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"

source .venv/bin/activate

cleanup() {
  docker compose -f docker-compose.postgres.yml down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose -f docker-compose.postgres.yml up -d

for _ in {1..40}; do
  if docker exec device-ingestion-postgres pg_isready -U postgres -d device_ingestion >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! docker exec device-ingestion-postgres pg_isready -U postgres -d device_ingestion >/dev/null 2>&1; then
  echo "PostgreSQL did not become ready in time." >&2
  exit 1
fi

export DEVICE_INGESTION_TEST_POSTGRES_DSN="postgresql://postgres:postgres@localhost:55433/device_ingestion"
pytest -m postgres_integration -q

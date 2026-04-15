#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"

source .venv/bin/activate

IOT_SERVICES_ROOT="${IOT_SERVICES_ROOT:-$(cd "${REPO_DIR}/.." && pwd)}"
FOUNDATION_DIR="${FOUNDATION_DIR:-${IOT_SERVICES_ROOT}/platform-foundation}"
FOUNDATION_SCRIPTS_DIR="${FOUNDATION_DIR}/deploy/production/scripts"
POSTGRES_SHARED_ENV_FILE="${POSTGRES_SHARED_ENV_FILE:-${FOUNDATION_SCRIPTS_DIR}/postgres-shared.env}"
export POSTGRES_SHARED_ENV_FILE

"${FOUNDATION_SCRIPTS_DIR}/run_shared_postgres_cluster.sh" up
"${FOUNDATION_SCRIPTS_DIR}/provision_shared_postgres.sh" --service device-ingestion-service --reset-db

if [[ -f "${POSTGRES_SHARED_ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${POSTGRES_SHARED_ENV_FILE}"
fi

DB_HOST="${POSTGRES_CLUSTER_HOST:-localhost}"
DB_PORT="${POSTGRES_CLUSTER_PORT:-55440}"
DB_NAME="${DEVICE_INGESTION_DB_NAME:-device_ingestion}"
APP_USER="${DEVICE_INGESTION_APP_ROLE:-svc_device_ingestion_app}"
APP_PASSWORD="${DEVICE_INGESTION_APP_PASSWORD:-dev_device_ingestion_app}"
MIGRATOR_USER="${DEVICE_INGESTION_MIGRATOR_ROLE:-svc_device_ingestion_migrator}"
MIGRATOR_PASSWORD="${DEVICE_INGESTION_MIGRATOR_PASSWORD:-dev_device_ingestion_migrator}"

export DEVICE_INGESTION_MIGRATOR_DSN="postgresql://${MIGRATOR_USER}:${MIGRATOR_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
./scripts/migrate_postgres.sh upgrade head

export DEVICE_INGESTION_TEST_POSTGRES_DSN="postgresql://${APP_USER}:${APP_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
pytest -m postgres_integration -q

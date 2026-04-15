#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"

source .venv/bin/activate

ACTION="${1:-upgrade}"
TARGET="${2:-head}"

MIGRATOR_DSN="${DEVICE_INGESTION_MIGRATOR_DSN:-${DEVICE_INGESTION_POSTGRES_DSN:-}}"
if [[ -z "$MIGRATOR_DSN" ]]; then
  echo "Error: set DEVICE_INGESTION_MIGRATOR_DSN (preferred) or DEVICE_INGESTION_POSTGRES_DSN" >&2
  exit 1
fi

case "$ACTION" in
  upgrade)
    alembic -x dsn="$MIGRATOR_DSN" upgrade "$TARGET"
    ;;
  downgrade)
    alembic -x dsn="$MIGRATOR_DSN" downgrade "$TARGET"
    ;;
  current)
    alembic -x dsn="$MIGRATOR_DSN" current
    ;;
  history)
    alembic history
    ;;
  *)
    echo "Usage: scripts/migrate_postgres.sh [upgrade|downgrade|current|history] [target]" >&2
    exit 1
    ;;
esac

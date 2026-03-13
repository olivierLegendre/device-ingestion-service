#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIAGRAM_DIR="$REPO_DIR/docs/diagrams"
INPUT="$DIAGRAM_DIR/schema-er.mmd"
OUTPUT="$DIAGRAM_DIR/schema-er.svg"

if [[ ! -f "$INPUT" ]]; then
  echo "Missing diagram source: $INPUT" >&2
  exit 1
fi

if command -v mmdc >/dev/null 2>&1; then
  mmdc -i "$INPUT" -o "$OUTPUT"
  echo "Wrote $OUTPUT"
  exit 0
fi

# Fallback: render with Mermaid CLI in Docker.
docker run --rm \
  -u "$(id -u):$(id -g)" \
  -v "$DIAGRAM_DIR:/data" \
  minlag/mermaid-cli:11.4.1 \
  -i /data/schema-er.mmd \
  -o /data/schema-er.svg

echo "Wrote $OUTPUT"

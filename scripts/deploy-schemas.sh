#!/usr/bin/env bash
set -euo pipefail

DEFAULT_DB="postgresql:///sinex_dev?host=/run/postgresql"
export DATABASE_URL="${DATABASE_URL:-$DEFAULT_DB}"

SCHEMA_DIR=${1:-schemas/v1}
if [ ! -d "$SCHEMA_DIR" ]; then
  echo "Schema directory '$SCHEMA_DIR' not found" >&2
  exit 1
fi

if ! command -v psql >/dev/null 2>&1; then
  echo "psql client is required to deploy schemas." >&2
  exit 1
fi

if ! psql "$DATABASE_URL" -c "SELECT 1" >/dev/null 2>&1; then
  echo "Unable to connect to ${DATABASE_URL}. Check credentials and try again." >&2
  exit 1
fi

required_exts=(pg_jsonschema pgx_ulid timescaledb vector)
missing=()
for ext in "${required_exts[@]}"; do
  if ! psql "$DATABASE_URL" -Atqc "SELECT 1 FROM pg_extension WHERE extname='${ext}'" >/dev/null 2>&1; then
    missing+=("$ext")
  fi
done

if [ "${#missing[@]}" -gt 0 ]; then
  echo "The following extensions are missing in the target database: ${missing[*]}" >&2
  echo "Install them before deploying schemas." >&2
  exit 1
fi

SCHEMA_CLI=(cargo run -p sinex-core --bin sinex-schema --features schema-manager --)

echo "🚀 Deploying schemas from $SCHEMA_DIR to ${DATABASE_URL}"

"${SCHEMA_CLI[@]}" sync --input "$SCHEMA_DIR"

echo "✅ Schemas synced"

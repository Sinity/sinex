#!/usr/bin/env bash
set -euo pipefail

# Best-effort sanity check that core schemas exist after migrations.
# Expects CI env to have set PGHOST/PGPORT/SUPERUSER (from ci-postgres.sh).

DB_NAME="${DATABASE_NAME:-sinex_dev}"
PGHOST="${PGHOST:-/run/postgresql}"
PGPORT="${PGPORT:-5432}"
SUPERUSER="${SUPERUSER:-postgres}"

psql -h "$PGHOST" -p "$PGPORT" -U "$SUPERUSER" -d "$DB_NAME" -v ON_ERROR_STOP=1 -c \
  "SELECT to_regclass('core.events') AS reg" | grep -q core.events
psql -h "$PGHOST" -p "$PGPORT" -U "$SUPERUSER" -d "$DB_NAME" -v ON_ERROR_STOP=1 -c \
  "SELECT to_regclass('sinex_schemas.event_payload_schemas') AS reg" | grep -q sinex_schemas.event_payload_schemas

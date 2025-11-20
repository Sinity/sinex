#!/usr/bin/env bash
set -euo pipefail

PGDATA="${PGDATA:-$PWD/postgres_data}"
PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-55432}"

rm -rf "$PGDATA"
mkdir -p "$PGDATA"

initdb --auth=trust --no-locale --encoding=UTF8 >/dev/null
cat <<EOF >>"$PGDATA/postgresql.conf"
unix_socket_directories = '$PWD'
listen_addresses = '127.0.0.1'
port = $PGPORT
shared_preload_libraries = 'timescaledb'
EOF

pg_ctl start -w -l postgres.log -o "-k $PWD -p $PGPORT" >/dev/null
cleanup() {
  pg_ctl stop >/dev/null
}
trap cleanup EXIT

SUPERUSER=$(id -un)
export PGHOST PGPORT SUPERUSER

if ! PGUSER="$SUPERUSER" psql -h "$PGHOST" -p "$PGPORT" -d postgres -tAc "SELECT 1 FROM pg_roles WHERE rolname = 'sinity'" | grep -q 1; then
  PGUSER="$SUPERUSER" psql -h "$PGHOST" -p "$PGPORT" -d postgres -v ON_ERROR_STOP=1 -c "CREATE ROLE sinity LOGIN CREATEDB;"
fi

if ! PGUSER="$SUPERUSER" psql -h "$PGHOST" -p "$PGPORT" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = 'sinex_dev'" | grep -q 1; then
  PGUSER="$SUPERUSER" psql -h "$PGHOST" -p "$PGPORT" -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE sinex_dev OWNER sinity;"
fi

ensure_extension() {
  local db="$1"
  shift
  for candidate in "$@"; do
    if PGUSER="$SUPERUSER" psql -h "$PGHOST" -p "$PGPORT" -d "$db" -tAc "SELECT 1 FROM pg_available_extensions WHERE name = '${candidate}'" | grep -q 1; then
      PGUSER="$SUPERUSER" psql -h "$PGHOST" -p "$PGPORT" -d "$db" -v ON_ERROR_STOP=1 -c "CREATE EXTENSION IF NOT EXISTS ${candidate};"
      return 0
    fi
  done
  echo "::error::None of the requested extensions ($*) are available in this PostgreSQL build."
  PGUSER="$SUPERUSER" psql -h "$PGHOST" -p "$PGPORT" -d "$db" -tAc "SELECT name FROM pg_available_extensions ORDER BY name;"
  exit 1
}

grant_schema_access() {
  local schema="$1"
  PGUSER="$SUPERUSER" psql -h "$PGHOST" -p "$PGPORT" -d sinex_dev -v ON_ERROR_STOP=1 -c "GRANT USAGE ON SCHEMA ${schema} TO sinity;"
  PGUSER="$SUPERUSER" psql -h "$PGHOST" -p "$PGPORT" -d sinex_dev -v ON_ERROR_STOP=1 -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ${schema} TO sinity;"
  PGUSER="$SUPERUSER" psql -h "$PGHOST" -p "$PGPORT" -d sinex_dev -v ON_ERROR_STOP=1 -c "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ${schema} TO sinity;"
  PGUSER="$SUPERUSER" psql -h "$PGHOST" -p "$PGPORT" -d sinex_dev -v ON_ERROR_STOP=1 -c "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA ${schema} TO sinity;"
}

export -f ensure_extension
export -f grant_schema_access

ensure_extension sinex_dev pgx_ulid ulid
ensure_extension sinex_dev pg_jsonschema
ensure_extension sinex_dev timescaledb
ensure_extension sinex_dev vector

for schema in core raw audit sinex_schemas metrics; do
  grant_schema_access "$schema"
done

export DATABASE_URL_APP="postgresql://sinity@${PGHOST}:${PGPORT}/sinex_dev"
export DATABASE_URL_SUPERUSER="postgresql://${SUPERUSER}@${PGHOST}:${PGPORT}/sinex_dev"
export DATABASE_URL="$DATABASE_URL_APP"

if [ ! -t 0 ]; then
  tmpfile=$(mktemp)
  cat >"$tmpfile"
  bash "$tmpfile"
  rm -f "$tmpfile"
fi

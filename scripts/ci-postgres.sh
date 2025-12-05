#!/usr/bin/env bash
set -euo pipefail

log_step() {
  printf '[ci-postgres] %s\n' "$*"
}

PGDATA="${PGDATA:-$PWD/postgres_data}"
PGHOST="${CI_PGHOST:-127.0.0.1}"
PGPORT="${CI_PGPORT:-55432}"
export PGDATA PGHOST PGPORT

stop_existing_postgres() {
  if [ -d "$PGDATA" ] && [ -f "$PGDATA/postmaster.pid" ]; then
    if pg_ctl -D "$PGDATA" status >/dev/null 2>&1; then
      log_step "Stopping previous PostgreSQL cluster in $PGDATA"
      pg_ctl -D "$PGDATA" -m fast stop >/dev/null || true
    fi
  fi

  mapfile -t orphan_pids < <(pgrep -f "postgres -k $PWD -p $PGPORT" || true)
  if [ "${#orphan_pids[@]}" -gt 0 ]; then
    log_step "Cleaning up orphaned postgres processes on port $PGPORT (${orphan_pids[*]})"
    kill "${orphan_pids[@]}" >/dev/null 2>&1 || true
    for pid in "${orphan_pids[@]}"; do
      while kill -0 "$pid" >/dev/null 2>&1; do
        sleep 0.1
      done
    done
  fi
}

stop_existing_postgres

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

INITIAL_SUPERUSER=$(id -un)
export PGHOST PGPORT

psql_exec_as() {
  local user="$1"
  local database="$2"
  shift 2
  PGUSER="$user" psql -q -h "$PGHOST" -p "$PGPORT" -d "$database" -v ON_ERROR_STOP=1 -c "$*" >/dev/null
}

if ! PGUSER="$INITIAL_SUPERUSER" psql -h "$PGHOST" -p "$PGPORT" -d postgres -tAc "SELECT 1 FROM pg_roles WHERE rolname = 'postgres'" | grep -q 1; then
  log_step "Creating superuser role postgres"
  psql_exec_as "$INITIAL_SUPERUSER" postgres "CREATE ROLE postgres SUPERUSER CREATEDB LOGIN;"
fi

SUPERUSER=postgres
export SUPERUSER

psql_exec() {
  local database="$1"
  shift
  PGUSER="$SUPERUSER" psql -q -h "$PGHOST" -p "$PGPORT" -d "$database" -v ON_ERROR_STOP=1 -c "$*" >/dev/null
}

if ! PGUSER="$SUPERUSER" psql -h "$PGHOST" -p "$PGPORT" -d postgres -tAc "SELECT 1 FROM pg_roles WHERE rolname = 'sinity'" | grep -q 1; then
  log_step "Creating role sinity"
  psql_exec postgres "CREATE ROLE sinity LOGIN CREATEDB;"
fi

# Ensure CI sessions satisfy RLS policies requiring sinex.operation_id
log_step "Configuring default sinex.operation_id for sinity"
psql_exec postgres "ALTER ROLE sinity SET sinex.operation_id = 'ci-tests';"

if ! PGUSER="$SUPERUSER" psql -h "$PGHOST" -p "$PGPORT" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = 'sinex_dev'" | grep -q 1; then
  log_step "Creating database sinex_dev"
  psql_exec postgres "CREATE DATABASE sinex_dev OWNER sinity;"
fi

ensure_extension() {
  local db="$1"
  shift
  for candidate in "$@"; do
    if PGUSER="$SUPERUSER" psql -h "$PGHOST" -p "$PGPORT" -d "$db" -tAc "SELECT 1 FROM pg_available_extensions WHERE name = '${candidate}'" | grep -q 1; then
      log_step "Ensuring extension ${candidate} in ${db}"
      psql_exec "$db" "CREATE EXTENSION IF NOT EXISTS ${candidate};"
      return 0
    fi
  done
  echo "::error::None of the requested extensions ($*) are available in this PostgreSQL build."
  PGUSER="$SUPERUSER" psql -h "$PGHOST" -p "$PGPORT" -d "$db" -tAc "SELECT name FROM pg_available_extensions ORDER BY name;"
  exit 1
}

grant_schema_access() {
  local schema="$1"
  log_step "Granting access to schema ${schema}"
  psql_exec sinex_dev "CREATE SCHEMA IF NOT EXISTS ${schema};"
  psql_exec sinex_dev "GRANT USAGE ON SCHEMA ${schema} TO sinity;"
  psql_exec sinex_dev "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ${schema} TO sinity;"
  psql_exec sinex_dev "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ${schema} TO sinity;"
  psql_exec sinex_dev "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA ${schema} TO sinity;"
  psql_exec sinex_dev "ALTER DEFAULT PRIVILEGES FOR ROLE ${SUPERUSER} IN SCHEMA ${schema} GRANT ALL PRIVILEGES ON TABLES TO sinity;"
  psql_exec sinex_dev "ALTER DEFAULT PRIVILEGES FOR ROLE ${SUPERUSER} IN SCHEMA ${schema} GRANT ALL PRIVILEGES ON SEQUENCES TO sinity;"
  psql_exec sinex_dev "ALTER DEFAULT PRIVILEGES FOR ROLE ${SUPERUSER} IN SCHEMA ${schema} GRANT EXECUTE ON FUNCTIONS TO sinity;"
}

export -f ensure_extension
export -f grant_schema_access

ensure_extension sinex_dev pgx_ulid ulid
ensure_extension sinex_dev pg_jsonschema
ensure_extension sinex_dev timescaledb
ensure_extension sinex_dev vector

# Grant access to all schemas (dynamically discovered from schema registry)
# This eliminates hardcoded lists and ensures new schemas are automatically included
while IFS= read -r schema; do
  grant_schema_access "$schema"
done < <(cargo run --quiet --bin schema-info -- list-schemas)

DATABASE_URL_APP="postgresql://sinity@${PGHOST}:${PGPORT}/sinex_dev"
DATABASE_URL_SUPERUSER="postgresql://${SUPERUSER}@${PGHOST}:${PGPORT}/sinex_dev"
export DATABASE_URL_APP DATABASE_URL_SUPERUSER
export DATABASE_URL="$DATABASE_URL_APP"

run_payload() {
  if [ "$#" -gt 0 ]; then
    "$@"
    return $?
  fi

  if [ ! -t 0 ]; then
    local tmpfile
    tmpfile=$(mktemp)
    {
      echo "export PGHOST=\"$PGHOST\""
      echo "export PGPORT=\"$PGPORT\""
      echo "export PGDATA=\"$PGDATA\""
      echo "export SUPERUSER=\"$SUPERUSER\""
      echo "export DATABASE_URL_APP=\"$DATABASE_URL_APP\""
      echo "export DATABASE_URL_SUPERUSER=\"$DATABASE_URL_SUPERUSER\""
      echo "export DATABASE_URL=\"$DATABASE_URL_APP\""
      cat
    } >"$tmpfile"
    if [ -n "${CI_POSTGRES_KEEP_SCRIPT:-}" ]; then
      cp "$tmpfile" "$CI_POSTGRES_KEEP_SCRIPT"
    fi
    bash "$tmpfile"
    local status=$?
    rm -f "$tmpfile"
    return $status
  fi

  echo "Usage: $0 <command> [args...] (or pipe a script via stdin)" >&2
  return 1
}

run_payload "$@"
status=$?
exit $status

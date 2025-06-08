#!/usr/bin/env bash
set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${BLUE}🗄️${NC}  $*"; }
success() { echo -e "${GREEN}✅${NC} $*"; }
warning() { echo -e "${YELLOW}⚠️${NC}  $*"; }
error() { echo -e "${RED}❌${NC} $*" >&2; }

EPHEMERAL_BASE="/tmp/sinex_ephemeral"
STATE_FILE="$HOME/.sinex_db_state"

save_state() {
  echo "$1" >"$STATE_FILE"
}

load_state() {
  if [ -f "$STATE_FILE" ]; then
    cat "$STATE_FILE"
  else
    echo "postgresql:///sinex_dev?host=/run/postgresql"
  fi
}

show_current() {
  local URL=$(load_state)
  if [[ "$URL" =~ sinex_dev ]]; then
    echo "sinex_dev"
  elif [[ "$URL" =~ /sinex\? ]]; then
    echo "sinex"
  elif [[ "$URL" =~ sinex_ephemeral_([0-9]+) ]]; then
    echo "tmp_${BASH_REMATCH[1]}"
  else
    echo "sinex_dev"
  fi
}

create_ephemeral() {
  local NUM="$1"
  local EPHEMERAL_DIR="${EPHEMERAL_BASE}_$NUM"
  local EPHEMERAL_URL="postgresql:///sinex_ephemeral_$NUM?host=$EPHEMERAL_DIR&port=5432$NUM"

  if [ -d "$EPHEMERAL_DIR" ] && pg_isready -h "$EPHEMERAL_DIR" -p "5432$NUM" >/dev/null 2>&1; then
    log "Reusing existing ephemeral database $NUM"
  else
    log "Creating ephemeral database $NUM"
    mkdir -p "$EPHEMERAL_DIR"/{data,logs}

    initdb -D "$EPHEMERAL_DIR/data" --no-locale --encoding=UTF8 >/dev/null

    echo "unix_socket_directories = '$EPHEMERAL_DIR'" >>"$EPHEMERAL_DIR/data/postgresql.conf"
    echo "shared_preload_libraries = 'timescaledb'" >>"$EPHEMERAL_DIR/data/postgresql.conf"
    echo "port = 5432$NUM" >>"$EPHEMERAL_DIR/data/postgresql.conf"

    pg_ctl -D "$EPHEMERAL_DIR/data" -l "$EPHEMERAL_DIR/logs/postgres.log" start >/dev/null

    for i in {1..10}; do
      if pg_isready -h "$EPHEMERAL_DIR" -p "5432$NUM" >/dev/null 2>&1; then
        break
      fi
      sleep 0.5
    done

    createdb -h "$EPHEMERAL_DIR" -p "5432$NUM" "sinex_ephemeral_$NUM"

    log "Running migrations on ephemeral database $NUM"
    DATABASE_URL="$EPHEMERAL_URL" sqlx migrate run --source migration >/dev/null 2>&1 || {
      error "Failed to run migrations on ephemeral database"
      exit 1
    }
  fi

  echo "$EPHEMERAL_URL"
}

TARGET="${1:-}"

if [ -z "$TARGET" ]; then
  CURRENT=$(show_current)
  URL=$(load_state)
  log "Current database: $CURRENT"
  echo "  URL: $URL"
  exit 0
fi

case "$TARGET" in
dev)
  log "Switching to development database"
  URL="postgresql:///sinex_dev?host=/run/postgresql"
  save_state "$URL"
  export DATABASE_URL="$URL"
  success "Switched to sinex_dev"
  ;;

prod)
  log "Switching to production database"
  URL="postgresql:///sinex?host=/run/postgresql"
  save_state "$URL"
  export DATABASE_URL="$URL"
  success "Switched to sinex (production)"
  ;;

tmp | tmp_*)
  if [ "$TARGET" = "tmp" ]; then
    NUM=0
  else
    NUM="${TARGET#tmp_}"
    if ! [[ "$NUM" =~ ^[0-9]$ ]]; then
      error "Invalid ephemeral database number. Use tmp or tmp_0 through tmp_9"
      exit 1
    fi
  fi

  log "Switching to ephemeral database $NUM"
  URL=$(create_ephemeral "$NUM")
  save_state "$URL"
  export DATABASE_URL="$URL"
  success "Switched to ephemeral database $NUM"
  ;;

reset)
  CURRENT=$(show_current)
  warning "This will reset the current database ($CURRENT)"
  read -p "Continue? [y/N] " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    log "Reset cancelled"
    exit 0
  fi

  case "$CURRENT" in
  sinex_dev | sinex)
    dropdb -h /run/postgresql "$CURRENT" 2>/dev/null || true
    "$0" setup "${CURRENT#sinex_}"
    ;;
  tmp*)
    NUM="${CURRENT#tmp_}"
    EPHEMERAL_DIR="${EPHEMERAL_BASE}_$NUM"
    pg_ctl -D "$EPHEMERAL_DIR/data" stop 2>/dev/null || true
    rm -rf "$EPHEMERAL_DIR"
    create_ephemeral "$NUM" >/dev/null
    success "Reset ephemeral database $NUM"
    ;;
  esac
  ;;

destroy)
  CURRENT=$(show_current)
  if [[ "$CURRENT" =~ ^tmp ]]; then
    NUM="${CURRENT#tmp_}"
    EPHEMERAL_DIR="${EPHEMERAL_BASE}_$NUM"
    warning "This will destroy ephemeral database $NUM"
    read -p "Continue? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
      log "Destroy cancelled"
      exit 0
    fi
    pg_ctl -D "$EPHEMERAL_DIR/data" stop 2>/dev/null || true
    rm -rf "$EPHEMERAL_DIR"
    success "Destroyed ephemeral database $NUM"
    URL="postgresql:///sinex_dev?host=/run/postgresql"
    save_state "$URL"
    export DATABASE_URL="$URL"
    success "Switched to sinex_dev"
  else
    error "Can only destroy ephemeral databases"
    exit 1
  fi
  ;;

setup)
  DB_TYPE="${2:-dev}"
  case "$DB_TYPE" in
  dev)
    log "Setting up development database"
    URL="postgresql:///sinex_dev?host=/run/postgresql"

    if ! pg_isready -h /run/postgresql >/dev/null 2>&1; then
      error "PostgreSQL is not running on /run/postgresql"
      error "Please ensure PostgreSQL is installed and running"
      error "On NixOS: services.postgresql.enable = true;"
      exit 1
    fi

    if ! psql -h /run/postgresql -lqt | cut -d \| -f 1 | grep -qw sinex_dev; then
      log "Creating database sinex_dev"
      createdb -h /run/postgresql sinex_dev || {
        error "Failed to create database. You may need to run:"
        error "  sudo -u postgres createdb sinex_dev"
        error "  sudo -u postgres psql -c \"GRANT ALL ON DATABASE sinex_dev TO $USER;\""
        exit 1
      }
    fi

    log "Running migrations (includes extensions)"
    DATABASE_URL="$URL" sqlx migrate run --source migration

    save_state "$URL"
    export DATABASE_URL="$URL"
    success "Development database ready"
    ;;

  prod)
    log "Setting up production database"
    URL="postgresql:///sinex?host=/run/postgresql"

    if ! pg_isready -h /run/postgresql >/dev/null 2>&1; then
      error "PostgreSQL is not running on /run/postgresql"
      exit 1
    fi

    if ! psql -h /run/postgresql -lqt | cut -d \| -f 1 | grep -qw sinex; then
      log "Creating database sinex"
      createdb -h /run/postgresql sinex || {
        error "Failed to create production database"
        exit 1
      }
    fi

    log "Running migrations (includes extensions)"
    DATABASE_URL="$URL" sqlx migrate run --source migration

    save_state "$URL"
    export DATABASE_URL="$URL"
    success "Production database ready"
    ;;

  *)
    error "Usage: db setup [dev|prod]"
    exit 1
    ;;
  esac
  ;;

shell | psql)
  CURRENT=$(show_current)
  case "$CURRENT" in
  sinex_dev)
    DATABASE_URL="postgresql:///sinex_dev?host=/run/postgresql"
    ;;
  sinex)
    DATABASE_URL="postgresql:///sinex?host=/run/postgresql"
    ;;
  tmp*)
    NUM="${CURRENT#tmp_}"
    DATABASE_URL="postgresql:///sinex_ephemeral_$NUM?host=/tmp/sinex_ephemeral_$NUM&port=5432$NUM"
    ;;
  esac
  log "Connecting to $CURRENT database"
  psql "$DATABASE_URL"
  ;;

*)
  error "Usage: db [command] [args]"
  echo "Commands:"
  echo "  db              - Show current database"
  echo "  db dev          - Switch to development database"
  echo "  db prod         - Switch to production database"
  echo "  db tmp          - Switch to ephemeral database 0"
  echo "  db tmp_N        - Switch to ephemeral database N (0-9)"
  echo "  db reset        - Reset current database"
  echo "  db destroy      - Destroy current ephemeral database"
  echo "  db setup [dev|prod] - Initialize dev or prod database"
  echo "  db shell        - Connect to current database with psql"
  exit 1
  ;;
esac


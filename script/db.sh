#!/usr/bin/env bash
set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${BLUE}🗄️${NC}  $*" >&2; }
success() { echo -e "${GREEN}✅${NC} $*" >&2; }
warning() { echo -e "${YELLOW}⚠️${NC}  $*" >&2; }
error() { echo -e "${RED}❌${NC} $*" >&2; }

# State file for current database
DB_STATE_FILE=".current-db"

# Get current database from state file
current_db() {
  if [ -f "$DB_STATE_FILE" ]; then
    cat "$DB_STATE_FILE"
  else
    echo "sinex_dev"  # default
  fi
}

# Get database URL for a given database name
get_db_url() {
  local db_name="${1:-$(current_db)}"
  echo "postgresql:///$db_name?host=/run/postgresql"
}

# Setup database
setup_db() {
  local db_name="$1"
  local url="$(get_db_url "$db_name")"

  if ! pg_isready -h /run/postgresql >/dev/null 2>&1; then
    error "PostgreSQL is not running on /run/postgresql"
    exit 1
  fi

  if ! psql -h /run/postgresql -lqt | cut -d \| -f 1 | grep -qw "$db_name"; then
    log "Creating database $db_name"
    createdb -h /run/postgresql "$db_name" || {
      error "Failed to create database $db_name"
      exit 1
    }
  fi
  
  # Save current database choice
  echo "$db_name" > "$DB_STATE_FILE"
  
  # Export for current session
  export DATABASE_URL="$url"
  
  sqlx migrate run --source migration >&2
  success "Database $db_name ready (current)"
}

# Always export DATABASE_URL based on current database
export DATABASE_URL="$(get_db_url)"

case "${1:-}" in
"" | status)
  echo "Current database: $(current_db)"
  echo "DATABASE_URL: $DATABASE_URL"
  ;;

dev)
  setup_db "sinex_dev"
  ;;

prod)
  setup_db "sinex"
  ;;

reset)
  db_name=$(current_db)
  if [[ "$db_name" == "sinex" ]]; then
    error "Will not reset production database. To reset manually:"
    echo "  dropdb -h /run/postgresql sinex; db prod"
    exit 1
  fi

  warning "Reset database $db_name? [y/N]"
  read -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    log "Cancelled"
    exit 0
  fi

  dropdb -h /run/postgresql "$db_name" 2>/dev/null || true
  setup_db "sinex_dev"
  ;;

shell | psql)
  log "Connecting to $(current_db)"
  psql "$DATABASE_URL"
  ;;

get-url)
  # Just output the URL, no logging
  echo "$DATABASE_URL"
  ;;

*)
  error "Usage: db [status|dev|prod|reset|shell|get-url]"
  exit 1
  ;;
esac

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

# Get current database from DATABASE_URL
current_db() {
  local url="${DATABASE_URL:-postgresql:///sinex_dev?host=/run/postgresql}"
  if [[ "$url" =~ postgresql:///([^?]+) ]]; then
    echo "${BASH_REMATCH[1]}"
  else
    echo "unknown"
  fi
}

# Setup database
setup_db() {
  local db_name="$1"
  local url="postgresql:///$db_name?host=/run/postgresql"

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
  export DATABASE_URL="$url"

  log "Running migrations"
  sqlx migrate run --source migration

  success "Database $db_name ready"
}

case "${1:-}" in
"" | status)
  echo "Current database: $(current_db)"
  echo "DATABASE_URL: ${DATABASE_URL:-<not set>}"
  ;;

dev)
  setup_db "sinex_dev"
  ;;

prod)
  setup_db "sinex"
  ;;

setup)
  case "${2:-dev}" in
  dev)
    setup_db "sinex_dev"
    ;;
  prod)
    setup_db "sinex"
    ;;
  *)
    error "Usage: db setup [dev|prod]"
    exit 1
    ;;
  esac
  ;;

reset)
  db_name=$(current_db)
  if [[ "$db_name" == "sinex" ]]; then
    error "Cannot reset production database. To reset manually:"
    echo "  dropdb -h /run/postgresql sinex"
    echo "  db prod"
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
  if [ -z "${DATABASE_URL:-}" ]; then
    error "DATABASE_URL not set"
    exit 1
  fi
  log "Connecting to $(current_db)"
  psql "$DATABASE_URL"
  ;;

*)
  error "Usage: db [status|dev|prod|setup|reset|shell]"
  exit 1
  ;;
esac


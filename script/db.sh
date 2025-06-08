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
  
  log "Running migrations"
  DATABASE_URL="$url" sqlx migrate run --source migration
  
  export DATABASE_URL="$url"
  success "Database $db_name ready"
}

# Create ephemeral database
create_ephemeral() {
  local num="$1"
  local dir="/tmp/sinex_ephemeral_$num"
  local url="postgresql:///sinex_ephemeral_$num?host=$dir&port=5432$num"
  
  if [ -d "$dir" ] && pg_isready -h "$dir" -p "5432$num" >/dev/null 2>&1; then
    log "Reusing ephemeral database $num"
  else
    log "Creating ephemeral database $num"
    mkdir -p "$dir"/{data,logs}
    
    initdb -D "$dir/data" --no-locale --encoding=UTF8 >/dev/null
    
    echo "unix_socket_directories = '$dir'" >> "$dir/data/postgresql.conf"
    echo "shared_preload_libraries = 'timescaledb'" >> "$dir/data/postgresql.conf"
    echo "port = 5432$num" >> "$dir/data/postgresql.conf"
    
    pg_ctl -D "$dir/data" -l "$dir/logs/postgres.log" start >/dev/null
    
    for i in {1..10}; do
      if pg_isready -h "$dir" -p "5432$num" >/dev/null 2>&1; then break; fi
      sleep 0.5
    done
    
    createdb -h "$dir" -p "5432$num" "sinex_ephemeral_$num"
    DATABASE_URL="$url" sqlx migrate run --source migration >/dev/null
  fi
  
  export DATABASE_URL="$url"
  success "Ephemeral database $num ready"
}

case "${1:-}" in
  ""|status)
    echo "Current database: $(current_db)"
    echo "DATABASE_URL: ${DATABASE_URL:-<not set>}"
    ;;
  
  dev)
    setup_db "sinex_dev"
    ;;
  
  prod)
    setup_db "sinex"
    ;;
  
  tmp|tmp_*)
    num="${1#tmp}"
    num="${num#_}"
    num="${num:-0}"
    if ! [[ "$num" =~ ^[0-9]$ ]]; then
      error "Invalid ephemeral database number: $num"
      exit 1
    fi
    create_ephemeral "$num"
    ;;
  
  reset)
    db_name=$(current_db)
    warning "Reset database $db_name? [y/N]"
    read -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
      log "Cancelled"
      exit 0
    fi
    
    if [[ "$db_name" =~ ^sinex_ephemeral_([0-9]+)$ ]]; then
      num="${BASH_REMATCH[1]}"
      dir="/tmp/sinex_ephemeral_$num"
      pg_ctl -D "$dir/data" stop 2>/dev/null || true
      rm -rf "$dir"
      create_ephemeral "$num"
    else
      dropdb -h /run/postgresql "$db_name" 2>/dev/null || true
      if [[ "$db_name" == "sinex" ]]; then
        setup_db "sinex"
      else
        setup_db "sinex_dev"
      fi
    fi
    ;;
  
  destroy)
    db_name=$(current_db)
    if [[ "$db_name" =~ ^sinex_ephemeral_([0-9]+)$ ]]; then
      num="${BASH_REMATCH[1]}"
      dir="/tmp/sinex_ephemeral_$num"
      warning "Destroy ephemeral database $num? [y/N]"
      read -n 1 -r
      echo
      if [[ $REPLY =~ ^[Yy]$ ]]; then
        pg_ctl -D "$dir/data" stop 2>/dev/null || true
        rm -rf "$dir"
        success "Destroyed ephemeral database $num"
        setup_db "sinex_dev"
      fi
    else
      error "Can only destroy ephemeral databases"
      exit 1
    fi
    ;;
  
  shell|psql)
    if [ -z "${DATABASE_URL:-}" ]; then
      error "DATABASE_URL not set"
      exit 1
    fi
    log "Connecting to $(current_db)"
    psql "$DATABASE_URL"
    ;;
  
  *)
    error "Usage: db [status|dev|prod|tmp|tmp_N|reset|destroy|shell]"
    exit 1
    ;;
esac
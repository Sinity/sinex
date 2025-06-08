#!/usr/bin/env bash
set -euo pipefail

# Colors
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log() { echo -e "${BLUE}🗄️${NC}  $*"; }
success() { echo -e "${GREEN}✅${NC} $*"; }
warning() { echo -e "${YELLOW}⚠️${NC}  $*"; }
error() { echo -e "${RED}❌${NC} $*" >&2; }

log "Updating SQLX offline cache..."

# Check database connectivity
if ! pg_isready -h /run/postgresql >/dev/null 2>&1; then
  error "PostgreSQL is not running on /run/postgresql. Please start PostgreSQL"
  exit 1
fi

# Check if database exists
if ! psql "$DATABASE_URL" -c "SELECT 1;" >/dev/null 2>&1; then
  error "Database $DATABASE_URL not accessible"
  exit 1
fi

# Ensure migrations are up to date
log "Running migrations..."
sqlx migrate run --source migration || {
  error "Failed to run migrations"
  exit 1
}

# Update the cache
log "Preparing SQLX offline cache..."
sqlx prepare --workspace -- --all-targets --all-features || {
  error "Failed to prepare SQLX cache"
  exit 1
}

success "SQLX cache updated successfully"
warning "Don't forget to commit the changes in .sqlx/"

# Show what changed
if command -v git >/dev/null 2>&1; then
  echo ""
  log "Changes to commit:"
  git status --porcelain .sqlx/ | sed 's/^/  /'
fi

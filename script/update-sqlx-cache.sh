#!/usr/bin/env bash
set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[SQLX]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }
warning() { echo -e "${YELLOW}[WARN]${NC} $*"; }

# Check if we're in the right directory
if [ ! -f "Cargo.toml" ]; then
    error "Must be run from project root (no Cargo.toml found)"
    exit 1
fi

# Check if DATABASE_URL is set
if [ -z "${DATABASE_URL:-}" ]; then
    warning "DATABASE_URL not set, trying default"
    export DATABASE_URL="postgresql:///sinex_dev"
fi

# Check if database is accessible
if ! cargo sqlx database setup 2>/dev/null; then
    warning "Database not accessible, trying to set it up"
    
    # Try to create database if it doesn't exist
    if command -v psql >/dev/null 2>&1; then
        psql -lqt | cut -d \| -f 1 | grep -qw sinex_dev || {
            log "Creating database sinex_dev"
            createdb sinex_dev || {
                error "Failed to create database. You may need to run:"
                error "  sudo -u postgres createdb sinex_dev"
                error "  sudo -u postgres psql -c \"GRANT ALL ON DATABASE sinex_dev TO $USER;\""
                exit 1
            }
        }
        
        # Run migrations
        log "Running migrations"
        cargo sqlx migrate run || {
            error "Failed to run migrations"
            exit 1
        }
    else
        error "PostgreSQL client not found. Please ensure database is set up."
        exit 1
    fi
fi

# Check if any SQL queries have changed
log "Checking for SQL query changes..."

# Create a temporary file to store current query hashes
TEMP_HASHES=$(mktemp)
trap "rm -f $TEMP_HASHES" EXIT

# Find all Rust files with sqlx queries and hash them
find . -name "*.rs" -type f -not -path "./target/*" -exec grep -l "sqlx::query" {} \; | while read -r file; do
    # Extract queries and hash them
    grep -A10 "sqlx::query" "$file" | sha256sum | cut -d' ' -f1 >> "$TEMP_HASHES"
done

# Compare with cached queries
NEEDS_UPDATE=false
if [ -f ".sqlx/query-hashes" ]; then
    if ! diff -q "$TEMP_HASHES" ".sqlx/query-hashes" >/dev/null 2>&1; then
        NEEDS_UPDATE=true
    fi
else
    NEEDS_UPDATE=true
fi

if [ "$NEEDS_UPDATE" = true ]; then
    log "SQL queries have changed, updating cache..."
    
    # Update the cache
    cargo sqlx prepare --workspace -- --all-targets --all-features || {
        error "Failed to update SQLX cache"
        exit 1
    }
    
    # Save the new hashes
    mkdir -p .sqlx
    mv "$TEMP_HASHES" ".sqlx/query-hashes"
    
    # Stage the changes
    git add .sqlx/
    
    log "SQLX cache updated successfully"
else
    log "SQLX cache is up to date"
fi
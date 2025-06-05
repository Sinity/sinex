#!/usr/bin/env bash
set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
DEFAULT_TEST_DB="postgres://sinex_test:testpass@localhost:5433/sinex_test"
DEFAULT_DEV_DB="postgresql:///sinex?host=\$PWD/.postgres"

show_help() {
    cat << EOF
Usage: $0 [OPTIONS] [MODE]

Unified database setup script for Sinex project.

MODES:
    --dev       Set up development database (default)
    --test      Set up test database  
    --reset     Reset development database (drop + recreate)
    --check     Check database connectivity

OPTIONS:
    --db-url URL    Override database URL
    --force         Force operations without confirmation
    --help, -h      Show this help

EXAMPLES:
    $0                      # Setup dev database
    $0 --reset              # Reset dev database
    $0 --test               # Setup test database
    $0 --check --test       # Check test database
    
ENVIRONMENT:
    DATABASE_URL            Development database URL
    TEST_DATABASE_URL       Test database URL
EOF
}

log() {
    echo -e "${BLUE}🗄️${NC}  $*"
}

success() {
    echo -e "${GREEN}✅${NC} $*"
}

warning() {
    echo -e "${YELLOW}⚠️${NC}  $*"
}

error() {
    echo -e "${RED}❌${NC} $*" >&2
}

# Parse arguments
MODE="dev"
FORCE=false
DB_URL=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --dev)
            MODE="dev"
            shift
            ;;
        --test)
            MODE="test"
            shift
            ;;
        --reset)
            MODE="reset"
            shift
            ;;
        --check)
            MODE="check"
            shift
            ;;
        --db-url)
            DB_URL="$2"
            shift 2
            ;;
        --force)
            FORCE=true
            shift
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Determine database URL
get_database_url() {
    local mode="$1"
    
    if [[ -n "$DB_URL" ]]; then
        echo "$DB_URL"
        return
    fi
    
    case "$mode" in
        test)
            echo "${TEST_DATABASE_URL:-$DEFAULT_TEST_DB}"
            ;;
        dev|reset)
            if [[ -n "${IN_NIX_SHELL:-}" ]]; then
                # In nix develop environment - use local postgres
                echo "${DATABASE_URL:-$DEFAULT_DEV_DB}"
            else
                # System postgres
                echo "${DATABASE_URL:-postgres://sinex:sinexpass@localhost:5432/sinex}"
            fi
            ;;
        *)
            echo "${DATABASE_URL:-$DEFAULT_DEV_DB}"
            ;;
    esac
}

# Check if PostgreSQL is available
check_postgres_available() {
    if command -v psql >/dev/null 2>&1; then
        return 0
    else
        error "PostgreSQL client 'psql' not found"
        if [[ -z "${IN_NIX_SHELL:-}" ]]; then
            error "Try running: nix develop"
        fi
        return 1
    fi
}

# Extract database name from URL
extract_db_name() {
    local url="$1"
    # Extract database name - handle various URL formats
    if [[ "$url" =~ postgres.*/([^?]+) ]]; then
        echo "${BASH_REMATCH[1]}"
    elif [[ "$url" =~ /([^/]+)$ ]]; then
        echo "${BASH_REMATCH[1]}"
    else
        echo "sinex"
    fi
}

# Setup development database
setup_dev_database() {
    local db_url="$1"
    
    log "Setting up development database"
    
    if [[ -n "${IN_NIX_SHELL:-}" ]]; then
        # Nix environment - use local postgres
        setup_nix_postgres "$db_url"
    else
        # System postgres
        setup_system_postgres "$db_url"
    fi
    
    run_migrations "$db_url"
    success "Development database setup complete"
}

# Setup test database
setup_test_database() {
    local db_url="$1"
    local db_name
    db_name=$(extract_db_name "$db_url")
    
    log "Setting up test database: $db_name"
    
    # Create test database
    createdb --if-not-exists "$db_name" 2>/dev/null || true
    
    # Enable extensions
    psql "$db_url" -c "CREATE EXTENSION IF NOT EXISTS ulid;" >/dev/null
    psql "$db_url" -c "CREATE EXTENSION IF NOT EXISTS vector;" >/dev/null
    psql "$db_url" -c "CREATE EXTENSION IF NOT EXISTS timescaledb;" >/dev/null
    
    run_migrations "$db_url"
    success "Test database setup complete"
}

# Setup nix postgres (local development)
setup_nix_postgres() {
    local db_url="$1"
    local pgdata="$PWD/.postgres"
    
    if [[ ! -d "$pgdata" ]]; then
        log "Initializing local PostgreSQL in $pgdata"
        
        # Initialize PostgreSQL
        initdb --no-locale --encoding=UTF8 -D "$pgdata" >/dev/null 2>&1
        
        # Configure PostgreSQL
        cat >> "$pgdata/postgresql.conf" << EOL
# Performance settings
shared_preload_libraries = 'timescaledb'
max_connections = 100
shared_buffers = 256MB
effective_cache_size = 1GB
EOL
    fi
    
    # Start PostgreSQL if not running
    if ! pg_ctl status -D "$pgdata" >/dev/null 2>&1; then
        log "Starting local PostgreSQL"
        pg_ctl -D "$pgdata" -l "$pgdata/logfile" -o "--unix_socket_directories='$pgdata'" start >/dev/null
        
        # Wait for PostgreSQL to be ready
        for i in {1..10}; do
            if pg_isready -h "$pgdata" >/dev/null 2>&1; then
                break
            fi
            sleep 0.5
        done
    fi
    
    # Create database and user
    createdb -h "$pgdata" sinex 2>/dev/null || true
    psql -h "$pgdata" -d postgres -c "CREATE ROLE sinex WITH LOGIN;" 2>/dev/null || true
    
    # Enable extensions
    psql -h "$pgdata" -d sinex -c "CREATE EXTENSION IF NOT EXISTS ulid;" >/dev/null
    psql -h "$pgdata" -d sinex -c "CREATE EXTENSION IF NOT EXISTS vector;" >/dev/null  
    psql -h "$pgdata" -d sinex -c "CREATE EXTENSION IF NOT EXISTS timescaledb;" >/dev/null
}

# Setup system postgres
setup_system_postgres() {
    local db_url="$1"
    local db_name
    db_name=$(extract_db_name "$db_url")
    
    log "Setting up system PostgreSQL database: $db_name"
    
    # Create database
    createdb --if-not-exists "$db_name" 2>/dev/null || true
    
    # Enable extensions
    psql "$db_url" -c "CREATE EXTENSION IF NOT EXISTS ulid;" >/dev/null
    psql "$db_url" -c "CREATE EXTENSION IF NOT EXISTS vector;" >/dev/null
    psql "$db_url" -c "CREATE EXTENSION IF NOT EXISTS timescaledb;" >/dev/null
}

# Reset database
reset_database() {
    local db_url="$1"
    local db_name
    db_name=$(extract_db_name "$db_url")
    
    if [[ "$FORCE" != "true" ]]; then
        warning "This will completely reset the database: $db_name"
        read -p "Are you sure? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            log "Reset cancelled"
            return 0
        fi
    fi
    
    log "Resetting database: $db_name"
    
    # Drop and recreate
    dropdb "$db_name" 2>/dev/null || true
    
    # Recreate database
    setup_dev_database "$db_url"
    
    success "Database reset complete"
}

# Run migrations
run_migrations() {
    local db_url="$1"
    
    log "Running database migrations"
    DATABASE_URL="$db_url" sqlx migrate run
}

# Check database connectivity
check_database() {
    local db_url="$1"
    local db_name
    db_name=$(extract_db_name "$db_url")
    
    log "Checking database connectivity: $db_name"
    
    if psql "$db_url" -c "SELECT 1;" >/dev/null 2>&1; then
        success "Database connection OK"
        
        # Check extensions
        local extensions
        extensions=$(psql "$db_url" -t -c "SELECT string_agg(extname, ', ') FROM pg_extension WHERE extname IN ('ulid', 'vector', 'timescaledb');" 2>/dev/null | xargs)
        
        if [[ -n "$extensions" ]]; then
            success "Extensions available: $extensions"
        else
            warning "No Sinex extensions found"
        fi
        
        # Check tables
        local table_count
        table_count=$(psql "$db_url" -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema IN ('raw', 'sinex_schemas');" 2>/dev/null | xargs)
        
        if [[ "$table_count" -gt 0 ]]; then
            success "Found $table_count Sinex tables"
        else
            warning "No Sinex tables found - run migrations"
        fi
        
        return 0
    else
        error "Database connection failed"
        return 1
    fi
}

# Main execution
main() {
    check_postgres_available || exit 1
    
    local db_url
    db_url=$(get_database_url "$MODE")
    
    log "Mode: $MODE"
    log "Database URL: $db_url"
    
    case "$MODE" in
        dev)
            setup_dev_database "$db_url"
            ;;
        test)
            setup_test_database "$db_url"
            ;;
        reset)
            reset_database "$db_url"
            ;;
        check)
            check_database "$db_url"
            ;;
        *)
            error "Unknown mode: $MODE"
            exit 1
            ;;
    esac
}

# Run main function
main "$@"
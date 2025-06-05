#!/usr/bin/env bash
# Setup test database for Sinex

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Setting up Sinex test database...${NC}"

# Check if TEST_DATABASE_URL is set
if [ -z "$TEST_DATABASE_URL" ]; then
    # Default to local postgres without port if in nix develop
    if [[ -n "${IN_NIX_SHELL:-}" ]]; then
        export TEST_DATABASE_URL="postgres://$USER@localhost/sinex_test"
    else
        export TEST_DATABASE_URL="postgres://sinex_test:test_password@localhost:5433/sinex_test"
    fi
    echo -e "${YELLOW}TEST_DATABASE_URL not set, using default: $TEST_DATABASE_URL${NC}"
fi

# Parse database connection details
DB_HOST=$(echo $TEST_DATABASE_URL | sed -E 's/.*@([^:\/]+).*/\1/')
DB_PORT=$(echo $TEST_DATABASE_URL | sed -E 's/.*:([0-9]+)\/.*/\1/' || echo "5432")
DB_NAME=$(echo $TEST_DATABASE_URL | sed -E 's/.*\/([^?]+).*/\1/')
DB_USER=$(echo $TEST_DATABASE_URL | sed -E 's/.*\/\/([^:@]+)[:@].*/\1/')

echo "Database: $DB_NAME"
echo "Host: $DB_HOST"
echo "Port: ${DB_PORT:-5432}"
echo "User: $DB_USER"

# Check if we're in nix develop environment
if [[ -n "${IN_NIX_SHELL:-}" ]]; then
    echo -e "${GREEN}Running in nix develop environment...${NC}"
    
    # In nix develop, don't use -U postgres, use current user
    # Check if we can connect
    if ! pg_isready -h $DB_HOST > /dev/null 2>&1; then
        echo -e "${RED}Error: Cannot connect to PostgreSQL at $DB_HOST${NC}"
        echo "Make sure PostgreSQL is running in nix develop"
        exit 1
    fi
    
    # Drop and recreate test database
    echo "Recreating test database..."
    dropdb --if-exists $DB_NAME || true
    createdb $DB_NAME
    
    # Enable extensions
    echo "Enabling PostgreSQL extensions..."
    psql -d $DB_NAME -c "CREATE EXTENSION IF NOT EXISTS pgx_ulid"
    psql -d $DB_NAME -c "CREATE EXTENSION IF NOT EXISTS timescaledb"
    psql -d $DB_NAME -c "CREATE EXTENSION IF NOT EXISTS vector"
    psql -d $DB_NAME -c "CREATE EXTENSION IF NOT EXISTS pg_jsonschema"
else
    # Outside nix develop, use traditional approach
    echo -e "${GREEN}Running with system PostgreSQL...${NC}"
    
    # Check if we can connect to postgres
    if ! pg_isready -h $DB_HOST -p ${DB_PORT:-5432} -U postgres > /dev/null 2>&1; then
        echo -e "${RED}Error: Cannot connect to PostgreSQL at $DB_HOST:${DB_PORT:-5432}${NC}"
        echo "Make sure PostgreSQL is running and accessible"
        exit 1
    fi
    
    # Create test user if it doesn't exist
    echo "Creating test user..."
    psql -h $DB_HOST -p ${DB_PORT:-5432} -U postgres -tc "SELECT 1 FROM pg_user WHERE usename = '$DB_USER'" | grep -q 1 || \
        psql -h $DB_HOST -p ${DB_PORT:-5432} -U postgres -c "CREATE USER $DB_USER WITH PASSWORD 'test_password' CREATEDB"
    
    # Drop and recreate test database
    echo "Recreating test database..."
    psql -h $DB_HOST -p ${DB_PORT:-5432} -U postgres -c "DROP DATABASE IF EXISTS $DB_NAME"
    psql -h $DB_HOST -p ${DB_PORT:-5432} -U postgres -c "CREATE DATABASE $DB_NAME OWNER $DB_USER"
    
    # Enable extensions
    echo "Enabling PostgreSQL extensions..."
    psql -h $DB_HOST -p ${DB_PORT:-5432} -U postgres -d $DB_NAME -c "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\""
    psql -h $DB_HOST -p ${DB_PORT:-5432} -U postgres -d $DB_NAME -c "CREATE EXTENSION IF NOT EXISTS timescaledb" || echo "TimescaleDB not available, skipping"
    psql -h $DB_HOST -p ${DB_PORT:-5432} -U postgres -d $DB_NAME -c "CREATE EXTENSION IF NOT EXISTS vector" || echo "pgvector not available, skipping"
    psql -h $DB_HOST -p ${DB_PORT:-5432} -U postgres -d $DB_NAME -c "CREATE EXTENSION IF NOT EXISTS pg_jsonschema" || echo "pg_jsonschema not available, skipping"
    psql -h $DB_HOST -p ${DB_PORT:-5432} -U postgres -d $DB_NAME -c "CREATE EXTENSION IF NOT EXISTS pgx_ulid" || echo "pgx_ulid not available, skipping"
    
    # Grant permissions
    echo "Granting permissions..."
    psql -h $DB_HOST -p ${DB_PORT:-5432} -U postgres -d $DB_NAME -c "GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $DB_USER"
    psql -h $DB_HOST -p ${DB_PORT:-5432} -U postgres -d $DB_NAME -c "GRANT ALL ON SCHEMA public TO $DB_USER"
fi

echo -e "${GREEN}Test database setup complete!${NC}"
echo ""
echo "To run tests:"
echo "  cargo test --all-features"
echo ""
echo "To run specific test:"
echo "  cargo test test_promotion_worker_end_to_end -- --exact"
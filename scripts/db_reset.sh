#!/usr/bin/env bash
# Database reset script for Sinex
set -euo pipefail

DB_NAME=${1:-sinex}
DB_USER=${2:-$USER}  # Default to current user in nix develop
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "🗄️  Resetting Sinex database: $DB_NAME"

# Check if we're in nix develop environment
if [[ -n "${IN_NIX_SHELL:-}" ]]; then
    # In nix develop, use direct commands without sudo
    echo "   Running in nix develop environment..."
    
    # Drop existing database
    echo "   Dropping existing database..."
    dropdb --if-exists "$DB_NAME" || true
    
    # Create database
    echo "   Creating database..."
    createdb "$DB_NAME"
else
    # Outside nix develop, try to use system postgres
    echo "   Running with system PostgreSQL..."
    
    # Drop existing database
    echo "   Dropping existing database..."
    if command -v sudo >/dev/null 2>&1; then
        sudo -u postgres dropdb --if-exists "$DB_NAME" || true
        echo "   Creating database..."
        sudo -u postgres createdb "$DB_NAME" -O "$DB_USER"
    else
        # If no sudo, assume current user has postgres permissions
        dropdb --if-exists "$DB_NAME" || true
        createdb "$DB_NAME"
    fi
fi

# Run migrations using sqlx
echo "   Running database migrations..."
export DATABASE_URL="${DATABASE_URL:-postgres://$DB_USER@localhost/$DB_NAME}"
cd "$PROJECT_ROOT"
sqlx migrate run

# Insert a schema change event to mark this reset
echo "   Recording schema change event..."
psql -d "$DB_NAME" -c "
INSERT INTO raw.events (source, event_type, payload, host)
VALUES (
    'sinex.operations.db_reset',
    'schema.reset',
    '{\"action\": \"reset\", \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}',
    '$(hostname)'
);" || echo "   (Could not record schema change event - this is normal if table structure changed)"

echo "✅ Database reset complete!"
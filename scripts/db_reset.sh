#!/usr/bin/env bash
# Database reset script for Sinex
set -euo pipefail

DB_NAME=${1:-sinex}
DB_USER=${2:-sinex}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "🗄️  Resetting Sinex database: $DB_NAME"

# Drop existing database
echo "   Dropping existing database..."
sudo -u postgres dropdb --if-exists "$DB_NAME"

# Create database
echo "   Creating database..."
sudo -u postgres createdb "$DB_NAME" -O "$DB_USER"

# Run migrations using sqlx
echo "   Running database migrations..."
export DATABASE_URL="postgres://$DB_USER@localhost/$DB_NAME"
cd "$PROJECT_ROOT"
sqlx migrate run

# Insert a schema change event to mark this reset
echo "   Recording schema change event..."
psql -U "$DB_USER" -d "$DB_NAME" -c "
INSERT INTO raw.events (source, event_type, payload, host)
VALUES (
    'sinex',
    'schema.change',
    '{\"change_description\": \"Applied schema reset with migrations $(date +%Y-%m-%d)\", \"applied_by\": \"db_reset.sh\"}',
    '$(hostname)'
);
"

echo "✅ Database reset completed successfully!"
echo "   Database: $DB_NAME"
echo "   User: $DB_USER"
echo "   Schema: Applied via sqlx migrations"
echo ""
echo "💡 Next steps:"
echo "   - Run tests: cargo test --all-features"
echo "   - Start workers: cargo run --bin sinex-promo-worker"
echo "   - Query events: exo query --last 1h"
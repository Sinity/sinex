#!/usr/bin/env bash
# Database reset script for Sinex Phase 2
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

# Apply schema
echo "   Applying master schema..."
if [ -f "$PROJECT_ROOT/database/master_schema.sql" ]; then
    psql -U "$DB_USER" -d "$DB_NAME" -f "$PROJECT_ROOT/database/master_schema.sql"
else
    echo "❌ Master schema file not found: $PROJECT_ROOT/database/master_schema.sql"
    exit 1
fi

# Insert a schema change event to mark this reset
echo "   Recording schema change event..."
psql -U "$DB_USER" -d "$DB_NAME" -c "
INSERT INTO raw.events (source, event_type, payload, host, ingestor_version)
VALUES (
    'sinex',
    'schema.change',
    '{\"change_description\": \"Applied Phase 2 schema reset from master_schema.sql $(date +%Y-%m-%d)\", \"applied_by\": \"db_reset.sh\"}',
    '$(hostname)',
    '0.2.0'
);
"

echo "✅ Database reset completed successfully!"
echo "   Database: $DB_NAME"
echo "   User: $DB_USER"
echo "   Tables created: raw.events, sinex_schemas.event_payload_schemas, sinex_schemas.agent_manifests"
echo ""
echo "💡 Next steps:"
echo "   - Start ingestors: ./scripts/run_local_dev.sh"
echo "   - Query events: ./cli/exo.py query --last 1h"
echo "   - Check schemas: ./cli/exo.py schema list"
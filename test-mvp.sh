#!/usr/bin/env bash
set -e

echo "🧠 Testing Sinex MVP"

# Check if we're in a Nix development environment
if ! command -v cargo &> /dev/null; then
    echo "⚠️  Please run: nix develop"
    exit 1
fi

# Check if PostgreSQL is running
if ! pg_isready -h localhost &> /dev/null; then
    echo "⚠️  PostgreSQL is not running. Please start it first."
    exit 1
fi

# Create test database if it doesn't exist
DB_NAME="sinex_test"
echo "📦 Setting up test database: $DB_NAME"

createdb "$DB_NAME" 2>/dev/null || echo "Database $DB_NAME already exists"
psql "$DB_NAME" < schema/mvp_schema.sql > /dev/null

# Test CLI with empty database
echo "🔍 Testing CLI with empty database"
export DATABASE_URL="postgresql://localhost/$DB_NAME"
./cli/exo.py sources
./cli/exo.py stats

# Insert some test data
echo "📊 Inserting test events"
psql "$DB_NAME" << EOF
INSERT INTO raw.events (source, payload, provenance) VALUES 
('hyprland', '{"type": "window_change", "data": {"class": "firefox", "title": "Test Page"}}', '{"test": true}'),
('hyprland', '{"type": "workspace_change", "data": {"workspace_id": 2}}', '{"test": true}'),
('test', '{"type": "manual", "data": {"message": "Test event"}}', '{"test": true}');
EOF

# Test CLI queries
echo "📋 Testing CLI queries"
./cli/exo.py sources
./cli/exo.py query --last 1h
./cli/exo.py query --source hyprland
./cli/exo.py query --json --limit 1

# Build the ingestor
echo "🔨 Testing Hyprland ingestor build"
cd ingestors/hyprland
cargo check
cargo test
cd ../..

echo "✅ MVP test completed successfully!"
echo ""
echo "🚀 To run the full system:"
echo "1. Start PostgreSQL and create the sinex database"
echo "2. Apply the schema: psql sinex < schema/mvp_schema.sql"
echo "3. Run the ingestor: cd ingestors/hyprland && cargo run"
echo "4. Query events: ./cli/exo.py query --last 1h"
echo ""
echo "📖 For NixOS integration, see README.md"
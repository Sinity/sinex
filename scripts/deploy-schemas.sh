#!/usr/bin/env bash
set -euo pipefail

# Script to deploy schemas from Git to PostgreSQL
# This syncs the schemas/ directory to the sinex_schemas.schema_registry table

# Database connection (uses DATABASE_URL or defaults to local dev)
DATABASE_URL="${DATABASE_URL:-postgresql:///sinex_dev?host=/run/postgresql}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "🚀 Deploying schemas to PostgreSQL"
echo "   Database: $DATABASE_URL"
echo

# Function to extract schema ID from the $id field
extract_schema_id() {
    local file="$1"
    jq -r '."$id" // empty' "$file" | sed 's|https://sinex.io/schemas/||'
}

# Function to extract version from path (e.g., v1/category/name.json -> v1)
extract_version() {
    local path="$1"
    echo "$path" | cut -d'/' -f1
}

# Function to deploy a single schema
deploy_schema() {
    local file="$1"
    local relative_path="${file#schemas/}"
    
    # Extract metadata
    local schema_id=$(extract_schema_id "$file")
    local version=$(extract_version "$relative_path")
    local content=$(cat "$file")
    
    if [ -z "$schema_id" ]; then
        echo -e "${YELLOW}⚠️  Skipping $relative_path (no \$id field)${NC}"
        return 1
    fi
    
    echo -n "Deploying $schema_id... "
    
    # Prepare SQL to upsert the schema
    cat > /tmp/deploy_schema.sql <<EOF
-- Deploy schema: $schema_id
BEGIN;

-- Ensure schema_registry table exists (migration should have created it)
DO \$\$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'sinex_schemas' 
        AND table_name = 'schema_registry'
    ) THEN
        RAISE EXCEPTION 'Table sinex_schemas.schema_registry does not exist. Run migrations first.';
    END IF;
END
\$\$;

-- Upsert the schema
INSERT INTO sinex_schemas.schema_registry (
    schema_id,
    version,
    schema_content,
    is_active,
    created_at,
    updated_at
) VALUES (
    '$schema_id',
    '$version',
    '$content'::jsonb,
    true,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
)
ON CONFLICT (schema_id, version) DO UPDATE SET
    schema_content = EXCLUDED.schema_content,
    updated_at = CURRENT_TIMESTAMP,
    is_active = true;

-- Deactivate older versions of the same schema
UPDATE sinex_schemas.schema_registry
SET is_active = false
WHERE schema_id = '$schema_id'
  AND version != '$version'
  AND is_active = true;

COMMIT;
EOF

    # Execute the SQL
    if psql "$DATABASE_URL" -f /tmp/deploy_schema.sql > /tmp/deploy_output.txt 2>&1; then
        echo -e "${GREEN}✓${NC}"
        return 0
    else
        echo -e "${RED}✗${NC}"
        echo -e "${RED}Error deploying $schema_id:${NC}"
        cat /tmp/deploy_output.txt | sed 's/^/  /'
        return 1
    fi
}

# Check if psql is available
if ! command -v psql &> /dev/null; then
    echo -e "${RED}❌ psql command not found${NC}"
    echo "Please ensure PostgreSQL client tools are installed"
    exit 1
fi

# Test database connection
echo -n "Testing database connection... "
if psql "$DATABASE_URL" -c "SELECT 1" > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗${NC}"
    echo -e "${RED}Failed to connect to database${NC}"
    exit 1
fi

# Find all schema files
SCHEMA_COUNT=0
DEPLOYED_COUNT=0
FAILED_COUNT=0

echo
echo "📋 Processing schemas..."
echo

find schemas -name "*.json" -type f | sort | while read -r schema_file; do
    ((SCHEMA_COUNT++)) || true
    
    if deploy_schema "$schema_file"; then
        ((DEPLOYED_COUNT++)) || true
    else
        ((FAILED_COUNT++)) || true
    fi
done

# Clean up
rm -f /tmp/deploy_schema.sql /tmp/deploy_output.txt

# Summary
echo
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Deployment Summary:"
echo "  Total schemas found: $SCHEMA_COUNT"
echo -e "  ${GREEN}Successfully deployed: $DEPLOYED_COUNT${NC}"
if [ "$FAILED_COUNT" -gt 0 ]; then
    echo -e "  ${RED}Failed: $FAILED_COUNT${NC}"
    exit 1
else
    echo -e "${GREEN}✅ All schemas deployed successfully${NC}"
fi

# Verify deployment
echo
echo "Verifying deployment..."
ACTIVE_SCHEMAS=$(psql "$DATABASE_URL" -t -c "SELECT COUNT(*) FROM sinex_schemas.schema_registry WHERE is_active = true" 2>/dev/null || echo "0")
echo "Active schemas in database: $ACTIVE_SCHEMAS"

echo
echo -e "${BLUE}💡 Next steps:${NC}"
echo "1. Test schema validation with: just test-schema-validation"
echo "2. Enable schema validation for new events in the collector"
echo "3. Monitor validation errors in the application logs"
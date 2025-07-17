#!/usr/bin/env bash
set -euo pipefail

# Script to check schema backward compatibility
# Usage: ./scripts/check-schema-compatibility.sh [base-branch]

BASE_BRANCH="${1:-master}"
SCHEMA_DIR="schemas"
TEMP_DIR=$(mktemp -d)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "📋 Checking schema compatibility against branch: $BASE_BRANCH"

# Check if json-schema-diff is installed
if ! command -v json-schema-diff &> /dev/null; then
    echo -e "${YELLOW}⚠️  json-schema-diff not found. Installing...${NC}"
    npm install -g json-schema-diff
fi

# Check if we're in a git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo -e "${RED}❌ Not in a git repository${NC}"
    exit 1
fi

# Fetch the base branch
git fetch origin "$BASE_BRANCH" 2>/dev/null || true

# Track if any breaking changes are found
BREAKING_CHANGES=0
COMPATIBLE_CHANGES=0

# Get list of all current schema files
find "$SCHEMA_DIR" -name "*.json" -type f | sort | while read -r current_schema; do
    relative_path="${current_schema#$SCHEMA_DIR/}"
    
    # Check if this schema exists in the base branch
    if git show "origin/$BASE_BRANCH:$current_schema" > "$TEMP_DIR/base.json" 2>/dev/null; then
        echo -n "Checking $relative_path... "
        
        # Run json-schema-diff and capture the output
        if json-schema-diff "$TEMP_DIR/base.json" "$current_schema" > "$TEMP_DIR/diff.txt" 2>&1; then
            echo -e "${GREEN}✓ Compatible${NC}"
            ((COMPATIBLE_CHANGES++)) || true
        else
            echo -e "${RED}✗ Breaking change detected${NC}"
            echo -e "${YELLOW}Details:${NC}"
            cat "$TEMP_DIR/diff.txt" | sed 's/^/  /'
            echo
            ((BREAKING_CHANGES++)) || true
        fi
    else
        echo -e "${GREEN}+ New schema: $relative_path${NC}"
    fi
done

# Check for removed schemas
git ls-tree -r "origin/$BASE_BRANCH" --name-only | grep "^$SCHEMA_DIR/.*\.json$" | while read -r base_schema; do
    if [ ! -f "$base_schema" ]; then
        echo -e "${RED}- Removed schema: ${base_schema#$SCHEMA_DIR/}${NC}"
        echo -e "${YELLOW}  ⚠️  Removing schemas is a breaking change!${NC}"
        ((BREAKING_CHANGES++)) || true
    fi
done

# Clean up
rm -rf "$TEMP_DIR"

echo
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if [ "$BREAKING_CHANGES" -gt 0 ]; then
    echo -e "${RED}❌ Found $BREAKING_CHANGES breaking changes${NC}"
    echo
    echo "Options:"
    echo "1. If these changes are intentional, increment the schema version"
    echo "2. Modify your changes to maintain backward compatibility"
    echo "3. Document the migration path for schema consumers"
    exit 1
else
    echo -e "${GREEN}✅ All schema changes are backward compatible${NC}"
    exit 0
fi
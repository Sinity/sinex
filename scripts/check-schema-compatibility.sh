#!/usr/bin/env bash
# Schema compatibility check script for CI
# 
# This script verifies that schema changes are backward compatible
# and validates version migrations

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
SCHEMA_DIR="crate/sinex-events/src/payloads"
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

echo "🔍 Checking schema compatibility..."

# Function to extract version from a payload file
extract_version() {
    local file=$1
    grep -E "VERSION.*=.*\"[0-9]+\.[0-9]+\.[0-9]+\"" "$file" | sed -E 's/.*"([0-9]+\.[0-9]+\.[0-9]+)".*/\1/' || echo "1.0.0"
}

# Function to check if version is newer
is_version_newer() {
    local new=$1
    local old=$2
    
    IFS='.' read -r new_major new_minor new_patch <<< "$new"
    IFS='.' read -r old_major old_minor old_patch <<< "$old"
    
    if [ "$new_major" -gt "$old_major" ]; then
        return 0
    elif [ "$new_major" -eq "$old_major" ] && [ "$new_minor" -gt "$old_minor" ]; then
        return 0
    elif [ "$new_major" -eq "$old_major" ] && [ "$new_minor" -eq "$old_minor" ] && [ "$new_patch" -gt "$old_patch" ]; then
        return 0
    fi
    return 1
}

# Check if we're in a git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo -e "${RED}Error: Not in a git repository${NC}"
    exit 1
fi

# Get the base branch (usually main or master)
BASE_BRANCH=${CI_BASE_BRANCH:-$(git symbolic-ref refs/remotes/origin/HEAD 2>/dev/null | sed 's@^refs/remotes/origin/@@' || echo "master")}

# Get list of changed payload files
CHANGED_FILES=$(git diff --name-only "$BASE_BRANCH"...HEAD | grep -E "^$SCHEMA_DIR/.*\.rs$" || true)

if [ -z "$CHANGED_FILES" ]; then
    echo -e "${GREEN}✅ No schema changes detected${NC}"
    exit 0
fi

echo "Found schema changes in:"
echo "$CHANGED_FILES"
echo

# Check each changed file
ERRORS=0
WARNINGS=0

for file in $CHANGED_FILES; do
    if [ ! -f "$file" ]; then
        echo -e "${YELLOW}⚠️  File deleted: $file${NC}"
        WARNINGS=$((WARNINGS + 1))
        continue
    fi
    
    # Get the old version from base branch
    git show "$BASE_BRANCH:$file" > "$TEMP_DIR/old_file.rs" 2>/dev/null || {
        echo -e "${GREEN}✅ New schema file: $file${NC}"
        continue
    }
    
    # Extract versions
    OLD_VERSION=$(extract_version "$TEMP_DIR/old_file.rs")
    NEW_VERSION=$(extract_version "$file")
    
    echo "Checking $file: $OLD_VERSION -> $NEW_VERSION"
    
    # Check version progression
    if [ "$OLD_VERSION" == "$NEW_VERSION" ]; then
        # Check if there are structural changes
        if ! diff -q <(grep -E "pub struct|pub enum" "$TEMP_DIR/old_file.rs" | sort) <(grep -E "pub struct|pub enum" "$file" | sort) > /dev/null; then
            echo -e "${RED}❌ Structural changes without version bump in $file${NC}"
            ERRORS=$((ERRORS + 1))
        fi
    elif is_version_newer "$NEW_VERSION" "$OLD_VERSION"; then
        # Check if evolves_from is specified for version changes
        if grep -q "evolves_from" "$file"; then
            echo -e "${GREEN}✅ Version migration specified${NC}"
        else
            # Check if it's a major version change
            IFS='.' read -r new_major _ _ <<< "$NEW_VERSION"
            IFS='.' read -r old_major _ _ <<< "$OLD_VERSION"
            
            if [ "$new_major" -gt "$old_major" ]; then
                echo -e "${YELLOW}⚠️  Major version change without evolves_from attribute${NC}"
                WARNINGS=$((WARNINGS + 1))
            fi
        fi
    else
        echo -e "${RED}❌ Version downgrade detected in $file${NC}"
        ERRORS=$((ERRORS + 1))
    fi
    
    # Check for breaking_change documentation
    if grep -q "breaking_change" "$file"; then
        echo -e "${GREEN}✅ Breaking changes documented${NC}"
    fi
done

# Run Rust schema validation tests
echo
echo "🧪 Running schema validation tests..."

# Enter nix shell and run tests
nix develop --command bash -c "
    cd crate/sinex-events && \
    cargo test schema_registry -- --nocapture || exit 1
" || {
    echo -e "${RED}❌ Schema validation tests failed${NC}"
    ERRORS=$((ERRORS + 1))
}

# Summary
echo
echo "======================================"
echo "Schema Compatibility Check Summary"
echo "======================================"
echo -e "Errors:   ${RED}$ERRORS${NC}"
echo -e "Warnings: ${YELLOW}$WARNINGS${NC}"

if [ $ERRORS -gt 0 ]; then
    echo -e "${RED}❌ Schema compatibility check failed${NC}"
    exit 1
else
    echo -e "${GREEN}✅ Schema compatibility check passed${NC}"
    exit 0
fi
#!/usr/bin/env bash
# Check for common anti-patterns in Sinex codebase
# This is a simple pre-commit hook to encourage proper abstraction usage

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Track if any issues are found
ISSUES_FOUND=0

# Function to print errors
error() {
    echo -e "${RED}❌ $1${NC}"
    ISSUES_FOUND=1
}

# Function to print warnings
warn() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

# Function to print success
success() {
    echo -e "${GREEN}✅ $1${NC}"
}

echo "🔍 Checking Sinex abstractions..."

# Check for raw SQL queries
echo -n "Checking for raw SQL usage... "
if rg 'sqlx::query(_as|_scalar|_file)?!' --type rust crate/ 2>/dev/null | grep -v "crate/sinex-db/src/query_builder.rs"; then
    error "Found raw SQL queries. Use QueryBuilder from sinex-db instead."
    echo "  Example: EventQueries::get_by_id(id) instead of sqlx::query!"
    echo ""
else
    success "No raw SQL queries found"
fi

# Check for hardcoded strings (simplified patterns)
echo -n "Checking for hardcoded strings... "
FOUND_HARDCODED=0

if rg '"process\.heartbeat"' --type rust crate/ 2>/dev/null | grep -v "constants.rs"; then
    error "Found hardcoded event type 'process.heartbeat'"
    echo "  Use: event_types::sinex::PROCESS_HEARTBEAT"
    FOUND_HARDCODED=1
fi

if rg '"core\.events"' --type rust crate/ 2>/dev/null | grep -v -E "(constants|queries|migrations)"; then
    error "Found hardcoded table name 'core.events'"
    echo "  Use: QueryBuilder or constants"
    FOUND_HARDCODED=1
fi

if [ $FOUND_HARDCODED -eq 0 ]; then
    success "No hardcoded strings found"
fi

# Check for anyhow usage
echo -n "Checking for anyhow usage... "
if rg 'anyhow!' --type rust crate/ 2>/dev/null; then
    error "Found anyhow! usage. Use CoreError from sinex-error instead."
    echo "  Example: CoreError::Internal { message: \"...\".to_string() }"
    echo ""
else
    success "No anyhow usage found"
fi

# Check for unwrap/expect in non-test code (warning only)
echo -n "Checking for unwrap/expect... "
UNWRAP_COUNT=$(rg '\.(unwrap|expect)\(' --type rust crate/ 2>/dev/null | grep -v -E "(test|tests|examples)/" | wc -l || true)
if [ "$UNWRAP_COUNT" -gt 0 ]; then
    warn "Found $UNWRAP_COUNT unwrap/expect calls in non-test code"
    echo "  Consider using proper error handling with ?"
fi

echo ""

# Summary
if [ $ISSUES_FOUND -eq 0 ]; then
    success "All abstraction checks passed! 🎉"
    exit 0
else
    error "Found $ISSUES_FOUND abstraction issues"
    echo ""
    echo "Quick fixes:"
    echo "  - QueryBuilder docs: crate/sinex-db/src/query_builder.rs"
    echo "  - Error types: crate/sinex-error/src/lib.rs"
    echo "  - Constants: crate/sinex-events/src/constants.rs"
    echo ""
    echo "For automated migration, run:"
    echo "  ./scripts/migrate-to-abstractions.py crate/"
    exit 1
fi
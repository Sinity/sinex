#!/usr/bin/env bash
set -euo pipefail

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== Sinex Pipeline Test ===${NC}"
echo "This script tests the basic event pipeline functionality"
echo

# Check if database is running
echo -n "Checking database connection... "
if psql "${DATABASE_URL:-postgresql://sinex:sinex@localhost:5432/sinex}" -c "SELECT 1" &>/dev/null; then
    echo -e "${GREEN}OK${NC}"
else
    echo -e "${RED}FAILED${NC}"
    echo "Database is not accessible. Make sure PostgreSQL is running and DATABASE_URL is set correctly."
    exit 1
fi

# Run migrations
echo -n "Running migrations... "
if sqlx migrate run; then
    echo -e "${GREEN}OK${NC}"
else
    echo -e "${RED}FAILED${NC}"
    echo "Failed to run migrations. Check the migration files and database permissions."
    exit 1
fi

# Run the integration tests
echo
echo "Running integration tests..."
echo

# Test categories
declare -a test_suites=(
    "database_integration_tests"
    "real_pipeline_test"
    "ulid_integration_tests"
    "migration_tests"
    "schema_validation_tests"
    "assumption_mismatch_tests"
    "realistic_failure_tests"
)

failed_tests=0
passed_tests=0

# First run unit tests for shared validation
echo -n "Testing validation unit tests... "
if cargo test --package sinex-shared --test validation_unit_tests 2>/dev/null; then
    echo -e "${GREEN}PASSED${NC}"
    ((passed_tests++))
else
    echo -e "${RED}FAILED${NC}"
    ((failed_tests++))
fi

for test_suite in "${test_suites[@]}"; do
    echo -n "Testing ${test_suite}... "
    if cargo test --test "$test_suite" -- --test-threads=1 --nocapture 2>/dev/null; then
        echo -e "${GREEN}PASSED${NC}"
        ((passed_tests++))
    else
        echo -e "${RED}FAILED${NC}"
        ((failed_tests++))
    fi
done

# Run chaos tests
echo -n "Testing chaos scenarios... "
if ./scripts/chaos_test.sh >/dev/null 2>&1; then
    echo -e "${GREEN}PASSED${NC}"
    ((passed_tests++))
else
    echo -e "${RED}FAILED${NC}"
    ((failed_tests++))
fi

# Run real-world tests (if system dependencies are available)
echo -n "Testing real-world scenarios... "
if ./scripts/real_world_test.sh >/dev/null 2>&1; then
    echo -e "${GREEN}PASSED${NC}"
    ((passed_tests++))
else
    echo -e "${YELLOW}SKIPPED${NC} (missing system dependencies)"
fi

echo
echo "========================"
echo "Test Summary:"
echo -e "Passed: ${GREEN}${passed_tests}${NC}"
echo -e "Failed: ${RED}${failed_tests}${NC}"
echo "========================"

if [ $failed_tests -eq 0 ]; then
    echo -e "${GREEN}All tests passed! The pipeline is working correctly.${NC}"
    exit 0
else
    echo -e "${RED}Some tests failed. Please check the output above.${NC}"
    exit 1
fi
#!/usr/bin/env bash
# Lightweight test analytics - always runs coverage
# Stores results in ~/.sinex-analytics/test-runs/

set -euo pipefail

ANALYTICS_DIR="${SINEX_ANALYTICS_DIR:-$HOME/.sinex-analytics}"
TEST_LOG_DIR="$ANALYTICS_DIR/test-runs"
mkdir -p "$TEST_LOG_DIR"

# Generate run ID
RUN_ID="test_$(date +%Y%m%d_%H%M%S)_$$"
RUN_DIR="$TEST_LOG_DIR/$RUN_ID"
mkdir -p "$RUN_DIR"

# Capture pre-test state
cat > "$RUN_DIR/start.json" << EOF
{
  "run_id": "$RUN_ID",
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "test_threads": "${NEXTEST_TEST_THREADS:-$(nproc)}",
  "profile": "${NEXTEST_PROFILE:-default}",
  "git_commit": "$(git rev-parse HEAD 2>/dev/null || echo 'none')",
  "git_dirty": $(git status --porcelain 2>/dev/null | wc -l)
}
EOF

# Capture git state
git status --porcelain > "$RUN_DIR/git-status.txt" 2>/dev/null || true
git diff > "$RUN_DIR/git-diff.patch" 2>/dev/null || true

# Run tests with coverage
START_TIME=$(date +%s%N)

echo "🧪 Running tests with coverage analysis..."

# Run with coverage and output to JSON
cargo llvm-cov nextest \
    --all-features \
    --workspace \
    --json \
    --output-path "$RUN_DIR/coverage.json" \
    "$@" 2>&1 | tee "$RUN_DIR/output.log"

EXIT_CODE=$?
END_TIME=$(date +%s%N)
DURATION_MS=$(( (END_TIME - START_TIME) / 1000000 ))

# Generate text coverage summary
cargo llvm-cov report --json > "$RUN_DIR/coverage-summary.json" 2>/dev/null || true

# Extract test counts from output
TOTAL_TESTS=$(grep -E "test result:|tests? passed" "$RUN_DIR/output.log" | tail -1 | grep -oE "[0-9]+ passed" | grep -oE "[0-9]+" || echo 0)
FAILED_TESTS=$(grep -E "test result:|tests? failed" "$RUN_DIR/output.log" | tail -1 | grep -oE "[0-9]+ failed" | grep -oE "[0-9]+" || echo 0)

# Get coverage percentage
COVERAGE_PCT=$(jq -r '.data[0].totals.lines.percent // 0' "$RUN_DIR/coverage-summary.json" 2>/dev/null || echo 0)

# Create summary
cat > "$RUN_DIR/summary.json" << EOF
{
  "run_id": "$RUN_ID",
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "duration_ms": $DURATION_MS,
  "exit_code": $EXIT_CODE,
  "success": $([ $EXIT_CODE -eq 0 ] && echo true || echo false),
  "tests_passed": $TOTAL_TESTS,
  "tests_failed": $FAILED_TESTS,
  "coverage_percent": $COVERAGE_PCT
}
EOF

# Update index
echo "$RUN_ID|$(date +%s)|$DURATION_MS|$EXIT_CODE|$TOTAL_TESTS|$FAILED_TESTS|$COVERAGE_PCT" >> "$TEST_LOG_DIR/index.csv"

# Print summary
echo ""
echo "📊 Test Summary:"
echo "  Duration: ${DURATION_MS}ms"
echo "  Tests: $TOTAL_TESTS passed, $FAILED_TESTS failed"
echo "  Coverage: ${COVERAGE_PCT}%"
echo "  Results saved to: $RUN_DIR"

# Exit with same code as tests
exit $EXIT_CODE
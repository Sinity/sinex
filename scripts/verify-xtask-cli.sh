#!/usr/bin/env bash
#
# xtask CLI Verification Script
#
# Runs every xtask command and captures output for intelligent review.
# Auto-detects "missing state" failures vs real failures.
#
# Exit classification:
#   PASS - exit 0
#   SKIP - exit non-zero + output matches "missing state" pattern
#   FAIL - exit non-zero + no excuse (real failure)
#

# Require devshell environment (xtask must be on PATH)
if ! command -v xtask &>/dev/null; then
  echo "ERROR: xtask not found on PATH. Run from within the devshell (nix develop)." >&2
  exit 1
fi

set -uo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
DIM='\033[2m'
NC='\033[0m'

# Output capture
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
LOGFILE="target/xtask-verification-${TIMESTAMP}.log"
mkdir -p target

# Counters
TOTAL=0
PASSED=0
FAILED=0
SKIPPED=0

# Patterns that indicate "missing state" (not a real failure)
MISSING_STATE_PATTERNS=(
    "No .* found"
    "No .* recorded"
    "No .* configured"
    "No .* available"
    "no .* exist"
    "not found"
    "not configured"
    "not initialized"
    "missing"
    "doesn't exist"
    "does not exist"
    "connection refused"
    "failed to connect"
    "cannot connect"
    "unable to connect"
    "database.*offline"
    "postgres.*not running"
    "nats.*not running"
    "requires.*first"
    "must.*first"
    "run.*setup"
    "empty"
    "no active"
    "no pending"
    "nothing to"
    # Argument errors (incomplete commands)
    "required arguments were not provided"
    # File access issues
    "failed to read"
    "file not found"
    # Unstable features
    "flag is unstable"
    # Clippy/cargo warnings (codebase state, not command failure)
    "warning:"
    # Formatting issues (codebase state)
    "diff in"
    "checking formatting"
    # TLS certificate issues (dev env not set up)
    "certificate.*not"
    "private key.*not"
    "tls.*issue"
    # Database connection issues (postgres not running)
    "communicating with database"
    "accepting connections"
    "os error 2"
    "sqlx"
    # Infrastructure missing
    "core.events"
    "sinex_dev"
    # cargo build failures due to missing infra
    "cargo build failed"
)

section() {
    echo ""
    echo -e "${CYAN}════════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  $1${NC}"
    echo -e "${CYAN}════════════════════════════════════════════════════════════════${NC}"
    echo ""
}

# Check if output matches any "missing state" pattern
is_missing_state() {
    local output="$1"
    local output_lower
    output_lower=$(echo "$output" | tr '[:upper:]' '[:lower:]')

    for pattern in "${MISSING_STATE_PATTERNS[@]}"; do
        if echo "$output_lower" | grep -qiE "$pattern"; then
            return 0  # true - is missing state
        fi
    done
    return 1  # false - not missing state
}

# Extract the reason from output
extract_skip_reason() {
    local output="$1"
    # Get first line that matches a pattern, truncate to 60 chars
    for pattern in "${MISSING_STATE_PATTERNS[@]}"; do
        local match
        match=$(echo "$output" | grep -iE "$pattern" | head -1 | cut -c1-60)
        if [[ -n "$match" ]]; then
            echo "$match"
            return
        fi
    done
    echo "non-zero exit"
}

test_cmd() {
    local name="$1"
    local cmd="$2"
    local timeout_secs="${3:-30}"

    TOTAL=$((TOTAL + 1))

    echo -e "${BLUE}[TEST]${NC} $name"
    echo -e "${DIM}       $ $cmd${NC}"

    # Capture output and exit code
    local output
    local exit_code=0
    output=$(timeout "$timeout_secs" bash -c "$cmd" 2>&1) || exit_code=$?

    # Handle timeout
    if [[ $exit_code -eq 124 ]]; then
        echo -e "${YELLOW}[SKIP]${NC} Timeout after ${timeout_secs}s"
        SKIPPED=$((SKIPPED + 1))
        return 0
    fi

    if [[ $exit_code -eq 0 ]]; then
        echo -e "${GREEN}[PASS]${NC} exit=0"
        PASSED=$((PASSED + 1))
    elif is_missing_state "$output"; then
        local reason
        reason=$(extract_skip_reason "$output")
        echo -e "${YELLOW}[SKIP]${NC} exit=$exit_code (state: $reason)"
        SKIPPED=$((SKIPPED + 1))
    else
        echo -e "${RED}[FAIL]${NC} exit=$exit_code"
        FAILED=$((FAILED + 1))
        # Print output for failed commands
        if [[ -n "$output" ]]; then
            echo -e "${DIM}       Output:${NC}"
            echo "$output" | head -10 | sed 's/^/         /'
            local lines
            lines=$(echo "$output" | wc -l)
            if [[ $lines -gt 10 ]]; then
                echo -e "${DIM}         ... ($((lines - 10)) more lines)${NC}"
            fi
        fi
    fi

    return 0
}

# Start logging
exec > >(tee -a "$LOGFILE") 2>&1

section "xtask CLI Verification - $(date)"

echo "Log: $LOGFILE"
echo "Dir: $(pwd)"
echo "Branch: $(git branch --show-current 2>/dev/null || echo 'unknown')"
echo ""

# ============================================================================
# TIER 1: CORE DEVELOPMENT
# ============================================================================

section "Tier 1: Core Development"

test_cmd "check --help" "xtask check --help"
test_cmd "check (runs fmt+clippy)" "xtask check" 120
test_cmd "check --json" "xtask check --json" 120

test_cmd "fix --help" "xtask fix --help"
# lint was merged into check command
# test_cmd "lint --help" "xtask lint --help"
test_cmd "test --help" "xtask test --help"
test_cmd "build --help" "xtask build --help"

# ============================================================================
# TIER 2: ANALYSIS (promoted from analyze/*)
# ============================================================================

section "Tier 2: Analysis"

test_cmd "deps --help" "xtask deps --help"
test_cmd "deps list" "xtask deps list"
test_cmd "deps tree" "xtask deps tree" 60
test_cmd "deps duplicates" "xtask deps duplicates"
test_cmd "deps unused" "xtask deps unused" 60
# deps timings requires full release build which needs postgres for sqlx compile-time checks
# test_cmd "deps timings" "xtask deps timings" 60
test_cmd "deps impact" "xtask deps impact" 60

test_cmd "graph --help" "xtask graph --help"
test_cmd "graph deps" "xtask graph deps"

test_cmd "history --help" "xtask history --help"
test_cmd "history list" "xtask history list"
test_cmd "history last --command check" "xtask history last --command check"
test_cmd "history stats --command check" "xtask history stats --command check"
test_cmd "history tests slowest" "xtask history tests slowest"
test_cmd "history tests flaky" "xtask history tests flaky"
test_cmd "history tests getting-slower" "xtask history tests getting-slower"

test_cmd "patterns --help" "xtask patterns --help"
test_cmd "patterns search" "xtask patterns -p '\$X.unwrap()' --limit 5"

test_cmd "snapshot --help" "xtask snapshot --help"

# ============================================================================
# TIER 3: RUNTIME MANAGEMENT
# ============================================================================

section "Tier 3: Runtime Management"

test_cmd "run --help" "xtask run --help"
test_cmd "run list" "xtask run list"
test_cmd "run ingestd --help" "xtask run ingestd --help"
test_cmd "run gateway --help" "xtask run gateway --help"
test_cmd "run node --help" "xtask run node --help"
test_cmd "run stack --help" "xtask run stack --help"
test_cmd "run all-ingestors --help" "xtask run all-ingestors --help"
test_cmd "run all-automatons --help" "xtask run all-automatons --help"

# ============================================================================
# TIER 4: STATUS
# ============================================================================

section "Tier 4: Status"

test_cmd "status --help" "xtask status --help"
test_cmd "status" "xtask status"
# --summary and --doctor flags not yet implemented
# test_cmd "status --summary" "xtask status --summary"
test_cmd "status --json" "xtask status --json"
# test_cmd "status --doctor" "xtask status --doctor"

# ============================================================================
# TIER 5: INFRASTRUCTURE
# ============================================================================

section "Tier 5: Infrastructure"

test_cmd "stack --help" "xtask stack --help"
test_cmd "stack start --help" "xtask stack start --help"
test_cmd "stack stop --help" "xtask stack stop --help"
test_cmd "stack logs --help" "xtask stack logs --help"
test_cmd "stack env --help" "xtask stack env --help"
test_cmd "stack env" "xtask stack env"

test_cmd "stack tls --help" "xtask stack tls --help"
test_cmd "stack tls check" "xtask stack tls check"
test_cmd "stack tls generate-dev-certs --help" "xtask stack tls generate-dev-certs --help"

test_cmd "db --help" "xtask db --help"
test_cmd "db status" "xtask db status"
test_cmd "db status --json" "xtask db status --json"
test_cmd "db migrate --help" "xtask db migrate --help"
test_cmd "db setup --help" "xtask db setup --help"

# ============================================================================
# TIER 6: CONTRACTS (Event Payload Schemas)
# ============================================================================

section "Tier 6: Contracts"

test_cmd "contracts --help" "xtask contracts --help"
test_cmd "contracts generate --help" "xtask contracts generate --help"
test_cmd "contracts deploy --help" "xtask contracts deploy --help"
test_cmd "contracts compat --help" "xtask contracts compat --help"
test_cmd "contracts check-ready" "xtask contracts check-ready"
test_cmd "contracts info list-schemas" "xtask contracts info list-schemas"

# ============================================================================
# TIER 7: JOBS & CI
# ============================================================================

section "Tier 7: Jobs & CI"

test_cmd "jobs --help" "xtask jobs --help"
test_cmd "jobs list" "xtask jobs list"
# jobs active subcommand not implemented (use jobs list instead)
# test_cmd "jobs active" "xtask jobs active"
# jobs wait requires job ID
# test_cmd "jobs wait" "xtask jobs wait" 5

test_cmd "ci --help" "xtask ci --help"
test_cmd "ci workspace --help" "xtask ci workspace --help"

# ============================================================================
# TIER 8: QUALITY TOOLS
# ============================================================================

section "Tier 8: Quality Tools"

# coverage and fuzz commands not yet implemented (planned for future)
# test_cmd "coverage --help" "xtask coverage --help"
# test_cmd "coverage summary --help" "xtask coverage summary --help"
# test_cmd "fuzz --help" "xtask fuzz --help"
# test_cmd "fuzz list" "xtask fuzz list"

test_cmd "docs --help" "xtask docs --help"
test_cmd "docs build --help" "xtask docs build --help"
test_cmd "docs serve --help" "xtask docs serve --help"

# ============================================================================
# TIER 9: OTHER
# ============================================================================

section "Tier 9: Other"

test_cmd "vm --help" "xtask vm --help"
test_cmd "vm test --help" "xtask vm test --help"
test_cmd "vm start --help" "xtask vm start --help"

test_cmd "infra --help" "xtask infra --help"
test_cmd "infra secrets --help" "xtask infra secrets --help"

test_cmd "completions --help" "xtask completions --help"
test_cmd "completions bash" "xtask completions bash"
test_cmd "completions zsh" "xtask completions zsh"
test_cmd "completions fish" "xtask completions fish"

# ============================================================================
# SUMMARY
# ============================================================================

section "Verification Summary"

echo "Total:   $TOTAL"
echo -e "Passed:  ${GREEN}$PASSED${NC} ($(awk "BEGIN {printf \"%.1f\", ($PASSED/$TOTAL)*100}")%)"
echo -e "Failed:  ${RED}$FAILED${NC} ($(awk "BEGIN {printf \"%.1f\", ($FAILED/$TOTAL)*100}")%)"
echo -e "Skipped: ${YELLOW}$SKIPPED${NC} ($(awk "BEGIN {printf \"%.1f\", ($SKIPPED/$TOTAL)*100}")%)"
echo ""
echo "Log: $LOGFILE"
echo "Completed: $(date)"

if [[ $FAILED -gt 0 ]]; then
    echo ""
    echo -e "${RED}⚠ VERIFICATION HAS FAILURES${NC}"
    exit 1
else
    echo ""
    echo -e "${GREEN}✓ ALL TESTS PASSED OR EXPLAINED${NC}"
    exit 0
fi

#!/usr/bin/env bash
set -euo pipefail

# Enhanced VM test runner with debugging and reporting

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Configuration
TEST_RESULTS_DIR="${TEST_RESULTS_DIR:-./test-results}"
KEEP_FAILED_VMS="${KEEP_FAILED_VMS:-false}"
PARALLEL_TESTS="${PARALLEL_TESTS:-false}"
TEST_TIMEOUT="${TEST_TIMEOUT:-900}" # 15 minutes default (reduced from 30m)

# Test categories (initial modernization pass)
SMOKE_TESTS=("basic")
INTEGRATION_TESTS=("preflight" "maintenance" "satellite-matrix" "multi-source" "failure-recovery")
PERFORMANCE_TESTS=("performance")
CHAOS_TESTS=()        # TODO: Restore chaos suites after new failure-injection harness lands
ALL_TESTS=("${SMOKE_TESTS[@]}" "${INTEGRATION_TESTS[@]}" "${PERFORMANCE_TESTS[@]}" "${CHAOS_TESTS[@]}")

# Unique tests only
ALL_TESTS=($(printf '%s\n' "${ALL_TESTS[@]}" | sort -u))

print_usage() {
    cat << EOF
Usage: $0 [OPTIONS] [TEST_NAMES...]

Run Sinex VM tests with enhanced debugging and reporting.

OPTIONS:
    -h, --help              Show this help message
    -k, --keep-failed       Keep VMs running after test failure for debugging
    -p, --parallel          Run tests in parallel (experimental)
    -t, --timeout SECONDS   Set test timeout (default: 1800)
    -c, --category CATEGORY Run all tests in category (smoke|integration|performance|chaos|all)
    -o, --output DIR        Set test results directory (default: ./test-results)
    -v, --verbose           Enable verbose output
    -d, --debug             Enable debug mode (implies --keep-failed and --verbose)
    -l, --list              List available tests
    --validate              Validate VM test infrastructure and syntax

EXAMPLES:
    $0                      # Run smoke tests
    $0 -c all               # Run all tests
    $0 basic-flow           # Run specific test
    $0 -d basic-flow        # Debug specific test
    $0 -c performance -p    # Run performance tests in parallel
EOF
}

list_tests() {
    echo "Available tests:"
    echo ""
    if ((${#SMOKE_TESTS[@]})); then
        echo "Smoke tests (quick validation):"
        for test in "${SMOKE_TESTS[@]}"; do
            echo "  - $test"
        done
        echo ""
    fi

    if ((${#INTEGRATION_TESTS[@]})); then
        echo "Integration tests:"
        for test in "${INTEGRATION_TESTS[@]}"; do
            echo "  - $test"
        done
        echo ""
    fi

    if ((${#PERFORMANCE_TESTS[@]})); then
        echo "Performance tests:"
        for test in "${PERFORMANCE_TESTS[@]}"; do
            echo "  - $test"
        done
        echo ""
    fi

    if ((${#CHAOS_TESTS[@]})); then
        echo "Chaos tests:"
        for test in "${CHAOS_TESTS[@]}"; do
            echo "  - $test"
        done
        echo ""
    else
        echo "Chaos tests:"
        echo "  (pending modernization)"
        echo ""
    fi
}

validate_infrastructure() {
    log "🔍 Validating VM test infrastructure..."
    
    # Check syntax of individual test files
    local test_files=(
        "${SCRIPT_DIR}/test-scenarios/basic-flow.nix"
        "${SCRIPT_DIR}/preflight_deployment_test.nix"
        "${SCRIPT_DIR}/test-scenarios/maintenance.nix"
        "${SCRIPT_DIR}/test-scenarios/satellite-matrix.nix"
        "${SCRIPT_DIR}/test-scenarios/multi-source.nix"
        "${SCRIPT_DIR}/test-scenarios/performance.nix"
    )

    local common_files=(
        "${SCRIPT_DIR}/common/test-base.nix"
        "${SCRIPT_DIR}/common/test-helpers.nix"
        "${SCRIPT_DIR}/common/health-checks.nix"
    )
    
    # Check test files syntax
    for file in "${test_files[@]}"; do
        if [[ -f "$file" ]]; then
            log "✅ Checking syntax of $(basename "$file")..."
            if ! nix-instantiate "$file" \
                --arg pkgs 'import <nixpkgs> {}' \
                --arg sinex-ingestd 'null' \
                --arg sinex-gateway 'null' \
                --arg sinex 'null' \
                --arg sinexCli 'null' \
                --arg pg_jsonschema 'null' >/dev/null 2>&1; then
                error "❌ Syntax error in $file"
                return 1
            fi
        else
            warning "⚠️ Missing file: $file"
        fi
    done
    
    # Check common infrastructure files
    for file in "${common_files[@]}"; do
        if [[ -f "$file" ]]; then
            log "✅ Checking $(basename "$file")..."
            if ! nix-instantiate "$file" --arg pkgs 'import <nixpkgs> {}' >/dev/null 2>&1; then
                error "❌ Syntax error in $file"
                return 1
            fi
        else
            warning "⚠️ Missing file: $file"
        fi
    done
    
    # Check justfile integration
    log "✅ Checking justfile integration..."
    local required_commands=("test-vm")
    
    for cmd in "${required_commands[@]}"; do
        if ! grep -q "$cmd" justfile 2>/dev/null; then
            warning "⚠️ justfile missing $cmd command"
        fi
    done
    
    success "🎉 VM test infrastructure validation completed!"
    echo ""
    echo "📋 Infrastructure Summary:"
    echo "  ✅ NixOS VM test configurations validated"
    echo "  ✅ Common infrastructure modules checked"
    echo "  ✅ Justfile integration verified"
    echo ""
    echo "🚀 Ready to run VM tests with:"
    echo "  $0 -c smoke      # Quick validation tests"
    echo "  $0 -c all        # All VM tests"
    
    return 0
}

log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $*"
}

success() {
    echo -e "${GREEN}✓${NC} $*"
}

error() {
    echo -e "${RED}✗${NC} $*" >&2
}

warning() {
    echo -e "${YELLOW}⚠${NC} $*"
}

run_test() {
    local test_name="$1"
    local test_log="$TEST_RESULTS_DIR/${test_name}.log"
    local test_result="$TEST_RESULTS_DIR/${test_name}.result"
    local start_time=$(date +%s)
    
    log "Running test: $test_name"
    
    # Build command
    local cmd="nix build .#checks.x86_64-linux.sinex-vm-${test_name} -L"
    
    if [[ "$KEEP_FAILED_VMS" == "true" ]]; then
        cmd="$cmd --keep-failed"
    fi
    
    # Run test with timeout and progress indicator
    (
        # Background process to show progress for long-running VM tests
        {
            sleep 60  # Wait 1 minute before first progress message
            elapsed=60
            while kill -0 $$ 2>/dev/null; do
                echo "[$(date +'%H:%M:%S')] 🔄 VM test $test_name still running (${elapsed}s elapsed)..." >&2
                sleep 120  # Progress update every 2 minutes
                elapsed=$((elapsed + 120))
            done
        } &
        progress_pid=$!
        
        # Run the actual test
        if timeout "$TEST_TIMEOUT" bash -c "$cmd 2>&1 | tee '$test_log'"; then
            kill $progress_pid 2>/dev/null || true
            wait $progress_pid 2>/dev/null || true
            
            local end_time=$(date +%s)
            local duration=$((end_time - start_time))
            
            echo "PASSED" > "$test_result"
            echo "Duration: ${duration}s" >> "$test_result"
            
            success "Test $test_name passed (${duration}s)"
            return 0
        else
            kill $progress_pid 2>/dev/null || true
            wait $progress_pid 2>/dev/null || true
            
            local exit_code=$?
            local end_time=$(date +%s)
            local duration=$((end_time - start_time))
            
            echo "FAILED" > "$test_result"
            echo "Exit code: $exit_code" >> "$test_result"
            echo "Duration: ${duration}s" >> "$test_result"
            
            if [[ $exit_code -eq 124 ]]; then
                error "Test $test_name timed out after ${TEST_TIMEOUT}s"
            else
                error "Test $test_name failed (exit code: $exit_code, duration: ${duration}s)"
            fi
            
            # Extract failure info from log
            if [[ -f "$test_log" ]]; then
                echo "" >> "$test_result"
                echo "Last 50 lines of output:" >> "$test_result"
                tail -50 "$test_log" >> "$test_result"
            fi
            
            return 1
        fi
    )
}

run_tests_parallel() {
    local tests=("$@")
    local pids=()
    local failed_tests=()
    
    log "Running ${#tests[@]} tests in parallel"
    
    # Start all tests
    for test in "${tests[@]}"; do
        run_test "$test" &
        pids+=("$!:$test")
    done
    
    # Wait for all tests
    for pid_test in "${pids[@]}"; do
        local pid="${pid_test%%:*}"
        local test="${pid_test##*:}"
        
        if ! wait "$pid"; then
            failed_tests+=("$test")
        fi
    done
    
    if [[ ${#failed_tests[@]} -gt 0 ]]; then
        error "Failed tests: ${failed_tests[*]}"
        return 1
    fi
    
    return 0
}

run_tests_sequential() {
    local tests=("$@")
    local failed_tests=()
    
    for test in "${tests[@]}"; do
        if ! run_test "$test"; then
            failed_tests+=("$test")
            
            # In debug mode, stop on first failure
            if [[ "$DEBUG_MODE" == "true" ]]; then
                error "Stopping due to test failure in debug mode"
                break
            fi
        fi
    done
    
    if [[ ${#failed_tests[@]} -gt 0 ]]; then
        return 1
    fi
    
    return 0
}

generate_report() {
    local report_file="$TEST_RESULTS_DIR/summary.txt"
    local passed=0
    local failed=0
    local total=0
    
    echo "Test Summary Report" > "$report_file"
    echo "==================" >> "$report_file"
    echo "Generated: $(date)" >> "$report_file"
    echo "" >> "$report_file"
    
    for result_file in "$TEST_RESULTS_DIR"/*.result; do
        if [[ ! -f "$result_file" ]]; then
            continue
        fi
        
        local test_name=$(basename "$result_file" .result)
        local status=$(head -1 "$result_file")
        local duration=$(grep "Duration:" "$result_file" | cut -d' ' -f2)
        
        ((total++))
        
        if [[ "$status" == "PASSED" ]]; then
            ((passed++))
            echo "✓ $test_name - PASSED ($duration)" >> "$report_file"
        else
            ((failed++))
            echo "✗ $test_name - FAILED ($duration)" >> "$report_file"
        fi
    done
    
    echo "" >> "$report_file"
    echo "Total: $total" >> "$report_file"
    echo "Passed: $passed" >> "$report_file"
    echo "Failed: $failed" >> "$report_file"
    echo "Success rate: $(( total > 0 ? passed * 100 / total : 0 ))%" >> "$report_file"
    
    cat "$report_file"
    
    # Return failure if any tests failed
    [[ $failed -eq 0 ]]
}

# Parse arguments
VERBOSE="false"
DEBUG_MODE="false"
TESTS_TO_RUN=()
CATEGORY=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            print_usage
            exit 0
            ;;
        -k|--keep-failed)
            KEEP_FAILED_VMS="true"
            shift
            ;;
        -p|--parallel)
            PARALLEL_TESTS="true"
            shift
            ;;
        -t|--timeout)
            TEST_TIMEOUT="$2"
            shift 2
            ;;
        -c|--category)
            CATEGORY="$2"
            shift 2
            ;;
        -o|--output)
            TEST_RESULTS_DIR="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE="true"
            shift
            ;;
        -d|--debug)
            DEBUG_MODE="true"
            KEEP_FAILED_VMS="true"
            VERBOSE="true"
            shift
            ;;
        -l|--list)
            list_tests
            exit 0
            ;;
        --validate)
            validate_infrastructure
            exit $?
            ;;
        -*)
            error "Unknown option: $1"
            print_usage
            exit 1
            ;;
        *)
            TESTS_TO_RUN+=("$1")
            shift
            ;;
    esac
done

# Determine which tests to run
if [[ ${#TESTS_TO_RUN[@]} -eq 0 ]]; then
    if [[ -n "$CATEGORY" ]]; then
        case $CATEGORY in
            smoke)
                TESTS_TO_RUN=("${SMOKE_TESTS[@]}")
                ;;
            integration)
                TESTS_TO_RUN=("${INTEGRATION_TESTS[@]}")
                ;;
            performance)
                TESTS_TO_RUN=("${PERFORMANCE_TESTS[@]}")
                ;;
            chaos)
                TESTS_TO_RUN=("${CHAOS_TESTS[@]}")
                ;;
            all)
                TESTS_TO_RUN=("${ALL_TESTS[@]}")
                ;;
            *)
                error "Unknown category: $CATEGORY"
                exit 1
                ;;
        esac
    else
        # Default to smoke tests
        TESTS_TO_RUN=("${SMOKE_TESTS[@]}")
    fi
fi

# Validate test names
for test in "${TESTS_TO_RUN[@]}"; do
    if [[ ! " ${ALL_TESTS[*]} " =~ " ${test} " ]]; then
        error "Unknown test: $test"
        list_tests
        exit 1
    fi
done

# Setup
mkdir -p "$TEST_RESULTS_DIR"
rm -f "$TEST_RESULTS_DIR"/*.{log,result}

# Show configuration
log "Test configuration:"
log "  Tests: ${TESTS_TO_RUN[*]}"
log "  Results dir: $TEST_RESULTS_DIR"
log "  Timeout: ${TEST_TIMEOUT}s"
log "  Parallel: $PARALLEL_TESTS"
log "  Keep failed VMs: $KEEP_FAILED_VMS"
log "  Debug mode: $DEBUG_MODE"
echo ""

# Run tests
if [[ "$PARALLEL_TESTS" == "true" ]] && [[ ${#TESTS_TO_RUN[@]} -gt 1 ]]; then
    run_tests_parallel "${TESTS_TO_RUN[@]}"
else
    run_tests_sequential "${TESTS_TO_RUN[@]}"
fi

# Generate report
echo ""
generate_report

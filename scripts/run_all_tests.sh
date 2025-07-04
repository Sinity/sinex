#!/usr/bin/env bash
# Comprehensive test runner for Sinex project
# Runs all test suites from fastest to slowest with clean output

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'
BOLD='\033[1m'

# Configuration
MAX_THREADS="${MAX_THREADS:-8}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="${PROJECT_ROOT}/test-results-$(date +%Y%m%d-%H%M%S)"

# Test tracking
declare -A TEST_STATUS
declare -A TEST_TIMES
TOTAL_PASSED=0
TOTAL_FAILED=0
TOTAL_IGNORED=0

# Create results directory
mkdir -p "$RESULTS_DIR"

# Helper functions
print_banner() {
    echo -e "\n${BLUE}$(printf '=%.0s' {1..80})${NC}"
    printf "${BOLD}%*s%s%*s${NC}\n" 25 "" "$1" 25 ""
    echo -e "${BLUE}$(printf '=%.0s' {1..80})${NC}\n"
}

print_section() {
    echo -e "\n${CYAN}━━━ $1 ━━━${NC}"
}

success() { echo -e "${GREEN}✓${NC} $1"; }
error() { echo -e "${RED}✗${NC} $1"; }
warning() { echo -e "${YELLOW}⚠${NC} $1"; }
info() { echo -e "${BLUE}ℹ${NC} $1"; }

start_timer() { echo "$(date +%s.%N)"; }
end_timer() { 
    awk "BEGIN { print $(date +%s.%N) - $1 }"
}

format_duration() {
    awk "BEGIN { 
        d = $1; m = int(d/60); s = d - (m*60); 
        printf \"%dm %.1fs\", m, s 
    }"
}

# Test execution function
run_test_suite() {
    local name="$1"
    local cmd="$2"
    local timeout="${3:-300}"
    
    echo -e "${CYAN}▶${NC} Running $name..."
    
    local start_time=$(start_timer)
    local log_file="${RESULTS_DIR}/${name}.log"
    
    if timeout "$timeout" bash -c "$cmd" &> "$log_file"; then
        local duration=$(end_timer "$start_time")
        
        # Extract test counts
        local passed=$(grep -E "test result:|passed.*failed.*ignored" "$log_file" | \
                      grep -oE "[0-9]+ passed" | awk '{sum += $1} END {print sum+0}')
        local failed=$(grep -E "test result:|passed.*failed.*ignored" "$log_file" | \
                      grep -oE "[0-9]+ failed" | awk '{sum += $1} END {print sum+0}')
        local ignored=$(grep -E "test result:|passed.*failed.*ignored" "$log_file" | \
                       grep -oE "[0-9]+ ignored" | awk '{sum += $1} END {print sum+0}')
        
        passed=${passed:-0}
        failed=${failed:-0}
        ignored=${ignored:-0}
        
        if [ "$failed" -eq 0 ]; then
            success "$name: ${passed} passed, ${ignored} ignored ($(format_duration $duration))"
            TEST_STATUS["$name"]="PASSED"
        else
            error "$name: ${passed} passed, ${failed} failed, ${ignored} ignored ($(format_duration $duration))"
            TEST_STATUS["$name"]="FAILED"
            echo -e "${RED}First few failures:${NC}"
            grep -A2 "^test.*FAILED" "$log_file" | head -10
        fi
        
        TEST_TIMES["$name"]="$duration"
        TOTAL_PASSED=$((TOTAL_PASSED + passed))
        TOTAL_FAILED=$((TOTAL_FAILED + failed))
        TOTAL_IGNORED=$((TOTAL_IGNORED + ignored))
    else
        local duration=$(end_timer "$start_time")
        error "$name: Failed or timed out ($(format_duration $duration))"
        TEST_STATUS["$name"]="ERROR"
        TEST_TIMES["$name"]="$duration"
        TOTAL_FAILED=$((TOTAL_FAILED + 1))
        
        echo -e "${RED}Last output:${NC}"
        tail -5 "$log_file"
    fi
}

# VM test function
run_vm_test() {
    local name="$1"
    local check_name="$2"
    
    echo -e "${CYAN}▶${NC} Running VM test: $name..."
    
    local start_time=$(start_timer)
    local log_file="${RESULTS_DIR}/vm-${name}.log"
    
    if timeout 600 nix build --impure ".#checks.x86_64-linux.${check_name}" -L &> "$log_file"; then
        local duration=$(end_timer "$start_time")
        success "VM $name: completed ($(format_duration $duration))"
        TEST_STATUS["vm-$name"]="PASSED"
        TOTAL_PASSED=$((TOTAL_PASSED + 1))
    else
        local duration=$(end_timer "$start_time")
        error "VM $name: failed ($(format_duration $duration))"
        TEST_STATUS["vm-$name"]="FAILED"
        TOTAL_FAILED=$((TOTAL_FAILED + 1))
        
        echo -e "${RED}VM test failure:${NC}"
        tail -10 "$log_file"
    fi
    
    TEST_TIMES["vm-$name"]="$duration"
}

# Main execution
main() {
    local overall_start=$(start_timer)
    
    print_banner "SINEX COMPREHENSIVE TEST SUITE"
    
    info "Configuration:"
    info "  Max threads: $MAX_THREADS"
    info "  Results directory: $RESULTS_DIR"
    info "  Started at: $(date)"
    
    cd "$PROJECT_ROOT"
    
    # Build first
    print_section "Building Project"
    info "Compiling all targets..."
    if cargo build --all-targets --all-features 2>&1 | tee "${RESULTS_DIR}/build.log" | grep -E "Compiling|Finished"; then
        success "Build completed successfully"
    else
        error "Build failed! Check ${RESULTS_DIR}/build.log"
        exit 1
    fi
    
    # Run tests from fastest to slowest
    print_section "Test Suites (Fastest to Slowest)"
    
    # Fast unit tests (< 30s each)
    run_test_suite "unit" "cargo test --test unit -- --test-threads=$MAX_THREADS"
    run_test_suite "integration" "cargo test --test integration -- --test-threads=$MAX_THREADS"
    run_test_suite "system" "cargo test --test system -- --test-threads=$MAX_THREADS"
    
    # Medium tests (30s - 2min)
    run_test_suite "stress" "cargo test stress_tests -- --test-threads=$MAX_THREADS"
    
    # Slower property-based tests (2-5min)
    run_test_suite "property" "cargo test --test property -- --test-threads=$MAX_THREADS --timeout=300"
    
    # Adversarial tests (can be slow)
    run_test_suite "adversarial" "cargo test --test adversarial -- --test-threads=$MAX_THREADS --timeout=300"
    
    # VM tests (slowest, if available)
    if command -v nix &> /dev/null; then
        print_section "VM Tests"
        
        # Check if snapshot infrastructure is available
        local vm_snapshot_script="$PROJECT_ROOT/test/nixos-vm/run-vm-tests-with-snapshots.sh"
        if [ -f "$vm_snapshot_script" ]; then
            info "Running VM tests with snapshot acceleration..."
            run_test_suite "vm-snapshots" "$vm_snapshot_script --quick" 600
        else
            info "Running traditional VM tests..."
            
            # Basic VM functionality
            run_vm_test "basic-flow" "sinex-vm-basic"
            
            # Additional VM tests if they exist
            if nix eval ".#checks.x86_64-linux" --apply "builtins.attrNames" 2>/dev/null | grep -q "sinex-vm-chaos"; then
                run_vm_test "chaos" "sinex-vm-chaos"
            fi
            
            if nix eval ".#checks.x86_64-linux" --apply "builtins.attrNames" 2>/dev/null | grep -q "sinex-vm-production"; then
                run_vm_test "production" "sinex-vm-production"
            fi
        fi
    else
        warning "VM tests require nix command - skipping"
    fi
    
    # Generate summary
    local total_time=$(end_timer "$overall_start")
    
    print_section "Test Summary"
    
    {
        echo "SINEX Test Suite Summary"
        echo "========================"
        echo "Date: $(date)"
        echo "Total Duration: $(format_duration $total_time)"
        echo
        echo "Overall Results:"
        echo "  Total Passed: $TOTAL_PASSED"
        echo "  Total Failed: $TOTAL_FAILED"
        echo "  Total Ignored: $TOTAL_IGNORED"
        echo
        echo "Test Suite Results:"
        echo "-------------------"
        
        for suite in "${!TEST_STATUS[@]}"; do
            local status="${TEST_STATUS[$suite]}"
            local time="${TEST_TIMES[$suite]:-0}"
            printf "%-20s %8s  (%s)\n" "$suite:" "$status" "$(format_duration $time)"
        done | sort
        
        echo
        if [ "$TOTAL_FAILED" -gt 0 ]; then
            echo "Status: FAILED"
        else
            echo "Status: SUCCESS"
        fi
    } | tee "${RESULTS_DIR}/summary.txt"
    
    echo
    if [ "$TOTAL_FAILED" -eq 0 ]; then
        print_banner "ALL TESTS PASSED!"
        success "Total: $TOTAL_PASSED passed, $TOTAL_IGNORED ignored in $(format_duration $total_time)"
    else
        print_banner "TESTS FAILED"
        error "Total: $TOTAL_PASSED passed, $TOTAL_FAILED failed, $TOTAL_IGNORED ignored in $(format_duration $total_time)"
        echo
        error "See $RESULTS_DIR for detailed logs"
        exit 1
    fi
    
    info "Full results saved to: $RESULTS_DIR"
}

# Run main function
main "$@"
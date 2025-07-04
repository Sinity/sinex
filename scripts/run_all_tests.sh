#!/usr/bin/env bash
# Comprehensive test runner for Sinex project
# Runs all test suites including VM tests with pretty output and summary report

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Configuration
MAX_THREADS="${MAX_THREADS:-4}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="${PROJECT_ROOT}/test-results-$(date +%Y%m%d-%H%M%S)"
SUMMARY_FILE="${RESULTS_DIR}/summary.txt"
DETAILED_LOG="${RESULTS_DIR}/detailed.log"

# Test categories
declare -A TEST_SUITES
declare -A TEST_STATUS
declare -A TEST_COUNTS
declare -A TEST_TIMES

# Initialize counters
TOTAL_PASSED=0
TOTAL_FAILED=0
TOTAL_IGNORED=0
TOTAL_TIME=0

# Create results directory
mkdir -p "$RESULTS_DIR"

# Helper functions
print_banner() {
    local text="$1"
    local width=80
    local padding=$(( (width - ${#text}) / 2 ))
    echo
    echo -e "${BLUE}$(printf '=%.0s' {1..80})${NC}"
    printf "${BOLD}%*s%s%*s${NC}\n" $padding "" "$text" $padding ""
    echo -e "${BLUE}$(printf '=%.0s' {1..80})${NC}"
    echo
}

print_section() {
    echo
    echo -e "${CYAN}━━━ $1 ━━━${NC}"
    echo
}

print_subsection() {
    echo -e "${PURPLE}▶ $1${NC}"
}

success() {
    echo -e "${GREEN}✓${NC} $1"
}

error() {
    echo -e "${RED}✗${NC} $1"
}

warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

# Progress spinner
spin() {
    local pid=$1
    local delay=0.1
    local spinstr='⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏'
    while [ "$(ps a | awk '{print $1}' | grep $pid)" ]; do
        local temp=${spinstr#?}
        printf " [%c]  " "$spinstr"
        local spinstr=$temp${spinstr%"$temp"}
        sleep $delay
        printf "\b\b\b\b\b\b"
    done
    printf "    \b\b\b\b"
}

# Timer functions
start_timer() {
    echo "$(date +%s.%N)"
}

end_timer() {
    local start_time=$1
    local end_time=$(date +%s.%N)
    # Use awk instead of bc for better compatibility
    awk "BEGIN { print $end_time - $start_time }"
}

format_duration() {
    local duration=$1
    # Use awk instead of bc for better floating point handling
    awk "BEGIN { 
        duration = $duration; 
        minutes = int(duration / 60); 
        seconds = duration - (minutes * 60); 
        printf \"%dm %.1fs\", minutes, seconds 
    }"
}

# Test execution functions
run_cargo_tests() {
    local suite_name="$1"
    local test_pattern="$2"
    local extra_args="${3:-}"
    
    print_subsection "Running $suite_name tests..."
    
    local start_time=$(start_timer)
    local log_file="${RESULTS_DIR}/${suite_name}.log"
    
    # Run tests and capture output with timeout
    if timeout 300 cargo test $test_pattern -- --test-threads=$MAX_THREADS $extra_args &> "$log_file"; then
        local duration=$(end_timer "$start_time")
        
        # Extract test counts more reliably
        local passed=$(grep "test result:" "$log_file" | grep -oE "[0-9]+ passed" | awk '{sum += $1} END {print sum+0}')
        local failed=$(grep "test result:" "$log_file" | grep -oE "[0-9]+ failed" | awk '{sum += $1} END {print sum+0}')
        local ignored=$(grep "test result:" "$log_file" | grep -oE "[0-9]+ ignored" | awk '{sum += $1} END {print sum+0}')
        
        passed=${passed:-0}
        failed=${failed:-0}
        ignored=${ignored:-0}
        
        if [ "$failed" -eq 0 ]; then
            success "$suite_name: ${passed} passed, ${ignored} ignored ($(format_duration $duration))"
            TEST_STATUS["$suite_name"]="PASSED"
        else
            error "$suite_name: ${passed} passed, ${failed} failed, ${ignored} ignored ($(format_duration $duration))"
            TEST_STATUS["$suite_name"]="FAILED"
            
            # Show failures
            echo -e "${RED}Failures:${NC}"
            grep -A5 "^---- " "$log_file" | head -20
        fi
        
        TEST_COUNTS["$suite_name"]="$passed:$failed:$ignored"
        TEST_TIMES["$suite_name"]="$duration"
        
        TOTAL_PASSED=$((TOTAL_PASSED + passed))
        TOTAL_FAILED=$((TOTAL_FAILED + failed))
        TOTAL_IGNORED=$((TOTAL_IGNORED + ignored))
    else
        error "$suite_name: Test execution failed"
        TEST_STATUS["$suite_name"]="ERROR"
        TEST_COUNTS["$suite_name"]="0:0:0"
        TEST_TIMES["$suite_name"]="0"
        
        # Show error
        tail -20 "$log_file"
    fi
}

run_vm_tests() {
    local category="$1"
    
    print_subsection "Running VM $category tests..."
    
    local start_time=$(start_timer)
    local log_file="${RESULTS_DIR}/vm-${category}.log"
    local vm_script_dir="${PROJECT_ROOT}/test/nixos-vm"
    local vm_script=""
    
    # Determine which VM script to use based on category
    case "$category" in
        "basic")
            vm_script="${vm_script_dir}/run-vm-tests.sh"
            ;;
        "snapshots")
            vm_script="${vm_script_dir}/run-vm-tests-with-snapshots.sh"
            ;;
        *)
            # For specific categories, use the snapshot runner with category
            vm_script="${vm_script_dir}/run-vm-tests-with-snapshots.sh"
            ;;
    esac
    
    if [ ! -f "$vm_script" ]; then
        warning "VM test runner not found at $vm_script"
        TEST_STATUS["vm-$category"]="SKIPPED"
        TEST_COUNTS["vm-$category"]="0:0:0"
        TEST_TIMES["vm-$category"]="0"
        return
    fi
    
    # Run VM tests with appropriate arguments
    local vm_cmd="$vm_script"
    if [ "$category" != "basic" ] && [ "$category" != "snapshots" ]; then
        vm_cmd="$vm_script -c $category -o ${RESULTS_DIR}/vm-${category}"
    fi
    
    if timeout 600 $vm_cmd &> "$log_file"; then
        local duration=$(end_timer "$start_time")
        
        # Extract results from VM test output
        local passed=$(grep -E "✓|PASS|SUCCESS" "$log_file" | wc -l)
        local failed=$(grep -E "✗|FAIL|ERROR" "$log_file" | wc -l)
        
        # If no explicit pass/fail markers, look for test completion
        if [ "$passed" -eq 0 ] && [ "$failed" -eq 0 ]; then
            if grep -q "All tests completed" "$log_file" || grep -q "Test suite finished" "$log_file"; then
                passed=1
            else
                failed=1
            fi
        fi
        
        if [ "$failed" -eq 0 ]; then
            success "VM $category: ${passed} scenarios passed ($(format_duration $duration))"
            TEST_STATUS["vm-$category"]="PASSED"
        else
            error "VM $category: ${passed} passed, ${failed} failed ($(format_duration $duration))"
            TEST_STATUS["vm-$category"]="FAILED"
        fi
        
        TEST_COUNTS["vm-$category"]="$passed:$failed:0"
        TEST_TIMES["vm-$category"]="$duration"
        
        TOTAL_PASSED=$((TOTAL_PASSED + passed))
        TOTAL_FAILED=$((TOTAL_FAILED + failed))
    else
        local duration=$(end_timer "$start_time")
        error "VM $category: Test execution failed or timed out ($(format_duration $duration))"
        TEST_STATUS["vm-$category"]="ERROR"
        TEST_COUNTS["vm-$category"]="0:1:0"
        TEST_TIMES["vm-$category"]="$duration"
        TOTAL_FAILED=$((TOTAL_FAILED + 1))
        
        # Show last few lines of log
        tail -10 "$log_file"
    fi
}

# Main execution
main() {
    local overall_start=$(start_timer)
    
    print_banner "SINEX COMPREHENSIVE TEST SUITE"
    
    info "Configuration:"
    info "  Max threads: $MAX_THREADS"
    info "  Results directory: $RESULTS_DIR"
    info "  Started at: $(date)"
    
    # Ensure we're in the project root
    cd "$PROJECT_ROOT"
    
    # Build everything first
    print_section "Building Project"
    info "Compiling all targets..."
    if cargo build --all-targets 2>&1 | tee "${RESULTS_DIR}/build.log" | grep -E "Compiling|Finished"; then
        success "Build completed successfully"
    else
        error "Build failed! Check ${RESULTS_DIR}/build.log for details"
        exit 1
    fi
    
    # Run library tests
    print_section "Library Tests"
    
    local libs=(
        "sinex-core"
        "sinex-db"
        "sinex-ulid"
        "sinex-collector"
        "sinex-worker"
        "sinex-events-fs"
        "sinex-events-desktop"
        "sinex-events-terminal"
        "sinex-events-system"
    )
    
    for lib in "${libs[@]}"; do
        run_cargo_tests "lib-$lib" "--package $lib --lib"
    done
    
    # Run integration test suites
    print_section "Integration Test Suites"
    
    run_cargo_tests "unit" "unit::"
    run_cargo_tests "integration" "integration::"
    run_cargo_tests "system" "system::"
    run_cargo_tests "property" "property::"
    run_cargo_tests "adversarial" "adversarial::"
    run_cargo_tests "stress" "stress_tests::"
    
    # Run ignored tests separately
    print_section "Ignored Tests"
    run_cargo_tests "ignored" "" "--ignored"
    
    # Run VM tests if available
    if command -v nix &> /dev/null && [ -n "${IN_NIX_SHELL:-}" ]; then
        print_section "VM Tests"
        
        # Check if VM test runner exists
        local vm_script_dir="${PROJECT_ROOT}/test/nixos-vm"
        if [ -d "$vm_script_dir" ]; then
            info "Running NixOS VM tests..."
            
            # Run basic VM functionality test
            if [ -f "$vm_script_dir/run-vm-tests.sh" ]; then
                run_vm_tests "basic"
            fi
            
            # Run VM tests with snapshots if available
            if [ -f "$vm_script_dir/run-vm-tests-with-snapshots.sh" ]; then
                run_vm_tests "snapshots"
            fi
            
            # Run individual test categories
            for category in smoke integration performance deployment; do
                if [ -f "$vm_script_dir/test-scenarios/${category}.nix" ]; then
                    run_vm_tests "$category"
                fi
            done
        else
            warning "VM test directory not found at $vm_script_dir"
            info "VM tests are available but not configured for this project"
        fi
    else
        warning "VM tests require nix-shell environment"
        info "Run 'nix develop' first to enable VM tests"
    fi
    
    # Calculate total time
    TOTAL_TIME=$(end_timer "$overall_start")
    
    # Generate summary report
    print_section "Test Summary Report"
    
    {
        echo "SINEX Test Suite Summary"
        echo "========================"
        echo "Date: $(date)"
        echo "Total Duration: $(format_duration $TOTAL_TIME)"
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
            local counts="${TEST_COUNTS[$suite]:-0:0:0}"
            local time="${TEST_TIMES[$suite]:-0}"
            IFS=':' read -r passed failed ignored <<< "$counts"
            
            printf "%-25s %8s  %4s passed, %4s failed, %4s ignored  (%s)\n" \
                "$suite:" "$status" "$passed" "$failed" "$ignored" "$(format_duration $time)"
        done | sort
        
        echo
        
        if [ "$TOTAL_FAILED" -gt 0 ]; then
            echo "Status: FAILED"
            echo
            echo "Failed test logs available in:"
            for suite in "${!TEST_STATUS[@]}"; do
                if [ "${TEST_STATUS[$suite]}" = "FAILED" ]; then
                    echo "  - ${RESULTS_DIR}/${suite}.log"
                fi
            done
        else
            echo "Status: SUCCESS"
        fi
    } | tee "$SUMMARY_FILE"
    
    # Print summary to console with colors
    echo
    if [ "$TOTAL_FAILED" -eq 0 ]; then
        print_banner "$(printf "${GREEN}ALL TESTS PASSED!${NC}")"
        success "Total: $TOTAL_PASSED passed, $TOTAL_IGNORED ignored in $(format_duration $TOTAL_TIME)"
    else
        print_banner "$(printf "${RED}TESTS FAILED${NC}")"
        error "Total: $TOTAL_PASSED passed, $TOTAL_FAILED failed, $TOTAL_IGNORED ignored in $(format_duration $TOTAL_TIME)"
        echo
        error "See $RESULTS_DIR for detailed logs"
        exit 1
    fi
    
    echo
    info "Full results saved to: $RESULTS_DIR"
    info "Summary report: $SUMMARY_FILE"
}

# Run main function
main "$@"
#!/usr/bin/env bash
# Comprehensive test runner for Sinex Pre-Flight Verification system
set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test configuration
TIMEOUT_SECS=${TIMEOUT_SECS:-300}
PARALLEL_JOBS=${PARALLEL_JOBS:-$(nproc)}
TEST_DATABASE_URL=${TEST_DATABASE_URL:-"postgresql:///sinex_test?host=/run/postgresql"}
VERBOSE=${VERBOSE:-false}

# Paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
TARGET_DIR="$PROJECT_ROOT/target"

echo -e "${BLUE}🧪 Sinex Pre-Flight Verification Test Suite${NC}"
echo "=================================================="
echo "Project Root: $PROJECT_ROOT"
echo "Test Database: $TEST_DATABASE_URL"
echo "Timeout: ${TIMEOUT_SECS}s"
echo "Parallel Jobs: $PARALLEL_JOBS"
echo ""

# Function to print section headers
print_section() {
    echo -e "\n${BLUE}=== $1 ===${NC}"
}

# Function to print success
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

# Function to print error
print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Function to print warning
print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

# Function to run command with timeout and error handling
run_with_timeout() {
    local cmd="$1"
    local description="$2"
    
    echo "Running: $description"
    if [ "$VERBOSE" = "true" ]; then
        echo "Command: $cmd"
    fi
    
    if timeout "$TIMEOUT_SECS" bash -c "$cmd"; then
        print_success "$description"
        return 0
    else
        local exit_code=$?
        if [ $exit_code -eq 124 ]; then
            print_error "$description (TIMEOUT after ${TIMEOUT_SECS}s)"
        else
            print_error "$description (exit code: $exit_code)"
        fi
        return $exit_code
    fi
}

# Function to check prerequisites
check_prerequisites() {
    print_section "Checking Prerequisites"
    
    # Check if we're in a Nix shell
    if [ -z "${IN_NIX_SHELL:-}" ]; then
        print_warning "Not in nix shell. Run 'nix develop' first for best results."
    else
        print_success "Running in nix development shell"
    fi
    
    # Check for required tools
    local required_tools=("cargo" "psql" "systemctl")
    for tool in "${required_tools[@]}"; do
        if command -v "$tool" >/dev/null 2>&1; then
            print_success "$tool is available"
        else
            print_error "$tool is not available"
            return 1
        fi
    done
    
    # Check database connectivity
    if psql "$TEST_DATABASE_URL" -c "SELECT 1" >/dev/null 2>&1; then
        print_success "Test database is accessible"
    else
        print_error "Test database is not accessible"
        echo "Please ensure PostgreSQL is running and test database exists:"
        echo "  createdb sinex_test"
        return 1
    fi
    
    # Check if database has required schema
    if psql "$TEST_DATABASE_URL" -c "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'raw'" >/dev/null 2>&1; then
        print_success "Database schema exists"
    else
        print_warning "Database schema not found - will be created during tests"
    fi
}

# Function to build the project
build_project() {
    print_section "Building Project"
    
    cd "$PROJECT_ROOT"
    
    # Build all packages including sinex-preflight
    run_with_timeout \
        "cargo build --workspace --all-targets" \
        "Building workspace"
    
    # Build release version for performance testing
    run_with_timeout \
        "cargo build --workspace --release" \
        "Building release version"
    
    # Verify sinex-preflight binary exists
    local preflight_binary="$TARGET_DIR/debug/sinex-preflight"
    if [ -f "$preflight_binary" ]; then
        print_success "sinex-preflight binary built successfully"
        # Make it available for tests
        export SINEX_PREFLIGHT_BINARY="$preflight_binary"
    else
        print_error "sinex-preflight binary not found at $preflight_binary"
        return 1
    fi
}

# Function to run unit tests
run_unit_tests() {
    print_section "Running Unit Tests"
    
    cd "$PROJECT_ROOT"
    
    # Set up test environment
    export DATABASE_URL="$TEST_DATABASE_URL"
    export RUST_LOG="info"
    export RUST_BACKTRACE="1"
    
    # Run unit tests for pre-flight verification modules
    run_with_timeout \
        "cargo test --package sinex-preflight --lib" \
        "Pre-flight verification unit tests"
    
    # Run specific unit tests for verification modules
    run_with_timeout \
        "cargo test --test '*' test::unit::preflight" \
        "Pre-flight unit test suite"
}

# Function to run integration tests
run_integration_tests() {
    print_section "Running Integration Tests"
    
    cd "$PROJECT_ROOT"
    
    # Set up test environment
    export DATABASE_URL="$TEST_DATABASE_URL"
    export RUST_LOG="info"
    export RUST_BACKTRACE="1"
    export SINEX_PREFLIGHT_BINARY="$TARGET_DIR/debug/sinex-preflight"
    
    # Run integration tests
    run_with_timeout \
        "cargo test --test '*' test::integration::preflight" \
        "Pre-flight integration tests"
    
    # Run database-specific integration tests
    run_with_timeout \
        "cargo test --test '*' preflight_verification_test" \
        "Pre-flight verification integration tests"
}

# Function to run system tests
run_system_tests() {
    print_section "Running System Tests"
    
    cd "$PROJECT_ROOT"
    
    # Set up test environment
    export DATABASE_URL="$TEST_DATABASE_URL"
    export RUST_LOG="info"
    export RUST_BACKTRACE="1"
    export SINEX_PREFLIGHT_BINARY="$TARGET_DIR/debug/sinex-preflight"
    
    # Run system-level tests
    run_with_timeout \
        "cargo test --test '*' test::system::preflight" \
        "Pre-flight system tests"
    
    # Run system test suite
    run_with_timeout \
        "cargo test --test '*' preflight_system_test" \
        "Pre-flight system test suite"
}

# Function to run VM tests
run_vm_tests() {
    print_section "Running NixOS VM Tests"
    
    cd "$PROJECT_ROOT"
    
    # Check if we can run NixOS tests
    if ! command -v nixos-test >/dev/null 2>&1; then
        print_warning "nixos-test not available, skipping VM tests"
        print_warning "Install with: nix-env -iA nixos.nixos-test"
        return 0
    fi
    
    # Run NixOS VM integration test
    run_with_timeout \
        "nixos-test test/nixos-vm/preflight_deployment_test.nix" \
        "NixOS VM deployment test"
}

# Function to run performance tests
run_performance_tests() {
    print_section "Running Performance Tests"
    
    cd "$PROJECT_ROOT"
    
    # Set up test environment
    export DATABASE_URL="$TEST_DATABASE_URL"
    export RUST_LOG="warn" # Reduce logging for performance tests
    export SINEX_PREFLIGHT_BINARY="$TARGET_DIR/release/sinex-preflight"
    
    # Run performance benchmarks
    run_with_timeout \
        "cargo test --release --test '*' test_performance" \
        "Performance benchmark tests"
    
    # Run verification performance test
    print_section "Direct Verification Performance Test"
    
    local start_time=$(date +%s%N)
    
    if "$TARGET_DIR/release/sinex-preflight" verify --timeout 120 --output json >/dev/null; then
        local end_time=$(date +%s%N)
        local duration_ms=$(( (end_time - start_time) / 1000000 ))
        print_success "Direct verification completed in ${duration_ms}ms"
        
        if [ $duration_ms -lt 30000 ]; then
            print_success "Performance is excellent (< 30s)"
        elif [ $duration_ms -lt 60000 ]; then
            print_success "Performance is good (< 60s)"
        else
            print_warning "Performance is acceptable but slow (> 60s)"
        fi
    else
        print_error "Direct verification failed"
        return 1
    fi
}

# Function to run stress tests
run_stress_tests() {
    print_section "Running Stress Tests"
    
    cd "$PROJECT_ROOT"
    
    # Set up test environment
    export DATABASE_URL="$TEST_DATABASE_URL"
    export RUST_LOG="warn"
    export SINEX_PREFLIGHT_BINARY="$TARGET_DIR/release/sinex-preflight"
    
    # Run concurrent verification stress test
    print_section "Concurrent Verification Stress Test"
    
    local concurrent_count=5
    local pids=()
    
    echo "Starting $concurrent_count concurrent verifications..."
    
    for i in $(seq 1 $concurrent_count); do
        (
            if "$TARGET_DIR/release/sinex-preflight" verify --timeout 90 --output json >/tmp/stress_test_$i.json 2>&1; then
                echo "Stress test $i: SUCCESS"
            else
                echo "Stress test $i: FAILED"
                exit 1
            fi
        ) &
        pids+=($!)
    done
    
    # Wait for all concurrent tests
    local failed_count=0
    for pid in "${pids[@]}"; do
        if ! wait "$pid"; then
            ((failed_count++))
        fi
    done
    
    # Clean up
    rm -f /tmp/stress_test_*.json
    
    if [ $failed_count -eq 0 ]; then
        print_success "All $concurrent_count concurrent verifications succeeded"
    else
        print_error "$failed_count out of $concurrent_count concurrent verifications failed"
        return 1
    fi
}

# Function to generate test report
generate_test_report() {
    print_section "Generating Test Report"
    
    local report_file="$PROJECT_ROOT/preflight-test-report.json"
    
    # Run a verification and capture detailed output
    if "$TARGET_DIR/release/sinex-preflight" verify --timeout 120 --output json > "$report_file" 2>/dev/null; then
        print_success "Test report generated: $report_file"
        
        # Extract key metrics
        local overall_status=$(jq -r '.overall_status' "$report_file" 2>/dev/null || echo "unknown")
        local duration_ms=$(jq -r '.duration_ms' "$report_file" 2>/dev/null || echo "unknown")
        local phase_count=$(jq -r '.phases | length' "$report_file" 2>/dev/null || echo "unknown")
        
        echo "  Overall Status: $overall_status"
        echo "  Duration: ${duration_ms}ms"
        echo "  Phases Tested: $phase_count"
        
        # Show phase results
        echo "  Phase Results:"
        if command -v jq >/dev/null 2>&1; then
            jq -r '.phases | to_entries[] | "    \(.key): \(.value.status)"' "$report_file" 2>/dev/null || true
        fi
    else
        print_error "Failed to generate test report"
        return 1
    fi
}

# Function to run all tests
run_all_tests() {
    local start_time=$(date +%s)
    local failed_tests=()
    
    echo "Starting comprehensive test suite..."
    echo ""
    
    # Run each test category, collecting failures
    check_prerequisites || failed_tests+=("prerequisites")
    build_project || failed_tests+=("build")
    run_unit_tests || failed_tests+=("unit")
    run_integration_tests || failed_tests+=("integration") 
    run_system_tests || failed_tests+=("system")
    run_vm_tests || failed_tests+=("vm")
    run_performance_tests || failed_tests+=("performance")
    run_stress_tests || failed_tests+=("stress")
    generate_test_report || failed_tests+=("report")
    
    local end_time=$(date +%s)
    local total_duration=$((end_time - start_time))
    
    print_section "Test Suite Summary"
    echo "Total Duration: ${total_duration}s"
    echo ""
    
    if [ ${#failed_tests[@]} -eq 0 ]; then
        print_success "🎉 ALL TESTS PASSED!"
        echo ""
        echo "The Sinex Pre-Flight Verification system is ready for deployment."
        echo "All verification phases, integration tests, and stress tests completed successfully."
        return 0
    else
        print_error "❌ SOME TESTS FAILED!"
        echo ""
        echo "Failed test categories:"
        for test in "${failed_tests[@]}"; do
            echo "  - $test"
        done
        echo ""
        echo "Please review the failed tests and fix any issues before deployment."
        return 1
    fi
}

# Main execution
main() {
    case "${1:-all}" in
        "prereq"|"prerequisites")
            check_prerequisites
            ;;
        "build")
            build_project
            ;;
        "unit")
            build_project
            run_unit_tests
            ;;
        "integration")
            build_project
            run_integration_tests
            ;;
        "system")
            build_project
            run_system_tests
            ;;
        "vm")
            build_project
            run_vm_tests
            ;;
        "performance"|"perf")
            build_project
            run_performance_tests
            ;;
        "stress")
            build_project
            run_stress_tests
            ;;
        "report")
            build_project
            generate_test_report
            ;;
        "all")
            run_all_tests
            ;;
        "help"|"-h"|"--help")
            echo "Usage: $0 [test-category]"
            echo ""
            echo "Test categories:"
            echo "  prereq       Check prerequisites only"
            echo "  build        Build project only"
            echo "  unit         Run unit tests"
            echo "  integration  Run integration tests"
            echo "  system       Run system tests"
            echo "  vm           Run NixOS VM tests"
            echo "  performance  Run performance tests"
            echo "  stress       Run stress tests"
            echo "  report       Generate test report"
            echo "  all          Run all tests (default)"
            echo ""
            echo "Environment variables:"
            echo "  TIMEOUT_SECS        Test timeout in seconds (default: 300)"
            echo "  PARALLEL_JOBS       Number of parallel test jobs (default: nproc)"
            echo "  TEST_DATABASE_URL   Test database URL"
            echo "  VERBOSE             Enable verbose output (true/false)"
            ;;
        *)
            print_error "Unknown test category: $1"
            echo "Run '$0 help' for usage information."
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"
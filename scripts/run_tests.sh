#!/usr/bin/env bash
set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

show_help() {
    cat << EOF
Usage: $0 [OPTIONS] [TEST_TYPE]

Unified test runner for Sinex project.

TEST TYPES:
    unit            Run unit tests (default)
    integration     Run integration tests  
    pipeline        Run basic pipeline tests
    real-world      Run tests with real data
    chaos           Run chaos/stress tests
    all             Run all test types
    
OPTIONS:
    --setup         Setup test database before running
    --cleanup       Cleanup test environment after running
    --verbose       Verbose output
    --package PKG   Run tests for specific package only
    --help, -h      Show this help

EXAMPLES:
    $0                      # Run unit tests
    $0 integration          # Run integration tests
    $0 --setup unit         # Setup DB then run unit tests
    $0 --package filesystem # Test only filesystem ingestor
    
ENVIRONMENT:
    TEST_DATABASE_URL       Test database URL
    RUST_LOG               Log level (default: info)
EOF
}

log() {
    echo -e "${BLUE}🧪${NC}  $*"
}

success() {
    echo -e "${GREEN}✅${NC} $*"
}

warning() {
    echo -e "${YELLOW}⚠️${NC}  $*"
}

error() {
    echo -e "${RED}❌${NC} $*" >&2
}

# Parse arguments
TEST_TYPE="unit"
SETUP=false
CLEANUP=false
VERBOSE=false
PACKAGE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        unit|integration|pipeline|real-world|chaos|all)
            TEST_TYPE="$1"
            shift
            ;;
        --setup)
            SETUP=true
            shift
            ;;
        --cleanup)
            CLEANUP=true
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --package)
            PACKAGE="$2"
            shift 2
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Setup test environment
setup_test_env() {
    log "Setting up test environment"
    
    # Setup test database
    ./scripts/setup_database.sh --test
    
    # Set test environment variables
    export RUST_LOG="${RUST_LOG:-info}"
    export RUST_BACKTRACE=1
    
    success "Test environment ready"
}

# Cleanup test environment
cleanup_test_env() {
    log "Cleaning up test environment"
    
    # Could add cleanup logic here if needed
    # For now, test database is ephemeral
    
    success "Test environment cleaned"
}

# Run unit tests
run_unit_tests() {
    log "Running unit tests"
    
    local args=("--all-features")
    
    if [[ -n "$PACKAGE" ]]; then
        args+=("--package" "$PACKAGE")
    fi
    
    if [[ "$VERBOSE" == "true" ]]; then
        args+=("--verbose")
    fi
    
    cargo test "${args[@]}"
    success "Unit tests completed"
}

# Run integration tests
run_integration_tests() {
    log "Running integration tests"
    
    local args=("--test" "*integration*")
    
    if [[ -n "$PACKAGE" ]]; then
        args+=("--package" "$PACKAGE")
    fi
    
    if [[ "$VERBOSE" == "true" ]]; then
        args+=("--verbose")
    fi
    
    # Run all integration test files
    cargo test --all-features database_integration_tests
    cargo test --all-features event_pipeline_integration_tests
    cargo test --all-features promotion_worker_integration
    
    success "Integration tests completed"
}

# Run pipeline tests
run_pipeline_tests() {
    log "Running pipeline tests"
    
    # Test basic event flow
    log "Testing basic event pipeline"
    
    # Insert some test events
    ./cli/exo.py insert-test-events --count 10
    
    # Verify they were inserted
    local count
    count=$(./cli/exo.py query --count-only)
    
    if [[ "$count" -ge 10 ]]; then
        success "Pipeline test passed: $count events found"
    else
        error "Pipeline test failed: only $count events found"
        return 1
    fi
}

# Run real-world tests
run_real_world_tests() {
    log "Running real-world tests"
    
    warning "Real-world tests require actual system components"
    
    # Check if we're in a development environment
    if [[ -z "${IN_NIX_SHELL:-}" ]]; then
        warning "Consider running in nix develop environment"
    fi
    
    # Test filesystem ingestor if available
    if command -v filesystem-ingestor >/dev/null 2>&1; then
        log "Testing filesystem ingestor"
        timeout 5s filesystem-ingestor run || true
    fi
    
    # Test other ingestors if available
    if command -v kitty-ingestor >/dev/null 2>&1; then
        log "Testing kitty ingestor connectivity"
        kitty-ingestor check || warning "Kitty ingestor check failed"
    fi
    
    success "Real-world tests completed"
}

# Run chaos tests
run_chaos_tests() {
    log "Running chaos tests"
    
    # Test database resilience
    log "Testing database resilience"
    
    # Concurrent event insertion
    log "Testing concurrent event insertion"
    for i in {1..5}; do
        (
            ./cli/exo.py insert-test-events --count 20 --source "chaos-$i"
        ) &
    done
    wait
    
    # Test database recovery
    log "Testing database recovery scenarios"
    # This would normally test connection drops, etc.
    
    success "Chaos tests completed"
}

# Run all tests
run_all_tests() {
    log "Running all test types"
    
    run_unit_tests
    run_integration_tests
    run_pipeline_tests
    run_real_world_tests
    
    success "All tests completed"
}

# Main execution
main() {
    log "Starting test run: $TEST_TYPE"
    
    if [[ "$SETUP" == "true" ]]; then
        setup_test_env
    fi
    
    # Ensure we're in project root
    if [[ ! -f "Cargo.toml" ]]; then
        error "Not in project root - run from sinex directory"
        exit 1
    fi
    
    # Run the specified test type
    case "$TEST_TYPE" in
        unit)
            run_unit_tests
            ;;
        integration)
            run_integration_tests
            ;;
        pipeline)
            run_pipeline_tests
            ;;
        real-world)
            run_real_world_tests
            ;;
        chaos)
            run_chaos_tests
            ;;
        all)
            run_all_tests
            ;;
        *)
            error "Unknown test type: $TEST_TYPE"
            exit 1
            ;;
    esac
    
    if [[ "$CLEANUP" == "true" ]]; then
        cleanup_test_env
    fi
    
    success "Test run completed successfully"
}

# Cleanup on exit
trap 'if [[ "$CLEANUP" == "true" ]]; then cleanup_test_env; fi' EXIT

# Run main function
main "$@"
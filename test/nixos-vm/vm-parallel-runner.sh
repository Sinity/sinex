#!/usr/bin/env bash
# Parallel VM Test Runner with Snapshot Support
# Agent Alpha - VM Infrastructure

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SNAPSHOT_MANAGER="${SCRIPT_DIR}/vm-snapshot-manager.sh"

# Configuration
MAX_PARALLEL_VMS="${MAX_PARALLEL_VMS:-10}"
VM_MEMORY="${VM_MEMORY:-2048}"
VM_TIMEOUT="${VM_TIMEOUT:-600}"
TEST_RESULTS_DIR="${TEST_RESULTS_DIR:-./parallel-test-results}"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo -e "${BLUE}[$(date '+%H:%M:%S')] PARALLEL:${NC} $*"
}

error() {
    echo -e "${RED}[ERROR]${NC} $*" >&2
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $*"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

# Job control for parallel VMs
declare -A VM_JOBS=()
declare -A VM_PIDS=()
declare -A VM_TESTS=()

# Initialize parallel test environment
init_parallel_env() {
    mkdir -p "$TEST_RESULTS_DIR"
    log "Initializing parallel test environment"
    log "Max parallel VMs: $MAX_PARALLEL_VMS"
    log "VM memory: ${VM_MEMORY}MB"
    log "Test timeout: ${VM_TIMEOUT}s"
}

# Check system resources for parallel execution
check_resources() {
    local total_mem_kb=$(grep MemTotal /proc/meminfo | awk '{print $2}')
    local total_mem_mb=$((total_mem_kb / 1024))
    local required_mem=$((MAX_PARALLEL_VMS * VM_MEMORY))
    
    if [[ $required_mem -gt $total_mem_mb ]]; then
        warn "High memory usage: ${required_mem}MB required, ${total_mem_mb}MB available"
        warn "Consider reducing MAX_PARALLEL_VMS or VM_MEMORY"
    fi
    
    log "System memory: ${total_mem_mb}MB, Required: ${required_mem}MB"
    return 0
}

# Start a single VM test
start_vm_test() {
    local test_name="$1"
    local vm_id="$2"
    local base_snapshot="${3:-basic-flow-ready}"
    
    local vm_clone_path
    vm_clone_path=$("$SNAPSHOT_MANAGER" clone "$base_snapshot" "$vm_id")
    
    if [[ -z "$vm_clone_path" ]]; then
        error "Failed to clone VM for test $test_name"
        return 1
    fi
    
    log "Starting VM test: $test_name (VM ID: $vm_id)"
    
    # Run test in background
    {
        local test_log="$TEST_RESULTS_DIR/${test_name}-${vm_id}.log"
        local test_result="$TEST_RESULTS_DIR/${test_name}-${vm_id}.result"
        local start_time=$(date +%s)
        
        # Run the actual test with timeout
        if timeout "$VM_TIMEOUT" run_single_vm_test "$test_name" "$vm_clone_path" > "$test_log" 2>&1; then
            local end_time=$(date +%s)
            local duration=$((end_time - start_time))
            
            echo "PASSED" > "$test_result"
            echo "Duration: ${duration}s" >> "$test_result"
            echo "VM ID: $vm_id" >> "$test_result"
            
            success "Test $test_name completed (${duration}s, VM: $vm_id)"
        else
            local exit_code=$?
            local end_time=$(date +%s)
            local duration=$((end_time - start_time))
            
            echo "FAILED" > "$test_result"
            echo "Exit code: $exit_code" >> "$test_result"
            echo "Duration: ${duration}s" >> "$test_result"
            echo "VM ID: $vm_id" >> "$test_result"
            
            error "Test $test_name failed (exit code: $exit_code, duration: ${duration}s, VM: $vm_id)"
        fi
        
        # Cleanup VM
        "$SNAPSHOT_MANAGER" cleanup "$vm_id"
        
    } &
    
    local job_pid=$!
    VM_JOBS["$vm_id"]="$job_pid"
    VM_PIDS["$job_pid"]="$vm_id"
    VM_TESTS["$vm_id"]="$test_name"
    
    log "Started job $job_pid for test $test_name (VM: $vm_id)"
}

# Run actual test inside VM
run_single_vm_test() {
    local test_name="$1"
    local vm_image="$2"
    
    # This would be enhanced to run specific tests
    # For now, use nix-build approach but with our cloned image
    log "Running test $test_name with VM image $vm_image"
    
    # Build the test with our custom VM image
    nix-build "${SCRIPT_DIR}/test-scenarios/${test_name}.nix" \
        --arg customDiskImage "\"$vm_image\"" \
        --arg vmProfile '"standard"' \
        -o "$TEST_RESULTS_DIR/result-${test_name}-$$"
}

# Wait for VM jobs and collect results
wait_for_jobs() {
    local running_jobs=()
    
    # Get list of running jobs
    for pid in "${!VM_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            running_jobs+=("$pid")
        fi
    done
    
    if [[ ${#running_jobs[@]} -eq 0 ]]; then
        return 0
    fi
    
    log "Waiting for ${#running_jobs[@]} running jobs..."
    
    # Wait for any job to complete
    wait -n "${running_jobs[@]}" 2>/dev/null || true
    
    # Clean up completed jobs
    for pid in "${!VM_PIDS[@]}"; do
        if ! kill -0 "$pid" 2>/dev/null; then
            local vm_id="${VM_PIDS[$pid]}"
            local test_name="${VM_TESTS[$vm_id]}"
            
            log "Job $pid completed (test: $test_name, VM: $vm_id)"
            
            unset VM_JOBS["$vm_id"]
            unset VM_PIDS["$pid"]
            unset VM_TESTS["$vm_id"]
        fi
    done
}

# Get number of active VMs
active_vm_count() {
    echo "${#VM_JOBS[@]}"
}

# Run multiple tests in parallel
run_parallel_tests() {
    local tests=("$@")
    local vm_counter=1
    local completed=0
    local total=${#tests[@]}
    
    log "Running $total tests with up to $MAX_PARALLEL_VMS parallel VMs"
    
    # Start initial batch of tests
    for test in "${tests[@]}"; do
        # Wait if we're at the limit
        while [[ $(active_vm_count) -ge $MAX_PARALLEL_VMS ]]; do
            wait_for_jobs
            sleep 1
        done
        
        start_vm_test "$test" "vm-$(printf "%03d" $vm_counter)"
        ((vm_counter++))
        
        # Brief pause to avoid overwhelming the system
        sleep 2
    done
    
    # Wait for all remaining jobs
    while [[ $(active_vm_count) -gt 0 ]]; do
        wait_for_jobs
        sleep 1
    done
    
    success "All $total tests completed"
}

# Generate parallel test report
generate_parallel_report() {
    local report_file="$TEST_RESULTS_DIR/parallel-summary.txt"
    local passed=0
    local failed=0
    local total=0
    local total_duration=0
    
    echo "Parallel VM Test Report" > "$report_file"
    echo "======================" >> "$report_file"
    echo "Generated: $(date)" >> "$report_file"
    echo "Max parallel VMs: $MAX_PARALLEL_VMS" >> "$report_file"
    echo "" >> "$report_file"
    
    for result_file in "$TEST_RESULTS_DIR"/*.result; do
        if [[ ! -f "$result_file" ]]; then
            continue
        fi
        
        local test_name=$(basename "$result_file" .result)
        local status=$(head -1 "$result_file")
        local duration=$(grep "Duration:" "$result_file" | cut -d' ' -f2 | sed 's/s//')
        local vm_id=$(grep "VM ID:" "$result_file" | cut -d' ' -f3)
        
        ((total++))
        total_duration=$((total_duration + duration))
        
        if [[ "$status" == "PASSED" ]]; then
            ((passed++))
            echo "✓ $test_name - PASSED (${duration}s, VM: $vm_id)" >> "$report_file"
        else
            ((failed++))
            echo "✗ $test_name - FAILED (${duration}s, VM: $vm_id)" >> "$report_file"
        fi
    done
    
    echo "" >> "$report_file"
    echo "Summary:" >> "$report_file"
    echo "Total tests: $total" >> "$report_file"
    echo "Passed: $passed" >> "$report_file"
    echo "Failed: $failed" >> "$report_file"
    echo "Success rate: $(( total > 0 ? passed * 100 / total : 0 ))%" >> "$report_file"
    echo "Total duration: ${total_duration}s" >> "$report_file"
    echo "Average duration: $(( total > 0 ? total_duration / total : 0 ))s" >> "$report_file"
    
    # Estimate time savings vs sequential
    local sequential_time=$total_duration
    local actual_time=$(( total_duration / MAX_PARALLEL_VMS + 60 )) # rough estimate
    echo "Est. sequential time: ${sequential_time}s" >> "$report_file"
    echo "Est. parallel time: ${actual_time}s" >> "$report_file"
    echo "Time savings: $(( sequential_time > actual_time ? (sequential_time - actual_time) * 100 / sequential_time : 0 ))%" >> "$report_file"
    
    cat "$report_file"
    
    # Return failure if any tests failed
    [[ $failed -eq 0 ]]
}

# Usage information
usage() {
    cat << EOF
Parallel VM Test Runner with Snapshot Support

Usage: $0 [OPTIONS] <test1> [test2] [test3] ...

OPTIONS:
    -h, --help              Show this help
    -p, --parallel N        Max parallel VMs (default: $MAX_PARALLEL_VMS)
    -m, --memory MB         Memory per VM (default: $VM_MEMORY)
    -t, --timeout SEC       Test timeout (default: $VM_TIMEOUT)
    -o, --output DIR        Results directory (default: $TEST_RESULTS_DIR)
    -s, --snapshot NAME     Base snapshot to use (default: basic-flow-ready)

EXAMPLES:
    # Run basic tests with 5 parallel VMs
    $0 -p 5 basic-flow multi-source performance
    
    # Run with custom memory and timeout
    $0 -m 4096 -t 900 chaos-engineering
    
    # Use specific snapshot
    $0 -s after-db-init basic-flow

ENVIRONMENT:
    MAX_PARALLEL_VMS        Maximum parallel VMs
    VM_MEMORY              Memory per VM in MB
    VM_TIMEOUT             Test timeout in seconds
EOF
}

# Parse command line arguments
parse_args() {
    local tests=()
    local base_snapshot="basic-flow-ready"
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                usage
                exit 0
                ;;
            -p|--parallel)
                MAX_PARALLEL_VMS="$2"
                shift 2
                ;;
            -m|--memory)
                VM_MEMORY="$2"
                shift 2
                ;;
            -t|--timeout)
                VM_TIMEOUT="$2"
                shift 2
                ;;
            -o|--output)
                TEST_RESULTS_DIR="$2"
                shift 2
                ;;
            -s|--snapshot)
                base_snapshot="$2"
                shift 2
                ;;
            -*)
                error "Unknown option: $1"
                usage
                exit 1
                ;;
            *)
                tests+=("$1")
                shift
                ;;
        esac
    done
    
    if [[ ${#tests[@]} -eq 0 ]]; then
        error "No tests specified"
        usage
        exit 1
    fi
    
    # Export for use in functions
    export BASE_SNAPSHOT="$base_snapshot"
    echo "${tests[@]}"
}

# Main execution
main() {
    local tests
    tests=($(parse_args "$@"))
    
    init_parallel_env
    check_resources
    
    # Ensure snapshot manager is available
    if [[ ! -x "$SNAPSHOT_MANAGER" ]]; then
        error "Snapshot manager not found or not executable: $SNAPSHOT_MANAGER"
        exit 1
    fi
    
    log "Starting parallel VM tests..."
    run_parallel_tests "${tests[@]}"
    
    echo ""
    generate_parallel_report
}

# Run main if called directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
#!/usr/bin/env bash

# Test Coverage Verification Script
# Ensures all test scenarios are preserved during refactoring

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORIGINAL_INVENTORY="${SCRIPT_DIR}/original_test_inventory.txt"
CURRENT_INVENTORY="${SCRIPT_DIR}/current_test_inventory.txt"
COVERAGE_REPORT="${SCRIPT_DIR}/coverage_verification_report.txt"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[$(date '+%H:%M:%S')] $*${NC}"
}

warn() {
    echo -e "${YELLOW}[WARNING] $*${NC}"
}

error() {
    echo -e "${RED}[ERROR] $*${NC}"
}

success() {
    echo -e "${GREEN}[SUCCESS] $*${NC}"
}

# Extract test function names from Rust files
extract_test_functions() {
    local search_dir="$1"
    
    # Find all test functions using various patterns
    find "$search_dir" -name "*.rs" -exec grep -H -n \
        -E "(#\[test\]|#\[tokio::test\]|#\[sinex_test\]|#\[cfg\(test\)\].*fn test_|^[[:space:]]*fn test_)" {} \; | \
        # Extract function names
        sed -E 's/.*fn ([a-zA-Z_][a-zA-Z0-9_]*)\s*\(.*/\1/' | \
        # Remove duplicates and sort
        sort -u | \
        # Filter to only actual test functions
        grep -E '^test_|_test$|test[A-Z]' || true
}

# Generate test inventory
generate_inventory() {
    local output_file="$1"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    log "Generating test inventory..."
    
    {
        echo "# Test Function Inventory"
        echo "# Generated: $timestamp"
        echo "# Total files analyzed: $(find test/ -name "*.rs" | wc -l)"
        echo ""
        
        # Extract all test functions
        extract_test_functions "test/"
        
    } > "$output_file"
    
    local count=$(grep -c '^test_\|_test$\|test[A-Z]' "$output_file" || echo "0")
    log "Found $count test functions"
}

# Compare inventories and generate report
compare_inventories() {
    if [[ ! -f "$ORIGINAL_INVENTORY" ]]; then
        error "Original inventory not found at $ORIGINAL_INVENTORY"
        error "Run: $0 --generate-baseline"
        return 1
    fi
    
    if [[ ! -f "$CURRENT_INVENTORY" ]]; then
        error "Current inventory not found at $CURRENT_INVENTORY"
        error "Run: $0 --generate-current"
        return 1
    fi
    
    log "Comparing test inventories..."
    
    # Extract just the test function names (skip comments)
    local original_tests=$(grep -v '^#' "$ORIGINAL_INVENTORY" | grep -v '^$' | sort)
    local current_tests=$(grep -v '^#' "$CURRENT_INVENTORY" | grep -v '^$' | sort)
    
    # Count tests
    local original_count=$(echo "$original_tests" | wc -l)
    local current_count=$(echo "$current_tests" | wc -l)
    
    # Find missing and new tests
    local missing_tests=$(comm -23 <(echo "$original_tests") <(echo "$current_tests"))
    local new_tests=$(comm -13 <(echo "$original_tests") <(echo "$current_tests"))
    local missing_count=$(echo "$missing_tests" | grep -c . || echo "0")
    local new_count=$(echo "$new_tests" | grep -c . || echo "0")
    
    # Generate report
    {
        echo "# Test Coverage Verification Report"
        echo "# Generated: $(date '+%Y-%m-%d %H:%M:%S')"
        echo ""
        echo "## Summary"
        echo "- Original test count: $original_count"
        echo "- Current test count: $current_count"
        echo "- Missing tests: $missing_count"
        echo "- New tests: $new_count"
        echo ""
        
        if [[ $missing_count -gt 0 ]]; then
            echo "## Missing Tests (CRITICAL)"
            echo "The following tests were found in the original inventory but not in the current codebase:"
            echo ""
            echo "$missing_tests"
            echo ""
        fi
        
        if [[ $new_count -gt 0 ]]; then
            echo "## New Tests"
            echo "The following tests are new or renamed:"
            echo ""
            echo "$new_tests"
            echo ""
        fi
        
        if [[ $missing_count -eq 0 ]]; then
            echo "## ✅ Coverage Status: PASSED"
            echo "All original tests are accounted for in the current codebase."
        else
            echo "## ❌ Coverage Status: FAILED"
            echo "Some original tests are missing from the current codebase."
        fi
        
    } > "$COVERAGE_REPORT"
    
    # Display summary
    log "Coverage verification complete"
    echo ""
    echo "=== COVERAGE VERIFICATION SUMMARY ==="
    echo "Original tests: $original_count"
    echo "Current tests: $current_count"
    echo "Missing tests: $missing_count"
    echo "New tests: $new_count"
    echo ""
    
    if [[ $missing_count -eq 0 ]]; then
        success "✅ All tests accounted for!"
        return 0
    else
        error "❌ $missing_count tests are missing!"
        echo ""
        warn "Missing tests:"
        echo "$missing_tests"
        echo ""
        warn "See full report at: $COVERAGE_REPORT"
        return 1
    fi
}

# Analyze test patterns for consolidation guidance
analyze_patterns() {
    log "Analyzing test patterns..."
    
    local pattern_report="${SCRIPT_DIR}/test_pattern_analysis.txt"
    
    {
        echo "# Test Pattern Analysis"
        echo "# Generated: $(date '+%Y-%m-%d %H:%M:%S')"
        echo ""
        
        echo "## Test File Distribution"
        echo "```"
        find test/ -name "*.rs" | grep -E "(unit|integration|system|adversarial|property)" | \
            cut -d'/' -f2 | sort | uniq -c | sort -nr
        echo "```"
        echo ""
        
        echo "## Common Test Prefixes"
        echo "```"
        extract_test_functions "test/" | \
            sed -E 's/^test_([a-zA-Z_]+)_.*/\1/' | \
            sort | uniq -c | sort -nr | head -20
        echo "```"
        echo ""
        
        echo "## Files with Single Tests (Consolidation Candidates)"
        echo "```"
        while IFS= read -r file; do
            local test_count=$(grep -c -E "(#\[test\]|#\[tokio::test\]|#\[sinex_test\])" "$file" || echo "0")
            if [[ $test_count -eq 1 ]]; then
                echo "$file (1 test)"
            elif [[ $test_count -lt 3 ]]; then
                echo "$file ($test_count tests)"
            fi
        done < <(find test/ -name "*.rs" -not -path "*/common/*" | sort)
        echo "```"
        echo ""
        
        echo "## Database Test Files"
        echo "```"
        find test/ -name "*.rs" | xargs grep -l "database\|TestPool\|DbPool" | sort
        echo "```"
        echo ""
        
    } > "$pattern_report"
    
    log "Pattern analysis saved to: $pattern_report"
}

# Show usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Test Coverage Verification Script for Sinex Test Suite Refactoring

OPTIONS:
    --generate-baseline     Generate baseline inventory from current codebase
    --generate-current      Generate current inventory from current codebase
    --verify               Compare inventories and generate coverage report
    --analyze              Analyze test patterns for consolidation guidance
    --help                 Show this help message

TYPICAL WORKFLOW:
    1. Before refactoring: $0 --generate-baseline
    2. After refactoring:  $0 --generate-current
    3. Verify coverage:    $0 --verify
    4. Analyze patterns:   $0 --analyze

FILES:
    - $ORIGINAL_INVENTORY: Baseline test inventory
    - $CURRENT_INVENTORY: Current test inventory
    - $COVERAGE_REPORT: Coverage verification report

EOF
}

# Main execution
main() {
    case "${1:-}" in
        --generate-baseline)
            generate_inventory "$ORIGINAL_INVENTORY"
            success "Baseline inventory generated at: $ORIGINAL_INVENTORY"
            ;;
        --generate-current)
            generate_inventory "$CURRENT_INVENTORY"
            success "Current inventory generated at: $CURRENT_INVENTORY"
            ;;
        --verify)
            generate_inventory "$CURRENT_INVENTORY"
            compare_inventories
            ;;
        --analyze)
            analyze_patterns
            ;;
        --help)
            usage
            ;;
        *)
            if [[ $# -eq 0 ]]; then
                log "Running full coverage verification..."
                generate_inventory "$CURRENT_INVENTORY"
                compare_inventories
                analyze_patterns
            else
                error "Unknown option: $1"
                usage
                exit 1
            fi
            ;;
    esac
}

# Execute main function
main "$@"
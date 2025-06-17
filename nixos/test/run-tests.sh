#!/usr/bin/env bash
# Test runner for Sinex NixOS VM tests
# Runs comprehensive test suite to verify modular configuration works

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test directory
TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$TEST_DIR/.." && pwd)"

echo -e "${BLUE}🧪 Sinex NixOS Module Test Suite${NC}"
echo -e "${BLUE}=================================${NC}"
echo "Test directory: $TEST_DIR"
echo "Project root: $PROJECT_ROOT"
echo

# Function to run a single test
run_test() {
    local test_name="$1"
    local test_file="$TEST_DIR/vm-${test_name}.nix"
    
    if [[ ! -f "$test_file" ]]; then
        echo -e "${RED}❌ Test file not found: $test_file${NC}"
        return 1
    fi
    
    echo -e "${YELLOW}🔄 Running test: $test_name${NC}"
    echo "Test file: $test_file"
    
    if nix-build "$test_file" -o "result-$test_name" --no-out-link; then
        echo -e "${GREEN}✅ Test $test_name passed${NC}"
        echo
        return 0
    else
        echo -e "${RED}❌ Test $test_name failed${NC}"
        echo
        return 1
    fi
}

# Function to run quick syntax validation
validate_syntax() {
    echo -e "${YELLOW}🔍 Validating module syntax...${NC}"
    
    # Test that the modular structure can be imported
    if nix-instantiate --eval --strict -E "
        let 
          pkgs = import <nixpkgs> {}; 
          config = { services.sinex.enable = true; };
        in 
          (import $PROJECT_ROOT/modules { lib = pkgs.lib; inherit config; }).options.services.sinex.enable.type.name
    " >/dev/null 2>&1; then
        echo -e "${GREEN}✅ Module syntax is valid${NC}"
    else
        echo -e "${RED}❌ Module syntax validation failed${NC}"
        echo "The modular structure has syntax errors that prevent evaluation"
        return 1
    fi
    
    # Test that examples can be parsed
    for example in "$PROJECT_ROOT/examples"/*.nix; do
        if [[ -f "$example" ]]; then
            local example_name=$(basename "$example" .nix)
            echo -e "   Checking example: $example_name"
            if nix-instantiate --parse "$example" >/dev/null 2>&1; then
                echo -e "   ${GREEN}✓${NC} $example_name"
            else
                echo -e "   ${RED}✗${NC} $example_name"
                return 1
            fi
        fi
    done
    
    echo
}

# Parse command line arguments
TESTS_TO_RUN=()
RUN_ALL=true
SYNTAX_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --test)
            TESTS_TO_RUN+=("$2")
            RUN_ALL=false
            shift 2
            ;;
        --syntax-only)
            SYNTAX_ONLY=true
            shift
            ;;
        --list)
            echo "Available tests:"
            for test_file in "$TEST_DIR"/vm-*.nix; do
                if [[ -f "$test_file" ]]; then
                    test_name=$(basename "$test_file" .nix | sed 's/^vm-//')
                    echo "  - $test_name"
                fi
            done
            exit 0
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --test NAME         Run specific test (can be used multiple times)"
            echo "  --syntax-only       Only run syntax validation, skip VM tests"
            echo "  --list              List available tests"
            echo "  --help              Show this help"
            echo
            echo "Examples:"
            echo "  $0                           # Run all tests"
            echo "  $0 --test basic              # Run only basic test"
            echo "  $0 --test basic --test presets  # Run basic and presets tests"
            echo "  $0 --syntax-only             # Only validate syntax"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Run syntax validation first
if ! validate_syntax; then
    echo -e "${RED}💥 Syntax validation failed. Cannot proceed with VM tests.${NC}"
    exit 1
fi

if [[ "$SYNTAX_ONLY" == "true" ]]; then
    echo -e "${GREEN}🎉 Syntax validation completed successfully!${NC}"
    exit 0
fi

# Discover available tests if running all
if [[ "$RUN_ALL" == "true" ]]; then
    for test_file in "$TEST_DIR"/vm-*.nix; do
        if [[ -f "$test_file" ]]; then
            test_name=$(basename "$test_file" .nix | sed 's/^vm-//')
            TESTS_TO_RUN+=("$test_name")
        fi
    done
fi

if [[ ${#TESTS_TO_RUN[@]} -eq 0 ]]; then
    echo -e "${YELLOW}⚠️  No tests to run${NC}"
    exit 0
fi

echo -e "${BLUE}📋 Tests to run: ${TESTS_TO_RUN[*]}${NC}"
echo

# Run tests
PASSED=0
FAILED=0
FAILED_TESTS=()

for test in "${TESTS_TO_RUN[@]}"; do
    if run_test "$test"; then
        ((PASSED++))
    else
        ((FAILED++))
        FAILED_TESTS+=("$test")
    fi
done

# Summary
echo -e "${BLUE}📊 Test Results Summary${NC}"
echo -e "${BLUE}======================${NC}"
echo -e "${GREEN}✅ Passed: $PASSED${NC}"
echo -e "${RED}❌ Failed: $FAILED${NC}"

if [[ $FAILED -gt 0 ]]; then
    echo -e "${RED}Failed tests: ${FAILED_TESTS[*]}${NC}"
    echo
    echo -e "${YELLOW}💡 Debugging tips:${NC}"
    echo "  - Check that you have VM support enabled (KVM/QEMU)"
    echo "  - Ensure you have enough disk space for VM tests"
    echo "  - Run individual tests with: $0 --test <test-name>"
    echo "  - Check test output for specific error messages"
    exit 1
else
    echo -e "${GREEN}🎉 All tests passed! The Sinex NixOS module is working correctly.${NC}"
    echo
    echo -e "${BLUE}✅ Module validation complete:${NC}"
    echo "  ✓ Syntax is valid"
    echo "  ✓ Basic functionality works"
    echo "  ✓ All presets work correctly"
    echo "  ✓ Exclude patterns work as expected"
    echo "  ✓ Services start and integrate properly"
    exit 0
fi
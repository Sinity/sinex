#!/usr/bin/env bash
# Test Modernization Helper Script
# 
# This script helps identify and modernize test patterns in the Sinex test suite.
# It uses ast-grep for pattern matching and provides automated transformations.

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Ensure we're in the project root
if [[ ! -f "Cargo.toml" ]] || [[ ! -d "test" ]]; then
    echo -e "${RED}Error: Must be run from the sinex project root${NC}"
    exit 1
fi

# Check for required tools
for tool in ast-grep rg sed; do
    if ! command -v "$tool" &> /dev/null; then
        echo -e "${RED}Error: $tool is required but not installed${NC}"
        exit 1
    fi
done

# Function to analyze test patterns
analyze_patterns() {
    echo -e "${GREEN}=== Analyzing Test Patterns ===${NC}"
    
    # Count repetitive test functions
    echo -e "\n${YELLOW}Repetitive test patterns:${NC}"
    rg -t rust "^#\[(sinex_)?test\]" test/ -A 1 | \
        grep "^async fn test_" | \
        sed 's/async fn test_//' | \
        sed 's/(.*$//' | \
        sed 's/_[0-9]+$//' | \
        sort | uniq -c | sort -rn | \
        awk '$1 > 2 {print $1 " similar tests: " $2}'
    
    # Find hardcoded test data
    echo -e "\n${YELLOW}Hardcoded test values (candidates for property testing):${NC}"
    rg -t rust 'assert(_eq)?!\(' test/ | \
        grep -E '(true|false|[0-9]+|"[^"]+")' | \
        head -10
    
    # Find sleep calls
    echo -e "\n${YELLOW}Sleep calls (candidates for smart waiting):${NC}"
    rg -t rust "sleep\(Duration::" test/ -C 1 || echo "None found!"
    
    # Find manual event creation
    echo -e "\n${YELLOW}Manual event creation (candidates for builders):${NC}"
    rg -t rust "RawEvent \{" test/ -A 5 | head -20 || echo "None found!"
}

# Function to generate property test from multiple similar tests
generate_property_test() {
    local pattern="$1"
    local output_file="$2"
    
    echo -e "${GREEN}Generating property test template for pattern: $pattern${NC}"
    
    cat > "$output_file" << 'EOF'
// Property-based test replacing multiple similar tests
sinex_proptest_async! {
    fn PATTERN_properties(
        input in arbitrary_input()
    ) {
        let ctx = TestContext::new().await;
        
        // Test properties that should hold for all inputs
        // TODO: Add specific property assertions here
        
        prop_assert!(true); // Replace with actual properties
    }
}
EOF
    
    sed -i "s/PATTERN/$pattern/g" "$output_file"
    echo -e "${GREEN}Generated template: $output_file${NC}"
}

# Function to convert sleeps to smart waiting
convert_sleeps() {
    echo -e "${GREEN}=== Converting Sleep Calls to Smart Waiting ===${NC}"
    
    # Find files with sleep calls
    local files=$(rg -t rust "sleep\(Duration::" test/ -l)
    
    if [[ -z "$files" ]]; then
        echo "No sleep calls found!"
        return
    fi
    
    for file in $files; do
        echo -e "${YELLOW}Processing: $file${NC}"
        
        # Create ast-grep rule for sleep conversion
        cat > /tmp/sleep-rule.yml << 'EOF'
rule:
  pattern: |
    tokio::time::sleep($DURATION).await;
    $$$AFTER
  fix: |
    // TODO: Replace with appropriate wait condition
    // ctx.wait_for_condition(|| async { /* condition */ }).await?;
    tokio::time::sleep($DURATION).await; // FIXME: Use smart waiting
    $$$AFTER
EOF
        
        ast-grep --rule /tmp/sleep-rule.yml "$file" --fix
    done
}

# Function to identify candidates for test macros
find_macro_candidates() {
    echo -e "${GREEN}=== Finding Test Macro Candidates ===${NC}"
    
    # Look for event insertion patterns
    echo -e "\n${YELLOW}Event insertion patterns:${NC}"
    ast-grep --pattern 'let $ID = sinex_db::insert_event($$$).await?;' test/ || true
    
    # Look for similar assertion patterns
    echo -e "\n${YELLOW}Repeated assertion patterns:${NC}"
    rg -t rust "assert_eq!\(.*\.source," test/ | head -5 || true
    rg -t rust "assert_eq!\(.*\.event_type," test/ | head -5 || true
}

# Function to create batch operation from loop
convert_loops_to_batch() {
    echo -e "${GREEN}=== Converting Loops to Batch Operations ===${NC}"
    
    # Find for loops creating events
    ast-grep --pattern '
for $VAR in $RANGE {
    $$$BODY
}
' test/ --lang rust | grep -B2 -A10 "RawEvent\|EventBuilder" || true
}

# Function to generate modernization report
generate_report() {
    local output="test/MODERNIZATION_OPPORTUNITIES.md"
    
    echo -e "${GREEN}=== Generating Modernization Report ===${NC}"
    
    cat > "$output" << 'EOF'
# Test Modernization Opportunities

Generated: $(date)

## Summary

This report identifies opportunities to modernize the test suite using powerful abstractions.

EOF
    
    # Add analysis results
    {
        echo "## Pattern Analysis"
        echo '```'
        analyze_patterns
        echo '```'
        
        echo -e "\n## Recommendations"
        echo "1. Convert repetitive tests to property-based tests"
        echo "2. Replace sleep calls with condition-based waiting"
        echo "3. Use test macros for common patterns"
        echo "4. Convert manual loops to batch operations"
        
    } >> "$output"
    
    echo -e "${GREEN}Report generated: $output${NC}"
}

# Main menu
main() {
    echo -e "${GREEN}Sinex Test Modernization Helper${NC}"
    echo "================================"
    echo "1. Analyze test patterns"
    echo "2. Generate property test template"
    echo "3. Convert sleeps to smart waiting"
    echo "4. Find test macro candidates"
    echo "5. Convert loops to batch operations"
    echo "6. Generate full report"
    echo "7. Exit"
    
    read -p "Select option: " choice
    
    case $choice in
        1) analyze_patterns ;;
        2) 
            read -p "Pattern name: " pattern
            read -p "Output file: " output
            generate_property_test "$pattern" "$output"
            ;;
        3) convert_sleeps ;;
        4) find_macro_candidates ;;
        5) convert_loops_to_batch ;;
        6) generate_report ;;
        7) exit 0 ;;
        *) echo -e "${RED}Invalid option${NC}" ;;
    esac
    
    echo -e "\n${YELLOW}Press Enter to continue...${NC}"
    read
    main
}

# Run main menu if not sourced
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main
fi
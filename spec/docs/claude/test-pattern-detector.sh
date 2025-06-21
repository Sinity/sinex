#!/usr/bin/env bash
# Test Pattern Detection Script for Streamlining

set -euo pipefail

echo "=== Sinex Test Pattern Analysis ==="
echo

# Function to analyze a test file
analyze_file() {
    local file=$1
    local lines=$(wc -l < "$file")
    
    # Skip if file is too small
    if [ $lines -lt 50 ]; then
        return
    fi
    
    # Count patterns
    local db_setup=$(rg -c "create_test_pool|setup_test_db|PgPool" "$file" 2>/dev/null || echo 0)
    local manual_loops=$(rg -c "for.*in 0\.\.|while.*<" "$file" 2>/dev/null || echo 0)
    local sleeps=$(rg -c "sleep|Duration::from" "$file" 2>/dev/null || echo 0)
    local sql_inserts=$(rg -c "INSERT INTO|insert_event|sqlx::query!" "$file" 2>/dev/null || echo 0)
    local raw_events=$(rg -c "RawEvent.*\{|RawEventBuilder" "$file" 2>/dev/null || echo 0)
    local test_fns=$(rg -c "#\[test\]|#\[tokio::test\]|#\[sqlx::test\]" "$file" 2>/dev/null || echo 0)
    
    # Calculate complexity score
    local complexity=$((db_setup * 10 + manual_loops * 5 + sleeps * 3 + sql_inserts * 4 + raw_events * 2))
    
    # Only report files with significant patterns
    if [ $complexity -gt 20 ]; then
        echo "$file|$lines|$complexity|$db_setup|$manual_loops|$sleeps|$sql_inserts|$raw_events|$test_fns"
    fi
}

# Header
echo "File|Lines|Complexity|DB Setup|Loops|Sleeps|SQL|Events|Tests"
echo "----|-----|----------|--------|-----|------|---|------|-----"

# Find all test files and analyze
find test/ -name "*.rs" -type f | while read -r file; do
    analyze_file "$file"
done | sort -t'|' -k3 -nr | head -50

echo
echo "=== Top Conversion Candidates ==="
echo

# Find files using old patterns but not new utilities
echo "Files NOT using new test utilities:"
find test/ -name "*.rs" -type f | while read -r file; do
    if ! rg -q "EventScenarioBuilder|WorkerScenarioBuilder|TestScenario|parameterized::" "$file" 2>/dev/null; then
        lines=$(wc -l < "$file")
        if [ $lines -gt 200 ]; then
            echo "  - $file ($lines lines)"
        fi
    fi
done | sort -k3 -nr | head -20

echo
echo "=== Pattern Distribution ==="
echo

echo "Database setup patterns: $(rg "create_test_pool|setup_test_db" test/ --type rust | wc -l)"
echo "Manual event creation: $(rg "RawEvent.*\{" test/ --type rust | wc -l)"
echo "Manual loops: $(rg "for.*in 0\.\." test/ --type rust | wc -l)"
echo "Sleep calls: $(rg "sleep.*Duration" test/ --type rust | wc -l)"
echo "Direct SQL: $(rg "INSERT INTO" test/ --type rust | wc -l)"

echo
echo "=== Recommended Conversion Order ==="
echo

# Group by test category and size
echo "1. Large Integration Tests (>500 lines):"
find test/integration -name "*.rs" -type f -exec wc -l {} + | sort -nr | awk '$1>500 {print "   - " $2 " (" $1 " lines)"}' | head -10

echo
echo "2. Complex Adversarial Tests:"
find test/adversarial -name "*.rs" -type f -exec wc -l {} + | sort -nr | awk '$1>400 {print "   - " $2 " (" $1 " lines)"}' | head -10

echo
echo "3. Worker/Concurrency Tests:"
find test/ -name "*worker*.rs" -o -name "*concurrent*.rs" | xargs wc -l | sort -nr | awk '$1>200 {print "   - " $2 " (" $1 " lines)"}'

echo
echo "=== Automation Opportunities ==="
echo

# Find files with similar patterns that could use the same builder
echo "Files with similar event patterns:"
rg -l "filesystem.*file\.(created|modified)" test/ --type rust | head -5
echo
echo "Files with similar worker patterns:"
rg -l "work_queue|worker_id|processing" test/ --type rust | head -5
echo
echo "Files with similar validation patterns:"
rg -l "assert.*valid|validation.*fail" test/ --type rust | head -5

echo
echo "=== Next Steps ==="
echo "1. Run: ast-grep --pattern 'RawEvent { $$$ }' test/"
echo "2. Create domain-specific test builders for each component"
echo "3. Use this analysis to prioritize conversion work"
echo "4. Track progress with: git diff --stat test/"
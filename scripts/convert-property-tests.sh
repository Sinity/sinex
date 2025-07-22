#!/usr/bin/env bash

# Script to convert property tests to use property builders

set -euo pipefail

echo "Converting property tests to use property builders..."

# Property test files that need conversion
PROPERTY_TEST_FILES=(
    "test/property/automation_property_test.rs"
    "test/property/checkpoint_property_test.rs"
    "test/property/satellite_property_test.rs"
    "test/property/schema_property_test.rs"
    "test/property/ulid_property_test.rs"
    "test/adversarial/enhanced_boundary_test.rs"
    "test/unit/ulid_comprehensive_test.rs"
)

# Files that might have property tests but are disabled
DISABLED_FILES=(
    "test/property/event_model_fuzzing_test.rs"
    "test/property/event_property_test.rs"
    "test/property/event_property_test_snapshot.rs"
    "test/property/redis_streams_property_test.rs"
    "test/property/queue_property_test.rs"
)

# Counter for conversions
CONVERTED=0
SKIPPED=0
ERRORS=0

# Function to check if file uses manual event creation
check_manual_creation() {
    local file="$1"
    if grep -q "create_raw_event\|RawEvent {" "$file" 2>/dev/null; then
        return 0
    fi
    return 1
}

# Function to add property builder import if not present
add_import() {
    local file="$1"
    if ! grep -q "use crate::common::property_builders::\*;" "$file"; then
        # Add import after other use statements
        sed -i '0,/^use .*$/s//use crate::common::property_builders::*;\n&/' "$file"
        echo "  ✓ Added property_builders import"
    fi
}

# Process each file
for file in "${PROPERTY_TEST_FILES[@]}"; do
    if [[ ! -f "$file" ]]; then
        echo "⚠️  File not found: $file"
        ((SKIPPED++))
        continue
    fi
    
    echo "Processing: $file"
    
    # Check if it has proptest
    if ! grep -q "proptest\|#\[proptest\]" "$file" 2>/dev/null; then
        echo "  → No proptest found, skipping"
        ((SKIPPED++))
        continue
    fi
    
    # Check if it uses manual event creation
    if ! check_manual_creation "$file"; then
        echo "  → No manual event creation found, skipping"
        ((SKIPPED++))
        continue
    fi
    
    # Create backup
    cp "$file" "${file}.bak"
    
    # Add import
    add_import "$file"
    
    # Convert manual event creation patterns
    echo "  → Converting event creation patterns..."
    
    # Pattern 1: Replace create_raw_event calls with TestEventBuilder
    sed -i 's/crate::common::events::create_raw_event(\s*\([^,]*\),\s*\([^,]*\),\s*\([^,]*\),\s*\([^)]*\))/TestEventBuilder::new().source(\1).event_type(\2).payload(\3).timestamp(\4).build()/g' "$file"
    
    # Pattern 2: Replace manual RawEvent construction
    # This is more complex and would need AST-based transformation for safety
    
    # Pattern 3: Replace event generation in proptest strategies
    # Look for patterns like (event_sources(), event_types(), event_payloads())
    sed -i 's/(event_sources(), event_types(), event_payloads())/arbitrary_event()/g' "$file"
    
    # Pattern 4: Replace filesystem event patterns
    if grep -q "file\.\(created\|modified\|deleted\)" "$file"; then
        echo "  → Found filesystem events, using filesystem_event() strategy"
        # This would need more sophisticated replacement
    fi
    
    # Verify the file still compiles
    if cargo check --tests --no-default-features -p sinex-test --test "$(basename "$file" .rs)" 2>/dev/null; then
        echo "  ✓ Successfully converted"
        ((CONVERTED++))
        rm "${file}.bak"
    else
        echo "  ✗ Conversion failed, reverting"
        mv "${file}.bak" "$file"
        ((ERRORS++))
    fi
    
    echo ""
done

echo "=== Disabled Files (for reference) ==="
for file in "${DISABLED_FILES[@]}"; do
    if [[ -f "$file" ]]; then
        echo "- $file (disabled in mod.rs)"
    fi
done

echo ""
echo "=== Conversion Summary ==="
echo "Converted: $CONVERTED files"
echo "Skipped: $SKIPPED files"
echo "Errors: $ERRORS files"
echo ""
echo "Note: This is a basic conversion. Manual review is recommended for:"
echo "- Complex event construction patterns"
echo "- Filesystem-specific events"
echo "- Custom event validation logic"
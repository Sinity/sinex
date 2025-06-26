#!/usr/bin/env bash
set -euo pipefail

# FORGE: Fix Import Dependencies Script
# Resolves compilation issues discovered during error pattern analysis

echo "🔧 FORGE: Fixing import dependencies to enable error transformations..."

# Track files that were modified
MODIFIED_FILES=()

# Function to add import if not already present
add_import_if_missing() {
    local file="$1"
    local import="$2"
    
    if [[ -f "$file" ]] && ! grep -q "$import" "$file"; then
        echo "Adding import to $file: $import"
        # Add import after existing use statements
        if grep -q "^use " "$file"; then
            # Find last use statement and add after it
            sed -i "/^use /a\\
$import" "$file"
        else
            # Add import at beginning after mod declarations
            sed -i "1i\\
$import" "$file"
        fi
        MODIFIED_FILES+=("$file")
    fi
}

# Function to fix missing JsonValue imports
fix_jsonvalue_imports() {
    echo "Fixing JsonValue imports..."
    
    # Common files that use JsonValue
    local jsonvalue_files=(
        "crate/sinex-events/src/asciinema.rs"
        "crate/sinex-events/src/hyprland.rs"
        "crate/sinex-events/src/terminal.rs"
        "crate/sinex-events/src/filesystem.rs"
    )
    
    for file in "${jsonvalue_files[@]}"; do
        if [[ -f "$file" ]] && grep -q "JsonValue" "$file"; then
            add_import_if_missing "$file" "use serde_json::Value as JsonValue;"
        fi
    done
}

# Function to fix missing Timestamp imports
fix_timestamp_imports() {
    echo "Fixing Timestamp imports..."
    
    # Files that use Timestamp
    local timestamp_files=(
        "crate/sinex-events/src/asciinema.rs"
        "crate/sinex-events/src/filesystem.rs"
        "crate/sinex-events/src/terminal.rs"
    )
    
    for file in "${timestamp_files[@]}"; do
        if [[ -f "$file" ]] && grep -q "Timestamp" "$file"; then
            add_import_if_missing "$file" "use chrono::DateTime;"
            add_import_if_missing "$file" "use chrono::Utc;"
        fi
    done
}

# Function to fix missing CoreError imports
fix_core_error_imports() {
    echo "Fixing CoreError imports..."
    
    # Files that use CoreError
    local core_error_files=(
        "crate/sinex-events/src/asciinema.rs"
        "crate/sinex-events/src/hyprland.rs"
        "crate/sinex-events/src/terminal.rs"
        "crate/sinex-events/src/filesystem.rs"
        "crate/sinex-collector/src/config.rs"
    )
    
    for file in "${core_error_files[@]}"; do
        if [[ -f "$file" ]] && grep -q "CoreError" "$file"; then
            add_import_if_missing "$file" "use sinex_core::error_context::{CoreError, ErrorContext, ResultExt};"
        fi
    done
}

# Function to fix Path/PathBuf imports
fix_path_imports() {
    echo "Fixing Path/PathBuf imports..."
    
    local path_files=(
        "crate/sinex-events/src/asciinema.rs"
        "crate/sinex-events/src/filesystem.rs"
        "crate/sinex-collector/src/config.rs"
    )
    
    for file in "${path_files[@]}"; do
        if [[ -f "$file" ]] && (grep -q "Path" "$file" || grep -q "PathBuf" "$file"); then
            add_import_if_missing "$file" "use std::path::{Path, PathBuf};"
        fi
    done
}

# Main execution
main() {
    echo "Starting import fixes for error transformation prerequisites..."
    
    fix_jsonvalue_imports
    fix_timestamp_imports  
    fix_core_error_imports
    fix_path_imports
    
    # Verify compilation status
    echo ""
    echo "🔍 Checking compilation status..."
    if cargo check --workspace --quiet 2>/dev/null; then
        echo "✅ Compilation successful! Ready for error transformations."
    else
        echo "⚠️  Compilation issues remain. Running detailed check:"
        cargo check --workspace
        echo ""
        echo "❌ Additional import fixes may be needed."
        echo "   Review compilation errors and add missing imports manually."
    fi
    
    # Report modified files
    if [[ ${#MODIFIED_FILES[@]} -gt 0 ]]; then
        echo ""
        echo "📝 Modified files:"
        printf ' - %s\n' "${MODIFIED_FILES[@]}"
        echo ""
        echo "💡 Review changes with: git diff"
    else
        echo ""
        echo "ℹ️  No import modifications needed."
    fi
}

main "$@"
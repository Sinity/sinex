#!/usr/bin/env bash
# Script to systematically apply camino migration across the codebase

set -euo pipefail

echo "=== Applying camino migration systematically ==="

# Step 1: Add camino to all crates that use PathBuf
echo "1. Adding camino to crates..."

CRATES=(
    "crate/satellites/sinex-terminal-satellite"
    "crate/satellites/sinex-fs-watcher"
    "crate/satellites/sinex-system-satellite"
    "crate/satellites/sinex-search-automaton"
    "crate/satellites/sinex-terminal-command-canonicalizer"
    "crate/lib/sinex-satellite-sdk"
    "crate/lib/sinex-test-utils"
)

for crate_dir in "${CRATES[@]}"; do
    cargo_toml="$crate_dir/Cargo.toml"
    if [ -f "$cargo_toml" ]; then
        if ! grep -q "camino" "$cargo_toml"; then
            echo "Adding camino to $cargo_toml"
            # Add after the first [dependencies] line
            sed -i '/^\[dependencies\]/a camino = { workspace = true }' "$cargo_toml"
        fi
    fi
done

# Step 2: Apply common replacements
echo "2. Applying common replacements..."

# For each crate, update imports and types
for crate_dir in "${CRATES[@]}"; do
    if [ -d "$crate_dir/src" ]; then
        echo "Processing $crate_dir..."
        
        # Replace imports
        find "$crate_dir/src" -name "*.rs" -type f -exec sed -i \
            -e 's/use std::path::{Path, PathBuf};/use camino::{Utf8Path, Utf8PathBuf};/g' \
            -e 's/use std::path::PathBuf;/use camino::Utf8PathBuf;/g' \
            -e 's/use std::path::Path;/use camino::Utf8Path;/g' \
            {} \;
        
        # Replace type annotations
        find "$crate_dir/src" -name "*.rs" -type f -exec sed -i \
            -e 's/: PathBuf/: Utf8PathBuf/g' \
            -e 's/: &Path/: \&Utf8Path/g' \
            -e 's/: Option<PathBuf>/: Option<Utf8PathBuf>/g' \
            -e 's/: Vec<PathBuf>/: Vec<Utf8PathBuf>/g' \
            -e 's/-> PathBuf/-> Utf8PathBuf/g' \
            -e 's/-> &Path/-> \&Utf8Path/g' \
            -e 's/-> Option<PathBuf>/-> Option<Utf8PathBuf>/g' \
            -e 's/<PathBuf>/<Utf8PathBuf>/g' \
            -e 's/<Vec<PathBuf>>/<Vec<Utf8PathBuf>>/g' \
            -e 's/HashMap<PathBuf,/HashMap<Utf8PathBuf,/g' \
            {} \;
        
        # Replace constructors
        find "$crate_dir/src" -name "*.rs" -type f -exec sed -i \
            -e 's/PathBuf::from(/Utf8PathBuf::from(/g' \
            -e 's/PathBuf::new(/Utf8PathBuf::new(/g' \
            -e 's/Path::new(/Utf8Path::new(/g' \
            {} \;
    fi
done

# Step 3: Handle special cases
echo "3. Handling special cases..."

# Fix .exists() calls - Utf8Path/Utf8PathBuf have exists() method
# Fix .display() calls - use .as_str() instead
find crate -name "*.rs" -type f -exec sed -i \
    -e 's/\.display()/\.as_str()/g' \
    {} \;

# Fix dirs:: functions
find crate -name "*.rs" -type f -exec sed -i \
    -e 's/dirs::home_dir()/dirs::home_dir().and_then(|p| Utf8PathBuf::from_path_buf(p).ok())/g' \
    -e 's/dirs::config_dir()/dirs::config_dir().and_then(|p| Utf8PathBuf::from_path_buf(p).ok())/g' \
    -e 's/dirs::data_dir()/dirs::data_dir().and_then(|p| Utf8PathBuf::from_path_buf(p).ok())/g' \
    {} \;

# Fix PathBuf::from("/tmp") patterns
find crate -name "*.rs" -type f -exec sed -i \
    -e 's/PathBuf::from("\/tmp")/Utf8PathBuf::from("\/tmp")/g' \
    {} \;

# Step 4: Handle path conversions
echo "4. Adding conversions for non-UTF8 paths..."

# For notify and other libraries that return std::path::Path
# We need to add conversions
cat > /tmp/path_conversion_helpers.rs << 'EOF'
/// Convert std::path::Path to Utf8Path with error handling
fn path_to_utf8(path: &std::path::Path) -> Result<&camino::Utf8Path, SatelliteError> {
    camino::Utf8Path::from_path(path)
        .ok_or_else(|| SatelliteError::Configuration(format!("Path is not UTF-8: {:?}", path)))
}

/// Convert std::path::PathBuf to Utf8PathBuf with error handling
fn pathbuf_to_utf8(path: std::path::PathBuf) -> Result<camino::Utf8PathBuf, SatelliteError> {
    camino::Utf8PathBuf::from_path_buf(path)
        .map_err(|p| SatelliteError::Configuration(format!("Path is not UTF-8: {:?}", p)))
}
EOF

echo ""
echo "=== Migration Applied ==="
echo "Next steps:"
echo "1. Run 'cargo check' to see remaining errors"
echo "2. Handle notify Event paths that need conversion"
echo "3. Update any APIs that interface with external libraries"
echo "4. Test that path operations still work correctly"
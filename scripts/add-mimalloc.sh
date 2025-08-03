#!/usr/bin/env bash
# Script to add mimalloc to all binaries

set -euo pipefail

# Template for mimalloc code
MIMALLOC_CODE='#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;
'

# Function to add mimalloc to a main.rs file
add_mimalloc() {
    local file=$1
    echo "Processing: $file"
    
    # Check if mimalloc is already present
    if grep -q "mimalloc" "$file"; then
        echo "  ✓ Already has mimalloc"
        return
    fi
    
    # Create temporary file
    local tmpfile=$(mktemp)
    
    # Find the first line after use statements or before fn main
    # This is a bit tricky, so we'll insert after the first non-comment, non-attribute line
    awk -v code="$MIMALLOC_CODE" '
        !inserted && /^use / { uses_found = 1 }
        !inserted && uses_found && !/^use / && !/^#\[/ && !/^\/\// {
            print code
            inserted = 1
        }
        { print }
    ' "$file" > "$tmpfile"
    
    # If we didn't insert yet (no use statements), insert before fn main
    if ! grep -q "MiMalloc" "$tmpfile"; then
        awk -v code="$MIMALLOC_CODE" '
            !inserted && /^(async )?fn main/ {
                print code
                inserted = 1
            }
            { print }
        ' "$file" > "$tmpfile"
    fi
    
    # Replace original file
    mv "$tmpfile" "$file"
    echo "  ✅ Added mimalloc"
}

# Process all main.rs files
echo "Adding mimalloc to all binaries..."
echo ""

# Core binaries
for file in crate/core/*/src/main.rs; do
    if [[ -f "$file" ]]; then
        add_mimalloc "$file"
    fi
done

# Satellite binaries  
for file in crate/satellites/*/src/main.rs; do
    if [[ -f "$file" ]]; then
        add_mimalloc "$file"
    fi
done

echo ""
echo "Done! Now updating Cargo.toml files..."

# Function to add mimalloc dependency to Cargo.toml
add_mimalloc_dep() {
    local dir=$1
    local cargo_toml="$dir/Cargo.toml"
    
    echo "Checking: $cargo_toml"
    
    if grep -q "mimalloc" "$cargo_toml"; then
        echo "  ✓ Already has mimalloc dependency"
        return
    fi
    
    # Add mimalloc to dependencies section
    # We need to be careful to add it in the right place
    awk '
        /^\[dependencies\]/ {
            print
            print "mimalloc = { workspace = true }"
            next
        }
        { print }
    ' "$cargo_toml" > "$cargo_toml.tmp"
    
    mv "$cargo_toml.tmp" "$cargo_toml"
    echo "  ✅ Added mimalloc dependency"
}

# Add dependency to all binary crates
for dir in crate/core/* crate/satellites/*; do
    if [[ -d "$dir" && -f "$dir/Cargo.toml" && -f "$dir/src/main.rs" ]]; then
        add_mimalloc_dep "$dir"
    fi
done

echo ""
echo "✅ Mimalloc has been added to all binaries!"
echo ""
echo "Next steps:"
echo "1. Run 'cargo check' to verify everything compiles"
echo "2. Run 'cargo fmt' to format the code"
echo "3. Commit the changes"
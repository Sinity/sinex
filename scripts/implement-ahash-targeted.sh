#!/usr/bin/env bash
# Script to implement ahash in performance-critical parts of the codebase

set -euo pipefail

echo "Implementing ahash in performance-critical components..."
echo ""

# List of performance-critical crates where ahash would be most beneficial
CRITICAL_CRATES=(
    "sinex-ingestd"          # High throughput event processing
    "sinex-db"               # Database operations with caching
    "sinex-satellite-sdk"    # Core SDK used by all satellites
    "sinex-types"            # Core types used everywhere
    "sinex-gateway"          # API gateway with request routing
)

# Function to add ahash types to a crate
add_ahash_types() {
    local crate_dir=$1
    local lib_file="$crate_dir/src/lib.rs"
    
    # Skip if lib.rs doesn't exist
    if [[ ! -f "$lib_file" ]]; then
        lib_file="$crate_dir/src/main.rs"
        if [[ ! -f "$lib_file" ]]; then
            return
        fi
    fi
    
    echo "Adding ahash type aliases to $crate_dir..."
    
    # Check if ahash types are already defined
    if grep -q "type HashMap" "$lib_file"; then
        echo "  ℹ️  Type aliases already exist"
        return
    fi
    
    # Add type aliases at the beginning of the file after initial comments/attributes
    local tmpfile=$(mktemp)
    
    awk '
        BEGIN { added = 0 }
        
        # Skip initial comments and attributes
        /^\/\/|^#\[|^$/ && !added { print; next }
        
        # Add type aliases before first non-comment line
        !added {
            print "// Performance-optimized hash collections using ahash"
            print "pub type HashMap<K, V> = ahash::AHashMap<K, V>;"
            print "pub type HashSet<T> = ahash::AHashSet<T>;"
            print ""
            added = 1
        }
        
        { print }
    ' "$lib_file" > "$tmpfile"
    
    mv "$tmpfile" "$lib_file"
    echo "  ✅ Added ahash type aliases"
}

# Function to update imports in a file to use crate-level HashMap/HashSet
update_imports() {
    local file=$1
    local crate_name=$2
    
    # Skip if file doesn't use HashMap or HashSet
    if ! grep -q "HashMap\|HashSet" "$file"; then
        return
    fi
    
    local tmpfile=$(mktemp)
    local changed=false
    
    # Replace std::collections imports with crate imports
    awk -v crate="$crate_name" '
        # Replace std::collections::HashMap imports
        /use std::collections::\{.*HashMap.*\}/ {
            # Extract other items in the import
            match($0, /use std::collections::\{(.*)\}/, items)
            split(items[1], item_list, ",")
            
            # Separate HashMap/HashSet from others
            other_items = ""
            has_map = 0
            has_set = 0
            
            for (i in item_list) {
                item = item_list[i]
                gsub(/^[ \t]+|[ \t]+$/, "", item)  # trim
                
                if (item ~ /HashMap/) has_map = 1
                else if (item ~ /HashSet/) has_set = 1
                else {
                    if (other_items != "") other_items = other_items ", "
                    other_items = other_items item
                }
            }
            
            # Print appropriate imports
            if (has_map) print "use " crate "::HashMap;"
            if (has_set) print "use " crate "::HashSet;"
            if (other_items != "") print "use std::collections::{" other_items "};"
            
            next
        }
        
        /use std::collections::HashMap/ {
            print "use " crate "::HashMap;"
            next
        }
        
        /use std::collections::HashSet/ {
            print "use " crate "::HashSet;"
            next
        }
        
        { print }
    ' "$file" > "$tmpfile"
    
    if ! cmp -s "$file" "$tmpfile"; then
        mv "$tmpfile" "$file"
        changed=true
    else
        rm "$tmpfile"
    fi
    
    echo $changed
}

# Process critical crates
for crate in "${CRITICAL_CRATES[@]}"; do
    crate_dir="crate/lib/$crate"
    if [[ ! -d "$crate_dir" ]]; then
        crate_dir="crate/core/$crate"
        if [[ ! -d "$crate_dir" ]]; then
            echo "⚠️  Crate $crate not found"
            continue
        fi
    fi
    
    echo "Processing crate: $crate"
    
    # Add ahash dependency
    cargo_toml="$crate_dir/Cargo.toml"
    if ! grep -q "ahash" "$cargo_toml"; then
        awk '
            /^\[dependencies\]/ {
                print
                print "ahash = { workspace = true }"
                next
            }
            { print }
        ' "$cargo_toml" > "$cargo_toml.tmp"
        mv "$cargo_toml.tmp" "$cargo_toml"
        echo "  ✅ Added ahash dependency"
    fi
    
    # Add type aliases
    add_ahash_types "$crate_dir"
    
    # Update imports in all files in this crate
    changed_count=0
    while IFS= read -r file; do
        if [[ $(update_imports "$file" "$crate") == "true" ]]; then
            changed_count=$((changed_count + 1))
        fi
    done < <(find "$crate_dir" -name "*.rs" -type f)
    
    echo "  ✅ Updated $changed_count files in $crate"
    echo ""
done

# Also update key hot paths in other crates that import from critical crates
echo "Updating imports in dependent crates..."

# Find files that import from critical crates
for crate in "${CRITICAL_CRATES[@]}"; do
    echo "Finding files that import from $crate..."
    
    while IFS= read -r file; do
        # Skip files in the crate itself
        if [[ "$file" == *"/$crate/"* ]]; then
            continue
        fi
        
        # Check if file imports HashMap/HashSet from this crate
        if grep -q "use $crate.*HashMap\|use $crate.*HashSet" "$file"; then
            echo "  Already uses $crate hash types: $file"
        fi
    done < <(grep -r "use $crate" crate --include="*.rs" -l 2>/dev/null || true)
done

echo ""
echo "✅ Ahash implementation complete!"
echo ""
echo "Summary of changes:"
echo "- Added ahash dependency to performance-critical crates"
echo "- Created type aliases for HashMap and HashSet using ahash"
echo "- Updated imports within those crates to use the optimized versions"
echo ""
echo "Benefits:"
echo "- Faster hash operations in hot paths"
echo "- No API changes required"
echo "- Easy to extend to other crates as needed"
echo ""
echo "Next steps:"
echo "1. Run 'cargo check' to verify everything compiles"
echo "2. Run 'cargo fmt' to format the code"
echo "3. Run benchmarks to measure performance improvement"
echo "4. Commit the changes"
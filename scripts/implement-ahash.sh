#!/usr/bin/env bash
# Script to replace std::collections::{HashMap, HashSet} with ahash versions

set -euo pipefail

echo "Implementing ahash throughout the codebase..."
echo ""

# Function to add ahash dependency to Cargo.toml if not present
add_ahash_dep() {
    local cargo_toml=$1
    
    if grep -q "ahash" "$cargo_toml"; then
        return
    fi
    
    # Check if this is a library or binary crate
    if grep -q "\[dependencies\]" "$cargo_toml"; then
        # Add ahash after [dependencies]
        awk '
            /^\[dependencies\]/ {
                print
                print "ahash = { workspace = true }"
                next
            }
            { print }
        ' "$cargo_toml" > "$cargo_toml.tmp"
        
        mv "$cargo_toml.tmp" "$cargo_toml"
        echo "  ✅ Added ahash dependency to $cargo_toml"
    fi
}

# Function to process a Rust file
process_rust_file() {
    local file=$1
    local changed=false
    
    # Check if file uses HashMap or HashSet
    if ! grep -q "HashMap\|HashSet" "$file"; then
        return
    fi
    
    echo "Processing: $file"
    
    # Create a temporary file
    local tmpfile=$(mktemp)
    
    # Process the file
    awk '
        # Track if we have added ahash imports
        BEGIN { added_ahash = 0 }
        
        # Replace std::collections imports
        /^use std::collections::\{.*HashMap.*\}/ {
            if (!added_ahash) {
                print "use ahash::AHashMap;"
                added_ahash = 1
            }
            # Keep the line but comment it out for reference
            print "// " $0 " // Replaced with ahash"
            next
        }
        
        /^use std::collections::\{.*HashSet.*\}/ {
            if (!added_ahash) {
                print "use ahash::AHashSet;"
                added_ahash = 1
            }
            # Keep the line but comment it out for reference
            print "// " $0 " // Replaced with ahash"
            next
        }
        
        /^use std::collections::HashMap/ {
            print "use ahash::AHashMap;"
            print "// " $0 " // Replaced with ahash"
            next
        }
        
        /^use std::collections::HashSet/ {
            print "use ahash::AHashSet;"
            print "// " $0 " // Replaced with ahash"
            next
        }
        
        # Print all other lines
        { print }
    ' "$file" > "$tmpfile"
    
    # Now replace HashMap with AHashMap and HashSet with AHashSet in the code
    sed -i \
        -e 's/HashMap<\([^>]*\)>/AHashMap<\1>/g' \
        -e 's/HashSet<\([^>]*\)>/AHashSet<\1>/g' \
        -e 's/: HashMap\b/: AHashMap/g' \
        -e 's/: HashSet\b/: AHashSet/g' \
        -e 's/HashMap::/AHashMap::/g' \
        -e 's/HashSet::/AHashSet::/g' \
        "$tmpfile"
    
    # Check if anything changed
    if ! cmp -s "$file" "$tmpfile"; then
        mv "$tmpfile" "$file"
        echo "  ✅ Replaced HashMap/HashSet with ahash versions"
        changed=true
        
        # Get the crate directory
        local crate_dir=$(echo "$file" | sed -E 's|^(crate/[^/]+/[^/]+).*|\1|')
        if [[ -f "$crate_dir/Cargo.toml" ]]; then
            add_ahash_dep "$crate_dir/Cargo.toml"
        fi
    else
        rm "$tmpfile"
        echo "  ℹ️  No changes needed"
    fi
}

# Find all Rust files and process them
total_files=0
changed_files=0

while IFS= read -r file; do
    total_files=$((total_files + 1))
    if process_rust_file "$file"; then
        changed_files=$((changed_files + 1))
    fi
done < <(find crate -name "*.rs" -type f)

echo ""
echo "✅ Processed $total_files files, changed $changed_files files"
echo ""
echo "Next steps:"
echo "1. Run 'cargo check' to verify everything compiles"
echo "2. Run 'cargo fmt' to format the code"
echo "3. Review the changes to ensure correctness"
echo "4. Commit the changes"
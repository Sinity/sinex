#!/usr/bin/env bash
set -euo pipefail

echo "Updating imports to use sinex crate..."

# Update sinex_types imports
find crate -name "*.rs" -type f | while read -r file; do
    # Skip the sinex crate itself
    if [[ "$file" =~ crate/sinex/src/lib.rs ]]; then
        continue
    fi
    
    # Skip the crates that sinex re-exports
    if [[ "$file" =~ crate/sinex-(types|events|db|telemetry|preflight|satellite-sdk|nats|services|annex|test-utils)/ ]]; then
        continue
    fi
    
    # Replace sinex_types imports with sinex
    sed -i 's/use sinex_types::/use sinex::/g' "$file"
    sed -i 's/use sinex_events::/use sinex::/g' "$file"
    sed -i 's/use sinex_db::/use sinex::/g' "$file"
    
    # Update multi-line imports
    sed -i '/^use sinex_types::{$/,/^};$/s/sinex_types/sinex/' "$file"
    sed -i '/^use sinex_events::{$/,/^};$/s/sinex_events/sinex/' "$file"
    sed -i '/^use sinex_db::{$/,/^};$/s/sinex_db/sinex/' "$file"
done

echo "Import updates complete!"
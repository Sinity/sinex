#!/usr/bin/env bash

# Fix Event::from_payload calls to remove the ? operator
# Since from_payload now returns Self instead of Result

echo "Updating Event::from_payload calls..."

# Find all Rust files and update them
find crate/ -name "*.rs" -type f | while read -r file; do
    # Skip the event.rs file itself and test files
    if [[ "$file" == *"/models/event.rs" ]] || [[ "$file" == *"/test/"* ]]; then
        continue
    fi
    
    # Check if file contains Event::from_payload
    if grep -q "Event::from_payload.*?" "$file"; then
        echo "Updating: $file"
        
        # Replace patterns:
        # 1. Event::from_payload(...)? -> Event::from_payload(...)
        # 2. Event::from_payload(...).ok() -> Some(Event::from_payload(...))
        sed -i.bak -E '
            s/Event::from_payload\(([^)]+)\)\?/Event::from_payload(\1)/g
            s/Event::from_payload\(([^)]+)\)\.ok\(\)/Some(Event::from_payload(\1))/g
        ' "$file"
        
        # Clean up backup files
        rm -f "${file}.bak"
    fi
done

echo "Done! Now let me check what was updated..."

# Show a sample of the changes
echo ""
echo "Sample of updated files:"
find crate/ -name "*.rs" -type f | xargs grep -l "Event::from_payload" | grep -v "/models/event.rs" | head -5
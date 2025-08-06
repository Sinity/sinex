#!/usr/bin/env python3
"""
Update Event::from_payload calls to remove ? operator since it now returns Self
"""

import os
import re
import sys

def process_file(filepath):
    """Process a single Rust file to update Event::from_payload calls"""
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Skip if no Event::from_payload
    if 'Event::from_payload' not in content:
        return False
    
    original_content = content
    
    # Pattern to match Event::from_payload(...) followed by ?
    # This handles multi-line payload construction
    pattern = r'(Event::from_payload\([^;]+?\))\s*\?'
    
    # Replace the pattern - remove the ?
    content = re.sub(pattern, r'\1', content, flags=re.DOTALL)
    
    # Also handle .ok() pattern
    pattern2 = r'(Event::from_payload\([^;]+?\))\.ok\(\)'
    content = re.sub(pattern2, r'Some(\1)', content, flags=re.DOTALL)
    
    if content != original_content:
        print(f"Updating: {filepath}")
        with open(filepath, 'w') as f:
            f.write(content)
        return True
    
    return False

def main():
    updated_count = 0
    
    # Walk through crate directory
    for root, dirs, files in os.walk('crate/'):
        # Skip .git and target directories
        if '.git' in root or 'target' in root:
            continue
            
        for file in files:
            if file.endswith('.rs'):
                filepath = os.path.join(root, file)
                
                # Skip the event.rs file itself
                if filepath.endswith('/models/event.rs'):
                    continue
                
                if process_file(filepath):
                    updated_count += 1
    
    print(f"\nUpdated {updated_count} files")
    
    # Check for any remaining issues
    print("\nChecking for remaining Event::from_payload with ? ...")
    os.system('grep -r "Event::from_payload.*?" crate/ | grep -v ".git" | grep -v "/models/event.rs" | head -5 || echo "None found!"')

if __name__ == '__main__':
    main()
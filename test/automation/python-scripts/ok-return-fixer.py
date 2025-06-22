#!/usr/bin/env python3
"""
Automated Ok(()) return insertion for Result<()> functions.

This script intelligently adds missing Ok(()) returns to test functions
that use the ? operator but don't return Ok(()) explicitly.
"""

import re
import sys
import os
from pathlib import Path

def fix_result_functions(filepath):
    """Add Ok(()) returns to Result<()> functions missing them."""
    with open(filepath, 'r') as f:
        content = f.read()
    
    lines = content.split('\n')
    modified = False
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Look for Result<()> function signatures (both Box<dyn Error> and anyhow::Result)
        if (re.search(r'async fn \w+\(.*\) -> (Result<\(\)>|Result<\(\), Box<dyn|anyhow::Result<\(\)>)', line)):
            # Find the closing brace of this function
            brace_count = 0
            found_opening = False
            
            j = i
            while j < len(lines):
                if '{' in lines[j]:
                    found_opening = True
                    brace_count += lines[j].count('{')
                if found_opening:
                    brace_count -= lines[j].count('}')
                    if brace_count == 0:
                        # This is the closing brace of the function
                        # Check if the previous non-empty line contains Ok(())
                        prev_line_idx = j - 1
                        while prev_line_idx >= 0 and not lines[prev_line_idx].strip():
                            prev_line_idx -= 1
                        
                        if prev_line_idx >= 0 and 'Ok(())' not in lines[prev_line_idx]:
                            # Insert Ok(()) before the closing brace
                            indent = ' ' * (len(lines[j]) - len(lines[j].lstrip()))
                            lines.insert(j, indent + '    Ok(())')
                            modified = True
                            print(f"Added Ok(()) to function at line {i+1} in {filepath}")
                        break
                j += 1
        i += 1
    
    if modified:
        with open(filepath, 'w') as f:
            f.write('\n'.join(lines))
        print(f"Successfully modified {filepath}")
        return True
    else:
        print(f"No changes needed for {filepath}")
        return False

def process_directory(directory):
    """Process all Rust files in a directory."""
    modified_files = []
    test_dir = Path(directory)
    
    for rust_file in test_dir.rglob("*.rs"):
        if fix_result_functions(rust_file):
            modified_files.append(rust_file)
    
    return modified_files

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 ok-return-fixer.py <directory>")
        print("Example: python3 ok-return-fixer.py test/")
        sys.exit(1)
    
    directory = sys.argv[1]
    if not os.path.exists(directory):
        print(f"Directory {directory} does not exist")
        sys.exit(1)
    
    print(f"🔧 Processing Result<()> functions in {directory}...")
    modified_files = process_directory(directory)
    
    print(f"\n✅ Processed {len(modified_files)} files:")
    for file in modified_files:
        print(f"  - {file}")
    
    if modified_files:
        print(f"\n🧪 Verifying compilation...")
        import subprocess
        result = subprocess.run(["cargo", "check", "--workspace"], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Compilation successful!")
        else:
            print("❌ Compilation failed:")
            print(result.stderr)
            return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
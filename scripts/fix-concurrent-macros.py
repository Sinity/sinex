#!/usr/bin/env python3
"""
Fix broken test_concurrent_operations! macro calls in chaos engineering test.
"""

import re
from pathlib import Path

def fix_concurrent_macros(content: str) -> str:
    """Fix broken test_concurrent_operations! macro syntax."""
    
    # Fix pattern: );); -> );
    content = re.sub(r'\);\s*\);', ');', content)
    
    # Fix pattern where there's trailing text after macro
    # Find all test_concurrent_operations! calls
    lines = content.split('\n')
    fixed_lines = []
    i = 0
    
    while i < len(lines):
        line = lines[i]
        
        # If we find a test_concurrent_operations! macro start
        if 'test_concurrent_operations!' in line:
            macro_lines = [line]
            brace_count = line.count('{') - line.count('}')
            paren_count = line.count('(') - line.count(')')
            i += 1
            
            # Collect the entire macro
            while i < len(lines) and (brace_count > 0 or paren_count > 0):
                macro_lines.append(lines[i])
                brace_count += lines[i].count('{') - lines[i].count('}')
                paren_count += lines[i].count('(') - lines[i].count(')')
                i += 1
            
            # Check if last line has extra content after );
            if macro_lines:
                last_line = macro_lines[-1]
                # Remove any trailing text after );
                if ');' in last_line:
                    parts = last_line.split(');')
                    if len(parts) > 1 and parts[1].strip():
                        # Keep only up to the first );
                        macro_lines[-1] = parts[0] + ');'
            
            fixed_lines.extend(macro_lines)
        else:
            fixed_lines.append(line)
            i += 1
    
    # Also remove orphaned function fragments
    cleaned_lines = []
    skip_next = False
    
    for i, line in enumerate(fixed_lines):
        if skip_next:
            skip_next = False
            continue
            
        # Skip lines that are just "should succeed");" or similar fragments
        if re.match(r'^\s*(should succeed|hould be|\);)\s*["\)]', line):
            continue
            
        # Skip orphaned Ok(()) }
        if i > 0 and line.strip() == 'Ok(())' and i + 1 < len(fixed_lines) and fixed_lines[i+1].strip() == '}':
            # Check if this is part of a function or just orphaned
            if i + 2 < len(fixed_lines) and not fixed_lines[i+2].startswith(('///', '#[', 'async fn', 'fn', '}')):
                continue
        
        cleaned_lines.append(line)
    
    return '\n'.join(cleaned_lines)

def main():
    """Fix the chaos engineering test file."""
    file_path = Path('test/adversarial/chaos_engineering_test.rs')
    
    if not file_path.exists():
        print(f"Error: {file_path} not found")
        return
    
    print(f"Fixing {file_path}...")
    content = file_path.read_text()
    
    fixed = fix_concurrent_macros(content)
    
    if fixed != content:
        file_path.write_text(fixed)
        print("✓ Fixed macro syntax issues")
    else:
        print("✓ No changes needed")

if __name__ == '__main__':
    main()
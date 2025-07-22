#!/usr/bin/env python3
"""
Fix issues in restored adversarial tests.

This script:
1. Removes duplicate imports
2. Fixes missing closing braces and syntax errors
3. Converts remaining old patterns to new ones
4. Ensures tests compile properly
"""

import re
import sys
from pathlib import Path
from typing import List, Tuple

def remove_duplicate_imports(content: str) -> str:
    """Remove duplicate 'use crate::common::prelude::*;' lines."""
    lines = content.split('\n')
    seen_prelude = False
    new_lines = []
    
    for line in lines:
        if line.strip() == 'use crate::common::prelude::*;':
            if not seen_prelude:
                new_lines.append(line)
                seen_prelude = True
            # Skip duplicate
        else:
            new_lines.append(line)
    
    return '\n'.join(new_lines)

def fix_test_concurrent_operations_macro(content: str) -> str:
    """Fix broken test_concurrent_operations! macro usage."""
    # Pattern: test_concurrent_operations!(test_name, count, |...| { ... }, |...| { ... });
    
    # Find broken usages with syntax errors
    pattern = r'test_concurrent_operations!\s*\(\s*(\w+),\s*(\d+),\s*\|[^|]+\|[^{]+\{[^}]*\},\s*\|[^|]+\|[^{]+\{[^}]*\}\s*\);[^;]*\);'
    
    def fix_macro(match):
        # Extract the content and fix double closing
        content = match.group(0)
        # Remove extra ); at the end
        content = re.sub(r'\);\s*\);', ');', content)
        # Remove trailing text after the macro
        content = re.sub(r'\);.*$', ');', content, flags=re.MULTILINE)
        return content
    
    content = re.sub(pattern, fix_macro, content, flags=re.MULTILINE | re.DOTALL)
    
    # Also fix cases where the macro is incomplete
    # Pattern: macro followed by random text
    pattern2 = r'(test_concurrent_operations!\([^)]+\);)[^}\n]+(?=\n)'
    content = re.sub(pattern2, r'\1', content)
    
    return content

def fix_missing_closing_braces(content: str) -> str:
    """Fix functions that are missing closing braces."""
    lines = content.split('\n')
    
    # Track function definitions and their brace counts
    in_function = False
    function_start = -1
    brace_count = 0
    fixed_lines = []
    
    for i, line in enumerate(lines):
        # Detect async function start
        if re.match(r'^async fn test_\w+.*\{', line):
            in_function = True
            function_start = i
            brace_count = 1
        elif in_function:
            brace_count += line.count('{') - line.count('}')
            
            # If we're at the end of a function but missing closing brace
            if brace_count == 1 and i + 1 < len(lines) and (
                lines[i + 1].startswith('async fn') or 
                lines[i + 1].startswith('///') or
                lines[i + 1].startswith('#[') or
                lines[i + 1].startswith('// =')
            ):
                # Add missing Ok(()) and closing brace
                if not line.strip().endswith('Ok(())'):
                    fixed_lines.append(line)
                    fixed_lines.append('    Ok(())')
                    fixed_lines.append('}')
                else:
                    fixed_lines.append(line)
                    fixed_lines.append('}')
                in_function = False
                brace_count = 0
                continue
        
        fixed_lines.append(line)
        
        if in_function and brace_count == 0:
            in_function = False
    
    # Handle case where file ends without closing brace
    if in_function and brace_count > 0:
        fixed_lines.append('    Ok(())')
        fixed_lines.append('}')
    
    return '\n'.join(fixed_lines)

def fix_insert_event_calls(content: str) -> str:
    """Convert insert_event calls to use ctx."""
    # Pattern: insert_event(&pool, &event)
    content = re.sub(
        r'insert_event\(&pool,',
        r'ctx.insert_event(',
        content
    )
    
    # Pattern: sinex_db::insert_event(&pool, 
    content = re.sub(
        r'sinex_db::insert_event\(&pool,',
        r'ctx.insert_event(',
        content
    )
    
    # Pattern: sinex_db::insert_event_with_validator(&pool,
    content = re.sub(
        r'sinex_db::insert_event_with_validator\(&pool,',
        r'ctx.insert_event_with_validator(',
        content
    )
    
    return content

def fix_resource_helpers(content: str) -> str:
    """Fix resource helper usage."""
    # resources::temp_dir() -> tempfile::TempDir::new()
    content = re.sub(
        r'resources::temp_dir\(\)',
        r'tempfile::TempDir::new()',
        content
    )
    
    return content

def fix_test_macros(content: str) -> str:
    """Fix test macro usage patterns."""
    # Fix timeout specifications
    content = re.sub(
        r'#\[sinex_test\(timeout_ms = (\d+)\)\]',
        r'#[sinex_test(timeout = \1)]',
        content
    )
    
    # Fix ignore syntax
    content = re.sub(
        r'#\[ignore = "([^"]+)"\]',
        r'#[ignore] // \1',
        content
    )
    
    return content

def process_file(file_path: Path) -> bool:
    """Process a single test file."""
    print(f"Processing {file_path.name}...")
    
    try:
        content = file_path.read_text()
        original = content
        
        # Apply fixes
        content = remove_duplicate_imports(content)
        content = fix_test_concurrent_operations_macro(content)
        content = fix_missing_closing_braces(content)
        content = fix_insert_event_calls(content)
        content = fix_resource_helpers(content)
        content = fix_test_macros(content)
        
        # Write back if changed
        if content != original:
            file_path.write_text(content)
            print(f"  ✓ Fixed {file_path.name}")
            return True
        else:
            print(f"  - No changes needed for {file_path.name}")
            return False
    except Exception as e:
        print(f"  ✗ Error processing {file_path.name}: {e}")
        return False

def main():
    """Main fixing process."""
    print("🔧 Fixing Adversarial Tests")
    print("=" * 50)
    
    adversarial_dir = Path('test/adversarial')
    test_files = [
        'attack_simulation_test.rs',
        'boundary_test.rs', 
        'chaos_engineering_test.rs',
        'concurrency_test.rs',
        'enhanced_boundary_test.rs',
        'security_test.rs',
    ]
    
    fixed_count = 0
    
    for test_file in test_files:
        file_path = adversarial_dir / test_file
        if file_path.exists():
            if process_file(file_path):
                fixed_count += 1
        else:
            print(f"  ⚠️  {test_file} not found")
    
    print(f"\n✅ Fixed {fixed_count} files")
    print("\n📝 Next steps:")
    print("  1. Review the fixed files for any remaining issues")
    print("  2. Run 'cargo check --tests' to verify compilation")
    print("  3. Manually fix any complex patterns the script missed")

if __name__ == '__main__':
    main()
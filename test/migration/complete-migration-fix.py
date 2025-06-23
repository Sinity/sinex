#!/usr/bin/env python3
"""
Complete fix for migrated test files - adds imports and fixes all references
"""

import re
import sys
from pathlib import Path

def add_missing_imports(content: str) -> str:
    """Add missing imports for TestContext and sinex_test"""
    
    # Check if already has the imports
    if 'use crate::common::test_context::TestContext;' in content:
        return content
    
    # Find where to insert imports (after other use statements)
    lines = content.split('\n')
    last_use_idx = -1
    
    for i, line in enumerate(lines):
        if line.startswith('use '):
            last_use_idx = i
    
    if last_use_idx == -1:
        # No use statements, add at beginning after module docs
        for i, line in enumerate(lines):
            if not line.startswith('//') and line.strip():
                last_use_idx = i - 1
                break
    
    # Insert the imports
    imports = [
        'use crate::common::test_context::TestContext;',
        'use crate::common::sinex_test;',
    ]
    
    # Add anyhow::Result if we have #[sinex_test] but not the import
    if '#[sinex_test]' in content and 'use anyhow::Result;' not in content:
        imports.append('use anyhow::Result;')
    
    # Insert imports
    for imp in reversed(imports):
        lines.insert(last_use_idx + 1, imp)
    
    return '\n'.join(lines)

def fix_all_pool_references(content: str) -> str:
    """Fix all pool references comprehensively"""
    
    # All pool reference patterns
    replacements = [
        # Direct function calls with &pool
        (r'(\w+::)?(\w+)\(&pool\b', r'\1\2(ctx.pool()'),
        (r'(\w+::)?(\w+)\( &pool\b', r'\1\2( ctx.pool()'),
        (r', &pool\)', r', ctx.pool())'),
        (r' &pool,', r' ctx.pool(),'),
        (r'\(&pool,', r'(ctx.pool(),'),
        (r', &pool\b', r', ctx.pool()'),
        
        # Method calls on query results
        (r'\.fetch_one\(&pool\)', r'.fetch_one(ctx.pool())'),
        (r'\.fetch_all\(&pool\)', r'.fetch_all(ctx.pool())'),
        (r'\.fetch_optional\(&pool\)', r'.fetch_optional(ctx.pool())'),
        (r'\.execute\(&pool\)', r'.execute(ctx.pool())'),
        
        # pool variable references
        (r'\bpool\.', r'ctx.pool().'),
        
        # Special case for sqlx query macro
        (r'sqlx::query[_!]*\([^)]*\)\s*\.(\w+)\(&pool\)', r'sqlx::query\1(\2).\\3(ctx.pool())'),
    ]
    
    for pattern, replacement in replacements:
        content = re.sub(pattern, replacement, content)
    
    return content

def fix_test_function_signatures(content: str) -> str:
    """Ensure test functions have correct signatures"""
    
    # Fix functions that still have old signature
    content = re.sub(
        r'#\[sinex_test\]\s*\nasync fn (\w+)\(\) -> Result<\(\), Box<dyn std::error::Error>>',
        r'#[sinex_test]\nasync fn \1(ctx: TestContext) -> Result<()>',
        content
    )
    
    # Fix functions missing ctx parameter
    content = re.sub(
        r'#\[sinex_test\]\s*\nasync fn (\w+)\(\) -> Result<\(\)>',
        r'#[sinex_test]\nasync fn \1(ctx: TestContext) -> Result<()>',
        content
    )
    
    return content

def process_file(file_path: Path) -> bool:
    """Process a single file, return True if modified"""
    try:
        original_content = file_path.read_text()
        
        # Skip if not a migrated test file
        if '#[sinex_test]' not in original_content:
            return False
            
        # Apply all fixes
        new_content = original_content
        new_content = add_missing_imports(new_content)
        new_content = fix_all_pool_references(new_content)
        new_content = fix_test_function_signatures(new_content)
        
        # Only write if changed
        if new_content != original_content:
            file_path.write_text(new_content)
            print(f"✓ Fixed: {file_path}")
            return True
        else:
            return False
            
    except Exception as e:
        print(f"✗ Error processing {file_path}: {e}")
        return False

def main():
    """Main script"""
    # Find all test files
    test_dir = Path("test")
    test_files = list(test_dir.rglob("*_test*.rs"))
    
    print(f"Fixing {len(test_files)} test files")
    
    modified_count = 0
    for test_file in test_files:
        if process_file(test_file):
            modified_count += 1
    
    print(f"\nFixed {modified_count} files")
    
    if modified_count > 0:
        print("\nNow run: cargo check --tests")

if __name__ == "__main__":
    main()
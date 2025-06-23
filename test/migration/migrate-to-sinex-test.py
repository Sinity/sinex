#!/usr/bin/env python3
"""
Migrate tests from #[sqlx::test] to #[sinex_test]

This script performs a complete migration of test files to use the new
#[sinex_test] procedural macro with proper TestContext injection.
"""

import re
import sys
from pathlib import Path
from typing import List, Tuple

def migrate_test_file(content: str) -> str:
    """Migrate a single test file content"""
    
    # Step 1: Update imports
    content = re.sub(
        r'use crate::common::test_context::\{TestContext, TestConfig\};',
        'use crate::common::test_context::TestContext;\nuse crate::common::sinex_test;',
        content
    )
    
    # Add anyhow::Result import if not present
    if 'use anyhow::Result;' not in content and '#[sinex_test]' in content:
        # Find the last use statement
        last_use = max(m.end() for m in re.finditer(r'use .*;', content))
        content = content[:last_use] + '\nuse anyhow::Result;' + content[last_use:]
    
    # Step 2: Convert test functions
    # Pattern for #[sqlx::test] with pool parameter
    sqlx_test_pattern = re.compile(
        r'#\[sqlx::test\]\s*\n\s*async fn (\w+)\(pool: sqlx::PgPool\) -> Result<\(\), Box<dyn std::error::Error>>\s*\{',
        re.MULTILINE
    )
    
    # Replace with #[sinex_test]
    content = sqlx_test_pattern.sub(
        r'#[sinex_test]\nasync fn \1(ctx: TestContext) -> Result<()> {',
        content
    )
    
    # Step 3: Remove TestContext::with_pool creation
    ctx_creation_pattern = re.compile(
        r'\s*let ctx = TestContext::with_pool\(pool, TestConfig \{\s*\n\s*test_name: "[^"]*"\.to_string\(\),\s*\n\s*\.\.Default::default\(\)\s*\n\s*\}\)\.await\?;\s*\n',
        re.MULTILINE
    )
    content = ctx_creation_pattern.sub('', content)
    
    # Step 4: Update pool references
    # Direct &pool references
    content = re.sub(r'\(&pool\)', '(ctx.pool())', content)
    content = re.sub(r', &pool\)', ', ctx.pool())', content)
    content = re.sub(r' &pool,', ' ctx.pool(),', content)
    
    # Query pool references
    content = re.sub(
        r'\.fetch_(one|all|optional)\(&ctx\.pool\)',
        r'.fetch_\1(ctx.pool())',
        content
    )
    content = re.sub(
        r'\.execute\(&ctx\.pool\)',
        r'.execute(ctx.pool())',
        content
    )
    
    # Step 5: Update event builder pattern
    content = re.sub(
        r'ctx\.event_builder\(\)\s*\n\s*\.configure\(([^,]+), ([^)]+)\)',
        r'ctx.event_builder(\1, \2)',
        content
    )
    
    # Step 6: Clean up any remaining Result types
    content = re.sub(
        r'-> Result<\(\), Box<dyn std::error::Error>>',
        '-> Result<()>',
        content
    )
    
    return content

def process_file(file_path: Path) -> bool:
    """Process a single file, return True if modified"""
    try:
        original_content = file_path.read_text()
        
        # Skip if already migrated
        if '#[sinex_test]' in original_content:
            print(f"✓ Already migrated: {file_path}")
            return False
            
        # Skip if no sqlx tests
        if '#[sqlx::test]' not in original_content:
            print(f"- No sqlx tests: {file_path}")
            return False
        
        # Migrate the content
        new_content = migrate_test_file(original_content)
        
        # Only write if changed
        if new_content != original_content:
            file_path.write_text(new_content)
            print(f"✓ Migrated: {file_path}")
            return True
        else:
            print(f"- No changes: {file_path}")
            return False
            
    except Exception as e:
        print(f"✗ Error processing {file_path}: {e}")
        return False

def main():
    """Main migration script"""
    # Find all test files
    test_dir = Path("test")
    test_files = list(test_dir.rglob("*_test*.rs"))
    
    print(f"Found {len(test_files)} test files to check")
    
    modified_count = 0
    for test_file in test_files:
        if process_file(test_file):
            modified_count += 1
    
    print(f"\nMigration complete: {modified_count} files modified")
    
    # Compile check reminder
    if modified_count > 0:
        print("\nIMPORTANT: Run 'cargo check --tests' to verify the migration")

if __name__ == "__main__":
    main()
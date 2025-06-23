#!/usr/bin/env python3
"""
Fix remaining &pool references after migration to #[sinex_test]
"""

import re
import sys
from pathlib import Path

def fix_pool_references(content: str) -> str:
    """Fix all remaining pool references in migrated tests"""
    
    # Direct &pool references in function calls
    patterns = [
        # assertions::assert_event_inserted(&pool, ...)
        (r'assertions::assert_event_inserted\(&pool,', r'assertions::assert_event_inserted(ctx.pool(),'),
        # assertions::assert_event_insertion_fails(&pool, ...)
        (r'assertions::assert_event_insertion_fails\(&pool,', r'assertions::assert_event_insertion_fails(ctx.pool(),'),
        # common::event_exists(&pool, ...)
        (r'common::event_exists\(&pool,', r'common::event_exists(ctx.pool(),'),
        # common::get_event_by_id(&pool, ...)
        (r'common::get_event_by_id\(&pool,', r'common::get_event_by_id(ctx.pool(),'),
        # common::get_events_by_source(&pool, ...)
        (r'common::get_events_by_source\(&pool,', r'common::get_events_by_source(ctx.pool(),'),
        # common::get_recent_events(&pool, ...)
        (r'common::get_recent_events\(&pool,', r'common::get_recent_events(ctx.pool(),'),
        # queries::insert_event(&pool, ...)
        (r'queries::insert_event\(&pool,', r'queries::insert_event(ctx.pool(),'),
        # Any other function(&pool, ...) pattern
        (r'(\w+)\(&pool,', r'\1(ctx.pool(),'),
        # sqlx::query patterns
        (r'\.fetch_one\(&pool\)', r'.fetch_one(ctx.pool())'),
        (r'\.fetch_all\(&pool\)', r'.fetch_all(ctx.pool())'),
        (r'\.fetch_optional\(&pool\)', r'.fetch_optional(ctx.pool())'),
        (r'\.execute\(&pool\)', r'.execute(ctx.pool())'),
    ]
    
    for pattern, replacement in patterns:
        content = re.sub(pattern, replacement, content)
    
    return content

def process_file(file_path: Path) -> bool:
    """Process a single file, return True if modified"""
    try:
        original_content = file_path.read_text()
        
        # Only process files with #[sinex_test]
        if '#[sinex_test]' not in original_content:
            return False
            
        # Fix the content
        new_content = fix_pool_references(original_content)
        
        # Only write if changed
        if new_content != original_content:
            file_path.write_text(new_content)
            print(f"✓ Fixed pool references in: {file_path}")
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
    
    print(f"Checking {len(test_files)} test files for pool references")
    
    modified_count = 0
    for test_file in test_files:
        if process_file(test_file):
            modified_count += 1
    
    print(f"\nFixed pool references in {modified_count} files")

if __name__ == "__main__":
    main()
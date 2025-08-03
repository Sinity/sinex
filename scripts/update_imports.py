#!/usr/bin/env python3
"""Update imports from sinex_types, sinex_events, sinex_db to use sinex facade crate."""

import re
import os
from pathlib import Path

def update_imports_in_file(file_path):
    """Update imports in a single file."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    original_content = content
    
    # Pattern to match use statements
    patterns = [
        # Simple imports like: use sinex_types::error::SinexError;
        (r'use sinex_types::error::{([^}]+)};', r'use sinex::{\\1};'),
        (r'use sinex_types::domain::{([^}]+)};', r'use sinex::domain::{\\1};'),
        (r'use sinex_types::{([^}]+)};', r'use sinex::{\\1};'),
        (r'use sinex_types::([^;]+);', r'use sinex::\\1;'),
        
        # Events
        (r'use sinex_events::{([^}]+)};', r'use sinex::{\\1};'),
        (r'use sinex_events::([^;]+);', r'use sinex::\\1;'),
        
        # DB
        (r'use sinex_db::models::{([^}]+)};', r'use sinex::models::{\\1};'),
        (r'use sinex_db::models::([^;]+);', r'use sinex::models::\\1;'),
        (r'use sinex_db::repositories::{([^}]+)};', r'use sinex::repositories::{\\1};'),
        (r'use sinex_db::repositories::([^;]+);', r'use sinex::repositories::\\1;'),
        (r'use sinex_db::{([^}]+)};', r'use sinex::{\\1};'),
        (r'use sinex_db::([^;]+);', r'use sinex::\\1;'),
    ]
    
    for pattern, replacement in patterns:
        content = re.sub(pattern, replacement, content, flags=re.MULTILINE)
    
    # Check if we made any changes
    if content != original_content:
        with open(file_path, 'w') as f:
            f.write(content)
        return True
    return False

def main():
    """Update all Rust files in sinex-test-utils."""
    test_utils_dir = Path("/realm/project/sinex/crate/sinex-test-utils/src")
    
    updated_files = []
    for rust_file in test_utils_dir.rglob("*.rs"):
        if update_imports_in_file(rust_file):
            updated_files.append(rust_file)
    
    print(f"Updated {len(updated_files)} files:")
    for f in updated_files:
        print(f"  - {f}")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""Replace raw SQL queries for processor_manifests with query builder calls."""

import re
import os
from pathlib import Path
from typing import List, Tuple

def find_test_files(base_path: str) -> List[Path]:
    """Find all Rust test files."""
    test_dirs = ["test/integration", "test/property", "test/unit", "test/system"]
    files = []
    for dir_path in test_dirs:
        full_path = Path(base_path) / dir_path
        if full_path.exists():
            files.extend(full_path.glob("**/*.rs"))
    return files

def replace_simple_inserts(content: str) -> Tuple[str, int]:
    """Replace simple INSERT statements."""
    count = 0
    
    # Pattern for simple processor manifest inserts
    pattern = r'''sqlx::query\(!?\s*
        "INSERT\s+INTO\s+core\.processor_manifests\s+\(processor_name,\s*processor_type,\s*processor_version,\s*hostname\)\s*
         VALUES\s+\(\$1,\s*'automaton',\s*\$2,\s*\$3\)"(?:,)?
    \s*([^)]+)?\s*\)\s*
    \.bind\(([^)]+)\)\s*
    \.bind\(([^)]+)\)\s*
    \.bind\(([^)]+)\)'''
    
    def replacement(match):
        nonlocal count
        count += 1
        processor_name = match.group(2)
        processor_version = match.group(3)
        hostname = match.group(4)
        return f'''ProcessorManifestQueries::insert_manifest(
        {processor_name}.to_string(),
        "automaton".to_string(),
        {processor_version}.to_string(),
        {hostname}.to_string(),
    )'''
    
    content = re.sub(pattern, replacement, content, flags=re.MULTILINE | re.DOTALL | re.VERBOSE)
    return content, count

def replace_simple_deletes(content: str) -> Tuple[str, int]:
    """Replace simple DELETE statements."""
    count = 0
    
    # Pattern for simple deletes by name
    pattern = r'''sqlx::query\(!?\s*
        "DELETE\s+FROM\s+core\.processor_manifests\s+WHERE\s+processor_name\s*=\s*\$1"(?:,)?
    \s*([^)]+)?\s*\)\s*
    \.bind\(([^)]+)\)'''
    
    def replacement(match):
        nonlocal count
        count += 1
        processor_name = match.group(2)
        return f'ProcessorManifestQueries::delete_manifest({processor_name}.to_string())'
    
    content = re.sub(pattern, replacement, content, flags=re.MULTILINE | re.DOTALL | re.VERBOSE)
    
    # Pattern for deletes by name and type
    pattern2 = r'''sqlx::query\(!?\s*
        "DELETE\s+FROM\s+core\.processor_manifests\s+WHERE\s+processor_name\s*=\s*\$1\s+AND\s+processor_type\s*=\s*'automaton'"(?:,)?
    \s*([^)]+)?\s*\)\s*
    \.bind\(([^)]+)\)'''
    
    def replacement2(match):
        nonlocal count
        count += 1
        processor_name = match.group(2)
        return f'ProcessorManifestQueries::delete_manifest_by_type({processor_name}.to_string(), "automaton".to_string())'
    
    content = re.sub(pattern2, replacement2, content, flags=re.MULTILINE | re.DOTALL | re.VERBOSE)
    
    return content, count

def add_imports(content: str) -> str:
    """Add necessary imports if not present."""
    # Check if ProcessorManifestQueries is already imported
    if "ProcessorManifestQueries" not in content:
        # Find the last use statement
        use_pattern = r'(use\s+[^;]+;)'
        matches = list(re.finditer(use_pattern, content))
        if matches:
            last_use_pos = matches[-1].end()
            # Add the import after the last use statement
            content = content[:last_use_pos] + "\nuse sinex_db::queries::ProcessorManifestQueries;" + content[last_use_pos:]
    
    return content

def process_file(file_path: Path) -> Tuple[int, int]:
    """Process a single file and return counts."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    original_content = content
    total_replacements = 0
    
    # Apply replacements
    content, insert_count = replace_simple_inserts(content)
    total_replacements += insert_count
    
    content, delete_count = replace_simple_deletes(content)
    total_replacements += delete_count
    
    # Add imports if we made changes
    if total_replacements > 0:
        content = add_imports(content)
        
        # Write back only if changes were made
        if content != original_content:
            with open(file_path, 'w') as f:
                f.write(content)
            return total_replacements, 1
    
    return 0, 0

def main():
    """Main entry point."""
    base_path = "/realm/project/sinex"
    test_files = find_test_files(base_path)
    
    total_replacements = 0
    files_modified = 0
    
    for file_path in test_files:
        replacements, modified = process_file(file_path)
        total_replacements += replacements
        files_modified += modified
        
        if modified > 0:
            print(f"Modified {file_path}: {replacements} replacements")
    
    print(f"\nSummary:")
    print(f"Total files modified: {files_modified}")
    print(f"Total queries replaced: {total_replacements}")

if __name__ == "__main__":
    main()
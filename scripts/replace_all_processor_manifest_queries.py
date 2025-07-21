#!/usr/bin/env python3
"""Replace all processor_manifests raw SQL queries with query builders."""

import re
from pathlib import Path
from typing import List, Tuple

def replace_queries_in_file(file_path: Path) -> int:
    """Replace queries in a single file."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    original = content
    count = 0
    
    # Replace INSERT queries
    insert_pattern = r'''(\s*)//.*processor_manifests.*query builders.*\n\1sqlx::query!\(\s*\n\1\s*"INSERT INTO core\.processor_manifests \(processor_name, processor_type, processor_version, hostname\)\s*\n\1\s*VALUES \(\$1, 'automaton', '1\.0\.0', 'test-host'\)",\s*\n\1\s*([^\n]+)\s*\n\1\s*\)'''
    
    def insert_replacement(match):
        nonlocal count
        count += 1
        indent = match.group(1)
        var_name = match.group(2)
        return f'''{indent}ProcessorManifestQueries::insert_manifest(
{indent}    {var_name}.to_string(),
{indent}    "automaton".to_string(),
{indent}    "1.0.0".to_string(),
{indent}    "test-host".to_string(),
{indent})'''
    
    content = re.sub(insert_pattern, insert_replacement, content, flags=re.MULTILINE)
    
    # Replace DELETE queries
    delete_pattern = r'''(\s*)//.*processor_manifests.*query builders.*\n\1sqlx::query!\(\s*\n\1\s*"DELETE FROM core\.processor_manifests WHERE processor_name = \$1",\s*\n\1\s*([^\n]+)\s*\n\1\s*\)'''
    
    def delete_replacement(match):
        nonlocal count
        count += 1
        indent = match.group(1)
        var_name = match.group(2)
        return f'{indent}ProcessorManifestQueries::delete_manifest({var_name}.to_string())'
    
    content = re.sub(delete_pattern, delete_replacement, content, flags=re.MULTILINE)
    
    # Add import if needed and changes were made
    if count > 0 and "ProcessorManifestQueries" not in content:
        # Find the use statements section
        if "use sinex_db::queries::" in content:
            # Add to existing sinex_db::queries import
            content = re.sub(
                r'(use sinex_db::queries::{)([^}]+)(})',
                r'\1\2, ProcessorManifestQueries\3',
                content
            )
        else:
            # Add new import after last use statement
            last_use = list(re.finditer(r'use\s+[^;]+;', content))
            if last_use:
                pos = last_use[-1].end()
                content = content[:pos] + "\nuse sinex_db::queries::ProcessorManifestQueries;" + content[pos:]
    
    if content != original:
        with open(file_path, 'w') as f:
            f.write(content)
    
    return count

def main():
    files = [
        "/realm/project/sinex/test/integration/checkpoint_consistency_test.rs",
        "/realm/project/sinex/test/integration/checkpoint_consistency_test_refactored.rs",
        "/realm/project/sinex/test/integration/data_corruption_detection_test.rs",
    ]
    
    total = 0
    for file_path in files:
        path = Path(file_path)
        if path.exists():
            count = replace_queries_in_file(path)
            if count > 0:
                print(f"Replaced {count} queries in {path.name}")
            total += count
    
    print(f"\nTotal queries replaced: {total}")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Bulk import consolidation using the test prelude.

This script identifies files with many imports that could benefit from
using the test prelude and automatically applies the consolidation.
"""

import re
import sys
import os
from pathlib import Path

# Common imports covered by the prelude
PRELUDE_IMPORTS = {
    "anyhow::Result",
    "anyhow::{Result, Context as AnyhowContext}",
    "std::sync::{Arc, atomic::{AtomicBool, AtomicU64, AtomicU32, AtomicUsize, Ordering}}",
    "std::time::{Duration, Instant}",
    "std::collections::{HashMap, HashSet}",
    "serde::{Serialize, Deserialize}",
    "serde_json::{json, Value}",
    "sinex_ulid::Ulid",
    "sinex_core::{EventSource, EventSourceContext, RawEvent, RawEventBuilder}",
    "sqlx::PgPool",
    "tokio::sync::{mpsc, Barrier, Mutex, RwLock}",
    "tokio::time::{sleep, timeout, interval}",
    "futures::future::join_all",
    "tempfile::TempDir",
    "async_trait::async_trait",
    "rand::Rng",
}

def analyze_imports(filepath):
    """Analyze imports in a file and determine prelude benefit."""
    with open(filepath, 'r') as f:
        content = f.read()
    
    lines = content.split('\n')
    use_statements = []
    
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith('use ') and not stripped.startswith('use crate::common::prelude'):
            use_statements.append((i, stripped))
    
    # Count how many imports could be covered by prelude
    covered_count = 0
    for _, use_stmt in use_statements:
        for prelude_import in PRELUDE_IMPORTS:
            if prelude_import in use_stmt:
                covered_count += 1
                break
    
    total_imports = len(use_statements)
    coverage_percentage = (covered_count / total_imports * 100) if total_imports > 0 else 0
    
    return {
        'total_imports': total_imports,
        'covered_imports': covered_count,
        'coverage_percentage': coverage_percentage,
        'use_statements': use_statements,
        'worth_converting': total_imports >= 8 and coverage_percentage >= 60
    }

def consolidate_imports(filepath):
    """Consolidate imports using the prelude."""
    analysis = analyze_imports(filepath)
    
    if not analysis['worth_converting']:
        print(f"⚠ Skipping {filepath}: Only {analysis['total_imports']} imports, {analysis['coverage_percentage']:.1f}% coverage")
        return False
    
    with open(filepath, 'r') as f:
        content = f.read()
    
    lines = content.split('\n')
    
    # Find import block boundaries
    first_import = None
    last_import = None
    
    for i, line in enumerate(lines):
        if line.strip().startswith('use '):
            if first_import is None:
                first_import = i
            last_import = i
    
    if first_import is None:
        print(f"⚠ No imports found in {filepath}")
        return False
    
    # Collect imports to keep (not covered by prelude)
    imports_to_keep = []
    for _, use_stmt in analysis['use_statements']:
        keep_import = True
        for prelude_import in PRELUDE_IMPORTS:
            if prelude_import in use_stmt:
                keep_import = False
                break
        if keep_import and 'crate::common::prelude' not in use_stmt:
            imports_to_keep.append(use_stmt)
    
    # Build new import block
    new_imports = ['use crate::common::prelude::*;']
    
    if imports_to_keep:
        new_imports.append('')
        new_imports.append('// Project-specific imports not covered by prelude')
        new_imports.extend(imports_to_keep)
    
    # Replace import block
    new_lines = lines[:first_import] + new_imports + lines[last_import + 1:]
    
    with open(filepath, 'w') as f:
        f.write('\n'.join(new_lines))
    
    reduction = analysis['total_imports'] - len(imports_to_keep)
    percentage_reduction = (reduction / analysis['total_imports']) * 100
    
    print(f"✅ {filepath}: {analysis['total_imports']} → {len(imports_to_keep)} imports ({percentage_reduction:.1f}% reduction)")
    return True

def process_directory(directory):
    """Process all Rust files in a directory."""
    modified_files = []
    test_dir = Path(directory)
    
    print(f"🔍 Analyzing import patterns in {directory}...")
    
    # First pass: analyze all files
    candidates = []
    for rust_file in test_dir.rglob("*.rs"):
        if 'target/' in str(rust_file) or 'automation/' in str(rust_file):
            continue
            
        analysis = analyze_imports(rust_file)
        if analysis['worth_converting']:
            candidates.append((rust_file, analysis))
    
    print(f"📊 Found {len(candidates)} files worth converting")
    
    # Second pass: consolidate imports
    for rust_file, analysis in candidates:
        if consolidate_imports(rust_file):
            modified_files.append(rust_file)
    
    return modified_files

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 bulk-import-consolidator.py <directory>")
        print("Example: python3 bulk-import-consolidator.py test/")
        sys.exit(1)
    
    directory = sys.argv[1]
    if not os.path.exists(directory):
        print(f"Directory {directory} does not exist")
        sys.exit(1)
    
    print(f"🔧 Consolidating imports in {directory} using test prelude...")
    modified_files = process_directory(directory)
    
    print(f"\n✅ Modified {len(modified_files)} files:")
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
#!/usr/bin/env python3
"""
Restore and refactor deleted adversarial tests to use the modern test framework.

This script:
1. Recovers deleted test files from git history
2. Refactors them to use the modern test abstractions
3. Preserves critical security test scenarios
"""

import re
import sys
import subprocess
from pathlib import Path
from typing import List, Tuple

def run_cmd(cmd: List[str]) -> str:
    """Run command and return output."""
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error running {' '.join(cmd)}: {result.stderr}")
        sys.exit(1)
    return result.stdout

def recover_file(commit: str, file_path: str, output_path: str) -> bool:
    """Recover a file from git history."""
    try:
        content = run_cmd(['git', 'show', f'{commit}:{file_path}'])
        Path(output_path).write_text(content)
        print(f"✓ Recovered {file_path}")
        return True
    except Exception as e:
        print(f"✗ Failed to recover {file_path}: {e}")
        return False

def refactor_imports(content: str) -> str:
    """Replace old imports with modern prelude."""
    # Remove old imports
    lines = content.split('\n')
    new_lines = []
    skip_imports = False
    
    for line in lines:
        if line.startswith('use ') and not skip_imports:
            # Skip old imports until we find the first non-import line
            if 'crate::common::prelude::*' in line:
                new_lines.append(line)
                skip_imports = True
            continue
        else:
            new_lines.append(line)
    
    # Add modern import at the beginning (after module doc comments)
    import_added = False
    final_lines = []
    for i, line in enumerate(new_lines):
        if not import_added and not line.startswith('//') and line.strip():
            final_lines.append('use crate::common::prelude::*;')
            final_lines.append('')
            import_added = True
        final_lines.append(line)
    
    return '\n'.join(final_lines)

def refactor_test_functions(content: str) -> str:
    """Convert test functions to use modern test framework."""
    # Replace #[tokio::test] with #[sinex_test]
    content = re.sub(r'#\[tokio::test\]', '#[sinex_test]', content)
    
    # Add ctx parameter to async test functions
    content = re.sub(
        r'async fn (test_\w+)\(\) -> TestResult \{',
        r'async fn \1(ctx: TestContext) -> TestResult {',
        content
    )
    
    # Replace direct pool creation with ctx.pool()
    content = re.sub(
        r'let pool = .*create_test_pool.*\(\).*?;',
        r'let pool = ctx.pool().clone();',
        content
    )
    
    # Replace insert_event calls with ctx helper
    content = re.sub(
        r'insert_event\(&pool,',
        r'ctx.insert_event(',
        content
    )
    
    # Replace TestDatabase::new() with ctx usage
    content = re.sub(
        r'let .*db.*=.*TestDatabase::new\(\).*?;',
        r'// Database setup handled by TestContext',
        content
    )
    
    return content

def main():
    """Main restoration process."""
    print("🔧 Restoring Adversarial Tests")
    print("=" * 50)
    
    # Files to restore (commit before deletion)
    files_to_restore = [
        ('1078c25~1', 'test/adversarial/attack_simulation_test.rs'),
        ('1078c25~1', 'test/adversarial/boundary_test.rs'),
        ('1078c25~1', 'test/adversarial/chaos_engineering_test.rs'),
        ('1078c25~1', 'test/adversarial/concurrency_test.rs'),
        ('1078c25~1', 'test/adversarial/enhanced_boundary_test.rs'),
        ('1078c25~1', 'test/adversarial/security_test.rs'),
    ]
    
    # Create adversarial directory if it doesn't exist
    adversarial_dir = Path('test/adversarial')
    adversarial_dir.mkdir(parents=True, exist_ok=True)
    
    restored_files = []
    
    # Recover and refactor each file
    for commit, file_path in files_to_restore:
        filename = Path(file_path).name
        temp_path = f'/tmp/{filename}'
        final_path = adversarial_dir / filename
        
        # Recover file
        if recover_file(commit, file_path, temp_path):
            # Read content
            content = Path(temp_path).read_text()
            
            # Apply refactoring
            print(f"📝 Refactoring {filename}...")
            content = refactor_imports(content)
            content = refactor_test_functions(content)
            
            # Write refactored content
            final_path.write_text(content)
            restored_files.append(filename.replace('.rs', ''))
            print(f"✓ Refactored and saved {filename}")
    
    # Update mod.rs
    print("\n📝 Updating mod.rs...")
    mod_content = (adversarial_dir / 'mod.rs').read_text()
    
    # Add module declarations
    module_declarations = []
    for module in restored_files:
        if f'mod {module};' not in mod_content:
            module_declarations.append(f'mod {module};')
    
    if module_declarations:
        # Find where to insert (after existing mod declarations or at the beginning)
        lines = mod_content.split('\n')
        insert_pos = 0
        for i, line in enumerate(lines):
            if line.startswith('mod ') or line.startswith('pub mod '):
                insert_pos = i + 1
        
        # Insert new declarations
        for decl in module_declarations:
            lines.insert(insert_pos, decl)
            insert_pos += 1
        
        mod_content = '\n'.join(lines)
        (adversarial_dir / 'mod.rs').write_text(mod_content)
        print(f"✓ Added {len(module_declarations)} module declarations to mod.rs")
    
    print("\n✅ Restoration complete!")
    print(f"📁 Restored {len(restored_files)} test files")
    print("\n⚠️  Manual review recommended for:")
    print("  - Complex async patterns")
    print("  - Direct SQL queries (convert to QueryBuilder)")
    print("  - Config-based tests (update for env-only config)")
    print("  - Resource cleanup patterns")

if __name__ == '__main__':
    main()
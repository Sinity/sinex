#!/usr/bin/env python3
"""
Automated test migration tool for Sinex test framework.
Migrates tests from #[tokio::test] to #[sinex_test] pattern.
"""

import re
import sys
import os
from pathlib import Path
from typing import List, Tuple, Optional, Dict
import subprocess
import difflib
from dataclasses import dataclass

@dataclass
class MigrationChange:
    line_num: int
    original: str
    replacement: str
    description: str

class TestMigrator:
    def __init__(self, dry_run: bool = False, verbose: bool = False, quality: bool = False):
        self.dry_run = dry_run
        self.verbose = verbose
        self.quality = quality
        self.migration_stats = {
            'files_processed': 0,
            'tests_migrated': 0,
            'files_modified': 0,
            'errors': [],
            'warnings': []
        }
    
    def migrate_test_signature(self, content: str) -> Tuple[str, int, List[MigrationChange]]:
        """Migrate test function signatures from tokio::test to sinex_test."""
        count = 0
        changes = []
        lines = content.split('\n')
        
        # Track line numbers for better dry-run output
        for i in range(len(lines)):
            line = lines[i]
            
            # Check for #[tokio::test]
            if line.strip() == '#[tokio::test]':
                # Look ahead for the async fn
                if i + 1 < len(lines):
                    next_line = lines[i + 1]
                    fn_match = re.match(r'\s*async\s+fn\s+(\w+)\s*\(\s*\)', next_line)
                    if fn_match:
                        count += 1
                        test_name = fn_match.group(1)
                        
                        # Check if there's a return type
                        if '->' in next_line:
                            # Has return type, replace entire signature
                            new_signature = f'async fn {test_name}(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>>'
                            # Handle various return types including anyhow::Error, sqlx::Result, etc.
                            lines[i + 1] = re.sub(
                                r'async\s+fn\s+\w+\s*\(\s*\)\s*->\s*(?:Result<\(\)(?:,\s*[^>]+)?>\s*|anyhow::Result<\(\)>\s*|sqlx::Result<\(\)>\s*)',
                                new_signature + ' ',
                                next_line
                            )
                        else:
                            # No return type, need to handle the brace
                            if '{' in next_line:
                                # Brace on same line
                                lines[i + 1] = next_line.replace(
                                    f'async fn {test_name}()',
                                    f'async fn {test_name}(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>>'
                                )
                            else:
                                # Brace might be on next line
                                lines[i + 1] = next_line.replace(
                                    f'async fn {test_name}()',
                                    f'async fn {test_name}(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>>'
                                )
                        
                        # Replace the attribute
                        changes.append(MigrationChange(
                            line_num=i + 1,
                            original='#[tokio::test]',
                            replacement='#[sinex_test]',
                            description=f'Migrate test attribute for {test_name}'
                        ))
                        lines[i] = line.replace('#[tokio::test]', '#[sinex_test]')
        
        return '\n'.join(lines), count, changes
    
    def migrate_pool_usage(self, content: str) -> Tuple[str, List[str], List[MigrationChange]]:
        """Replace pool initialization and usage patterns."""
        warnings = []
        changes = []
        lines = content.split('\n')
        
        for i, line in enumerate(lines):
            original_line = line
            
            # Pattern: let pool = get_shared_test_pool().await?;
            if 'get_shared_test_pool' in line:
                line = re.sub(
                    r'let\s+(\w+)\s*=\s*(?:database_helpers::)?get_shared_test_pool\(\)\.await\?;',
                    r'let \1 = ctx.pool();',
                    line
                )
                if line != original_line:
                    changes.append(MigrationChange(
                        line_num=i + 1,
                        original=original_line.strip(),
                        replacement=line.strip(),
                        description='Replace get_shared_test_pool with ctx.pool()'
                    ))
            
            # Pattern: let pool = TestPool::new().await?;
            if 'TestPool::new' in line:
                line = re.sub(
                    r'let\s+(\w+)\s*=\s*TestPool::new\(\)\.await\?;',
                    r'let \1 = ctx.pool();',
                    line
                )
                if line != original_line:
                    changes.append(MigrationChange(
                        line_num=i + 1,
                        original=original_line.strip(),
                        replacement=line.strip(),
                        description='Replace TestPool::new with ctx.pool()'
                    ))
            
            # Pattern: TestPool::with_strategy(...)
            if 'TestPool::with_strategy' in line:
                line = re.sub(
                    r'let\s+(\w+)\s*=\s*TestPool::with_strategy\([^)]+\)\.await[^;]*;',
                    r'let \1 = ctx.pool();',
                    line
                )
                if line != original_line:
                    changes.append(MigrationChange(
                        line_num=i + 1,
                        original=original_line.strip(),
                        replacement=line.strip(),
                        description='Replace TestPool::with_strategy with ctx.pool()'
                    ))
            
            # Pattern: create_test_pool
            if 'create_test_pool' in line:
                line = re.sub(
                    r'let\s+(\w+)\s*=\s*(?:database_helpers::)?create_test_pool\(\)\.await\?;',
                    r'let \1 = ctx.pool();',
                    line
                )
                if line != original_line:
                    changes.append(MigrationChange(
                        line_num=i + 1,
                        original=original_line.strip(),
                        replacement=line.strip(),
                        description='Replace create_test_pool with ctx.pool()'
                    ))
            
            # Replace &pool with ctx.pool() (but not in strings or comments)
            if '&pool' in line and not line.strip().startswith('//') and '"' not in line:
                line = re.sub(r'\b&pool\b', 'ctx.pool()', line)
                if line != original_line:
                    changes.append(MigrationChange(
                        line_num=i + 1,
                        original=original_line.strip(),
                        replacement=line.strip(),
                        description='Replace &pool reference with ctx.pool()'
                    ))
            
            # Handle pool.clone() patterns
            if 'pool.clone()' in line:
                line = re.sub(r'\bpool\.clone\(\)', 'ctx.pool().clone()', line)
                if line != original_line:
                    changes.append(MigrationChange(
                        line_num=i + 1,
                        original=original_line.strip(),
                        replacement=line.strip(),
                        description='Replace pool.clone() with ctx.pool().clone()'
                    ))
            
            lines[i] = line
        
        # Check for complex patterns
        content = '\n'.join(lines)
        if re.search(r'pool\s*:\s*(?:PgPool|Pool<Postgres>)', content):
            warnings.append("Complex pool type annotation detected - manual review recommended")
        
        return content, warnings, changes
    
    def add_missing_ok_returns(self, content: str) -> Tuple[str, List[MigrationChange]]:
        """Add Ok(()) returns to tests that don't have them."""
        changes = []
        lines = content.split('\n')
        
        # Track if we're in a sinex_test function
        in_sinex_test = False
        brace_depth = 0
        test_start_line = -1
        
        for i, line in enumerate(lines):
            # Check if we're starting a sinex_test
            if '#[sinex_test]' in line:
                in_sinex_test = True
                test_start_line = i
                continue
            
            if in_sinex_test:
                # Count braces to track function body
                brace_depth += line.count('{') - line.count('}')
                
                # If we've closed all braces, we're done with this test
                if brace_depth == 0 and '{' in line:
                    # Check if the last non-empty line before closing brace has Ok(())
                    j = i - 1
                    while j > test_start_line and not lines[j].strip():
                        j -= 1
                    
                    if j > test_start_line:
                        last_line = lines[j].strip()
                        # If it doesn't end with Ok(()) or a return statement, add Ok(())
                        if not (last_line.endswith('Ok(())') or 
                                last_line.startswith('Ok(') or 
                                last_line.startswith('return') or
                                last_line == '}'):
                            # Insert Ok(()) before the closing brace
                            indent = '    '  # Default 4 spaces
                            # Try to match indentation of previous line
                            if lines[j]:
                                indent_match = re.match(r'^(\s*)', lines[j])
                                if indent_match:
                                    indent = indent_match.group(1)
                            
                            lines.insert(i, f'{indent}Ok(())')
                            changes.append(MigrationChange(
                                line_num=i + 1,
                                original='',
                                replacement=f'{indent}Ok(())',
                                description='Add missing Ok(()) return'
                            ))
                    
                    in_sinex_test = False
                    brace_depth = 0
        
        return '\n'.join(lines), changes
    
    def fix_unwraps(self, content: str) -> Tuple[str, List[MigrationChange]]:
        """Replace .unwrap() with ? operator where appropriate."""
        changes = []
        lines = content.split('\n')
        
        for i, line in enumerate(lines):
            original_line = line
            
            # Skip if it's a comment or string
            if line.strip().startswith('//') or '".unwrap()"' in line:
                continue
            
            # Pattern: .unwrap() at end of statement
            if '.unwrap();' in line:
                line = line.replace('.unwrap();', '?;')
                changes.append(MigrationChange(
                    line_num=i + 1,
                    original=original_line.strip(),
                    replacement=line.strip(),
                    description='Replace .unwrap() with ? operator'
                ))
            
            # Pattern: .unwrap() in assignments or expressions
            elif '.unwrap()' in line and not line.strip().endswith('.unwrap()'):
                # This is trickier - might be in middle of expression
                # For now, mark for manual review
                if '.unwrap().' in line or '.unwrap(),' in line:
                    # Don't auto-replace these complex cases
                    pass
            
            lines[i] = line
        
        return '\n'.join(lines), changes
    
    def fix_println(self, content: str) -> Tuple[str, List[MigrationChange]]:
        """Replace println! with tracing::info! or remove debug prints."""
        changes = []
        lines = content.split('\n')
        
        for i, line in enumerate(lines):
            original_line = line
            
            # Skip if it's a comment
            if line.strip().startswith('//'):
                continue
            
            # Pattern: println!("...") - likely debug output
            if 'println!' in line:
                # Check if it looks like debug output
                if any(marker in line.lower() for marker in ['debug', 'test', 'here', 'xxx', '---']):
                    # Remove debug prints
                    lines[i] = ''  # Remove the line
                    changes.append(MigrationChange(
                        line_num=i + 1,
                        original=original_line.strip(),
                        replacement='',
                        description='Remove debug println!'
                    ))
                else:
                    # Convert to tracing
                    line = line.replace('println!', 'tracing::info!')
                    lines[i] = line
                    changes.append(MigrationChange(
                        line_num=i + 1,
                        original=original_line.strip(),
                        replacement=line.strip(),
                        description='Replace println! with tracing::info!'
                    ))
        
        return '\n'.join(lines), changes
    
    def add_assert_messages(self, content: str) -> Tuple[str, List[MigrationChange]]:
        """Add descriptive messages to assertions."""
        changes = []
        lines = content.split('\n')
        
        for i, line in enumerate(lines):
            original_line = line
            
            # Skip if it's a comment
            if line.strip().startswith('//'):
                continue
            
            # Pattern: assert_eq!(a, b); without message
            match = re.search(r'assert_eq!\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*\);', line)
            if match:
                left = match.group(1).strip()
                right = match.group(2).strip()
                
                # Generate a reasonable message
                if 'len()' in left or 'len()' in right:
                    message = "length mismatch"
                elif 'is_empty()' in left or 'is_empty()' in right:
                    message = "expected empty but was not"
                elif 'contains' in left or 'contains' in right:
                    message = "substring not found"
                else:
                    # Generic message based on variable names
                    message = f"expected {right} but got {left}"
                
                new_line = line.replace(
                    match.group(0),
                    f'assert_eq!({left}, {right}, "{message}");'
                )
                
                if new_line != line:
                    lines[i] = new_line
                    changes.append(MigrationChange(
                        line_num=i + 1,
                        original=original_line.strip(),
                        replacement=new_line.strip(),
                        description='Add descriptive message to assertion'
                    ))
            
            # Pattern: assert!(condition); without message
            match = re.search(r'assert!\s*\(\s*([^)]+)\s*\);', line)
            if match and ',' not in match.group(1):  # No existing message
                condition = match.group(1).strip()
                
                # Generate message based on condition
                if '!' in condition:
                    message = f"expected {condition} to be true"
                elif '.is_' in condition:
                    message = f"condition {condition} failed"
                else:
                    message = f"assertion failed: {condition}"
                
                new_line = line.replace(
                    match.group(0),
                    f'assert!({condition}, "{message}");'
                )
                
                if new_line != line:
                    lines[i] = new_line
                    changes.append(MigrationChange(
                        line_num=i + 1,
                        original=original_line.strip(),
                        replacement=new_line.strip(),
                        description='Add descriptive message to assertion'
                    ))
        
        return '\n'.join(lines), changes
    
    def fix_imports(self, content: str) -> Tuple[str, List[MigrationChange]]:
        """Fix imports for migrated tests."""
        lines = content.split('\n')
        changes = []
        has_test_context_import = False
        import_insert_idx = 0
        
        # First pass: remove old imports and find where to insert
        new_lines = []
        for i, line in enumerate(lines):
            # Skip old imports
            if any(pattern in line for pattern in [
                'get_shared_test_pool',
                'TestPool',
                'create_test_pool'
            ]) and 'use' in line:
                changes.append(MigrationChange(
                    line_num=i + 1,
                    original=line.strip(),
                    replacement='',
                    description='Remove obsolete import'
                ))
                continue
            
            # Check for existing TestContext import
            if 'TestContext' in line or 'common::prelude::*' in line:
                has_test_context_import = True
            
            # Track where to insert import
            if line.strip().startswith('use '):
                import_insert_idx = len(new_lines) + 1
            
            new_lines.append(line)
        
        # Add TestContext import if missing
        if not has_test_context_import:
            import_line = 'use crate::common::prelude::*;'
            if import_insert_idx > 0:
                new_lines.insert(import_insert_idx, import_line)
                changes.append(MigrationChange(
                    line_num=import_insert_idx + 1,
                    original='',
                    replacement=import_line,
                    description='Add TestContext import'
                ))
            else:
                # No imports found, add at beginning
                new_lines.insert(0, import_line)
                new_lines.insert(1, '')
                changes.append(MigrationChange(
                    line_num=1,
                    original='',
                    replacement=import_line,
                    description='Add TestContext import at beginning'
                ))
        
        return '\n'.join(new_lines), changes
    
    def migrate_file(self, file_path: Path) -> bool:
        """Migrate a single test file."""
        try:
            original_content = file_path.read_text()
            content = original_content
            
            # Check if file needs any migration
            needs_migration = False
            if '#[tokio::test]' in content:
                needs_migration = True
            elif any(pattern in content for pattern in [
                'TestPool::new',
                'TestPool::with_strategy',
                'get_shared_test_pool', 
                'create_test_pool'
            ]):
                needs_migration = True
            
            if not needs_migration:
                return True
            
            all_changes = []
            
            # Apply migrations
            content, test_count, sig_changes = self.migrate_test_signature(content)
            all_changes.extend(sig_changes)
            
            content, warnings, pool_changes = self.migrate_pool_usage(content)
            all_changes.extend(pool_changes)
            
            content, import_changes = self.fix_imports(content)
            all_changes.extend(import_changes)
            
            content, ok_changes = self.add_missing_ok_returns(content)
            all_changes.extend(ok_changes)
            
            # Apply quality improvements if requested
            if self.quality:
                content, unwrap_changes = self.fix_unwraps(content)
                all_changes.extend(unwrap_changes)
                
                content, println_changes = self.fix_println(content)
                all_changes.extend(println_changes)
                
                content, assert_changes = self.add_assert_messages(content)
                all_changes.extend(assert_changes)
            
            # Add warnings to stats
            self.migration_stats['warnings'].extend(
                [(str(file_path), w) for w in warnings]
            )
            
            # Handle the result
            if content != original_content:
                self.migration_stats['tests_migrated'] += test_count
                self.migration_stats['files_modified'] += 1
                
                if self.dry_run:
                    print(f"\n📄 {file_path}")
                    print(f"   Would migrate {test_count} tests")
                    
                    if self.verbose and all_changes:
                        print("\n   Changes:")
                        for change in all_changes[:10]:  # Show first 10 changes
                            print(f"   Line {change.line_num}: {change.description}")
                            if change.original:
                                print(f"     - {change.original}")
                            if change.replacement:
                                print(f"     + {change.replacement}")
                        
                        if len(all_changes) > 10:
                            print(f"   ... and {len(all_changes) - 10} more changes")
                    
                    if warnings:
                        print("   ⚠️  Warnings:")
                        for w in warnings:
                            print(f"     - {w}")
                else:
                    file_path.write_text(content)
                    print(f"✅ {file_path}: Migrated {test_count} tests")
            
            return True
            
        except Exception as e:
            self.migration_stats['errors'].append(
                (str(file_path), f"Migration failed: {str(e)}")
            )
            print(f"❌ {file_path}: {e}")
            return False
    
    def find_test_files(self, path: Path) -> List[Path]:
        """Find all test files that need migration."""
        test_files = []
        
        if path.is_file():
            return [path] if path.suffix == '.rs' else []
        
        for file_path in path.rglob("*.rs"):
            if file_path.is_file():
                try:
                    content = file_path.read_text()
                    if '#[tokio::test]' in content or any(pattern in content for pattern in [
                        'get_shared_test_pool',
                        'TestPool::new',
                        'create_test_pool'
                    ]):
                        test_files.append(file_path)
                except Exception:
                    pass
        
        return sorted(test_files)
    
    def run_migration(self, path: Path) -> None:
        """Run the complete migration process."""
        # Find files
        if path.is_file():
            test_files = [path] if path.suffix == '.rs' else []
            print(f"🔍 Processing single file: {path}")
        else:
            print(f"🔍 Finding tests to migrate in {path}...")
            test_files = self.find_test_files(path)
            print(f"Found {len(test_files)} files to process")
        
        if not test_files:
            print("✨ No files need migration!")
            return
        
        if self.dry_run:
            print("\n🔍 DRY RUN - No files will be modified")
            print("=" * 60)
        
        # Process files
        for file_path in test_files:
            self.migration_stats['files_processed'] += 1
            self.migrate_file(file_path)
        
        # Print summary
        self.print_summary()
        
        # Validate compilation if not dry run
        if not self.dry_run and self.migration_stats['files_modified'] > 0:
            self.validate_compilation()
    
    def print_summary(self) -> None:
        """Print migration summary."""
        print(f"\n{'='*60}")
        print("📊 Migration Summary:")
        print(f"{'='*60}")
        print(f"Files processed: {self.migration_stats['files_processed']}")
        print(f"Files modified: {self.migration_stats['files_modified']}")
        print(f"Tests migrated: {self.migration_stats['tests_migrated']}")
        
        if self.migration_stats['warnings']:
            print(f"\n⚠️  Warnings ({len(self.migration_stats['warnings'])}):")
            # Group warnings by type
            warning_types = {}
            for file_path, warning in self.migration_stats['warnings']:
                warning_types.setdefault(warning, []).append(Path(file_path).name)
            
            for warning, files in warning_types.items():
                print(f"  {warning}:")
                for f in files[:3]:
                    print(f"    - {f}")
                if len(files) > 3:
                    print(f"    ... and {len(files) - 3} more files")
        
        if self.migration_stats['errors']:
            print(f"\n❌ Errors ({len(self.migration_stats['errors'])}):")
            for file_path, error in self.migration_stats['errors'][:5]:
                print(f"  {Path(file_path).name}: {error}")
            if len(self.migration_stats['errors']) > 5:
                print(f"  ... and {len(self.migration_stats['errors']) - 5} more errors")
        
        if self.dry_run:
            print(f"\n💡 This was a dry run. To apply changes, run without --dry-run")
    
    def validate_compilation(self) -> None:
        """Check if the code still compiles after migration."""
        print("\n✅ Validating compilation...")
        result = subprocess.run(
            ["cargo", "check", "--tests"],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print("  ✅ Code compiles successfully!")
        else:
            print("  ❌ Compilation failed!")
            print("  Run 'cargo check --tests' to see errors")
            print("\n  Common fixes:")
            print("  - Ensure all tests have (ctx: TestContext) parameter")
            print("  - Check that return type is Result<(), Box<dyn std::error::Error>>")
            print("  - Verify imports include: use crate::common::prelude::*;")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Migrate Sinex tests to new framework",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --dry-run                    # Preview all changes
  %(prog)s                              # Migrate all tests
  %(prog)s test/integration/            # Migrate specific directory
  %(prog)s test/unit/my_test.rs        # Migrate single file
  %(prog)s --dry-run --verbose          # Show detailed changes
  %(prog)s --quality                    # Also fix unwrap/println/asserts
  %(prog)s --dry-run --quality -v       # Preview all improvements
"""
    )
    parser.add_argument(
        "path",
        type=Path,
        nargs="?",
        default=Path("test"),
        help="Path to migrate (file or directory, default: test/)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without modifying files"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show detailed change information in dry-run mode"
    )
    parser.add_argument(
        "--quality", "-q",
        action="store_true",
        help="Also apply quality improvements (unwrap->?, println->tracing, add assert messages)"
    )
    
    args = parser.parse_args()
    
    if not args.path.exists():
        print(f"Error: Path {args.path} not found")
        sys.exit(1)
    
    migrator = TestMigrator(dry_run=args.dry_run, verbose=args.verbose, quality=args.quality)
    migrator.run_migration(args.path)

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Automated test migration tool for Sinex test framework.
Migrates tests from #[tokio::test] to #[sinex_test] pattern.
"""

import re
import sys
import os
from pathlib import Path
from typing import List, Tuple, Optional
import subprocess
import shutil
from datetime import datetime

class TestMigrator:
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.backup_dir = f"test_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.migration_stats = {
            'files_processed': 0,
            'tests_migrated': 0,
            'errors': [],
            'warnings': []
        }
    
    def create_backup(self, test_dir: Path) -> None:
        """Create backup of test directory before migration."""
        if not self.dry_run:
            print(f"💾 Creating backup in {self.backup_dir}...")
            shutil.copytree(test_dir, self.backup_dir)
    
    def migrate_test_signature(self, content: str) -> Tuple[str, int]:
        """Migrate test function signatures from tokio::test to sinex_test."""
        count = 0
        
        # Pattern 1: Simple tokio::test with various return types
        pattern1 = re.compile(
            r'#\[tokio::test\]\s*\n\s*async\s+fn\s+(\w+)\s*\(\s*\)\s*->\s*(?:Result<\(\)(?:,\s*\w+)?>\s*|anyhow::Result<\(\)>\s*)\{',
            re.MULTILINE
        )
        
        def replace1(match):
            nonlocal count
            count += 1
            test_name = match.group(1)
            return f'#[sinex_test]\nasync fn {test_name}(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {{'
        
        content = pattern1.sub(replace1, content)
        
        # Pattern 2: tokio::test without explicit return type
        pattern2 = re.compile(
            r'#\[tokio::test\]\s*\n\s*async\s+fn\s+(\w+)\s*\(\s*\)\s*\{',
            re.MULTILINE
        )
        
        def replace2(match):
            nonlocal count
            count += 1
            test_name = match.group(1)
            return f'#[sinex_test]\nasync fn {test_name}(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {{'
        
        content = pattern2.sub(replace2, content)
        
        return content, count
    
    def migrate_pool_usage(self, content: str) -> Tuple[str, List[str]]:
        """Replace pool initialization and usage patterns."""
        warnings = []
        
        # Pattern: let pool = get_shared_test_pool().await?;
        content = re.sub(
            r'let\s+(\w+)\s*=\s*(?:database_helpers::)?get_shared_test_pool\(\)\.await\?;',
            r'let \1 = ctx.pool();',
            content
        )
        
        # Pattern: let pool = TestPool::new().await?;
        content = re.sub(
            r'let\s+(\w+)\s*=\s*TestPool::new\(\)\.await\?;',
            r'let \1 = ctx.pool();',
            content
        )
        
        # Pattern: database_helpers::create_test_pool().await?
        content = re.sub(
            r'let\s+(\w+)\s*=\s*(?:database_helpers::)?create_test_pool\(\)\.await\?;',
            r'let \1 = ctx.pool();',
            content
        )
        
        # Replace &pool with ctx.pool()
        content = re.sub(r'&pool\b', 'ctx.pool()', content)
        
        # Handle pool.clone() patterns
        content = re.sub(r'pool\.clone\(\)', 'ctx.pool().clone()', content)
        
        # Check for complex pool patterns that need manual review
        if re.search(r'pool\s*:\s*(?:PgPool|Pool)', content):
            warnings.append("Complex pool pattern detected - manual review recommended")
        
        return content, warnings
    
    def fix_imports(self, content: str) -> str:
        """Fix imports for migrated tests."""
        lines = content.split('\n')
        new_lines = []
        has_test_context_import = False
        
        for line in lines:
            # Skip old imports
            if any(pattern in line for pattern in [
                'get_shared_test_pool',
                'TestPool',
                'create_test_pool'
            ]):
                continue
            
            # Check for existing TestContext import
            if 'TestContext' in line or 'common::prelude::*' in line:
                has_test_context_import = True
            
            new_lines.append(line)
        
        # Add TestContext import if missing
        if not has_test_context_import:
            # Find the right place to insert (after other use statements)
            insert_idx = 0
            for i, line in enumerate(new_lines):
                if line.strip().startswith('use '):
                    insert_idx = i + 1
                elif line.strip() and not line.strip().startswith('//'):
                    break
            
            if insert_idx == 0:
                # No use statements found, add at the beginning
                new_lines.insert(0, 'use crate::common::prelude::*;')
                new_lines.insert(1, '')
            else:
                new_lines.insert(insert_idx, 'use crate::common::prelude::*;')
        
        return '\n'.join(new_lines)
    
    def validate_migration(self, file_path: Path) -> List[str]:
        """Validate the migrated file for common issues."""
        errors = []
        content = file_path.read_text()
        
        # Check for remaining tokio::test
        if '#[tokio::test]' in content:
            errors.append("Still contains #[tokio::test]")
        
        # Check for sinex_test with missing ctx parameter
        if re.search(r'#\[sinex_test\]\s*\n\s*async\s+fn\s+\w+\s*\(\s*\)', content):
            errors.append("sinex_test function missing ctx parameter")
        
        # Check for old pool patterns
        if any(pattern in content for pattern in ['get_shared_test_pool', 'TestPool::new']):
            errors.append("Still contains old pool initialization patterns")
        
        return errors
    
    def migrate_file(self, file_path: Path) -> bool:
        """Migrate a single test file."""
        try:
            content = file_path.read_text()
            original_content = content
            
            # Skip if already migrated
            if '#[sinex_test]' in content and '#[tokio::test]' not in content:
                return True
            
            # Apply migrations
            content, test_count = self.migrate_test_signature(content)
            content, warnings = self.migrate_pool_usage(content)
            content = self.fix_imports(content)
            
            # Only write if changes were made
            if content != original_content:
                if not self.dry_run:
                    file_path.write_text(content)
                
                self.migration_stats['tests_migrated'] += test_count
                self.migration_stats['warnings'].extend(
                    [(str(file_path), w) for w in warnings]
                )
                
                # Validate the migration
                errors = self.validate_migration(file_path)
                if errors:
                    self.migration_stats['errors'].append(
                        (str(file_path), errors)
                    )
                
                print(f"  ✅ Migrated {test_count} tests in {file_path.name}")
            
            return True
            
        except Exception as e:
            self.migration_stats['errors'].append(
                (str(file_path), [f"Migration failed: {str(e)}"])
            )
            print(f"  ❌ Error migrating {file_path.name}: {e}")
            return False
    
    def find_test_files(self, test_dir: Path) -> List[Path]:
        """Find all test files that need migration."""
        test_files = []
        
        for file_path in test_dir.rglob("*.rs"):
            if file_path.is_file():
                content = file_path.read_text()
                if '#[tokio::test]' in content or 'get_shared_test_pool' in content:
                    test_files.append(file_path)
        
        return test_files
    
    def run_migration(self, test_dir: Path) -> None:
        """Run the complete migration process."""
        print("🔍 Finding tests to migrate...")
        test_files = self.find_test_files(test_dir)
        print(f"Found {len(test_files)} files to migrate")
        
        if not test_files:
            print("✨ No files need migration!")
            return
        
        # Create backup
        self.create_backup(test_dir)
        
        # Migrate files
        print("\n🔧 Running migration...")
        for file_path in test_files:
            self.migration_stats['files_processed'] += 1
            self.migrate_file(file_path)
        
        # Print summary
        self.print_summary()
        
        # Validate compilation if not dry run
        if not self.dry_run:
            self.validate_compilation()
    
    def print_summary(self) -> None:
        """Print migration summary."""
        print("\n📊 Migration Summary:")
        print(f"  Files processed: {self.migration_stats['files_processed']}")
        print(f"  Tests migrated: {self.migration_stats['tests_migrated']}")
        
        if self.migration_stats['warnings']:
            print(f"\n⚠️  Warnings ({len(self.migration_stats['warnings'])}):")
            for file_path, warning in self.migration_stats['warnings'][:5]:
                print(f"  {Path(file_path).name}: {warning}")
            if len(self.migration_stats['warnings']) > 5:
                print(f"  ... and {len(self.migration_stats['warnings']) - 5} more")
        
        if self.migration_stats['errors']:
            print(f"\n❌ Errors ({len(self.migration_stats['errors'])}):")
            for file_path, errors in self.migration_stats['errors']:
                print(f"  {Path(file_path).name}: {', '.join(errors)}")
    
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

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Migrate Sinex tests to new framework"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without modifying files"
    )
    parser.add_argument(
        "--file",
        type=Path,
        help="Migrate a specific file instead of all tests"
    )
    parser.add_argument(
        "--category",
        choices=["unit", "integration", "system", "adversarial", "property"],
        help="Migrate only tests in a specific category"
    )
    
    args = parser.parse_args()
    
    migrator = TestMigrator(dry_run=args.dry_run)
    
    if args.file:
        # Migrate single file
        if args.file.exists():
            migrator.migrate_file(args.file)
            migrator.print_summary()
        else:
            print(f"Error: File {args.file} not found")
            sys.exit(1)
    else:
        # Determine test directory
        if args.category:
            test_dir = Path(f"test/{args.category}")
        else:
            test_dir = Path("test")
        
        if not test_dir.exists():
            print(f"Error: Test directory {test_dir} not found")
            sys.exit(1)
        
        migrator.run_migration(test_dir)

if __name__ == "__main__":
    main()
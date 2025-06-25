#!/usr/bin/env python3
"""
AST-based test migration script for Sinex.
Uses ast-grep for structural transformations with the existing regex script as fallback.
"""

import subprocess
import sys
import tempfile
import yaml
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import shutil
import re

class ASTTestMigrator:
    def __init__(self, dry_run: bool = False, verbose: bool = False):
        self.dry_run = dry_run
        self.verbose = verbose
        self.project_root = self.find_project_root()
        self.temp_rules_dir = None
        self.stats = {
            'files_processed': 0,
            'tests_migrated': 0,
            'files_modified': 0,
            'errors': []
        }
    
    def find_project_root(self) -> Path:
        """Find project root by looking for Cargo.toml."""
        current = Path.cwd()
        while not (current / "Cargo.toml").exists():
            if current.parent == current:
                raise FileNotFoundError("Could not find project root (Cargo.toml)")
            current = current.parent
        return current
    
    def setup_temp_rules(self):
        """Create temporary rule files for each transformation."""
        self.temp_rules_dir = Path(tempfile.mkdtemp())
        
        # Rule 1: Migrate test attribute
        rule1 = {
            'id': 'migrate-test-attribute',
            'language': 'rust',
            'rule': {
                'pattern': '#[tokio::test]',
                'inside': {'kind': 'attribute_item'}
            },
            'fix': '#[sinex_test]'
        }
        
        # Rule 2: Migrate function signature
        rule2 = {
            'id': 'migrate-function-signature',
            'language': 'rust',
            'rule': {
                'kind': 'function_item',
                'has': {'field': 'attribute', 'pattern': '#[sinex_test]'},
                'pattern': 'async fn $NAME() $REST'
            },
            'fix': 'async fn $NAME(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> $REST'
        }
        
        # Rule 3: Add imports
        rule3 = {
            'id': 'add-imports',
            'language': 'rust',
            'rule': {
                'kind': 'source_file',
                'has': {'pattern': '#[sinex_test]'},
                'not': {'has': {'pattern': 'use crate::common::prelude::*;'}}
            },
            'fix': 'use crate::common::prelude::*;\n\n$$$'
        }
        
        # Rule 4: Migrate pool initialization patterns
        rule4 = {
            'id': 'migrate-pool-init',
            'language': 'rust',
            'rule': {
                'any': [
                    {'pattern': 'let $VAR = get_shared_test_pool().await?;'},
                    {'pattern': 'let $VAR = database_helpers::get_shared_test_pool().await?;'},
                    {'pattern': 'let $VAR = TestPool::new().await?;'},
                    {'pattern': 'let $VAR = create_test_pool().await?;'}
                ]
            },
            'fix': 'let $VAR = ctx.pool();'
        }
        
        # Save rules to temp files
        for i, rule in enumerate([rule1, rule2, rule3, rule4], 1):
            with open(self.temp_rules_dir / f"rule{i}.yml", 'w') as f:
                yaml.dump(rule, f)
        
        return self.temp_rules_dir
    
    def cleanup_temp_rules(self):
        """Remove temporary rule files."""
        if self.temp_rules_dir and self.temp_rules_dir.exists():
            shutil.rmtree(self.temp_rules_dir)
    
    def run_ast_grep_rule(self, rule_file: Path, target: Path) -> Tuple[bool, str]:
        """Run a single ast-grep rule."""
        cmd = ["ast-grep", "scan", "-r", str(rule_file), str(target)]
        
        if not self.dry_run:
            cmd.append("-U")  # Update files
        
        if self.verbose:
            cmd.append("--json=stream")
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            # ast-grep returns 0 for no matches, 1 for matches found
            if result.returncode not in [0, 1]:
                return False, result.stderr
            
            return True, result.stdout
        except Exception as e:
            return False, str(e)
    
    def apply_regex_fixes(self, content: str) -> str:
        """Apply additional regex-based fixes that ast-grep might miss."""
        # Fix &pool references (since ctx.pool() returns &PgPool)
        content = re.sub(r'\b&pool\b', 'pool', content)
        
        # Fix pool.clone() patterns
        content = re.sub(r'\bpool\.clone\(\)', 'ctx.pool().clone()', content)
        
        # Add missing Ok(()) returns
        lines = content.split('\n')
        in_test = False
        brace_depth = 0
        test_start = -1
        
        for i, line in enumerate(lines):
            if '#[sinex_test]' in line:
                in_test = True
                test_start = i
            elif in_test and '{' in line:
                brace_depth += line.count('{') - line.count('}')
                if brace_depth == 0:
                    # Check if we need to add Ok(())
                    last_line_idx = i - 1
                    while last_line_idx > test_start and lines[last_line_idx].strip() in ['', '}']:
                        last_line_idx -= 1
                    
                    last_line = lines[last_line_idx].strip()
                    if not any(last_line.endswith(x) for x in ['Ok(())', 'Ok(())?', '?;']) and \
                       not last_line.startswith('Ok(') and not last_line.startswith('return'):
                        # Add Ok(())
                        indent = '    '  # Assume standard indentation
                        lines.insert(i, f'{indent}Ok(())')
                    
                    in_test = False
                    brace_depth = 0
        
        return '\n'.join(lines)
    
    def migrate_file(self, file_path: Path) -> bool:
        """Migrate a single file using AST transformations."""
        try:
            # Check if file needs migration
            content = file_path.read_text()
            if '#[tokio::test]' not in content and 'get_shared_test_pool' not in content:
                return True
            
            self.stats['files_processed'] += 1
            
            if self.verbose:
                print(f"\n📄 Processing {file_path}")
            
            # Apply AST transformations in sequence
            for i in range(1, 5):
                rule_file = self.temp_rules_dir / f"rule{i}.yml"
                success, output = self.run_ast_grep_rule(rule_file, file_path)
                
                if not success:
                    self.stats['errors'].append((str(file_path), f"AST rule {i} failed: {output}"))
                    if self.verbose:
                        print(f"  ❌ Rule {i} failed: {output}")
                    return False
                
                if self.verbose and output:
                    print(f"  ✓ Rule {i} applied")
            
            # Apply additional regex fixes if not dry run
            if not self.dry_run:
                content = file_path.read_text()
                fixed_content = self.apply_regex_fixes(content)
                if fixed_content != content:
                    file_path.write_text(fixed_content)
                    if self.verbose:
                        print("  ✓ Applied additional fixes")
            
            self.stats['files_modified'] += 1
            self.stats['tests_migrated'] += content.count('#[tokio::test]')
            
            if not self.dry_run:
                print(f"✅ {file_path}: Migrated successfully")
            
            return True
            
        except Exception as e:
            self.stats['errors'].append((str(file_path), str(e)))
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
                    if '#[tokio::test]' in content or 'get_shared_test_pool' in content:
                        test_files.append(file_path)
                except Exception:
                    pass
        
        return sorted(test_files)
    
    def validate_compilation(self) -> bool:
        """Check if the code compiles."""
        print("\n🔍 Validating compilation...")
        result = subprocess.run(
            ["cargo", "check", "--tests"],
            capture_output=True,
            text=True,
            cwd=self.project_root
        )
        
        if result.returncode == 0:
            print("  ✅ Code compiles successfully!")
            return True
        else:
            print("  ❌ Compilation failed!")
            if self.verbose:
                print("\nCompilation errors:")
                for line in result.stderr.split('\n')[:20]:
                    if 'error' in line.lower():
                        print(f"  {line}")
            return False
    
    def run(self, path: Path):
        """Run the migration process."""
        try:
            # Setup temporary rules
            self.setup_temp_rules()
            
            # Find files to migrate
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
                self.migrate_file(file_path)
            
            # Print summary
            print(f"\n{'='*60}")
            print("📊 Migration Summary:")
            print(f"{'='*60}")
            print(f"Files processed: {self.stats['files_processed']}")
            print(f"Files modified: {self.stats['files_modified']}")
            print(f"Tests migrated: {self.stats['tests_migrated']}")
            
            if self.stats['errors']:
                print(f"\n❌ Errors ({len(self.stats['errors'])}):")
                for file_path, error in self.stats['errors'][:5]:
                    print(f"  {Path(file_path).name}: {error}")
            
            # Validate compilation if not dry run
            if not self.dry_run and self.stats['files_modified'] > 0:
                self.validate_compilation()
            
        finally:
            # Cleanup
            self.cleanup_temp_rules()

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="AST-based test migration for Sinex",
        epilog="""
Examples:
  %(prog)s --dry-run              # Preview changes
  %(prog)s test/unit/             # Migrate unit tests
  %(prog)s test/system/stress/    # Migrate specific directory
  %(prog)s --verbose              # Show detailed progress
"""
    )
    
    parser.add_argument(
        "path",
        type=Path,
        nargs="?",
        default=Path("test"),
        help="Path to migrate (default: test/)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without modifying files"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show detailed progress"
    )
    
    args = parser.parse_args()
    
    if not args.path.exists():
        print(f"Error: Path {args.path} not found")
        sys.exit(1)
    
    migrator = ASTTestMigrator(dry_run=args.dry_run, verbose=args.verbose)
    migrator.run(args.path)

if __name__ == "__main__":
    main()
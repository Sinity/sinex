#!/usr/bin/env python3
"""
Analyze gaps in test migration coverage.
"""

import re
import subprocess
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Set

class MigrationGapAnalyzer:
    def __init__(self):
        self.gaps = defaultdict(list)
        self.coverage = defaultdict(int)
    
    def analyze_all_patterns(self, test_dir: Path) -> None:
        """Analyze all patterns that need migration."""
        
        # Pattern categories to check
        patterns = {
            'tokio_tests': r'#\[tokio::test\]',
            'pool_get_shared': r'get_shared_test_pool',
            'pool_test_new': r'TestPool::new',
            'pool_with_strategy': r'TestPool::with_strategy',
            'pool_create': r'create_test_pool',
            'anyhow_result': r'-> Result<\(\), anyhow::Error>',
            'sqlx_result': r'-> sqlx::Result<\(\)>',
            'no_result': r'async fn \w+\(\)\s*\{',
            'unwrap_usage': r'\.unwrap\(\)',
            'expect_usage': r'\.expect\(',
            'assert_no_msg': r'assert_eq!\([^,]+,[^,]+\);',
            'println_usage': r'println!\(',
            'sleep_usage': r'thread::sleep|tokio::time::sleep',
            'hardcoded_uuid': r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}',
            'manual_raw_event': r'RawEvent\s*\{',
            'deprecated_imports': r'use.*(?:get_shared_test_pool|TestPool|create_test_pool)',
            'missing_ctx_import': lambda content: '#[sinex_test]' in content and 'use crate::common::prelude::*' not in content
        }
        
        for file_path in test_dir.rglob("*.rs"):
            if not file_path.is_file():
                continue
            
            try:
                content = file_path.read_text()
                
                for pattern_name, pattern in patterns.items():
                    if callable(pattern):
                        if pattern(content):
                            self.gaps[pattern_name].append(str(file_path))
                    else:
                        matches = re.findall(pattern, content)
                        if matches:
                            self.coverage[pattern_name] += len(matches)
                            if len(matches) > 5:  # Only track files with many occurrences
                                self.gaps[f"{pattern_name}_files"].append(
                                    (str(file_path), len(matches))
                                )
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
    
    def check_migration_effectiveness(self, test_dir: Path) -> None:
        """Run migration script and check what it actually fixes."""
        
        # Get list of files that would be modified
        result = subprocess.run(
            ['./script/migrate_tests.py', '--dry-run', str(test_dir)],
            capture_output=True,
            text=True
        )
        
        # Parse output for statistics
        for line in result.stdout.split('\n'):
            if 'Files processed:' in line:
                self.coverage['files_processed'] = int(line.split(':')[1].strip())
            elif 'Files modified:' in line:
                self.coverage['files_modified'] = int(line.split(':')[1].strip())
            elif 'Tests migrated:' in line:
                self.coverage['tests_migrated'] = int(line.split(':')[1].strip())
    
    def print_report(self) -> None:
        """Print comprehensive gap analysis."""
        print("🔍 Test Migration Gap Analysis")
        print("=" * 80)
        
        print("\n📊 Pattern Coverage:")
        print("-" * 80)
        
        # Migration script effectiveness
        print(f"Files that would be processed: {self.coverage.get('files_processed', 0)}")
        print(f"Files that would be modified: {self.coverage.get('files_modified', 0)}")
        print(f"Tests that would be migrated: {self.coverage.get('tests_migrated', 0)}")
        
        print("\n📈 Patterns Found in Codebase:")
        print("-" * 80)
        
        for pattern, count in sorted(self.coverage.items(), key=lambda x: x[1], reverse=True):
            if pattern not in ['files_processed', 'files_modified', 'tests_migrated']:
                print(f"{pattern:20} : {count:5} occurrences")
        
        print("\n🔴 High-Impact Files (>5 occurrences of patterns):")
        print("-" * 80)
        
        for pattern_type in ['unwrap_usage_files', 'assert_no_msg_files']:
            if pattern_type in self.gaps:
                print(f"\n{pattern_type.replace('_files', '').replace('_', ' ').title()}:")
                for file_path, count in sorted(self.gaps[pattern_type], key=lambda x: x[1], reverse=True)[:5]:
                    print(f"  {Path(file_path).name:40} : {count} occurrences")
        
        print("\n⚠️  Migration Gaps:")
        print("-" * 80)
        
        # Check what the migration script doesn't handle
        not_handled = [
            ('Error type conversions', 'anyhow::Error and sqlx::Result need manual conversion'),
            ('Unwrap/expect cleanup', f"{self.coverage.get('unwrap_usage', 0)} unwraps need manual review"),
            ('Assert messages', f"{self.coverage.get('assert_no_msg', 0)} assertions lack descriptive messages"),
            ('Debug prints', f"{self.coverage.get('println_usage', 0)} println! should be tracing::info!"),
            ('Hardcoded values', 'UUIDs and IDs should use generators'),
            ('Sleep patterns', 'Should use proper synchronization'),
        ]
        
        for issue, description in not_handled:
            print(f"❌ {issue}: {description}")
        
        print("\n💡 Recommendations:")
        print("-" * 80)
        print("1. Run migration script first for basic conversions")
        print("2. Use test_fix_suggester.py for quality issues")
        print("3. Manual review needed for:")
        print("   - Error type conversions (anyhow -> Box<dyn Error>)")
        print("   - Complex pool initialization patterns")
        print("   - Tests with custom cleanup or setup")
        print("4. Consider automation for high-frequency patterns:")
        print("   - Unwrap replacement (591 occurrences)")
        print("   - Assert message addition")

def main():
    analyzer = MigrationGapAnalyzer()
    test_dir = Path("test")
    
    print("Analyzing test patterns...")
    analyzer.analyze_all_patterns(test_dir)
    
    print("Checking migration script effectiveness...")
    analyzer.check_migration_effectiveness(test_dir)
    
    analyzer.print_report()

if __name__ == "__main__":
    main()
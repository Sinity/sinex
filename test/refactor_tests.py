#!/usr/bin/env python3
"""
Automated test refactoring script to migrate from raw SQL to query builders.

This script helps identify and refactor test files that use raw SQL queries
to use the new centralized query builder patterns.
"""

import os
import re
import sys
from pathlib import Path
from typing import List, Tuple, Dict
import argparse

# Patterns to detect raw SQL usage
SQL_PATTERNS = [
    (r'sqlx::query!\s*\(', 'sqlx::query!'),
    (r'sqlx::query_as!\s*\(', 'sqlx::query_as!'),
    (r'sqlx::query_scalar!\s*\(', 'sqlx::query_scalar!'),
    (r'sqlx::query\s*\(', 'sqlx::query'),
    (r'sqlx::query_as\s*\(', 'sqlx::query_as'),
    (r'sqlx::query_scalar\s*\(', 'sqlx::query_scalar'),
]

# Common SQL to query builder mappings
REFACTORING_PATTERNS = [
    # Event insertions
    {
        'pattern': r'INSERT INTO core\.events.*?RETURNING',
        'suggestion': 'Use TestEventBuilder::new(source, event_type).insert(&pool)',
        'category': 'event_insertion'
    },
    # Event queries
    {
        'pattern': r'SELECT.*?FROM core\.events.*?WHERE.*?event_id',
        'suggestion': 'Use TestQueries::get_event(&pool, event_id)',
        'category': 'event_query'
    },
    {
        'pattern': r'SELECT.*?FROM core\.events.*?WHERE.*?source',
        'suggestion': 'Use TestQueries::get_events_by_source(&pool, source, limit)',
        'category': 'event_filter'
    },
    # Checkpoint operations
    {
        'pattern': r'SELECT.*?FROM core\.automaton_checkpoints',
        'suggestion': 'Use TestQueries::get_checkpoint(&pool, automaton_name)',
        'category': 'checkpoint_query'
    },
    {
        'pattern': r'INSERT INTO core\.automaton_checkpoints.*?ON CONFLICT',
        'suggestion': 'Use TestCheckpointBuilder::new(automaton_name).insert(&pool)',
        'category': 'checkpoint_upsert'
    },
    # Count queries
    {
        'pattern': r'SELECT COUNT\(\*\).*?FROM core\.events',
        'suggestion': 'Use TestQueries::count_events_by_source(&pool, source)',
        'category': 'count_query'
    },
    # ULID conversions
    {
        'pattern': r'::uuid|::text.*?ulid|ulid.*?::uuid|ulid.*?::text',
        'suggestion': 'Remove manual ULID conversions - query builders handle this automatically',
        'category': 'ulid_conversion'
    }
]


class TestRefactorer:
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.stats = {
            'files_analyzed': 0,
            'files_with_sql': 0,
            'total_sql_queries': 0,
            'refactoring_suggestions': 0
        }
        self.findings: List[Dict] = []

    def analyze_file(self, file_path: Path) -> List[Dict]:
        """Analyze a single test file for SQL usage."""
        findings = []
        
        try:
            with open(file_path, 'r') as f:
                content = f.read()
                lines = content.split('\n')
            
            # Check for SQL patterns
            for pattern, name in SQL_PATTERNS:
                matches = list(re.finditer(pattern, content))
                for match in matches:
                    line_num = content[:match.start()].count('\n') + 1
                    context_start = max(0, line_num - 3)
                    context_end = min(len(lines), line_num + 20)
                    
                    # Extract the SQL query
                    sql_content = self._extract_sql_query(lines[line_num-1:context_end])
                    
                    # Find appropriate refactoring suggestion
                    suggestion = self._get_refactoring_suggestion(sql_content)
                    
                    findings.append({
                        'file': str(file_path),
                        'line': line_num,
                        'type': name,
                        'context': '\n'.join(lines[context_start:context_end]),
                        'suggestion': suggestion,
                        'sql_content': sql_content
                    })
            
        except Exception as e:
            print(f"Error analyzing {file_path}: {e}")
        
        return findings

    def _extract_sql_query(self, lines: List[str]) -> str:
        """Extract SQL query from code lines."""
        sql_lines = []
        in_string = False
        paren_count = 0
        
        for line in lines:
            if 'r#"' in line:
                in_string = True
            if '"#' in line and in_string:
                sql_lines.append(line)
                break
            if in_string or 'query' in line:
                sql_lines.append(line)
                paren_count += line.count('(') - line.count(')')
                if paren_count == 0 and sql_lines:
                    break
        
        return '\n'.join(sql_lines)

    def _get_refactoring_suggestion(self, sql_content: str) -> Dict:
        """Get refactoring suggestion based on SQL content."""
        for pattern_info in REFACTORING_PATTERNS:
            if re.search(pattern_info['pattern'], sql_content, re.IGNORECASE | re.DOTALL):
                return pattern_info
        
        return {
            'suggestion': 'Consider using appropriate TestQueries or builder method',
            'category': 'unknown'
        }

    def analyze_directory(self, directory: Path):
        """Analyze all test files in a directory."""
        test_files = list(directory.rglob('*test*.rs'))
        
        for file_path in test_files:
            self.stats['files_analyzed'] += 1
            findings = self.analyze_file(file_path)
            
            if findings:
                self.stats['files_with_sql'] += 1
                self.stats['total_sql_queries'] += len(findings)
                self.findings.extend(findings)

    def generate_report(self) -> str:
        """Generate a refactoring report."""
        report = ["# Test Refactoring Report\n"]
        report.append(f"## Summary")
        report.append(f"- Files analyzed: {self.stats['files_analyzed']}")
        report.append(f"- Files with SQL: {self.stats['files_with_sql']}")
        report.append(f"- Total SQL queries: {self.stats['total_sql_queries']}\n")
        
        # Group findings by category
        by_category: Dict[str, List[Dict]] = {}
        for finding in self.findings:
            category = finding['suggestion']['category']
            if category not in by_category:
                by_category[category] = []
            by_category[category].append(finding)
        
        report.append("## Findings by Category\n")
        for category, findings in sorted(by_category.items()):
            report.append(f"### {category.replace('_', ' ').title()} ({len(findings)} occurrences)\n")
            
            # Show first few examples
            for finding in findings[:3]:
                report.append(f"**File:** `{finding['file']}`")
                report.append(f"**Line:** {finding['line']}")
                report.append(f"**Suggestion:** {finding['suggestion']['suggestion']}")
                report.append("```rust")
                report.append(finding['context'])
                report.append("```\n")
            
            if len(findings) > 3:
                report.append(f"... and {len(findings) - 3} more\n")
        
        # Generate migration script snippets
        report.append("## Automated Refactoring Commands\n")
        report.append("Use these commands to bulk-refactor common patterns:\n")
        
        report.append("### 1. Simple INSERT replacements")
        report.append("```bash")
        report.append("# Replace simple event insertions")
        report.append('find test -name "*.rs" -exec sed -i \'s/sqlx::query!.*INSERT INTO core\\.events.*$/TestEventBuilder::new(source, event_type).insert(\\&pool).await?;/g\' {} \\;')
        report.append("```\n")
        
        report.append("### 2. Checkpoint query replacements")
        report.append("```bash")
        report.append("# Replace checkpoint queries")
        report.append('find test -name "*.rs" -exec sed -i \'s/sqlx::query_as!.*FROM core\\.automaton_checkpoints.*$/TestQueries::get_checkpoint(\\&pool, automaton_name).await?;/g\' {} \\;')
        report.append("```\n")
        
        return '\n'.join(report)

    def generate_refactoring_script(self) -> str:
        """Generate a shell script for automated refactoring."""
        script = ["#!/bin/bash", "# Automated test refactoring script", ""]
        
        # Group by file for efficiency
        by_file: Dict[str, List[Dict]] = {}
        for finding in self.findings:
            if finding['file'] not in by_file:
                by_file[finding['file']] = []
            by_file[finding['file']].append(finding)
        
        script.append("# Files to refactor:")
        for file_path in sorted(by_file.keys()):
            script.append(f"# - {file_path} ({len(by_file[file_path])} queries)")
        
        script.append("\n# Add imports to test files")
        script.append("for file in test/**/*.rs; do")
        script.append('  if grep -q "sqlx::query" "$file"; then')
        script.append('    sed -i \'1i use crate::common::query_helpers::TestQueries;\' "$file"')
        script.append('    sed -i \'1i use crate::common::builders::{TestEventBuilder, TestCheckpointBuilder};\' "$file"')
        script.append("  fi")
        script.append("done")
        
        return '\n'.join(script)


def main():
    parser = argparse.ArgumentParser(description='Refactor tests from raw SQL to query builders')
    parser.add_argument('--dry-run', action='store_true', help='Analyze without making changes')
    parser.add_argument('--report', type=str, help='Output report file path')
    parser.add_argument('--script', type=str, help='Generate refactoring script')
    args = parser.parse_args()
    
    refactorer = TestRefactorer(dry_run=args.dry_run)
    
    # Analyze test directory
    test_dir = Path('test')
    if not test_dir.exists():
        print("Error: 'test' directory not found")
        sys.exit(1)
    
    print("Analyzing test files...")
    refactorer.analyze_directory(test_dir)
    
    # Generate report
    report = refactorer.generate_report()
    if args.report:
        with open(args.report, 'w') as f:
            f.write(report)
        print(f"Report written to {args.report}")
    else:
        print(report)
    
    # Generate refactoring script
    if args.script:
        script = refactorer.generate_refactoring_script()
        with open(args.script, 'w') as f:
            f.write(script)
        os.chmod(args.script, 0o755)
        print(f"Refactoring script written to {args.script}")


if __name__ == '__main__':
    main()
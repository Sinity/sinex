#!/usr/bin/env python3
"""
Automated test macro conversion tool for the Sinex test suite.

This script identifies common test patterns and converts them to use
the appropriate test macros, significantly reducing boilerplate code.

Usage:
    ./convert_to_macros.py [--dry-run] [--pattern PATTERN] [--file FILE]
"""

import re
import sys
import os
from pathlib import Path
from typing import List, Tuple, Optional
import argparse
import subprocess

class TestPattern:
    """Base class for test pattern detection and conversion."""
    
    def __init__(self, name: str, pattern: str, macro_template: str):
        self.name = name
        self.pattern = re.compile(pattern, re.MULTILINE | re.DOTALL)
        self.macro_template = macro_template
    
    def matches(self, content: str) -> List[re.Match]:
        """Find all matches of this pattern in the content."""
        return list(self.pattern.finditer(content))
    
    def convert(self, match: re.Match) -> str:
        """Convert a match to the macro form."""
        return self.macro_template.format(**match.groupdict())

# Define conversion patterns
PATTERNS = [
    # Simple event insertion pattern
    TestPattern(
        "event_insertion",
        r'#\[sinex_test\]\s*async fn (?P<test_name>\w+)\(ctx: TestContext\).*?\{[^}]*?'
        r'let event = .*?EventFactory::new\("(?P<source>[^"]+)"\)'
        r'.*?\.create_event\("(?P<event_type>[^"]+)",\s*(?P<payload>json!\([^)]+\))\);'
        r'.*?assert_event_inserted.*?Ok\(\(\)\)\s*\}',
        '''test_event_insertion!(
    {test_name},
    "{source}",
    "{event_type}",
    {payload}
);'''
    ),
    
    # Batch event operations
    TestPattern(
        "batch_events",
        r'#\[sinex_test\]\s*async fn (?P<test_name>\w+)\(ctx: TestContext\).*?\{[^}]*?'
        r'for i in 0\.\.(?P<count>\d+).*?'
        r'EventFactory::new\("(?P<source>[^"]+)"\).*?'
        r'\.create_event\("(?P<event_type>[^"]+)".*?\}.*?'
        r'assert.*?(?P=count).*?Ok\(\(\)\)\s*\}',
        '''test_batch_events!(
    {test_name},
    "{source}",
    "{event_type}",
    {count},
    |pool, events| async move {{
        assert_eq!(events.len(), {count});
        Ok(())
    }}
);'''
    ),
    
    # Checkpoint flow pattern
    TestPattern(
        "checkpoint_flow",
        r'#\[sinex_test\]\s*async fn (?P<test_name>\w+)\(ctx: TestContext\).*?\{[^}]*?'
        r'CheckpointManager::new\([^,]+,\s*"(?P<automaton>[^"]+)".*?\);.*?'
        r'checkpoint\.processed_count = (?P<initial>\d+);.*?'
        r'checkpoint\.processed_count = (?P<updated>\d+);.*?'
        r'Ok\(\(\)\)\s*\}',
        '''test_checkpoint_flow!(
    {test_name},
    "{automaton}",
    {initial},
    {updated}
);'''
    ),
    
    # Time range query pattern
    TestPattern(
        "time_range",
        r'#\[sinex_test\]\s*async fn (?P<test_name>\w+)\(ctx: TestContext\).*?\{[^}]*?'
        r'for i in 0\.\.(?P<count>\d+).*?'
        r'get_events_in_time_range.*?Duration::(?P<unit>\w+)\((?P<start>\d+)\).*?'
        r'Duration::(?P<unit2>\w+)\((?P<end>\d+)\).*?'
        r'assert.*?len.*?(?P<expected>\d+).*?'
        r'Ok\(\(\)\)\s*\}',
        '''test_time_range_query!(
    {test_name},
    {count},
    chrono::Duration::{unit}(1),
    chrono::Duration::{unit}(-{start}),
    chrono::Duration::{unit2}({end}),
    {expected}
);'''
    ),
    
    # Event filter pattern
    TestPattern(
        "event_filter",
        r'#\[sinex_test\]\s*async fn (?P<test_name>\w+)\(ctx: TestContext\).*?\{[^}]*?'
        r'let sources = \[(?P<sources>[^\]]+)\];.*?'
        r'events_per_source = (?P<per_source>\d+);.*?'
        r'WHERE source = \'(?P<filter>[^\']+)\'.*?'
        r'assert.*?len.*?(?P<expected>\d+).*?'
        r'Ok\(\(\)\)\s*\}',
        '''test_event_filter!(
    {test_name},
    &[{sources}],
    {per_source},
    "{filter}",
    {expected}
);'''
    ),
]

def analyze_file(filepath: Path) -> List[Tuple[TestPattern, re.Match]]:
    """Analyze a file and return all matching patterns."""
    content = filepath.read_text()
    matches = []
    
    for pattern in PATTERNS:
        for match in pattern.matches(content):
            matches.append((pattern, match))
    
    return matches

def convert_file(filepath: Path, dry_run: bool = False) -> int:
    """Convert tests in a file to use macros."""
    original_content = filepath.read_text()
    content = original_content
    conversions = 0
    
    # Sort matches by position (reverse) to avoid offset issues
    matches = analyze_file(filepath)
    matches.sort(key=lambda x: x[1].start(), reverse=True)
    
    for pattern, match in matches:
        if dry_run:
            print(f"Would convert {pattern.name} in {filepath}:")
            print(f"  Test: {match.group('test_name')}")
        else:
            # Replace the match with the macro
            macro_code = pattern.convert(match)
            content = content[:match.start()] + macro_code + content[match.end():]
            conversions += 1
    
    if not dry_run and conversions > 0:
        # Add macro import if not present
        if "use crate::common::test_macros::*;" not in content:
            # Find the right place to add the import
            import_pos = content.find("use crate::common::prelude::*;")
            if import_pos != -1:
                import_pos = content.find("\n", import_pos) + 1
                content = content[:import_pos] + "use crate::common::test_macros::*;\n" + content[import_pos:]
        
        filepath.write_text(content)
        print(f"Converted {conversions} tests in {filepath}")
    
    return conversions

def find_test_files(directory: Path, pattern: Optional[str] = None) -> List[Path]:
    """Find all test files matching the pattern."""
    test_files = []
    
    for filepath in directory.rglob("*_test.rs"):
        if pattern and pattern not in str(filepath):
            continue
        test_files.append(filepath)
    
    return test_files

def run_ast_grep_analysis(directory: Path) -> dict:
    """Run ast-grep to find additional patterns."""
    patterns = {
        "assert_event_inserted": 'assertions::assert_event_inserted($$$)',
        "tokio_spawn": 'tokio::spawn(async move { $$$ })',
        "futures_join_all": 'futures::future::join_all($$$)',
        "checkpoint_manager": 'CheckpointManager::new($$$)',
        "redis_stream": 'RedisStreamClient::new($$$)',
    }
    
    results = {}
    for name, pattern in patterns.items():
        try:
            cmd = ["ast-grep", "--pattern", pattern, str(directory)]
            result = subprocess.run(cmd, capture_output=True, text=True)
            count = len(result.stdout.strip().split('\n')) if result.stdout else 0
            results[name] = count
        except:
            results[name] = 0
    
    return results

def main():
    parser = argparse.ArgumentParser(description="Convert Sinex tests to use macros")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be converted")
    parser.add_argument("--pattern", help="Only process files matching this pattern")
    parser.add_argument("--file", help="Convert a specific file")
    parser.add_argument("--analyze", action="store_true", help="Analyze patterns without converting")
    
    args = parser.parse_args()
    
    test_dir = Path(__file__).parent.parent  # test/ directory
    
    if args.analyze:
        # Run analysis
        print("Analyzing test patterns...")
        ast_results = run_ast_grep_analysis(test_dir)
        print("\nPattern occurrences:")
        for pattern, count in ast_results.items():
            print(f"  {pattern}: {count}")
        
        # Count macro usage
        macro_usage = 0
        total_tests = 0
        for filepath in find_test_files(test_dir):
            content = filepath.read_text()
            macro_usage += len(re.findall(r'test_\w+!', content))
            total_tests += len(re.findall(r'#\[sinex_test\]', content))
        
        print(f"\nMacro usage: {macro_usage}/{total_tests} ({macro_usage/total_tests*100:.1f}%)")
        return
    
    if args.file:
        # Convert specific file
        filepath = Path(args.file)
        if filepath.exists():
            convert_file(filepath, args.dry_run)
        else:
            print(f"File not found: {filepath}")
            sys.exit(1)
    else:
        # Convert all matching files
        test_files = find_test_files(test_dir, args.pattern)
        total_conversions = 0
        
        print(f"Found {len(test_files)} test files")
        
        for filepath in test_files:
            conversions = convert_file(filepath, args.dry_run)
            total_conversions += conversions
        
        print(f"\nTotal conversions: {total_conversions}")
        
        if not args.dry_run and total_conversions > 0:
            print("\nRunning cargo check to verify conversions...")
            result = subprocess.run(["cargo", "check", "--tests"], cwd=test_dir.parent)
            if result.returncode == 0:
                print("✅ All tests compile successfully!")
            else:
                print("❌ Compilation errors detected. Please review the changes.")

if __name__ == "__main__":
    main()
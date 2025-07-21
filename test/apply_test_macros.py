#!/usr/bin/env python3
"""
Apply test macros systematically across the Sinex test suite.

This script identifies patterns in test files that can be replaced with
the test macros defined in test/common/test_macros.rs.
"""

import os
import re
from pathlib import Path
from typing import List, Tuple, Dict
import ast
import subprocess

# Base path for test directory
TEST_DIR = Path(__file__).parent

# Macro patterns to search for
MACRO_PATTERNS = {
    'test_event_insertion': {
        'description': 'Simple event insertion followed by retrieval/assertion',
        'patterns': [
            # Pattern: TestEventBuilder::new(...).insert(...) followed by get/assert
            r'let\s+(\w+)\s*=\s*TestEventBuilder::new\s*\(\s*"([^"]+)"\s*,\s*"([^"]+)"\s*\)',
            r'\.with_payload\s*\(([^)]+)\)',
            r'\.insert\s*\(&pool\)',
            r'let\s+\w+\s*=\s*TestQueries::get_event.*\(\s*&pool\s*,\s*\w+\.id\s*\)',
        ],
    },
    'test_invalid_event': {
        'description': 'Invalid event tests that expect errors',
        'patterns': [
            r'TestEventBuilder::new.*\.insert.*\.await;?\s*assert!\(.*\.is_err\(\)\)',
            r'assert_event_insertion_fails',
        ],
    },
    'test_batch_events': {
        'description': 'Batch event creation patterns',
        'patterns': [
            r'BatchEventBuilder::new\s*\(',
            r'for\s+\w+\s+in\s+0\.\.\d+.*TestEventBuilder::new',
        ],
    },
    'test_checkpoint_flow': {
        'description': 'Checkpoint create/update/verify patterns',
        'patterns': [
            r'TestCheckpointBuilder::new.*\.insert',
            r'create_checkpoint.*update_checkpoint',
        ],
    },
    'test_concurrent_operations': {
        'description': 'Concurrent operation tests',
        'patterns': [
            r'tokio::spawn.*async\s+move',
            r'futures::future::try_join_all',
        ],
    },
    'test_time_range_query': {
        'description': 'Time-based queries',
        'patterns': [
            r'get_events_in_range.*chrono::Duration',
            r'TestQueries::get_events_in_range',
        ],
    },
    'test_event_filter': {
        'description': 'Event filtering by source/type',
        'patterns': [
            r'get_events_by_source',
            r'TestQueries::get_events_by_source',
        ],
    },
}

def find_test_files() -> List[Path]:
    """Find all Rust test files in the test directory."""
    test_files = []
    for pattern in ['**/*_test.rs', '**/test_*.rs', '**/tests.rs']:
        test_files.extend(TEST_DIR.glob(pattern))
    
    # Filter out non-test files and examples
    test_files = [
        f for f in test_files 
        if 'example' not in str(f).lower() 
        and 'snapshot' not in str(f).lower()
        and 'refactored' not in str(f).lower()
        and f.is_file()
    ]
    
    return sorted(set(test_files))

def analyze_file(file_path: Path) -> Dict[str, List[Tuple[int, str]]]:
    """Analyze a file for macro-replaceable patterns."""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return {}
    
    findings = {}
    lines = content.split('\n')
    
    # Check each test function
    test_fn_pattern = re.compile(r'^\s*(#\[sinex_test.*?\])?\s*async\s+fn\s+(\w+)\s*\(')
    
    i = 0
    while i < len(lines):
        match = test_fn_pattern.match(lines[i])
        if match:
            test_name = match.group(2)
            
            # Extract test body (find matching braces)
            start = i
            brace_count = 0
            test_body_lines = []
            
            for j in range(i, len(lines)):
                line = lines[j]
                test_body_lines.append(line)
                
                # Count braces
                brace_count += line.count('{') - line.count('}')
                if brace_count == 0 and '{' in line:
                    # Found end of function
                    test_body = '\n'.join(test_body_lines)
                    
                    # Check for each macro pattern
                    for macro_name, macro_info in MACRO_PATTERNS.items():
                        for pattern in macro_info['patterns']:
                            if re.search(pattern, test_body, re.DOTALL | re.MULTILINE):
                                if macro_name not in findings:
                                    findings[macro_name] = []
                                findings[macro_name].append((start + 1, test_name))
                                break
                    
                    i = j
                    break
        
        i += 1
    
    return findings

def generate_report(all_findings: Dict[Path, Dict]) -> str:
    """Generate a summary report of findings."""
    report = ["# Test Macro Application Report\n"]
    
    total_conversions = 0
    macro_counts = {}
    
    for file_path, findings in all_findings.items():
        if not findings:
            continue
            
        report.append(f"\n## {file_path.relative_to(TEST_DIR)}\n")
        
        for macro_name, locations in findings.items():
            if macro_name not in macro_counts:
                macro_counts[macro_name] = 0
            
            macro_counts[macro_name] += len(locations)
            total_conversions += len(locations)
            
            report.append(f"- **{macro_name}**: {len(locations)} potential conversions")
            for line_num, test_name in locations[:3]:  # Show first 3
                report.append(f"  - Line {line_num}: `{test_name}`")
            if len(locations) > 3:
                report.append(f"  - ... and {len(locations) - 3} more")
    
    # Summary
    report.insert(1, f"**Total potential conversions**: {total_conversions}\n")
    report.insert(2, "\n### Macro Usage Summary\n")
    for macro_name, count in sorted(macro_counts.items(), key=lambda x: x[1], reverse=True):
        report.insert(3, f"- `{macro_name}`: {count} uses")
    
    return '\n'.join(report)

def main():
    """Main entry point."""
    print("Analyzing test files for macro conversion opportunities...")
    
    test_files = find_test_files()
    print(f"Found {len(test_files)} test files to analyze")
    
    all_findings = {}
    
    for file_path in test_files:
        findings = analyze_file(file_path)
        if findings:
            all_findings[file_path] = findings
            print(f"✓ {file_path.relative_to(TEST_DIR)}: {sum(len(v) for v in findings.values())} opportunities")
        else:
            print(f"  {file_path.relative_to(TEST_DIR)}: no simple conversions")
    
    # Generate report
    report = generate_report(all_findings)
    report_path = TEST_DIR / "MACRO_CONVERSION_ANALYSIS.md"
    
    with open(report_path, 'w') as f:
        f.write(report)
    
    print(f"\nReport written to: {report_path}")
    
    # Print summary
    total = sum(sum(len(v) for v in findings.values()) for findings in all_findings.values())
    print(f"\nSummary: {total} potential macro conversions across {len(all_findings)} files")

if __name__ == "__main__":
    main()
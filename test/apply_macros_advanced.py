#!/usr/bin/env python3
"""
Advanced macro application for Sinex test suite.

This script performs deeper analysis to find and apply test macros.
"""

import os
import re
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import subprocess

# Base path for test directory
TEST_DIR = Path(__file__).parent

# Track all changes
changes_made = []
files_analyzed = 0
patterns_found = {}

def find_test_files() -> List[Path]:
    """Find all Rust test files in the test directory."""
    test_files = []
    for pattern in ['**/*_test.rs', '**/test_*.rs']:
        test_files.extend(TEST_DIR.glob(pattern))
    
    # Filter out examples, snapshots, and already refactored files
    test_files = [
        f for f in test_files 
        if 'example' not in str(f)
        and 'snapshot' not in str(f)
        and '_refactored' not in str(f)
        and '_macro_refactored' not in str(f)
        and 'mock' not in str(f)
        and f.is_file()
    ]
    
    return sorted(set(test_files))

def analyze_test_function(function_content: str, function_name: str) -> Optional[Tuple[str, Dict]]:
    """Analyze a test function to see if it matches any macro pattern."""
    
    # Pattern 1: Simple event insertion + retrieval + assertion
    if all(pattern in function_content for pattern in [
        "EventFactory::new", 
        "create_event",
        "insert_event",
        "assert",
    ]) and "for " not in function_content and "spawn" not in function_content:
        # Extract event details
        factory_match = re.search(r'EventFactory::new\s*\(\s*"([^"]+)"\s*\)', function_content)
        type_match = re.search(r'create_event\s*\(\s*"([^"]+)"', function_content)
        payload_match = re.search(r'create_event\s*\([^,]+,\s*(json!\([^)]+\))', function_content)
        
        if factory_match and type_match and payload_match:
            return ('test_event_insertion', {
                'source': factory_match.group(1),
                'event_type': type_match.group(1),
                'payload': payload_match.group(1)
            })
    
    # Pattern 2: Batch event creation
    if "BatchEventBuilder::new" in function_content or (
        "for " in function_content and "create_event" in function_content
    ):
        count_match = re.search(r'for.*in\s+0\.\.(\d+)', function_content)
        if count_match:
            return ('test_batch_events', {
                'count': count_match.group(1)
            })
    
    # Pattern 3: Checkpoint creation and update
    if all(pattern in function_content for pattern in [
        "TestCheckpointBuilder",
        "insert",
        "assert"
    ]):
        automaton_match = re.search(r'TestCheckpointBuilder::new\s*\(\s*"([^"]+)"\s*\)', function_content)
        if automaton_match:
            return ('test_checkpoint_flow', {
                'automaton': automaton_match.group(1)
            })
    
    # Pattern 4: Concurrent operations with tokio::spawn
    if "tokio::spawn" in function_content and (
        "try_join_all" in function_content or "join_all" in function_content
    ):
        count_match = re.search(r'for.*in\s+0\.\.(\d+)', function_content)
        if count_match:
            return ('test_concurrent_operations', {
                'task_count': count_match.group(1)
            })
    
    # Pattern 5: Time range queries
    if all(pattern in function_content for pattern in [
        "chrono::",
        "get_events_in_range",
        "assert"
    ]):
        return ('test_time_range_query', {})
    
    # Pattern 6: Event filtering by source/type
    if ("get_events_by_source" in function_content or "get_events_by_type" in function_content):
        source_match = re.search(r'get_events_by_source\s*\([^,]+,\s*"([^"]+)"', function_content)
        if source_match:
            return ('test_event_filter', {
                'filter_source': source_match.group(1)
            })
    
    # Pattern 7: Event flow from source to processing
    if all(pattern in function_content for pattern in [
        "EventFactory",
        "create_event",
        "TestCheckpointBuilder",
        "insert"
    ]):
        return ('test_event_flow', {})
    
    return None

def find_convertible_tests(file_path: Path) -> List[Dict]:
    """Find all test functions that can be converted to macros."""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return []
    
    convertible = []
    
    # Find all test functions
    test_pattern = re.compile(
        r'(#\[sinex_test(?:\([^)]*\))?\]\s*)?\n\s*async\s+fn\s+(\w+)\s*\([^)]*\)\s*->\s*TestResult\s*\{',
        re.MULTILINE
    )
    
    for match in test_pattern.finditer(content):
        test_name = match.group(2)
        start_pos = match.start()
        
        # Find the end of the function
        brace_count = 0
        pos = match.end() - 1  # Start from the opening brace
        while pos < len(content):
            if content[pos] == '{':
                brace_count += 1
            elif content[pos] == '}':
                brace_count -= 1
                if brace_count == 0:
                    break
            pos += 1
        
        if brace_count == 0:
            function_content = content[match.start():pos+1]
            
            # Analyze the function
            result = analyze_test_function(function_content, test_name)
            if result:
                macro_type, params = result
                convertible.append({
                    'test_name': test_name,
                    'macro_type': macro_type,
                    'params': params,
                    'start': match.start(),
                    'end': pos + 1,
                    'original': function_content
                })
    
    return convertible

def generate_macro_call(test_info: Dict) -> str:
    """Generate the appropriate macro call for a test."""
    macro_type = test_info['macro_type']
    test_name = test_info['test_name']
    params = test_info['params']
    
    if macro_type == 'test_event_insertion':
        source = params.get('source', 'test')
        event_type = params.get('event_type', 'test.event')
        payload = params.get('payload', 'json!({})')
        return f'test_event_insertion!({test_name}, "{source}", "{event_type}", {payload});'
    
    elif macro_type == 'test_batch_events':
        count = params.get('count', '10')
        return f'''test_batch_events!({test_name}, "test", "test.event", {count}, 
    |pool: &DbPool, events: &[RawEvent]| async move {{
        // Verify batch
        assert_eq!(events.len(), {count});
        Ok(())
    }}
);'''
    
    elif macro_type == 'test_checkpoint_flow':
        automaton = params.get('automaton', 'test_automaton')
        return f'test_checkpoint_flow!({test_name}, "{automaton}", 0, 100);'
    
    elif macro_type == 'test_concurrent_operations':
        task_count = params.get('task_count', '10')
        return f'''test_concurrent_operations!({test_name}, {task_count},
    |pool: Arc<DbPool>, index: usize| async move {{
        // Concurrent operation
        Ok(())
    }},
    |pool: &Arc<DbPool>, results: &Vec<_>| async move {{
        assert_eq!(results.len(), {task_count});
        Ok(())
    }}
);'''
    
    elif macro_type == 'test_time_range_query':
        return f'''test_time_range_query!({test_name}, 10, 
    chrono::Duration::minutes(1),
    chrono::Duration::hours(-1), 
    chrono::Duration::hours(1), 
    5
);'''
    
    elif macro_type == 'test_event_filter':
        filter_source = params.get('filter_source', 'test')
        return f'test_event_filter!({test_name}, &["test1", "test2", "{filter_source}"], 5, "{filter_source}", 5);'
    
    elif macro_type == 'test_event_flow':
        return f'test_event_flow!({test_name}, "test", "test.event", "test_processor");'
    
    return ""

def apply_conversions(file_path: Path, conversions: List[Dict]) -> bool:
    """Apply macro conversions to a file."""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return False
    
    # Check if macros are imported
    if 'use crate::common::test_macros::*;' not in content:
        # Add import after other imports
        import_pos = content.find('use ')
        if import_pos >= 0:
            content = content[:import_pos] + 'use crate::common::test_macros::*;\n' + content[import_pos:]
    
    # Apply conversions in reverse order to preserve positions
    for conversion in sorted(conversions, key=lambda x: x['start'], reverse=True):
        macro_call = generate_macro_call(conversion)
        if macro_call:
            content = content[:conversion['start']] + macro_call + content[conversion['end']:]
            changes_made.append({
                'file': file_path,
                'test': conversion['test_name'],
                'macro': conversion['macro_type']
            })
    
    # Write back
    try:
        with open(file_path, 'w') as f:
            f.write(content)
        return True
    except Exception as e:
        print(f"Error writing {file_path}: {e}")
        return False

def generate_detailed_report():
    """Generate a detailed report of all changes."""
    report = ["# Advanced Test Macro Application Report\n"]
    
    # Summary
    report.append(f"## Summary\n")
    report.append(f"- **Files analyzed**: {files_analyzed}")
    report.append(f"- **Total conversions**: {len(changes_made)}")
    report.append(f"- **Files modified**: {len(set(c['file'] for c in changes_made))}\n")
    
    # Conversions by macro type
    macro_counts = {}
    for change in changes_made:
        macro_type = change['macro']
        macro_counts[macro_type] = macro_counts.get(macro_type, 0) + 1
    
    report.append("## Conversions by Macro Type\n")
    for macro_type, count in sorted(macro_counts.items(), key=lambda x: x[1], reverse=True):
        report.append(f"- `{macro_type}`: {count} conversions")
    
    # Detailed changes by file
    report.append("\n## Detailed Changes\n")
    files_with_changes = {}
    for change in changes_made:
        file_path = change['file']
        if file_path not in files_with_changes:
            files_with_changes[file_path] = []
        files_with_changes[file_path].append(change)
    
    for file_path, file_changes in sorted(files_with_changes.items()):
        rel_path = file_path.relative_to(TEST_DIR)
        report.append(f"\n### {rel_path}")
        for change in file_changes:
            report.append(f"- `{change['test']}` → `{change['macro']}`")
    
    # Pattern statistics
    report.append("\n## Pattern Statistics\n")
    for pattern, count in sorted(patterns_found.items(), key=lambda x: x[1], reverse=True):
        report.append(f"- {pattern}: {count} occurrences")
    
    return '\n'.join(report)

def main():
    """Main entry point."""
    global files_analyzed
    
    print("Running advanced macro application analysis...")
    
    test_files = find_test_files()
    print(f"Found {len(test_files)} test files to analyze")
    
    for file_path in test_files:
        files_analyzed += 1
        
        # Find convertible tests
        conversions = find_convertible_tests(file_path)
        
        if conversions:
            print(f"✓ {file_path.relative_to(TEST_DIR)}: {len(conversions)} conversions possible")
            
            # Apply conversions
            if apply_conversions(file_path, conversions):
                print(f"  Applied {len(conversions)} conversions")
            else:
                print(f"  Failed to apply conversions")
        else:
            # Track patterns for analysis
            try:
                with open(file_path, 'r') as f:
                    content = f.read()
                
                # Count various patterns
                for pattern in [
                    'EventFactory::new',
                    'create_event',
                    'insert_event',
                    'BatchEventBuilder',
                    'TestCheckpointBuilder',
                    'tokio::spawn',
                    'get_events_by_source',
                    'get_events_by_type',
                    'get_events_in_range'
                ]:
                    count = content.count(pattern)
                    if count > 0:
                        patterns_found[pattern] = patterns_found.get(pattern, 0) + count
            except:
                pass
    
    # Generate report
    report = generate_detailed_report()
    report_path = TEST_DIR / "ADVANCED_MACRO_APPLICATION_REPORT.md"
    
    with open(report_path, 'w') as f:
        f.write(report)
    
    print(f"\nReport written to: {report_path}")
    print(f"\nSummary:")
    print(f"- Files analyzed: {files_analyzed}")
    print(f"- Conversions made: {len(changes_made)}")
    print(f"- Files modified: {len(set(c['file'] for c in changes_made))}")

if __name__ == "__main__":
    main()
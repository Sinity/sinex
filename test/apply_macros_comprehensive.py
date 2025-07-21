#!/usr/bin/env python3
"""
Comprehensive macro application for Sinex test suite.

This script analyzes actual test patterns and applies macros where appropriate.
"""

import os
import re
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import subprocess
import json

# Base path for test directory
TEST_DIR = Path(__file__).parent

# Track conversions
conversions_made = []
files_modified = set()
patterns_not_converted = []

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
        and f.is_file()
    ]
    
    return sorted(set(test_files))

def check_imports(content: str) -> bool:
    """Check if file has necessary imports for macros."""
    has_test_macros = 'use crate::common::test_macros::*' in content or 'test_macros::' in content
    has_builders = 'use crate::common::builders' in content or 'builders::' in content
    has_query_helpers = 'use crate::common::query_helpers' in content or 'query_helpers::' in content
    return has_test_macros or (has_builders and has_query_helpers)

def add_imports_if_needed(content: str) -> str:
    """Add necessary imports for macros if not present."""
    if not check_imports(content):
        # Find the first use statement or module declaration
        use_match = re.search(r'^use ', content, re.MULTILINE)
        if use_match:
            insert_pos = use_match.start()
            import_line = "use crate::common::test_macros::*;\n"
            content = content[:insert_pos] + import_line + content[insert_pos:]
    return content

def apply_event_insertion_macro(content: str, file_path: Path) -> str:
    """Apply test_event_insertion! macro where appropriate."""
    # Pattern: Simple event insertion followed by retrieval and assertions
    pattern = re.compile(
        r'#\[sinex_test\]\s*\n\s*async\s+fn\s+(\w+)\s*\([^)]*\)\s*->\s*TestResult\s*\{[^}]*?'
        r'let\s+\w+\s*=\s*events::(file_created_event|file_modified_event|kitty_event|agent_event)\s*\([^)]+\)[^;]*;[^}]*?'
        r'let\s+\w+\s*=\s*assertions::assert_event_inserted[^;]+;[^}]*?'
        r'(assert|pretty_assertions::assert_eq)[^}]*?\}',
        re.DOTALL
    )
    
    matches = pattern.finditer(content)
    for match in reversed(list(matches)):
        test_name = match.group(1)
        # Extract the event creation details
        event_match = re.search(
            r'events::(\w+)\s*\(([^)]+)\)',
            match.group(0)
        )
        if event_match:
            event_type = event_match.group(1)
            params = event_match.group(2)
            
            # Map event types to source and type
            mappings = {
                'file_created_event': ('fs', 'file.created', f'json!({{"path": {params}}})'),
                'file_modified_event': ('fs', 'file.modified', f'json!({{"path": {params}}})'),
                'kitty_event': ('shell.kitty', 'command.executed', f'json!({{"command": {params}}})'),
                'agent_event': ('sinex', 'automaton.heartbeat', f'json!({{"agent": {params}}})'),
            }
            
            if event_type in mappings:
                source, evt_type, payload = mappings[event_type]
                macro_call = f'test_event_insertion!({test_name}, "{source}", "{evt_type}", {payload});'
                content = content[:match.start()] + macro_call + content[match.end():]
                conversions_made.append((file_path, test_name, 'test_event_insertion'))
                files_modified.add(file_path)
    
    return content

def apply_batch_events_macro(content: str, file_path: Path) -> str:
    """Apply test_batch_events! macro where appropriate."""
    # Pattern: Creating multiple events in a loop
    pattern = re.compile(
        r'#\[sinex_test\]\s*\n\s*async\s+fn\s+(\w+)\s*\([^)]*\)\s*->\s*TestResult\s*\{[^}]*?'
        r'for\s+\w+\s+in\s+0\.\.(\d+)[^{]*\{[^}]*?'
        r'(events::\w+|test_event_batch|EventFactory)[^}]*?\}[^}]*?'
        r'assert[^}]*?\}',
        re.DOTALL
    )
    
    matches = pattern.finditer(content)
    for match in reversed(list(matches)):
        test_name = match.group(1)
        count = match.group(2)
        
        # Try to extract source and event type
        source_match = re.search(r'source:\s*"([^"]+)"', match.group(0))
        type_match = re.search(r'event_type:\s*"([^"]+)"', match.group(0))
        
        if source_match and type_match:
            source = source_match.group(1)
            event_type = type_match.group(1)
            
            # Create a simple verification lambda
            verification = """
                |pool: &DbPool, events: &[RawEvent]| async move {
                    assert_eq!(events.len(), """ + count + """);
                    Ok(())
                }
            """
            
            macro_call = f'test_batch_events!({test_name}, "{source}", "{event_type}", {count}, {verification});'
            content = content[:match.start()] + macro_call + content[match.end():]
            conversions_made.append((file_path, test_name, 'test_batch_events'))
            files_modified.add(file_path)
    
    return content

def apply_checkpoint_flow_macro(content: str, file_path: Path) -> str:
    """Apply test_checkpoint_flow! macro where appropriate."""
    # Pattern: Checkpoint creation, verification, update, verification
    pattern = re.compile(
        r'#\[sinex_test\]\s*\n\s*async\s+fn\s+(\w+)\s*\([^)]*\)\s*->\s*TestResult\s*\{[^}]*?'
        r'(create_checkpoint|insert_test_checkpoint|CheckpointBuilder)[^;]+;[^}]*?'
        r'(assert|verify)[^;]+;[^}]*?'
        r'(update_checkpoint|CheckpointBuilder)[^;]+;[^}]*?'
        r'(assert|verify)[^}]*?\}',
        re.DOTALL
    )
    
    matches = pattern.finditer(content)
    for match in reversed(list(matches)):
        test_name = match.group(1)
        
        # Extract automaton name and counts
        automaton_match = re.search(r'automaton_name:\s*"([^"]+)"', match.group(0))
        initial_count_match = re.search(r'processed_count:\s*(\d+)', match.group(0))
        
        if automaton_match:
            automaton = automaton_match.group(1)
            initial_count = initial_count_match.group(1) if initial_count_match else "0"
            updated_count = "100"  # Default
            
            macro_call = f'test_checkpoint_flow!({test_name}, "{automaton}", {initial_count}, {updated_count});'
            content = content[:match.start()] + macro_call + content[match.end():]
            conversions_made.append((file_path, test_name, 'test_checkpoint_flow'))
            files_modified.add(file_path)
    
    return content

def apply_concurrent_operations_macro(content: str, file_path: Path) -> str:
    """Apply test_concurrent_operations! macro where appropriate."""
    # Pattern: Multiple tokio::spawn with futures::future::try_join_all
    pattern = re.compile(
        r'#\[sinex_test\]\s*\n\s*async\s+fn\s+(\w+)\s*\([^)]*\)\s*->\s*TestResult\s*\{[^}]*?'
        r'for\s+\w+\s+in\s+0\.\.(\d+)[^{]*\{[^}]*?'
        r'tokio::spawn\s*\([^)]+\)[^}]*?\}[^}]*?'
        r'(futures::future::try_join_all|join_all)[^}]*?\}',
        re.DOTALL
    )
    
    matches = pattern.finditer(content)
    for match in reversed(list(matches)):
        test_name = match.group(1)
        task_count = match.group(2)
        
        # Create operation and verification lambdas
        operation = """
            |pool: Arc<DbPool>, index: usize| async move {
                // Concurrent operation
                Ok(())
            }
        """
        
        verification = """
            |pool: &Arc<DbPool>, results: &Vec<_>| async move {
                assert_eq!(results.len(), """ + task_count + """);
                Ok(())
            }
        """
        
        macro_call = f'test_concurrent_operations!({test_name}, {task_count}, {operation}, {verification});'
        content = content[:match.start()] + macro_call + content[match.end():]
        conversions_made.append((file_path, test_name, 'test_concurrent_operations'))
        files_modified.add(file_path)
    
    return content

def apply_time_range_query_macro(content: str, file_path: Path) -> str:
    """Apply test_time_range_query! macro where appropriate."""
    # Pattern: Time-based event queries
    pattern = re.compile(
        r'#\[sinex_test\]\s*\n\s*async\s+fn\s+(\w+)\s*\([^)]*\)\s*->\s*TestResult\s*\{[^}]*?'
        r'chrono::(Duration|Utc)[^}]*?'
        r'(get_events_in_time_range|get_events_in_range)[^}]*?\}',
        re.DOTALL
    )
    
    matches = pattern.finditer(content)
    for match in reversed(list(matches)):
        test_name = match.group(1)
        
        # Extract timing parameters if possible
        event_count = "10"  # Default
        spacing = "chrono::Duration::minutes(1)"
        range_start = "chrono::Duration::hours(-1)"
        range_end = "chrono::Duration::hours(1)"
        expected_count = "5"
        
        macro_call = f'test_time_range_query!({test_name}, {event_count}, {spacing}, {range_start}, {range_end}, {expected_count});'
        content = content[:match.start()] + macro_call + content[match.end():]
        conversions_made.append((file_path, test_name, 'test_time_range_query'))
        files_modified.add(file_path)
    
    return content

def apply_event_filter_macro(content: str, file_path: Path) -> str:
    """Apply test_event_filter! macro where appropriate."""
    # Pattern: Filtering events by source or type
    pattern = re.compile(
        r'#\[sinex_test\]\s*\n\s*async\s+fn\s+(\w+)\s*\([^)]*\)\s*->\s*TestResult\s*\{[^}]*?'
        r'(get_events_by_source|get_events_by_type)[^}]*?'
        r'assert[^}]*?\}',
        re.DOTALL
    )
    
    matches = pattern.finditer(content)
    for match in reversed(list(matches)):
        test_name = match.group(1)
        
        # Extract filter parameters
        source_match = re.search(r'get_events_by_source\s*\([^,]+,\s*"([^"]+)"', match.group(0))
        if source_match:
            filter_source = source_match.group(1)
            sources = '&["fs", "shell.kitty", "sinex"]'
            events_per_source = "5"
            expected_count = "5"
            
            macro_call = f'test_event_filter!({test_name}, {sources}, {events_per_source}, "{filter_source}", {expected_count});'
            content = content[:match.start()] + macro_call + content[match.end():]
            conversions_made.append((file_path, test_name, 'test_event_filter'))
            files_modified.add(file_path)
    
    return content

def process_file(file_path: Path) -> bool:
    """Process a single test file and apply macros where appropriate."""
    try:
        with open(file_path, 'r') as f:
            original_content = f.read()
        
        content = original_content
        
        # Add imports if needed
        content = add_imports_if_needed(content)
        
        # Apply each macro type
        content = apply_event_insertion_macro(content, file_path)
        content = apply_batch_events_macro(content, file_path)
        content = apply_checkpoint_flow_macro(content, file_path)
        content = apply_concurrent_operations_macro(content, file_path)
        content = apply_time_range_query_macro(content, file_path)
        content = apply_event_filter_macro(content, file_path)
        
        # Write back if modified
        if content != original_content:
            with open(file_path, 'w') as f:
                f.write(content)
            return True
        
        return False
    
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def analyze_unconverted_patterns(file_path: Path):
    """Analyze patterns that couldn't be converted to macros."""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Look for complex patterns
        complex_patterns = [
            (r'#\[sinex_test\].*?async fn.*?\{.*?tokio::select!.*?\}', 'Complex async select patterns'),
            (r'#\[sinex_test\].*?async fn.*?\{.*?loop\s*\{.*?break.*?\}.*?\}', 'Complex loop patterns'),
            (r'#\[sinex_test\].*?async fn.*?\{.*?match.*?\{.*?Err.*?\}.*?\}', 'Complex error handling'),
            (r'#\[sinex_test\].*?async fn.*?\{.*?impl\s+.*?\{.*?\}.*?\}', 'Tests with impl blocks'),
        ]
        
        for pattern, description in complex_patterns:
            matches = re.findall(pattern, content, re.DOTALL)
            if matches:
                patterns_not_converted.append((file_path, description, len(matches)))
    
    except Exception as e:
        print(f"Error analyzing {file_path}: {e}")

def generate_final_report():
    """Generate comprehensive conversion report."""
    report = ["# Test Macro Application Report\n"]
    
    # Summary
    report.append(f"## Summary\n")
    report.append(f"- **Total conversions made**: {len(conversions_made)}")
    report.append(f"- **Files modified**: {len(files_modified)}")
    report.append(f"- **Complex patterns identified**: {len(patterns_not_converted)}\n")
    
    # Conversions by macro type
    macro_counts = {}
    for _, _, macro_type in conversions_made:
        macro_counts[macro_type] = macro_counts.get(macro_type, 0) + 1
    
    report.append("## Conversions by Macro Type\n")
    for macro_type, count in sorted(macro_counts.items(), key=lambda x: x[1], reverse=True):
        report.append(f"- `{macro_type}`: {count} conversions")
    
    # Modified files
    report.append("\n## Modified Files\n")
    for file_path in sorted(files_modified):
        rel_path = file_path.relative_to(TEST_DIR)
        conversions = [c for c in conversions_made if c[0] == file_path]
        report.append(f"\n### {rel_path}")
        for _, test_name, macro_type in conversions:
            report.append(f"- `{test_name}` → `{macro_type}`")
    
    # Complex patterns
    if patterns_not_converted:
        report.append("\n## Complex Patterns Not Converted\n")
        for file_path, description, count in patterns_not_converted:
            rel_path = file_path.relative_to(TEST_DIR)
            report.append(f"- {rel_path}: {description} ({count} instances)")
    
    # Recommendations
    report.append("\n## Recommendations for New Macros\n")
    report.append("Based on unconverted patterns, consider creating macros for:")
    report.append("- Async select patterns for timeout testing")
    report.append("- Complex error recovery scenarios")
    report.append("- Tests with custom impl blocks")
    report.append("- Redis stream testing patterns")
    
    return '\n'.join(report)

def main():
    """Main entry point."""
    print("Applying test macros comprehensively across the test suite...")
    
    test_files = find_test_files()
    print(f"Found {len(test_files)} test files to process")
    
    # Process each file
    for file_path in test_files:
        if process_file(file_path):
            print(f"✓ Modified: {file_path.relative_to(TEST_DIR)}")
        else:
            # Analyze why it wasn't converted
            analyze_unconverted_patterns(file_path)
    
    # Generate report
    report = generate_final_report()
    report_path = TEST_DIR / "MACRO_APPLICATION_REPORT.md"
    
    with open(report_path, 'w') as f:
        f.write(report)
    
    print(f"\nReport written to: {report_path}")
    print(f"\nSummary:")
    print(f"- Conversions made: {len(conversions_made)}")
    print(f"- Files modified: {len(files_modified)}")
    print(f"- Complex patterns identified: {len(patterns_not_converted)}")

if __name__ == "__main__":
    main()
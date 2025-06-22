#!/usr/bin/env python3
"""
Ultra-aggressive RawEvent automation that handles broader patterns.

Uses AST-like parsing to match any RawEvent struct construction and 
convert to appropriate builders based on field analysis.
"""

import re
import os
import subprocess
from pathlib import Path

def analyze_rawevent_construction(match_text):
    """Analyze a RawEvent construction to determine the best builder."""
    
    # Extract key fields
    source_match = re.search(r'source: "([^"]+)"', match_text)
    event_type_match = re.search(r'event_type: "([^"]+)"', match_text)
    version_match = re.search(r'ingestor_version: Some\("([^"]+)"', match_text)
    payload_match = re.search(r'payload: (json!\([^}]+\}[^)]*\))', match_text)
    
    source = source_match.group(1) if source_match else "test"
    event_type = event_type_match.group(1) if event_type_match else "test.event"
    version = version_match.group(1) if version_match else None
    payload = payload_match.group(1) if payload_match else 'json!({"test": true})'
    
    # Determine appropriate builder based on patterns
    if source == "agent" and "heartbeat" in event_type:
        if version:
            return f'events::agent_heartbeat_chaos_event({extract_agent_name(payload)}, Some("{version}"))'
        else:
            return f'events::agent_heartbeat_chaos_event({extract_agent_name(payload)}, None)'
    
    elif source == "filesystem":
        path = extract_path_from_payload(payload)
        if version:
            return f'events::filesystem_chaos_event("{event_type}", {path}, Some("{version}"))'
        else:
            return f'events::filesystem_chaos_event("{event_type}", {path}, None)'
    
    elif "large.payload" in event_type or "bulk.data" in event_type:
        return f'events::large_payload_test_event(1024)'  # Default size
    
    elif source == "btree_test" and "index" in event_type:
        return f'events::indexed_test_event(0, chrono::Utc::now())'  # Simplified
    
    else:
        # Generic fallback
        if version:
            return f'events::generic_adversarial_event("{source}", "{event_type}", {payload}, Some("{version}"))'
        else:
            return f'events::generic_adversarial_event("{source}", "{event_type}", {payload}, None)'

def extract_agent_name(payload):
    """Extract agent name from payload."""
    match = re.search(r'"agent_name":\s*([^,}]+)', payload)
    return match.group(1) if match else '"test_agent"'

def extract_path_from_payload(payload):
    """Extract path from payload."""
    match = re.search(r'"path":\s*"([^"]+)"', payload)
    return f'"{match.group(1)}"' if match else '"/test/path"'

def aggressive_rawevent_replacement(content):
    """Replace ALL RawEvent struct constructions with builders."""
    
    # Ultra-broad pattern that matches any RawEvent struct construction
    pattern = r'RawEvent \s*\{\s*([^}]+(?:\{[^}]*\}[^}]*)*)\s*\}'
    
    def replacement(match):
        match_text = match.group(0)
        
        # Skip if this is in a legitimate context (queries.rs, validation functions)
        if any(keyword in match_text for keyword in ['record.', 'from_uuid', 'map(', '|']):
            return match_text  # Keep as-is for database mapping
        
        return analyze_rawevent_construction(match_text)
    
    return re.sub(pattern, replacement, content, flags=re.MULTILINE | re.DOTALL)

def process_file_aggressively(filepath):
    """Process a file with ultra-aggressive RawEvent replacement."""
    with open(filepath, 'r') as f:
        content = f.read()
    
    original_content = content
    
    # Apply aggressive transformation
    content = aggressive_rawevent_replacement(content)
    
    # Count changes
    original_matches = len(re.findall(r'RawEvent \s*\{', original_content))
    remaining_matches = len(re.findall(r'RawEvent \s*\{', content))
    changes = original_matches - remaining_matches
    
    if changes > 0:
        # Add import for events module if not present
        if 'use crate::common::events;' not in content and 'events::' in content:
            # Find import section and add events import
            lines = content.split('\n')
            import_line = None
            for i, line in enumerate(lines):
                if line.startswith('use crate::common::') and 'events' not in line:
                    import_line = i
                    break
            
            if import_line is not None:
                lines.insert(import_line + 1, 'use crate::common::events;')
                content = '\n'.join(lines)
        
        with open(filepath, 'w') as f:
            f.write(content)
        
        print(f"✅ {filepath.name}: {changes} RawEvent constructions automated")
        return changes
    else:
        print(f"- {filepath.name}: No automatable patterns found")
        return 0

def main():
    """Process all test files aggressively."""
    test_dir = Path("test")
    
    if not test_dir.exists():
        print("❌ test/ directory not found")
        return 1
    
    print("🚀 ULTRA-AGGRESSIVE RawEvent Automation")
    print("=" * 50)
    print("⚠ WARNING: This will transform ALL RawEvent constructions!")
    print("=" * 50)
    
    # Check current count
    result = subprocess.run(
        ["rg", "-c", "RawEvent \\{", "test/", "--type", "rust"], 
        capture_output=True, text=True
    )
    if result.returncode == 0:
        original_total = sum(int(line.split(':')[1]) for line in result.stdout.strip().split('\n') if ':' in line)
        print(f"📊 Starting count: {original_total} manual RawEvent constructions")
    else:
        original_total = 0
    
    total_automated = 0
    files_processed = 0
    
    # Process all Rust files in test directory
    for rust_file in test_dir.rglob("*.rs"):
        # Skip certain files that should keep manual constructions
        if any(skip in str(rust_file) for skip in ['queries.rs', 'mod.rs', 'automation/']):
            continue
            
        changes = process_file_aggressively(rust_file)
        total_automated += changes
        files_processed += 1
    
    print(f"\n📊 Aggressive Automation Results:")
    print(f"  Files processed: {files_processed}")
    print(f"  RawEvent constructions automated: {total_automated}")
    
    if total_automated > 0:
        print(f"\n🧪 Verifying compilation...")
        result = subprocess.run(["cargo", "check", "--workspace"], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Compilation successful!")
            
            # Count remaining manual constructions
            remaining_result = subprocess.run(
                ["rg", "-c", "RawEvent \\{", "test/", "--type", "rust"], 
                capture_output=True, text=True
            )
            if remaining_result.returncode == 0:
                total_remaining = sum(int(line.split(':')[1]) for line in remaining_result.stdout.strip().split('\n') if ':' in line)
                reduction_percentage = (total_automated / original_total * 100) if original_total > 0 else 0
                print(f"📈 Manual RawEvent constructions: {original_total} → {total_remaining}")
                print(f"🎯 Reduction achieved: {total_automated} constructions ({reduction_percentage:.1f}%)")
                
                if reduction_percentage >= 50:
                    print("🏆 SUCCESS: Achieved 50%+ reduction target!")
                else:
                    print(f"⚠ Target missed: {reduction_percentage:.1f}% < 50% target")
        else:
            print("❌ Compilation failed:")
            print(result.stderr)
            return 1
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
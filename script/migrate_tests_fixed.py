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
    def __init__(self, dry_run: bool = False, verbose: bool = False, quality: bool = False, aggressive: bool = False):
        self.dry_run = dry_run
        self.verbose = verbose
        self.quality = quality
        self.aggressive = aggressive
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
                            # Has return type, replace entire signature more carefully
                            new_signature = f'async fn {test_name}(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>>'
                            
                            # Use a more precise replacement - find the function signature and replace it
                            fn_pattern = rf'async\s+fn\s+{re.escape(test_name)}\s*\(\s*\)\s*->\s*[^{{]+' 
                            lines[i + 1] = re.sub(fn_pattern, new_signature, next_line)
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
        
        # Track which variable names we've removed so we can replace their usage
        removed_pool_vars = set()
        
        for i, line in enumerate(lines):
            original_line = line
            
            # Pattern: let pool = get_shared_test_pool().await?;
            # Remove the line entirely since we use ctx.pool() directly
            if 'get_shared_test_pool' in line:
                match = re.search(r'let\s+(\w+)\s*=\s*(?:database_helpers::)?get_shared_test_pool\(\)\.await\?;', line)
                if match:
                    var_name = match.group(1)
                    removed_pool_vars.add(var_name)
                    line = '// Removed: using ctx.pool() directly instead'
                    changes.append(MigrationChange(
                        line_num=i + 1,
                        original=original_line.strip(),
                        replacement=line.strip(),
                        description=f'Remove get_shared_test_pool - use ctx.pool() directly (var: {var_name})'
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
        
        # Second pass: replace usage of removed pool variables
        for i, line in enumerate(lines):
            original_line = line
            
            for var_name in removed_pool_vars:
                # Replace &var_name with ctx.pool()
                if f'&{var_name}' in line and not line.strip().startswith('//'):
                    line = re.sub(rf'\b&{var_name}\b', 'ctx.pool()', line)
                    if line != original_line:
                        changes.append(MigrationChange(
                            line_num=i + 1,
                            original=original_line.strip(),
                            replacement=line.strip(),
                            description=f'Replace &{var_name} reference with ctx.pool()'
                        ))
                        break  # Only process one replacement per line
            
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
        
        # Find all sinex_test functions and their ranges
        test_functions = []
        for i, line in enumerate(lines):
            if '#[sinex_test]' in line:
                # Find the function signature
                for j in range(i + 1, min(i + 5, len(lines))):
                    if 'async fn' in lines[j] and 'TestContext' in lines[j]:
                        test_functions.append((i, j))
                        break
        
        # Process each test function
        for attr_line, fn_line in test_functions:
            # Find the function body range
            brace_depth = 0
            function_start = -1
            function_end = -1
            
            for i in range(fn_line, len(lines)):
                line = lines[i]
                if '{' in line and function_start == -1:
                    function_start = i
                
                if function_start != -1:
                    brace_depth += line.count('{') - line.count('}')
                    if brace_depth == 0:
                        function_end = i
                        break
            
            if function_start == -1 or function_end == -1:
                continue
            
            # Find the last non-empty, non-comment line before closing brace
            last_statement_line = -1
            for i in range(function_end - 1, function_start, -1):
                line = lines[i].strip()
                if line and not line.startswith('//') and line != '}':
                    last_statement_line = i
                    break
            
            if last_statement_line == -1:
                continue
            
            last_line = lines[last_statement_line].strip()
            
            # Check if it already returns Ok or has a return statement
            if (last_line.endswith('Ok(())') or 
                last_line.endswith('Ok(())?') or
                last_line.startswith('Ok(') or 
                last_line.startswith('return') or
                '?' in last_line and last_line.endswith(';')):
                continue
            
            # Get the indentation for the closing brace line
            indent_match = re.match(r'^(\s*)', lines[function_end])
            if indent_match:
                base_indent = indent_match.group(1)
                statement_indent = base_indent + '    '  # Add one level of indentation
            else:
                statement_indent = '    '
            
            # Insert Ok(()) before the closing brace
            lines.insert(function_end, f'{statement_indent}Ok(())')
            changes.append(MigrationChange(
                line_num=function_end + 1,
                original='',
                replacement=f'{statement_indent}Ok(())',
                description='Add missing Ok(()) return'
            ))
        
        return '\n'.join(lines), changes
    
    def fix_variable_shadowing(self, content: str) -> Tuple[str, List[MigrationChange]]:
        """Fix variable shadowing issues where TestContext parameter is shadowed."""
        changes = []
        lines = content.split('\n')
        
        # Find sinex_test functions
        for i, line in enumerate(lines):
            if '#[sinex_test]' in line:
                # Find the function signature
                for j in range(i + 1, min(i + 5, len(lines))):
                    if 'async fn' in lines[j] and 'TestContext' in lines[j]:
                        # Extract the parameter name (usually 'ctx')
                        param_match = re.search(r'(\w+):\s*TestContext', lines[j])
                        if not param_match:
                            break
                        
                        param_name = param_match.group(1)
                        
                        # Find function body and look for shadowing
                        brace_depth = 0
                        function_start = -1
                        function_end = -1
                        
                        for k in range(j, len(lines)):
                            line_k = lines[k]
                            if '{' in line_k and function_start == -1:
                                function_start = k
                            
                            if function_start != -1:
                                brace_depth += line_k.count('{') - line_k.count('}')
                                if brace_depth == 0:
                                    function_end = k
                                    break
                        
                        if function_start == -1 or function_end == -1:
                            break
                        
                        # Look for variable shadowing within the function
                        for k in range(function_start + 1, function_end):
                            line_k = lines[k]
                            
                            # Check for variable declarations that shadow the parameter
                            shadow_pattern = rf'let\s+{param_name}\s*='
                            if re.search(shadow_pattern, line_k):
                                # Rename the shadowing variable
                                new_var_name = f'{param_name}_local'
                                new_line = re.sub(
                                    rf'let\s+{param_name}\s*=',
                                    f'let {new_var_name} =',
                                    line_k
                                )
                                
                                if new_line != line_k:
                                    lines[k] = new_line
                                    changes.append(MigrationChange(
                                        line_num=k + 1,
                                        original=line_k.strip(),
                                        replacement=new_line.strip(),
                                        description=f'Rename shadowing variable {param_name} to {new_var_name}'
                                    ))
                                    
                                    # Update all subsequent references in the function
                                    for l in range(k + 1, function_end):
                                        line_l = lines[l]
                                        # Replace usage of the old variable name (but not the parameter)
                                        # Be careful not to replace the parameter name
                                        updated_line = re.sub(
                                            rf'\b{param_name}(?!\s*:|\()',  # Don't match parameter declarations or method calls on the parameter
                                            new_var_name,
                                            line_l
                                        )
                                        if updated_line != line_l:
                                            lines[l] = updated_line
                                            changes.append(MigrationChange(
                                                line_num=l + 1,
                                                original=line_l.strip(),
                                                replacement=updated_line.strip(),
                                                description=f'Update reference from {param_name} to {new_var_name}'
                                            ))
                        break
        
        return '\n'.join(lines), changes
    
    def fix_unwraps(self, content: str, aggressive: bool = False) -> Tuple[str, List[MigrationChange]]:
        """Replace .unwrap() with ? operator where appropriate."""
        changes = []
        lines = content.split('\n')
        
        # Define replacement patterns
        basic_patterns = [
            # Basic end-of-statement unwrap
            (r'\.unwrap\(\);', '?;'),
            # Await unwrap pattern
            (r'\.await\.unwrap\(\)', '.await?'),
        ]
        
        aggressive_patterns = [
            # Assignment patterns
            (r'let\s+(\w+)\s*=\s*(.*?)\.unwrap\(\);', r'let \1 = \2?;'),
            # Function call results
            (r'=\s*(\w+)\(\)\.unwrap\(\)', r'= \1()?'),
            # Method call results
            (r'=\s*(\w+)\.(\w+)\(\)\.unwrap\(\)', r'= \1.\2()?'),
            # Unwrap in function arguments (careful with this one)
            (r'(\w+)\(([^)]*?)\.unwrap\(\)\)', r'\1(\2?)'),
        ]
        
        patterns = basic_patterns
        if aggressive:
            patterns.extend(aggressive_patterns)
        
        for i, line in enumerate(lines):
            original_line = line
            
            # Skip if it's a comment or string literal
            if line.strip().startswith('//') or '".unwrap()"' in line or "'.unwrap()'" in line:
                continue
            
            # Skip if it's in an assert - these often should stay as unwrap
            if 'assert' in line:
                continue
            
            # Apply patterns
            modified = False
            for pattern, replacement in patterns:
                new_line = re.sub(pattern, replacement, line)
                if new_line != line:
                    line = new_line
                    modified = True
                    break
            
            if modified:
                lines[i] = line
                changes.append(MigrationChange(
                    line_num=i + 1,
                    original=original_line.strip(),
                    replacement=line.strip(),
                    description='Replace .unwrap() with ? operator'
                ))
        
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
    
    def generate_assert_message(self, left: str, right: str, is_eq: bool = True) -> str:
        """Generate contextual assertion messages based on common patterns."""
        
        # Length comparisons
        if 'len()' in left or 'len()' in right:
            if right.isdigit():
                return f"expected length {right} but got {{}}"
            return "length mismatch: expected {} but got {}"
        
        # Empty checks
        elif 'is_empty()' in left:
            return "expected empty collection but was not"
        elif 'is_empty()' in right:
            return "expected non-empty collection but was empty"
        
        # Error/Ok checks
        elif 'is_err()' in left:
            return "expected Error variant but got Ok"
        elif 'is_ok()' in left:
            return "expected Ok variant but got Error"
        
        # Count comparisons
        elif 'count' in left.lower() and right.isdigit():
            return f"expected count of {right} but got {{}}"
        
        # Size comparisons
        elif 'size' in left.lower() and right.isdigit():
            return f"expected size {right} but got {{}}"
        
        # Contains checks
        elif 'contains(' in left:
            return "expected substring/item not found in collection"
        
        # Boolean comparisons
        elif right == 'true':
            return f"expected {left} to be true"
        elif right == 'false':
            return f"expected {left} to be false"
        
        # Status/state checks
        elif 'status' in left.lower():
            return f"unexpected status: expected {right}"
        elif 'state' in left.lower():
            return f"unexpected state: expected {right}"
        
        # Numeric comparisons
        elif right.isdigit() or (right.startswith('-') and right[1:].isdigit()):
            return f"expected value {right} but got {{}}"
        
        # Default but still contextual
        if is_eq:
            return f"assertion failed: expected {right}"
        else:
            return f"assertion failed: {left}"
    
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
                
                message = self.generate_assert_message(left, right, is_eq=True)
                
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
            
            # Pattern: assert_ne!(a, b); without message
            match = re.search(r'assert_ne!\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*\);', line)
            if match:
                left = match.group(1).strip()
                right = match.group(2).strip()
                
                message = f"expected values to be different but both were {left}"
                
                new_line = line.replace(
                    match.group(0),
                    f'assert_ne!({left}, {right}, "{message}");'
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
                
                # Generate contextual message based on condition
                if '!' in condition and not condition.startswith('!'):
                    # Negation in the middle
                    message = f"assertion failed: {condition}"
                elif condition.startswith('!'):
                    # Negation at start
                    inner = condition[1:].strip()
                    message = f"expected {inner} to be false"
                elif '.is_' in condition:
                    # is_* method calls
                    if '.is_empty()' in condition:
                        message = "expected empty collection"
                    elif '.is_some()' in condition:
                        message = "expected Some variant but got None"
                    elif '.is_none()' in condition:
                        message = "expected None but got Some"
                    elif '.is_ok()' in condition:
                        message = "expected Ok but got Error"
                    elif '.is_err()' in condition:
                        message = "expected Error but got Ok"
                    else:
                        message = f"{condition} check failed"
                elif 'contains(' in condition:
                    message = "substring/item not found"
                elif '==' in condition:
                    parts = condition.split('==')
                    if len(parts) == 2:
                        message = f"expected {parts[0].strip()} to equal {parts[1].strip()}"
                    else:
                        message = f"equality check failed: {condition}"
                elif '>' in condition or '<' in condition:
                    message = f"comparison failed: {condition}"
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
    
    def fix_sleep_patterns_conservative(self, content: str) -> Tuple[str, List[MigrationChange]]:
        """Replace obviously safe sleep patterns with proper synchronization."""
        changes = []
        lines = content.split('\n')
        
        for i in range(len(lines) - 3):  # Need lookahead for context
            line = lines[i]
            
            # Skip if it's a comment
            if line.strip().startswith('//'):
                continue
            
            # Look for sleep patterns
            if ('sleep(' in line or 'time::sleep' in line) and 'Duration::from_' in line:
                # Extract duration if possible
                duration_match = re.search(r'from_(?:secs|millis)\((\d+)\)', line)
                if not duration_match:
                    continue
                
                duration_value = int(duration_match.group(1))
                duration_unit = 'secs' if 'from_secs' in line else 'millis'
                
                # Only consider sleeps >= 1 second (likely synchronization)
                if duration_unit == 'millis' and duration_value < 1000:
                    continue
                
                # Check next 3-5 lines for assertions or checks
                lookahead_lines = lines[i+1:i+6]
                has_assertion = any('assert' in l for l in lookahead_lines)
                has_check = any(pattern in ' '.join(lookahead_lines) for pattern in [
                    'count', 'len()', 'is_empty', 'contains', 'should', 'expected'
                ])
                
                # Check if this is in a sinex_test (has TestContext available)
                has_test_context = False
                # Look backwards for function signature
                for j in range(max(0, i-10), i):
                    if 'ctx: TestContext' in lines[j]:
                        has_test_context = True
                        break
                
                if has_test_context and (has_assertion or has_check):
                    # Safe to replace
                    indent = re.match(r'^(\s*)', line).group(1)
                    new_line = f'{indent}ctx.wait_for_work_queue(0).await?;'
                    
                    lines[i] = new_line
                    changes.append(MigrationChange(
                        line_num=i + 1,
                        original=line.strip(),
                        replacement=new_line.strip(),
                        description=f'Replace {duration_value}{duration_unit} sleep with wait_for_work_queue'
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
            
            content, shadow_changes = self.fix_variable_shadowing(content)
            all_changes.extend(shadow_changes)
            
            content, ok_changes = self.add_missing_ok_returns(content)
            all_changes.extend(ok_changes)
            
            # Apply quality improvements if requested
            if self.quality:
                content, unwrap_changes = self.fix_unwraps(content, aggressive=self.aggressive)
                all_changes.extend(unwrap_changes)
                
                content, println_changes = self.fix_println(content)
                all_changes.extend(println_changes)
                
                content, assert_changes = self.add_assert_messages(content)
                all_changes.extend(assert_changes)
                
                # Conservative sleep replacement (only in conservative mode or always?)
                if not self.aggressive:  # Conservative by default
                    content, sleep_changes = self.fix_sleep_patterns_conservative(content)
                    all_changes.extend(sleep_changes)
            
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
  %(prog)s --quality                    # Conservative quality improvements
  %(prog)s --aggressive                 # Aggressive quality improvements
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
    parser.add_argument(
        "--aggressive", "-a",
        action="store_true",
        help="Use aggressive mode for quality improvements (more unwrap patterns, skip sleep fixes)"
    )
    
    args = parser.parse_args()
    
    if not args.path.exists():
        print(f"Error: Path {args.path} not found")
        sys.exit(1)
    
    # Aggressive mode implies quality
    if args.aggressive:
        args.quality = True
    
    migrator = TestMigrator(
        dry_run=args.dry_run, 
        verbose=args.verbose, 
        quality=args.quality,
        aggressive=args.aggressive
    )
    migrator.run_migration(args.path)

if __name__ == "__main__":
    main()
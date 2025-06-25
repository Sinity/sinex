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
        
        i = 0
        while i < len(lines):
            line = lines[i]
            
            if line.strip() == '#[tokio::test]':
                fn_line_idx = -1
                for j in range(i + 1, min(i + 5, len(lines))):
                    if 'async fn' in lines[j]:
                        fn_line_idx = j
                        break
                
                if fn_line_idx != -1:
                    fn_line = lines[fn_line_idx]
                    fn_match = re.match(r'(\s*)async\s+fn\s+(\w+)\s*\(\s*\)(.*)$', fn_line)
                    if fn_match:
                        count += 1
                        indent = fn_match.group(1)
                        test_name = fn_match.group(2)
                        rest_of_line = fn_match.group(3)
                        
                        new_signature = f'{indent}async fn {test_name}(ctx: TestContext) -> Result<(), Box<dyn std::error.Error>>'
                        
                        if '->' in rest_of_line:
                            if '{' in rest_of_line:
                                brace_idx = rest_of_line.index('{')
                                new_signature += ' ' + rest_of_line[brace_idx:]
                            else:
                                j = fn_line_idx + 1
                                while j < len(lines) and '{' not in lines[j]:
                                    lines[j] = ''
                                    j += 1
                                
                                if j < len(lines) and '{' in lines[j]:
                                    brace_line = lines[j]
                                    brace_idx = brace_line.index('{')
                                    new_signature += ' ' + brace_line[brace_idx:]
                                    lines[j] = ''
                                else:
                                    new_signature += ' {'
                        elif '{' in rest_of_line:
                            new_signature += ' ' + rest_of_line.strip()
                        else:
                            if fn_line_idx + 1 < len(lines) and '{' in lines[fn_line_idx + 1]:
                                new_signature += ' {'
                            else:
                                new_signature += ' {'
                        
                        lines[fn_line_idx] = new_signature
                        
                        changes.append(MigrationChange(
                            line_num=i + 1,
                            original='#[tokio::test]',
                            replacement='#[sinex_test]',
                            description=f'Migrate test attribute for {test_name}'
                        ))
                        lines[i] = line.replace('#[tokio::test]', '#[sinex_test]')
            
            i += 1
        
        return '\n'.join(lines), count, changes
    
    def migrate_pool_usage(self, content: str) -> Tuple[str, List[str], List[MigrationChange]]:
        """Replace pool initialization and usage patterns."""
        warnings = []
        changes = []
        lines = content.split('\n')
        
        pool_vars = set()
        
        for i, line in enumerate(lines):
            original_line = line
            
            pool_patterns = [
                (r'let\s+(\w+)\s*=\s*(?:database_helpers::)?get_shared_test_pool\(\)\.await\?;', 'get_shared_test_pool'),
                (r'let\s+(\w+)\s*=\s*TestPool::new\(\)\.await\?;', 'TestPool::new'),
                (r'let\s+(\w+)\s*=\s*TestPool::with_strategy\([^)]+\)\.await[^;]*;', 'TestPool::with_strategy'),
                (r'let\s+(\w+)\s*=\s*(?:database_helpers::)?create_test_pool\(\)\.await\?;', 'create_test_pool'),
            ]
            
            for pattern, desc in pool_patterns:
                match = re.search(pattern, line)
                if match:
                    var_name = match.group(1)
                    pool_vars.add(var_name)
                    indent_match = re.match(r'^(\s*)', line)
                    indent = indent_match.group(1) if indent_match else ''
                    line = f'{indent}let {var_name} = ctx.pool();'
                    changes.append(MigrationChange(
                        line_num=i + 1,
                        original=original_line.strip(),
                        replacement=line.strip(),
                        description=f'Replace {desc} with ctx.pool()'
                    ))
                    break
            
            if 'pool' in pool_vars:
                if '&pool' in line and not line.strip().startswith('//') and not self._is_in_string(line, '&pool'):
                    line = re.sub(r'\b&pool\b', 'pool', line)
                    if line != original_line:
                        changes.append(MigrationChange(
                            line_num=i + 1,
                            original=original_line.strip(),
                            replacement=line.strip(),
                            description='Replace &pool with pool (ctx.pool() returns &PgPool)'
                        ))
                
                if 'pool.clone()' in line and not self._is_in_string(line, 'pool.clone()'):
                    line = re.sub(r'\bpool\.clone\(\)', 'ctx.pool().clone()', line)
                    if line != original_line:
                        changes.append(MigrationChange(
                            line_num=i + 1,
                            original=original_line.strip(),
                            replacement=line.strip(),
                            description='Replace pool.clone() with ctx.pool().clone()'
                        ))
            
            for pool_var in pool_vars:
                if pool_var != 'pool':
                    if f'&{pool_var}' in line and not self._is_in_string(line, f'&{pool_var}'):
                        line = re.sub(rf'\b&{pool_var}\b', pool_var, line)
                        if line != original_line:
                            changes.append(MigrationChange(
                                line_num=i + 1,
                                original=original_line.strip(),
                                replacement=line.strip(),
                                description=f'Replace &{pool_var} with {pool_var}'
                            ))
            
            lines[i] = line
        
        content = '\n'.join(lines)
        if re.search(r'pool\s*:\s*(?:PgPool|Pool<Postgres>)', content):
            warnings.append("Complex pool type annotation detected - manual review recommended")
        
        if 'Runtime::new()' in content or 'tokio::runtime' in content:
            warnings.append("Manual Runtime creation detected - may need manual adjustment")
        
        return content, warnings, changes
    
    def _is_in_string(self, line: str, text: str) -> bool:
        idx = line.find(text)
        if idx == -1:
            return False
        
        before = line[:idx]
        single_quotes = before.count("'') - before.count("\'")
        double_quotes = before.count('"') - before.count('\"')
        
        return (single_quotes % 2 == 1) or (double_quotes % 2 == 1)
    
    def add_missing_ok_returns(self, content: str) -> Tuple[str, List[MigrationChange]]:
        changes = []
        lines = content.split('\n')
        
        test_functions = []
        for i, line in enumerate(lines):
            if '#[sinex_test]' in line:
                for j in range(i + 1, min(i + 5, len(lines))):
                    if 'async fn' in lines[j] and 'TestContext' in lines[j]:
                        test_functions.append((i, j))
                        break
        
        for attr_line, fn_line in reversed(test_functions):
            brace_depth = 0
            function_start = -1
            function_end = -1
            in_string = False
            in_char = False
            escaped = False
            
            for i in range(fn_line, len(lines)):
                line = lines[i]
                j = 0
                while j < len(line):
                    char = line[j]
                    
                    if escaped:
                        escaped = False
                        j += 1
                        continue
                    
                    if char == '\\':
                        escaped = True
                        j += 1
                        continue
                    
                    if char == '"' and not in_char:
                        in_string = not in_string
                    elif char == "'" and not in_string:
                        in_char = not in_char
                    elif not in_string and not in_char:
                        if char == '{':
                            if function_start == -1:
                                function_start = i
                            brace_depth += 1
                        elif char == '}':
                            brace_depth -= 1
                            if brace_depth == 0 and function_start != -1:
                                function_end = i
                                break
                    j += 1
                
                if function_end != -1:
                    break
            
            if function_start == -1 or function_end == -1:
                continue
            
            last_statement_line = -1
            last_meaningful_content = ""
            
            for i in range(function_end - 1, function_start, -1):
                line = lines[i].strip()
                if not line or line.startswith('//') or line == '}':
                    continue
                    
                last_statement_line = i
                last_meaningful_content = line
                break
            
            if last_statement_line == -1:
                indent_match = re.match(r'^(\s*)}', lines[function_end])
                if indent_match:
                    base_indent = indent_match.group(1)
                    statement_indent = base_indent + '    '
                else:
                    statement_indent = '    '
                
                lines.insert(function_end, f'{statement_indent}Ok(())')
                changes.append(MigrationChange(
                    line_num=function_end + 1,
                    original='',
                    replacement=f'{statement_indent}Ok(())',
                    description='Add Ok(()) to empty function body'
                ))
                continue
            
            ok_patterns = [
                r'\bOk\s*\(.*\)\s*$',
                r'^\s*Ok\s*\(',
                r'^\s*return\s+',
                r'\?\s*;?\s*$',
                r'}\.await\?\s*;?\s*$',
            ]
            
            has_ok_return = any(re.search(pattern, last_meaningful_content) for pattern in ok_patterns)
            
            if not has_ok_return:
                indent_match = re.match(r'^(\s*)', lines[last_statement_line])
                if indent_match:
                    statement_indent = indent_match.group(1)
                else:
                    statement_indent = '    '
                
                insert_line = last_statement_line + 1
                
                if insert_line > function_end:
                    insert_line = function_end
                
                lines.insert(insert_line, f'{statement_indent}Ok(())')
                changes.append(MigrationChange(
                    line_num=insert_line + 1,
                    original='',
                    replacement=f'{statement_indent}Ok(())',
                    description='Add missing Ok(()) return'
                ))
        
        return '\n'.join(lines), changes
    
    def fix_variable_shadowing(self, content: str) -> Tuple[str, List[MigrationChange]]:
        changes = []
        lines = content.split('\n')
        
        for i, line in enumerate(lines):
            if '#[sinex_test]' in line:
                for j in range(i + 1, min(i + 5, len(lines))):
                    if 'async fn' in lines[j] and 'TestContext' in lines[j]:
                        param_match = re.search(r'(\w+):\s*TestContext', lines[j])
                        if not param_match:
                            break
                        
                        param_name = param_match.group(1)
                        
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
                        
                        for k in range(function_start + 1, function_end):
                            line_k = lines[k]
                            
                            shadow_pattern = rf'let\s+{param_name}\s*='
                            if re.search(shadow_pattern, line_k):
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
                                    
                                    for l in range(k + 1, function_end):
                                        line_l = lines[l]
                                        updated_line = re.sub(
                                            rf'\b{param_name}(?!\s*:|\()',
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
        changes = []
        lines = content.split('\n')
        
        patterns = [
            (r'\.unwrap\(\);', '?;'),
            (r'\.await\.unwrap\(\)', '.await?'),
            (r'let\s+(\w+)\s*=\s*(.*?)\.unwrap\(\);', r'let \1 = \2?;'),
            (r'=\s*(.*?)\.unwrap\(\)', r'= \1?'),
        ]

        for i, line in enumerate(lines):
            original_line = line
            
            if line.strip().startswith('//') or '"\\.unwrap()"' in line:
                continue
            
            if 'assert' in line and '.unwrap()' in line:
                continue

            modified_line = line
            for pattern, replacement in patterns:
                modified_line = re.sub(pattern, replacement, modified_line)

            if modified_line != original_line:
                lines[i] = modified_line
                changes.append(MigrationChange(
                    line_num=i + 1,
                    original=original_line.strip(),
                    replacement=modified_line.strip(),
                    description='Replace .unwrap() with ?'
                ))
        
        return '\n'.join(lines), changes
    
    def fix_println(self, content: str) -> Tuple[str, List[MigrationChange]]:
        changes = []
        lines = content.split('\n')
        
        for i, line in enumerate(lines):
            original_line = line
            
            if line.strip().startswith('//'):
                continue
            
            if 'println!' in line:
                if any(marker in line.lower() for marker in ['debug', 'test', 'here', 'xxx', '---']):
                    lines[i] = ''
                    changes.append(MigrationChange(
                        line_num=i + 1,
                        original=original_line.strip(),
                        replacement='[removed debug print]',
                        description='Remove debug println!'
                    ))
                else:
                    lines[i] = line.replace('println!', 'tracing::info!')
                    changes.append(MigrationChange(
                        line_num=i + 1,
                        original=original_line.strip(),
                        replacement=lines[i].strip(),
                        description='Replace println! with tracing::info!'
                    ))
        
        return '\n'.join(lines), changes

    def generate_assert_message(self, left: str, right: str, operator: str) -> str:
        if 'len()' in left:
            return f"length mismatch: expected {right} but got {{}}"
        if 'is_empty()' in left:
            return f"expected collection to be empty but it was not"
        if 'is_err()' in left:
            return "expected operation to fail but it succeeded"
        if 'is_ok()' in left:
            return "expected operation to succeed but it failed"
        if 'count' in left:
            return f"count mismatch: expected {right} but got {{}}"
        if right in ['true', 'false']:
            return f"expected {left} to be {right}"
        return f"assertion failed ({operator}): expected {right}, got {{}}"

    def add_assert_messages(self, content: str) -> Tuple[str, List[MigrationChange]]:
        changes = []
        lines = content.split('\n')
        
        for i, line in enumerate(lines):
            original_line = line

            if line.strip().startswith('//'):
                continue

            match = re.search(r'assert_eq!\s*\(([^,]+),\s*([^,]+)\s*\);', line)
            if match:
                left, right = match.group(1).strip(), match.group(2).strip()
                message = self.generate_assert_message(left, right, "eq")
                new_line = f'{line.rsplit(");", 1)[0]}, "{message}");'
                lines[i] = new_line
                changes.append(MigrationChange(
                    line_num=i+1,
                    original=original_line.strip(),
                    replacement=new_line.strip(),
                    description="Add message to assert_eq!"
                ))
                continue

            match = re.search(r'assert!\s*\(([^,)]+)\);', line)
            if match:
                condition = match.group(1).strip()
                message = f"assertion failed: {condition}"
                if '==' in condition:
                    parts = [p.strip() for p in condition.split('==', 1)]
                    message = self.generate_assert_message(parts[0], parts[1], "eq")
                elif '!=' in condition:
                    parts = [p.strip() for p in condition.split('!=', 1)]
                    message = self.generate_assert_message(parts[0], parts[1], "ne")

                new_line = f'{line.rsplit(");", 1)[0]}, "{message}");'
                lines[i] = new_line
                changes.append(MigrationChange(
                    line_num=i+1,
                    original=original_line.strip(),
                    replacement=new_line.strip(),
                    description="Add message to assert!"
                ))

        return '\n'.join(lines), changes
    
    def fix_sleep_patterns_conservative(self, content: str) -> Tuple[str, List[MigrationChange]]:
        changes = []
        lines = content.split('\n')
        
        for i in range(len(lines) - 3):
            line = lines[i]
            
            if line.strip().startswith('//'):
                continue
            
            if ('sleep(' in line or 'time::sleep' in line) and 'Duration::from_' in line:
                duration_match = re.search(r'from_(?:secs|millis)\((\d+)\)', line)
                if not duration_match:
                    continue
                
                duration_value = int(duration_match.group(1))
                duration_unit = 'secs' if 'from_secs' in line else 'millis'
                
                if duration_unit == 'millis' and duration_value < 1000:
                    continue
                
                lookahead_lines = lines[i+1:i+6]
                has_assertion = any('assert' in l for l in lookahead_lines)
                has_check = any(pattern in ' '.join(lookahead_lines) for pattern in [
                    'count', 'len()', 'is_empty', 'contains', 'should', 'expected'
                ])
                
                has_test_context = False
                for j in range(max(0, i-10), i):
                    if 'ctx: TestContext' in lines[j]:
                        has_test_context = True
                        break
                
                if has_test_context and (has_assertion or has_check):
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
        lines = content.split('\n')
        changes = []
        has_test_context_import = False
        import_insert_idx = 0
        
        new_lines = []
        for i, line in enumerate(lines):
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
            
            if 'TestContext' in line or 'common::prelude::*' in line:
                has_test_context_import = True
            
            if line.strip().startswith('use '):
                import_insert_idx = len(new_lines) + 1
            
            new_lines.append(line)
        
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
        try:
            original_content = file_path.read_text()
            content = original_content
            
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
            
            if self.quality:
                content, unwrap_changes = self.fix_unwraps(content, aggressive=self.aggressive)
                all_changes.extend(unwrap_changes)
                
                content, println_changes = self.fix_println(content)
                all_changes.extend(println_changes)
                
                content, assert_changes = self.add_assert_messages(content)
                all_changes.extend(assert_changes)
                
                content, sleep_changes = self.fix_sleep_patterns_conservative(content)
                all_changes.extend(sleep_changes)
            
            self.migration_stats['warnings'].extend(
                [(str(file_path), w) for w in warnings]
            )
            
            if content != original_content:
                self.migration_stats['tests_migrated'] += test_count
                self.migration_stats['files_modified'] += 1
                
                if self.dry_run:
                    print(f"\n📄 {file_path}")
                    print(f"   Would migrate {test_count} tests")
                    
                    if self.verbose and all_changes:
                        print("\n   Changes:")
                        for change in all_changes[:10]:
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
        
        for file_path in test_files:
            self.migration_stats['files_processed'] += 1
            self.migrate_file(file_path)
        
        self.print_summary()
        
        if not self.dry_run and self.migration_stats['files_modified'] > 0:
            self.validate_compilation()
    
    def print_summary(self) -> None:
        print(f"\n{'='*60}")
        print("📊 Migration Summary:")
        print(f"{'='*60}")
        print(f"Files processed: {self.migration_stats['files_processed']}")
        print(f"Files modified: {self.migration_stats['files_modified']}")
        print(f"Tests migrated: {self.migration_stats['tests_migrated']}")
        
        if self.migration_stats['warnings']:
            print(f"\n⚠️  Warnings ({len(self.migration_stats['warnings'])}):")
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
            print("\n  Compilation errors:")
            error_lines = result.stderr.split('\n')
            shown = 0
            for line in error_lines:
                if 'error' in line.lower() or 'cannot find' in line or '-->' in line:
                    print(f"    {line}")
                    shown += 1
                    if shown > 10:
                        print("    ... (run 'cargo check --tests' to see all errors)")
                        break
            
            print("\n  Common fixes:")
            print("  - Ensure all tests have (ctx: TestContext) parameter")
            print("  - Check that return type is Result<(), Box<dyn std::error.Error>>")
            print("  - Verify imports include: use crate::common::prelude::*;")
            print("  - Replace &pool with pool since ctx.pool() returns &PgPool")

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
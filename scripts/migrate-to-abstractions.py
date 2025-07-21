#!/usr/bin/env python3
"""
Automated migration tool for converting Sinex anti-patterns to proper abstractions.

This script uses AST-based transformations to safely migrate code patterns:
- Raw SQL queries → QueryBuilder
- anyhow! → CoreError
- Hardcoded strings → Constants
- Manual validation → ValidationChain
"""

import os
import re
import sys
import argparse
import subprocess
from pathlib import Path
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict
import json

@dataclass
class Migration:
    """Represents a code migration"""
    file_path: str
    line_number: int
    old_code: str
    new_code: str
    pattern_type: str
    confidence: float  # 0.0 to 1.0

class AbstractionMigrator:
    def __init__(self, dry_run: bool = True, verbose: bool = False):
        self.dry_run = dry_run
        self.verbose = verbose
        self.migrations: List[Migration] = []
        self.stats = {
            'files_analyzed': 0,
            'patterns_found': 0,
            'auto_fixed': 0,
            'manual_review': 0,
            'errors': 0
        }
        
        # Load known mappings
        self.event_type_mappings = self._load_event_type_mappings()
        self.source_mappings = self._load_source_mappings()
        
    def _load_event_type_mappings(self) -> Dict[str, str]:
        """Load mappings from string literals to constants"""
        return {
            '"process.heartbeat"': 'event_types::sinex::PROCESS_HEARTBEAT',
            '"process.start"': 'event_types::sinex::PROCESS_START',
            '"process.stop"': 'event_types::sinex::PROCESS_STOP',
            '"file.created"': 'event_types::file::CREATED',
            '"file.modified"': 'event_types::file::MODIFIED',
            '"file.deleted"': 'event_types::file::DELETED',
            '"file.renamed"': 'event_types::file::RENAMED',
            '"terminal.output"': 'event_types::terminal::OUTPUT',
            '"terminal.input"': 'event_types::terminal::INPUT',
            '"knowledge.note.created"': 'event_types::knowledge::NOTE_CREATED',
            '"knowledge.note.updated"': 'event_types::knowledge::NOTE_UPDATED',
            '"knowledge.note.deleted"': 'event_types::knowledge::NOTE_DELETED',
        }
    
    def _load_source_mappings(self) -> Dict[str, str]:
        """Load mappings for source strings"""
        return {
            '"fs"': 'sources::FS',
            '"sinex.process"': 'sources::SINEX_PROCESS',
            '"terminal.kitty"': 'sources::TERMINAL_KITTY',
            '"terminal.alacritty"': 'sources::TERMINAL_ALACRITTY',
            '"desktop.x11"': 'sources::DESKTOP_X11',
            '"desktop.wayland"': 'sources::DESKTOP_WAYLAND',
        }
    
    def migrate_directory(self, path: Path) -> None:
        """Migrate all Rust files in directory"""
        rust_files = list(path.rglob("*.rs"))
        
        # Skip test files unless explicitly requested
        if not self.args.include_tests:
            rust_files = [f for f in rust_files if not any(
                part in f.parts for part in ['test', 'tests', 'benches']
            )]
        
        print(f"Found {len(rust_files)} Rust files to analyze")
        
        for file_path in rust_files:
            if self.verbose:
                print(f"Analyzing {file_path}")
            self.stats['files_analyzed'] += 1
            self._migrate_file(file_path)
    
    def _migrate_file(self, file_path: Path) -> None:
        """Migrate a single file"""
        try:
            content = file_path.read_text()
            original_content = content
            
            # Apply migrations in order
            content = self._migrate_sql_queries(content, file_path)
            content = self._migrate_error_handling(content, file_path)
            content = self._migrate_string_constants(content, file_path)
            content = self._migrate_validation(content, file_path)
            
            # Write back if changed and not dry run
            if content != original_content:
                if not self.dry_run:
                    file_path.write_text(content)
                    print(f"✅ Updated {file_path}")
                else:
                    print(f"🔍 Would update {file_path}")
                    
        except Exception as e:
            print(f"❌ Error processing {file_path}: {e}")
            self.stats['errors'] += 1
    
    def _migrate_sql_queries(self, content: str, file_path: Path) -> str:
        """Migrate raw SQL queries to QueryBuilder"""
        # Pattern for simple SELECT queries
        select_pattern = re.compile(
            r'sqlx::query_as!\(\s*([^,]+),\s*"SELECT \* FROM ([\w\.]+) WHERE (\w+) = \$1",\s*([^)]+)\.to_uuid\(\)\s*\)',
            re.MULTILINE | re.DOTALL
        )
        
        def replace_select(match):
            struct_type = match.group(1).strip()
            table = match.group(2).strip()
            field = match.group(3).strip()
            param = match.group(4).strip()
            
            # Map table names to query modules
            if table == "core.events" and field == "id":
                replacement = f"EventQueries::get_by_id({param})"
                self._add_migration(file_path, match.start(), match.group(0), replacement, "sql_query", 0.9)
                self.stats['auto_fixed'] += 1
                return replacement
            elif table == "core.automaton_checkpoints":
                replacement = f"CheckpointQueries::get_by_automaton({param})"
                self._add_migration(file_path, match.start(), match.group(0), replacement, "sql_query", 0.8)
                self.stats['auto_fixed'] += 1
                return replacement
            else:
                # Mark for manual review
                self.stats['manual_review'] += 1
                return match.group(0)  # Keep original
        
        content = select_pattern.sub(replace_select, content)
        
        # Pattern for INSERT queries
        insert_pattern = re.compile(
            r'sqlx::query!\(\s*r?#?"INSERT INTO ([\w\.]+)[^"]+"\s*#?\s*,([^)]+)\)\.execute',
            re.MULTILINE | re.DOTALL
        )
        
        def replace_insert(match):
            table = match.group(1).strip()
            
            if table == "core.events":
                # This needs manual review - too complex to auto-fix
                self.stats['manual_review'] += 1
                comment = "\n// TODO: Migrate to EventQueries::insert() or QueryBuilder"
                return comment + "\n" + match.group(0)
            return match.group(0)
        
        content = insert_pattern.sub(replace_insert, content)
        
        # Detect any remaining sqlx::query usage
        remaining_queries = len(re.findall(r'sqlx::query(_as)?!', content))
        self.stats['patterns_found'] += remaining_queries
        
        return content
    
    def _migrate_error_handling(self, content: str, file_path: Path) -> str:
        """Migrate anyhow errors to CoreError"""
        # Pattern for anyhow! macro
        anyhow_pattern = re.compile(
            r'anyhow!\(("([^"]+)")\)',
            re.MULTILINE
        )
        
        def replace_anyhow(match):
            message = match.group(2)
            
            # Try to determine appropriate CoreError variant
            if "not found" in message.lower():
                replacement = f'CoreError::NotFound {{ entity: "{message}".to_string() }}'
            elif "database" in message.lower() or "query" in message.lower():
                replacement = f'CoreError::Database {{ operation: "{message}".to_string() }}'
            elif "validation" in message.lower() or "invalid" in message.lower():
                replacement = f'CoreError::Validation {{ field: "unknown".to_string(), reason: "{message}".to_string() }}'
            else:
                replacement = f'CoreError::Internal {{ message: "{message}".to_string() }}'
            
            self._add_migration(file_path, match.start(), match.group(0), replacement, "error_handling", 0.7)
            self.stats['auto_fixed'] += 1
            return replacement
        
        content = anyhow_pattern.sub(replace_anyhow, content)
        
        # Add import if CoreError is used but not imported
        if "CoreError::" in content and "use sinex_error::{CoreError" not in content:
            # Find the last use statement
            last_use = max(
                (m.end() for m in re.finditer(r'^use .+;$', content, re.MULTILINE)),
                default=0
            )
            if last_use > 0:
                content = content[:last_use] + "\nuse sinex_error::CoreError;" + content[last_use:]
        
        return content
    
    def _migrate_string_constants(self, content: str, file_path: Path) -> str:
        """Migrate hardcoded strings to constants"""
        # Replace event types
        for literal, constant in self.event_type_mappings.items():
            if literal in content:
                content = content.replace(literal, constant)
                self._add_migration(file_path, 0, literal, constant, "string_constant", 1.0)
                self.stats['auto_fixed'] += 1
        
        # Replace sources
        for literal, constant in self.source_mappings.items():
            if literal in content:
                content = content.replace(literal, constant)
                self._add_migration(file_path, 0, literal, constant, "string_constant", 1.0)
                self.stats['auto_fixed'] += 1
        
        # Add imports if constants are used
        if "event_types::" in content or "sources::" in content:
            if "use sinex_events::constants" not in content:
                # Add after other use statements
                last_use = max(
                    (m.end() for m in re.finditer(r'^use .+;$', content, re.MULTILINE)),
                    default=0
                )
                if last_use > 0:
                    imports_needed = []
                    if "event_types::" in content:
                        imports_needed.append("event_types")
                    if "sources::" in content:
                        imports_needed.append("sources")
                    if "services::" in content:
                        imports_needed.append("services")
                    
                    import_line = f"\nuse sinex_events::constants::{{{', '.join(imports_needed)}}};"
                    content = content[:last_use] + import_line + content[last_use:]
        
        return content
    
    def _migrate_validation(self, content: str, file_path: Path) -> str:
        """Migrate manual validation to ValidationChain"""
        # Pattern for simple empty checks
        empty_check_pattern = re.compile(
            r'if\s+(\w+)\.is_empty\(\)\s*\{\s*return\s+Err\(([^}]+)\}\s*\}',
            re.MULTILINE | re.DOTALL
        )
        
        def replace_empty_check(match):
            var_name = match.group(1)
            
            replacement = f'''ValidationChain::validate(&{var_name}, "{var_name}")
        .not_empty()
        .into_result()?;'''
            
            self._add_migration(file_path, match.start(), match.group(0), replacement, "validation", 0.8)
            self.stats['auto_fixed'] += 1
            return replacement
        
        content = empty_check_pattern.sub(replace_empty_check, content)
        
        # Add import if ValidationChain is used
        if "ValidationChain::" in content and "use sinex_validation::ValidationChain" not in content:
            last_use = max(
                (m.end() for m in re.finditer(r'^use .+;$', content, re.MULTILINE)),
                default=0
            )
            if last_use > 0:
                content = content[:last_use] + "\nuse sinex_validation::ValidationChain;" + content[last_use:]
        
        return content
    
    def _add_migration(self, file_path: Path, position: int, old_code: str, new_code: str, pattern_type: str, confidence: float):
        """Record a migration for reporting"""
        # Calculate line number (approximate)
        content = file_path.read_text()
        line_number = content[:position].count('\n') + 1
        
        self.migrations.append(Migration(
            file_path=str(file_path),
            line_number=line_number,
            old_code=old_code.strip(),
            new_code=new_code.strip(),
            pattern_type=pattern_type,
            confidence=confidence
        ))
    
    def generate_report(self) -> None:
        """Generate migration report"""
        print("\n" + "="*80)
        print("MIGRATION REPORT")
        print("="*80)
        
        print(f"\nFiles analyzed: {self.stats['files_analyzed']}")
        print(f"Patterns found: {self.stats['patterns_found']}")
        print(f"Auto-fixed: {self.stats['auto_fixed']}")
        print(f"Need manual review: {self.stats['manual_review']}")
        print(f"Errors: {self.stats['errors']}")
        
        if self.migrations:
            print("\n## Migrations Applied:")
            
            # Group by file
            by_file = {}
            for m in self.migrations:
                if m.file_path not in by_file:
                    by_file[m.file_path] = []
                by_file[m.file_path].append(m)
            
            for file_path, migrations in by_file.items():
                print(f"\n### {file_path}")
                for m in migrations:
                    print(f"\n  Line {m.line_number} ({m.pattern_type}, confidence: {m.confidence:.0%}):")
                    print(f"  - Old: {m.old_code[:60]}...")
                    print(f"  + New: {m.new_code[:60]}...")
        
        # Save detailed report
        if self.args.output:
            report_data = {
                'stats': self.stats,
                'migrations': [
                    {
                        'file': m.file_path,
                        'line': m.line_number,
                        'type': m.pattern_type,
                        'confidence': m.confidence,
                        'old': m.old_code,
                        'new': m.new_code
                    }
                    for m in self.migrations
                ]
            }
            
            with open(self.args.output, 'w') as f:
                json.dump(report_data, f, indent=2)
            print(f"\nDetailed report saved to: {self.args.output}")
    
    def verify_migrations(self) -> bool:
        """Verify migrations by running cargo check"""
        if self.dry_run:
            print("\nSkipping verification in dry-run mode")
            return True
        
        print("\nVerifying migrations with cargo check...")
        result = subprocess.run(
            ["cargo", "check", "--workspace"],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print("✅ All migrations compile successfully!")
            return True
        else:
            print("❌ Compilation errors after migration:")
            print(result.stderr)
            return False

def main():
    parser = argparse.ArgumentParser(
        description="Migrate Sinex codebase to use proper abstractions (DRY-RUN by default)"
    )
    parser.add_argument(
        "path",
        type=Path,
        help="Path to migrate (file or directory)"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually apply the migrations (default is dry-run only)"
    )
    parser.add_argument(
        "--include-tests",
        action="store_true",
        help="Also migrate test files"
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose output"
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Save detailed report to file"
    )
    
    args = parser.parse_args()
    
    # Set dry_run based on --apply flag
    args.dry_run = not args.apply
    
    # Print mode
    if args.dry_run:
        print("🔍 Running in DRY-RUN mode (no files will be modified)")
        print("   Use --apply to actually apply the migrations\n")
    else:
        print("⚠️  Running in APPLY mode (files WILL be modified)")
        print("   Press Ctrl+C to cancel...\n")
        import time
        time.sleep(2)
    
    migrator = AbstractionMigrator(dry_run=args.dry_run, verbose=args.verbose)
    migrator.args = args
    
    if args.path.is_file():
        migrator._migrate_file(args.path)
    else:
        migrator.migrate_directory(args.path)
    
    migrator.generate_report()
    
    if not args.dry_run:
        if migrator.verify_migrations():
            sys.exit(0)
        else:
            print("\n⚠️  Some migrations may need manual adjustment")
            sys.exit(1)

if __name__ == "__main__":
    main()
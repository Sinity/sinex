#!/usr/bin/env python3
"""
Generate LLM-friendly text files from Sinex repository subsets.

This script creates text files optimized for LLM consumption with token counting,
organized sections, and configurable subsets of the codebase.
"""

import os
import sys
import subprocess
import json
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Optional, Set, Tuple
import argparse
from dataclasses import dataclass
from collections import defaultdict

@dataclass
class FileInfo:
    path: Path
    rel_path: str
    size: int
    tokens: int
    content: str
    is_test: bool = False
    has_rustdoc: bool = False
    crate: Optional[str] = None

class LLMContextGenerator:
    def __init__(self, base_dir: Path, use_ttok: bool = True):
        self.base_dir = base_dir
        self.files: List[FileInfo] = []
        self.use_ttok = use_ttok
        
    def count_tokens(self, text: str) -> int:
        """Count tokens using ttok if available, otherwise estimate."""
        if self.use_ttok:
            try:
                result = subprocess.run(
                    ["ttok"], 
                    input=text.encode('utf-8'),
                    capture_output=True,
                    text=False
                )
                if result.returncode == 0:
                    # ttok outputs the count as a string
                    output = result.stdout.decode().strip()
                    # Sometimes ttok includes extra info, get just the number
                    lines = output.split('\n')
                    for line in reversed(lines):
                        if line.strip().isdigit():
                            return int(line.strip())
                    # If no pure digit line found, try to parse the first line
                    if lines and lines[0].strip().isdigit():
                        return int(lines[0].strip())
            except (subprocess.SubprocessError, FileNotFoundError):
                pass
        
        # Fallback: rough estimate of 1 token per 4 characters
        return len(text) // 4
    
    def is_text_file(self, path: Path) -> bool:
        """Check if file is text-based."""
        try:
            result = subprocess.run(
                ["file", "--mime-type", "-b", str(path)],
                capture_output=True,
                text=True
            )
            return result.stdout.strip().startswith('text/')
        except:
            # Fallback: check common extensions
            text_extensions = {
                '.rs', '.py', '.toml', '.md', '.txt', '.yaml', '.yml',
                '.json', '.sh', '.nix', '.sql', '.js', '.ts', '.html',
                '.css', '.xml', '.env', '.gitignore', '.lock'
            }
            return path.suffix.lower() in text_extensions
    
    def find_files(self, patterns: List[str], exclude_patterns: List[str] = None) -> List[Path]:
        """Find files matching patterns, excluding certain paths."""
        exclude_patterns = exclude_patterns or []
        exclude_dirs = {'.git', '.obsidian', 'target', 'node_modules', '.sqlx'}
        
        files = []
        for pattern in patterns:
            if pattern.startswith("**"):
                # Recursive glob
                for path in self.base_dir.rglob(pattern.lstrip("**/").lstrip("/")):
                    if path.is_file():
                        # Check if any parent dir should be excluded
                        if not any(part in exclude_dirs for part in path.parts):
                            if not any(path.match(ep) for ep in exclude_patterns):
                                files.append(path)
            else:
                # Direct path or simple glob
                base_path = self.base_dir / pattern
                if base_path.exists() and base_path.is_file():
                    files.append(base_path)
                else:
                    for path in self.base_dir.glob(pattern):
                        if path.is_file():
                            if not any(path.match(ep) for ep in exclude_patterns):
                                files.append(path)
        
        return sorted(set(files))
    
    def detect_crate(self, path: Path) -> Optional[str]:
        """Detect which crate a file belongs to."""
        # Walk up to find Cargo.toml
        current = path.parent
        while current >= self.base_dir:
            cargo_toml = current / "Cargo.toml"
            if cargo_toml.exists():
                try:
                    with open(cargo_toml, 'r') as f:
                        content = f.read()
                        # More flexible regex to find package name
                        # Handle both single-line and multi-line formats
                        match = re.search(r'\[package\].*?name\s*=\s*"([^"]+)"', content, re.DOTALL)
                        if match:
                            return match.group(1)
                except:
                    pass
            if current == self.base_dir:
                break
            current = current.parent
        return None
    
    def extract_rustdoc(self, content: str) -> Tuple[str, bool]:
        """Extract rustdoc comments and return content with rustdoc flag."""
        # For now, just detect if file has rustdoc
        has_rustdoc = bool(re.search(r'//[/!]|/\*[*!]', content))
        return content, has_rustdoc
    
    def strip_rustdoc(self, content: str) -> str:
        """Remove rustdoc comments from Rust code."""
        lines = []
        in_block_comment = False
        
        for line in content.split('\n'):
            # Skip line rustdoc comments
            if line.strip().startswith('///') or line.strip().startswith('//!'):
                continue
            
            # Handle block rustdoc comments
            if '/*!' in line or '/**' in line:
                in_block_comment = True
                # If the comment ends on the same line, just remove it
                if '*/' in line:
                    before = line[:line.index('/*')]
                    after = line[line.index('*/') + 2:]
                    line = before + after
                    in_block_comment = False
                else:
                    # Just take the part before the comment
                    line = line[:line.index('/*')] if '/*' in line else ''
            
            if in_block_comment:
                if '*/' in line:
                    # Take the part after the comment ends
                    line = line[line.index('*/') + 2:]
                    in_block_comment = False
                else:
                    # Skip this line entirely
                    continue
            
            lines.append(line)
        
        return '\n'.join(lines)
    
    def analyze_dependencies(self) -> Dict[str, Set[str]]:
        """Analyze import dependencies between files."""
        file_deps = {}
        
        for file_info in self.files:
            if file_info.path.suffix == '.rs':
                deps = set()
                
                # Extract use statements
                for line in file_info.content.splitlines():
                    line = line.strip()
                    if line.startswith('use '):
                        # Extract the module path
                        match = re.match(r'use\s+(?:crate::)?([^:;{\s]+)', line)
                        if match:
                            module_path = match.group(1)
                            # Convert module path to potential file paths
                            if module_path.startswith('super'):
                                # Relative import - need to resolve
                                continue
                            
                            # Look for corresponding files
                            for other_file in self.files:
                                # Simple heuristic: check if module path matches file path
                                other_rel = other_file.rel_path.replace('/', '::').replace('.rs', '')
                                if module_path in other_rel or other_rel.endswith(module_path):
                                    deps.add(other_file.rel_path)
                
                file_deps[file_info.rel_path] = deps
        
        return file_deps
    
    def topological_sort(self, dependencies: Dict[str, Set[str]]) -> List[str]:
        """Sort files topologically based on dependencies."""
        # Build adjacency list
        graph = defaultdict(list)
        in_degree = defaultdict(int)
        all_files = set()
        
        for file, deps in dependencies.items():
            all_files.add(file)
            for dep in deps:
                all_files.add(dep)
                graph[dep].append(file)
                in_degree[file] += 1
        
        # Find files with no dependencies
        queue = [f for f in all_files if in_degree[f] == 0]
        result = []
        
        while queue:
            # Sort queue to ensure deterministic output
            queue.sort()
            current = queue.pop(0)
            result.append(current)
            
            # Reduce in-degree of dependent files
            for neighbor in graph[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        # Add any files not in the dependency graph
        for file_info in self.files:
            if file_info.rel_path not in result:
                result.append(file_info.rel_path)
        
        return result
    
    def load_files(self, patterns: List[str], exclude_patterns: List[str] = None, smart_order: bool = False):
        """Load files matching patterns, optionally with smart ordering."""
        paths = self.find_files(patterns, exclude_patterns)
        
        for path in paths:
            if not self.is_text_file(path):
                continue
                
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                rel_path = str(path.relative_to(self.base_dir))
                size = len(content.encode('utf-8'))
                tokens = self.count_tokens(content)
                
                # Detect if it's a test file
                is_test = '/test/' in rel_path or rel_path.startswith('test/')
                
                # Extract rustdoc if Rust file
                if path.suffix == '.rs':
                    content, has_rustdoc = self.extract_rustdoc(content)
                else:
                    has_rustdoc = False
                
                # Detect crate for Rust files
                crate = self.detect_crate(path) if path.suffix == '.rs' else None
                
                self.files.append(FileInfo(
                    path=path,
                    rel_path=rel_path,
                    size=size,
                    tokens=tokens,
                    content=content,
                    is_test=is_test,
                    has_rustdoc=has_rustdoc,
                    crate=crate
                ))
            except Exception as e:
                print(f"Warning: Could not read {path}: {e}", file=sys.stderr)
        
        # Apply smart ordering if requested
        if smart_order and self.files:
            print("Analyzing dependencies for smart ordering...")
            deps = self.analyze_dependencies()
            sorted_paths = self.topological_sort(deps)
            
            # Reorder files based on topological sort
            file_map = {f.rel_path: f for f in self.files}
            ordered_files = []
            for path in sorted_paths:
                if path in file_map:
                    ordered_files.append(file_map[path])
            
            # Group by crate if multiple crates
            crate_groups = defaultdict(list)
            for f in ordered_files:
                if f.crate:
                    crate_groups[f.crate].append(f)
                else:
                    crate_groups["_no_crate"].append(f)
            
            # Order crates by dependency (foundational crates first)
            # This could be made dynamic by analyzing Cargo.toml dependencies
            crate_order = ["sinex-types", "sinex-macros", "sinex-db", "sinex-services", "sinex-satellite-sdk"]
            
            # For better ordering, also sort files within each crate
            # Put lib.rs first, then mod.rs files, then others
            def sort_key(f):
                if f.path.name == "lib.rs":
                    return (0, f.rel_path)
                elif f.path.name == "mod.rs":
                    return (1, f.rel_path)
                elif f.rel_path.endswith("/mod.rs"):
                    return (2, f.rel_path)
                else:
                    return (3, f.rel_path)
            
            for crate in crate_groups:
                crate_groups[crate].sort(key=sort_key)
            
            final_order = []
            # Add known crates in order
            for crate in crate_order:
                if crate in crate_groups:
                    final_order.extend(crate_groups[crate])
                    del crate_groups[crate]
            
            # Add remaining crates
            for crate in sorted(crate_groups.keys()):
                final_order.extend(crate_groups[crate])
            
            self.files = final_order
            print(f"Files reordered based on dependencies (foundational → dependent)")
    
    def generate_markdown(self, 
                         title: str,
                         include_rustdoc: bool = True,
                         group_by_crate: bool = False,
                         show_dependencies: bool = False) -> str:
        """Generate markdown output."""
        output = []
        
        # Header
        output.append(f"# {title}")
        output.append("")
        output.append("---")
        output.append(f"generated: {datetime.now(timezone.utc).isoformat()}")
        output.append(f"base_directory: {self.base_dir}")
        output.append(f"total_files: {len(self.files)}")
        output.append(f"total_tokens: {sum(f.tokens for f in self.files)}")
        output.append(f"total_size: {sum(f.size for f in self.files)} bytes")
        if show_dependencies:
            output.append("ordering: dependency-aware (foundational → dependent)")
        output.append("---")
        output.append("")
        
        # Group files if requested
        if group_by_crate and any(f.crate for f in self.files):
            # Group by crate
            crate_files = defaultdict(list)
            no_crate_files = []
            
            for f in self.files:
                if f.crate:
                    crate_files[f.crate].append(f)
                else:
                    no_crate_files.append(f)
            
            # Table of contents by crate
            output.append("## Table of Contents")
            output.append("")
            
            for crate in sorted(crate_files.keys()):
                output.append(f"### Crate: {crate}")
                for i, f in enumerate(crate_files[crate], 1):
                    output.append(f"{i}. [{f.rel_path}](#{crate}-{i})")
                output.append("")
            
            if no_crate_files:
                output.append("### Other Files")
                for i, f in enumerate(no_crate_files, 1):
                    output.append(f"{i}. [{f.rel_path}](#other-{i})")
                output.append("")
            
            # Content by crate
            for crate in sorted(crate_files.keys()):
                output.append(f"## Crate: {crate}")
                output.append("")
                for i, f in enumerate(crate_files[crate], 1):
                    output.extend(self._format_file(f, f"{crate}-{i}", include_rustdoc))
                    output.append("")
            
            if no_crate_files:
                output.append("## Other Files")
                output.append("")
                for i, f in enumerate(no_crate_files, 1):
                    output.extend(self._format_file(f, f"other-{i}", include_rustdoc))
                    output.append("")
        else:
            # Simple file listing
            output.append("## Table of Contents")
            output.append("")
            for i, f in enumerate(self.files, 1):
                output.append(f"{i}. [{f.rel_path}](#file-{i})")
            output.append("")
            
            # Files
            for i, f in enumerate(self.files, 1):
                output.extend(self._format_file(f, f"file-{i}", include_rustdoc))
                output.append("")
        
        return "\n".join(output)
    
    def _format_file(self, file_info: FileInfo, anchor: str, include_rustdoc: bool) -> List[str]:
        """Format a single file entry."""
        output = []
        
        output.append(f'<a id="{anchor}"></a>')
        output.append(f"### {file_info.rel_path}")
        output.append("")
        output.append(f"- Size: {file_info.size:,} bytes")
        output.append(f"- Tokens: {file_info.tokens:,}")
        if file_info.crate:
            output.append(f"- Crate: {file_info.crate}")
        if file_info.is_test:
            output.append("- Type: Test file")
        if file_info.has_rustdoc and not include_rustdoc:
            output.append("- Note: Rustdoc comments excluded")
        output.append("")
        
        # Detect language for syntax highlighting
        lang_map = {
            '.rs': 'rust',
            '.py': 'python',
            '.toml': 'toml',
            '.md': 'markdown',
            '.sh': 'bash',
            '.nix': 'nix',
            '.sql': 'sql',
            '.json': 'json',
            '.yaml': 'yaml',
            '.yml': 'yaml',
        }
        lang = lang_map.get(file_info.path.suffix, '')
        
        output.append(f"```{lang}")
        
        # Optionally strip rustdoc
        content = file_info.content
        if not include_rustdoc and file_info.has_rustdoc:
            content = self.strip_rustdoc(content)
        
        output.append(content)
        output.append("```")
        
        return output
    
    def generate_rustdoc_markdown(self) -> str:
        """Generate documentation using cargo doc and convert to markdown."""
        import tempfile
        import shutil
        
        print("Generating rustdoc documentation...")
        
        # Run cargo doc
        result = subprocess.run(
            ["cargo", "doc", "--no-deps", "--workspace"],
            cwd=self.base_dir,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            return f"# Error generating rustdoc\n\nFailed to run cargo doc:\n```\n{result.stderr}\n```\n"
        
        # Find the generated doc directory
        doc_dir = self.base_dir / "target" / "doc"
        if not doc_dir.exists():
            return "# Error: No documentation generated\n\nCould not find target/doc directory"
        
        output = []
        output.append("# Sinex Generated Documentation")
        output.append("")
        output.append("Generated from `cargo doc` output. This is a simplified markdown representation.")
        output.append("")
        output.append("---")
        output.append(f"generated: {datetime.now(timezone.utc).isoformat()}")
        output.append("---")
        output.append("")
        
        # For now, just list what documentation was generated
        # In a full implementation, we'd parse the HTML and convert to markdown
        output.append("## Available Documentation")
        output.append("")
        
        # Find all index.html files
        crate_docs = []
        for index_file in doc_dir.rglob("*/index.html"):
            if "src" not in index_file.parts:  # Skip source files
                crate_name = index_file.parent.name
                if crate_name != "doc":
                    crate_docs.append(crate_name)
        
        for crate in sorted(set(crate_docs)):
            output.append(f"- {crate}")
        
        output.append("")
        output.append("## Note")
        output.append("")
        output.append("Full HTML to Markdown conversion would require additional dependencies.")
        output.append("Consider using tools like `pandoc` or `html2text` for complete conversion.")
        output.append("")
        output.append("For now, run `cargo doc --open` to view the full documentation in your browser.")
        
        return "\n".join(output)

def main():
    parser = argparse.ArgumentParser(description="Generate LLM-friendly context from Sinex repository")
    parser.add_argument("preset", nargs="?", help="Preset configuration to use")
    parser.add_argument("-o", "--output", help="Output file (default: based on preset)")
    parser.add_argument("-d", "--directory", default=".", help="Base directory (default: current)")
    parser.add_argument("--no-rustdoc", action="store_true", help="Exclude rustdoc comments")
    parser.add_argument("--group-by-crate", action="store_true", help="Group files by crate")
    parser.add_argument("--patterns", nargs="+", help="Custom file patterns")
    parser.add_argument("--exclude", nargs="+", help="Exclude patterns")
    parser.add_argument("--list-presets", action="store_true", help="List available presets")
    parser.add_argument("--report", action="store_true", help="Report token estimates for all presets")
    parser.add_argument("--report-crates", action="store_true", help="Report token estimates by crate")
    parser.add_argument("--report-summary", action="store_true", help="Quick summary of major components")
    parser.add_argument("--analyze", action="store_true", help="Analyze code structure and patterns")
    parser.add_argument("--deps", action="store_true", help="Analyze crate dependencies")
    parser.add_argument("--modules", action="store_true", help="Analyze module structure")
    parser.add_argument("--full-analysis", action="store_true", help="Run all analysis types")
    parser.add_argument("--deps-graph", action="store_true", help="Generate dependency graph in DOT format")
    
    args = parser.parse_args()
    
    # Define presets
    presets = {
        "core": {
            "title": "Sinex Core Codebase",
            "patterns": [
                "crate/core/*/src/**/*.rs",
                "crate/lib/*/src/**/*.rs",
                "crate/core/*/Cargo.toml",
                "crate/lib/*/Cargo.toml",
            ],
            "exclude": ["**/tests/**", "**/test/**", "**/benches/**", "**/migration/**"],
            "output": "llm-context-core.md"
        },
        "core-with-rustdoc": {
            "title": "Sinex Core Codebase with Documentation",
            "patterns": [
                "crate/core/*/src/**/*.rs",
                "crate/lib/*/src/**/*.rs",
                "crate/core/*/Cargo.toml",
                "crate/lib/*/Cargo.toml",
            ],
            "exclude": ["**/tests/**", "**/test/**", "**/benches/**", "**/migration/**"],
            "output": "llm-context-core-docs.md",
            "include_rustdoc": True
        },
        "tests": {
            "title": "Sinex Test Suite",
            "patterns": [
                "test/**/*.rs",
                "crate/*/tests/**/*.rs",
            ],
            "output": "llm-context-tests.md"
        },
        "docs": {
            "title": "Sinex Documentation",
            "patterns": [
                "*.md",
                "docs/**/*.md",
                "spec/**/*.md",
            ],
            "output": "llm-context-docs.md"
        },
        "satellite": {
            "title": "Sinex Satellite Services",
            "patterns": [
                "crate/satellites/*/src/**/*.rs",
                "crate/core/sinex-ingestd/src/**/*.rs",
            ],
            "output": "llm-context-satellites.md",
            "group_by_crate": True
        },
        "db": {
            "title": "Sinex Database Layer",
            "patterns": [
                "crate/lib/sinex-db/src/**/*.rs",
                "crate/lib/sinex-db/migration/src/**/*.rs",
                "db/migrations/**/*.sql",
            ],
            "output": "llm-context-db.md"
        },
        "minimal": {
            "title": "Sinex Minimal Overview",
            "patterns": [
                "crate/sinex-*/src/lib.rs",
                "crate/sinex-*/src/main.rs",
                "README.md",
                "CLAUDE.md",
            ],
            "output": "llm-context-minimal.md"
        },
        "rustdoc-generated": {
            "title": "Sinex Generated Documentation (from cargo doc)",
            "patterns": [],  # Special case - will generate from cargo doc
            "output": "llm-context-rustdoc-generated.md",
            "generate_rustdoc": True
        },
        "smart": {
            "title": "Sinex Core (Dependency-Ordered)",
            "patterns": [
                "crate/core/*/src/**/*.rs",
                "crate/lib/*/src/**/*.rs",
            ],
            "exclude": ["**/tests/**", "**/test/**", "**/benches/**", "**/migration/**"],
            "output": "llm-context-smart.md",
            "smart_order": True,
            "group_by_crate": True
        }
    }
    
    base_dir = Path(args.directory).resolve()
    
    if args.list_presets:
        print("Available presets:")
        for name, config in presets.items():
            print(f"  {name:20} - {config['title']}")
            print(f"  {'':20}   Output: {config['output']}")
            print(f"  {'':20}   Tokens: ~{len(str(config)) * 100} (estimate)")
        return
    
    if args.report:
        print("Token Report for All Presets")
        print("=" * 100)
        print(f"{'Preset':<20} {'Files':<10} {'Tokens':<15} {'w/o RustDoc':<15} {'Size (MB)':<10} {'Title'}")
        print("-" * 100)
        
        total_tokens = 0
        total_files = 0
        total_size = 0
        
        for name, config in sorted(presets.items()):
            if config.get("generate_rustdoc", False):
                # Skip rustdoc-generated preset in regular report
                continue
                
            generator = LLMContextGenerator(base_dir, use_ttok=False)
            generator.load_files(
                config["patterns"],
                config.get("exclude", [])
            )
            
            file_count = len(generator.files)
            token_count = sum(f.tokens for f in generator.files)
            
            # Calculate tokens without rustdoc
            tokens_no_rustdoc = 0
            for f in generator.files:
                if f.path.suffix == '.rs' and f.has_rustdoc:
                    stripped = generator.strip_rustdoc(f.content)
                    tokens_no_rustdoc += generator.count_tokens(stripped)
                else:
                    tokens_no_rustdoc += f.tokens
            
            size_mb = sum(f.size for f in generator.files) / (1024 * 1024)
            
            print(f"{name:<20} {file_count:<10} {token_count:<15,} {tokens_no_rustdoc:<15,} {size_mb:<10.2f} {config['title']}")
            
            total_tokens += token_count
            total_files += file_count
            total_size += size_mb
        
        print("-" * 100)
        print(f"{'TOTAL':<20} {total_files:<10} {total_tokens:<15,} {'':15} {total_size:<10.2f}")
        print()
        print("Note: Token counts are estimates. Actual counts may vary based on model tokenizer.")
        return
    
    if args.report_crates:
        print("Token Report by Crate")
        print("=" * 80)
        
        # Collect all Rust files
        generator = LLMContextGenerator(base_dir, use_ttok=False)
        patterns = [
            "crate/**/*.rs",
            "crate/**/*.toml",
        ]
        generator.load_files(patterns, ["**/target/**", "**/tests/**", "**/benches/**"])
        
        # Group by crate
        crate_stats = defaultdict(lambda: {"files": 0, "tokens": 0, "size": 0, "has_tests": False})
        no_crate_stats = {"files": 0, "tokens": 0, "size": 0}
        
        for f in generator.files:
            if f.crate:
                crate_stats[f.crate]["files"] += 1
                crate_stats[f.crate]["tokens"] += f.tokens
                crate_stats[f.crate]["size"] += f.size
                if f.is_test:
                    crate_stats[f.crate]["has_tests"] = True
            else:
                no_crate_stats["files"] += 1
                no_crate_stats["tokens"] += f.tokens
                no_crate_stats["size"] += f.size
        
        # Group crates by category
        core_crates = []
        lib_crates = []
        satellite_crates = []
        
        for crate in sorted(crate_stats.keys()):
            # Find first file from this crate to check its path
            crate_file = next((f for f in generator.files if f.crate == crate), None)
            if crate_file and len(crate_file.path.parts) > 2:
                if crate_file.path.parts[1] == "core":
                    core_crates.append(crate)
                elif crate_file.path.parts[1] == "lib":
                    lib_crates.append(crate)
                elif crate_file.path.parts[1] == "satellites":
                    satellite_crates.append(crate)
        
        # Print by category
        def print_crate_group(title, crates):
            if not crates:
                return
            print(f"\n{title}")
            print("-" * 80)
            print(f"{'Crate':<40} {'Files':<10} {'Tokens':<15} {'Size (KB)':<10}")
            print("-" * 80)
            
            group_files = 0
            group_tokens = 0
            group_size = 0
            
            for crate in crates:
                stats = crate_stats[crate]
                size_kb = stats["size"] / 1024
                print(f"{crate:<40} {stats['files']:<10} {stats['tokens']:<15,} {size_kb:<10.1f}")
                group_files += stats["files"]
                group_tokens += stats["tokens"]
                group_size += size_kb
            
            print("-" * 80)
            print(f"{'Subtotal':<40} {group_files:<10} {group_tokens:<15,} {group_size:<10.1f}")
            return group_tokens
        
        total_tokens = 0
        total_tokens += print_crate_group("Core Services", core_crates) or 0
        total_tokens += print_crate_group("Libraries", lib_crates) or 0
        total_tokens += print_crate_group("Satellites", satellite_crates) or 0
        
        if no_crate_stats["files"] > 0:
            print(f"\n{'Other Files':<40} {no_crate_stats['files']:<10} {no_crate_stats['tokens']:<15,} {no_crate_stats['size']/1024:<10.1f}")
            total_tokens += no_crate_stats["tokens"]
        
        print("\n" + "=" * 80)
        print(f"Total tokens across all crates: {total_tokens:,}")
        print("\nNote: Token counts are estimates. Actual counts may vary based on model tokenizer.")
        return
    
    if args.full_analysis:
        # Run all analyses
        args.report_summary = True
        args.analyze = True
        args.deps = True
        args.modules = True
    
    if args.report_summary:
        print("Sinex Token Summary Report")
        print("=" * 80)
        
        # Quick estimates for major components
        components = {
            "Core Services (ingestd, gateway, etc)": {
                "patterns": ["crate/core/*/src/**/*.rs"],
                "exclude": ["**/tests/**"]
            },
            "Libraries (db, sdk, utils, etc)": {
                "patterns": ["crate/lib/*/src/**/*.rs"],
                "exclude": ["**/tests/**", "**/migration/**"]
            },
            "Satellites (all event sources)": {
                "patterns": ["crate/satellites/*/src/**/*.rs"],
                "exclude": ["**/tests/**"]
            },
            "Tests (all test files)": {
                "patterns": ["test/**/*.rs", "crate/**/tests/**/*.rs"],
                "exclude": []
            },
            "Documentation (markdown files)": {
                "patterns": ["*.md", "docs/**/*.md", "spec/**/*.md"],
                "exclude": ["**/target/**", "**/node_modules/**"]
            },
        }
        
        print(f"{'Component':<40} {'Files':<10} {'Tokens':<15} {'Size (MB)':<10}")
        print("-" * 80)
        
        total_tokens = 0
        total_files = 0
        component_tokens = {}
        
        for name, config in components.items():
            generator = LLMContextGenerator(base_dir, use_ttok=False)
            generator.load_files(config["patterns"], config.get("exclude", []))
            
            file_count = len(generator.files)
            token_count = sum(f.tokens for f in generator.files)
            size_mb = sum(f.size for f in generator.files) / (1024 * 1024)
            
            print(f"{name:<40} {file_count:<10} {token_count:<15,} {size_mb:<10.2f}")
            component_tokens[name] = token_count
            total_tokens += token_count
            total_files += file_count
        
        print("-" * 80)
        print(f"{'TOTAL':<40} {total_files:<10} {total_tokens:<15,} ")
        print()
        
        # Calculate useful combinations
        core_libs = component_tokens.get("Core Services (ingestd, gateway, etc)", 0) + component_tokens.get("Libraries (db, sdk, utils, etc)", 0)
        code_only = total_tokens - component_tokens.get("Documentation (markdown files)", 0) - component_tokens.get("Tests (all test files)", 0)
        code_with_satellites = core_libs + component_tokens.get("Satellites (all event sources)", 0)
        
        print("Quick context size estimates:")
        print(f"  - Small context (core + libs): ~{core_libs:,} tokens")
        print(f"  - Medium context (+ satellites): ~{code_with_satellites:,} tokens")
        print(f"  - Full codebase (no docs/tests): ~{code_only:,} tokens")
        print(f"  - Everything: ~{total_tokens:,} tokens")
        return
    
    if args.analyze:
        print("Sinex Codebase Analysis")
        print("=" * 80)
        
        # Load all Rust files
        generator = LLMContextGenerator(base_dir, use_ttok=False)
        patterns = ["crate/**/*.rs"]
        generator.load_files(patterns, ["**/target/**"])
        
        # Basic metrics
        total_files = len(generator.files)
        total_lines = sum(len(f.content.splitlines()) for f in generator.files)
        total_tokens = sum(f.tokens for f in generator.files)
        
        # Code analysis
        struct_count = 0
        enum_count = 0
        trait_count = 0
        impl_count = 0
        fn_count = 0
        async_fn_count = 0
        test_count = 0
        unsafe_count = 0
        todo_count = 0
        
        # Import analysis
        import_freq = defaultdict(int)
        
        for f in generator.files:
            content = f.content
            lines = content.splitlines()
            
            for line in lines:
                # Basic pattern matching (not perfect but good enough)
                if re.match(r'^\s*pub\s+struct\s+\w+', line) or re.match(r'^\s*struct\s+\w+', line):
                    struct_count += 1
                elif re.match(r'^\s*pub\s+enum\s+\w+', line) or re.match(r'^\s*enum\s+\w+', line):
                    enum_count += 1
                elif re.match(r'^\s*pub\s+trait\s+\w+', line) or re.match(r'^\s*trait\s+\w+', line):
                    trait_count += 1
                elif re.match(r'^\s*impl\s+', line) or re.match(r'^\s*impl<', line):
                    impl_count += 1
                elif re.match(r'^\s*(pub\s+)?(async\s+)?fn\s+\w+', line):
                    fn_count += 1
                    if 'async' in line:
                        async_fn_count += 1
                elif re.match(r'^\s*#\[test\]', line) or re.match(r'^\s*#\[tokio::test\]', line):
                    test_count += 1
                elif 'unsafe' in line and '{' in line:
                    unsafe_count += 1
                elif 'TODO' in line or 'FIXME' in line or 'HACK' in line:
                    todo_count += 1
                
                # Track imports
                if line.strip().startswith('use '):
                    import_match = re.match(r'use\s+([^:;\s{]+)', line.strip())
                    if import_match:
                        import_name = import_match.group(1)
                        import_freq[import_name] += 1
        
        # File size analysis
        file_sizes = [(f.rel_path, len(f.content.splitlines())) for f in generator.files]
        file_sizes.sort(key=lambda x: x[1], reverse=True)
        
        # Test coverage
        test_files = [f for f in generator.files if f.is_test or 'test' in f.rel_path]
        src_files = [f for f in generator.files if not f.is_test and 'test' not in f.rel_path]
        
        print(f"\n📊 Basic Metrics")
        print(f"  Total files: {total_files}")
        print(f"  Total lines: {total_lines:,}")
        print(f"  Total tokens: {total_tokens:,}")
        print(f"  Average file size: {total_lines // total_files if total_files > 0 else 0} lines")
        
        print(f"\n🏗️  Code Structure")
        print(f"  Structs: {struct_count}")
        print(f"  Enums: {enum_count}")
        print(f"  Traits: {trait_count}")
        print(f"  Impl blocks: {impl_count}")
        print(f"  Functions: {fn_count} (async: {async_fn_count})")
        print(f"  Tests: {test_count}")
        
        print(f"\n⚠️  Code Quality Indicators")
        print(f"  Unsafe blocks: {unsafe_count}")
        print(f"  TODO/FIXME/HACK: {todo_count}")
        print(f"  Test coverage: {len(test_files)} test files / {len(src_files)} source files")
        
        print(f"\n📦 Most Common Imports (top 10)")
        top_imports = sorted(import_freq.items(), key=lambda x: x[1], reverse=True)[:10]
        for imp, count in top_imports:
            print(f"  {imp}: {count} times")
        
        print(f"\n📏 Largest Files (top 10)")
        for path, lines in file_sizes[:10]:
            print(f"  {path}: {lines} lines")
        
        # Crate-specific analysis
        crate_metrics = defaultdict(lambda: {"files": 0, "lines": 0, "functions": 0})
        for f in generator.files:
            if f.crate:
                crate_metrics[f.crate]["files"] += 1
                crate_metrics[f.crate]["lines"] += len(f.content.splitlines())
                crate_metrics[f.crate]["functions"] += len(re.findall(r'^\s*(pub\s+)?(async\s+)?fn\s+\w+', f.content, re.MULTILINE))
        
        if crate_metrics:
            print(f"\n📚 Crate Complexity (top 10 by line count)")
            crate_list = [(crate, metrics) for crate, metrics in crate_metrics.items()]
            crate_list.sort(key=lambda x: x[1]["lines"], reverse=True)
            for crate, metrics in crate_list[:10]:
                avg_file_size = metrics["lines"] // metrics["files"] if metrics["files"] > 0 else 0
                print(f"  {crate}: {metrics['files']} files, {metrics['lines']} lines, {metrics['functions']} functions")
                print(f"    Average file size: {avg_file_size} lines")
        
        return
    
    if args.deps:
        print("Crate Dependency Analysis")
        print("=" * 80)
        
        # Run cargo tree to get dependencies
        print("\n📊 Dependency Tree (simplified)")
        print("-" * 40)
        result = subprocess.run(
            ["cargo", "tree", "--workspace", "--depth", "2", "--prefix", "none"],
            cwd=base_dir,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            # Parse and display key information
            lines = result.stdout.splitlines()
            
            # Count direct dependencies per crate
            crate_deps = defaultdict(set)
            current_crate = None
            
            for line in lines:
                if line and not line.startswith(' '):
                    # This is a workspace crate
                    parts = line.split()
                    if parts:
                        current_crate = parts[0]
                elif current_crate and line.strip():
                    # This is a dependency
                    parts = line.strip().split()
                    if parts:
                        dep_name = parts[0]
                        if not dep_name.startswith('sinex'):  # External deps only
                            crate_deps[current_crate].add(dep_name)
            
            # Show crates with most external dependencies
            print("\n📦 External Dependencies by Crate")
            sorted_crates = sorted(crate_deps.items(), key=lambda x: len(x[1]), reverse=True)
            for crate, deps in sorted_crates[:10]:
                print(f"  {crate}: {len(deps)} dependencies")
                # Show top 5 deps
                for dep in sorted(deps)[:5]:
                    print(f"    - {dep}")
                if len(deps) > 5:
                    print(f"    ... and {len(deps) - 5} more")
        
        # Analyze internal dependencies
        print("\n🔗 Internal Dependency Graph")
        print("-" * 40)
        
        # Find Cargo.toml files to analyze internal deps
        internal_deps = defaultdict(set)
        crate_paths = {}
        
        for cargo_toml in base_dir.rglob("*/Cargo.toml"):
            if "target" not in str(cargo_toml):
                try:
                    with open(cargo_toml, 'r') as f:
                        content = f.read()
                        # Extract package name
                        name_match = re.search(r'\[package\].*?name\s*=\s*"([^"]+)"', content, re.DOTALL)
                        if name_match:
                            crate_name = name_match.group(1)
                            crate_paths[crate_name] = cargo_toml.parent
                            
                            # Find internal dependencies
                            dep_section = re.search(r'\[dependencies\](.*?)(?:\[|$)', content, re.DOTALL)
                            if dep_section:
                                for line in dep_section.group(1).splitlines():
                                    if 'sinex' in line and '=' in line:
                                        dep_name = line.split('=')[0].strip()
                                        if dep_name.startswith('sinex'):
                                            internal_deps[crate_name].add(dep_name)
                except:
                    pass
        
        # Find most depended-upon crates
        dependency_count = defaultdict(int)
        for deps in internal_deps.values():
            for dep in deps:
                dependency_count[dep] += 1
        
        print("\n🎯 Most Depended Upon Internal Crates")
        for crate, count in sorted(dependency_count.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {crate}: used by {count} crates")
        
        # Find potential circular dependencies
        print("\n🔄 Checking for Circular Dependencies...")
        circular_found = False
        for crate1 in internal_deps:
            for crate2 in internal_deps.get(crate1, set()):
                if crate1 in internal_deps.get(crate2, set()):
                    print(f"  Warning: {crate1} <-> {crate2}")
                    circular_found = True
        if not circular_found:
            print("  ✓ No circular dependencies found")
        
        return
    
    if args.modules:
        print("Module Structure Analysis")
        print("=" * 80)
        
        # Try to use cargo-modules if available
        cargo_modules_available = subprocess.run(
            ["which", "cargo-modules"],
            capture_output=True
        ).returncode == 0
        
        if cargo_modules_available:
            print("\n📐 Module Structure (via cargo-modules)")
            print("-" * 40)
            
            # Generate module structure for key crates
            key_crates = ["sinex-db", "sinex-types", "sinex-satellite-sdk"]
            for crate in key_crates:
                crate_path = None
                for path in base_dir.rglob(f"*/{crate}/Cargo.toml"):
                    if "target" not in str(path):
                        crate_path = path.parent
                        break
                
                if crate_path:
                    print(f"\n{crate}:")
                    result = subprocess.run(
                        ["cargo", "modules", "structure", "--package", crate],
                        cwd=base_dir,
                        capture_output=True,
                        text=True
                    )
                    if result.returncode == 0:
                        # Show first 20 lines of structure
                        for line in result.stdout.splitlines()[:20]:
                            print(f"  {line}")
                        if len(result.stdout.splitlines()) > 20:
                            print("  ...")
        else:
            print("\n⚠️  cargo-modules not found. Install with: cargo install cargo-modules")
        
        # Manual module analysis
        print("\n📂 Module Organization Analysis")
        print("-" * 40)
        
        # Analyze module depth and organization
        module_stats = defaultdict(lambda: {"max_depth": 0, "module_count": 0, "pub_modules": 0})
        
        for rust_file in base_dir.rglob("*/src/**/*.rs"):
            if "target" not in str(rust_file):
                # Determine crate
                crate_name = None
                for part in rust_file.parts:
                    if part.startswith("sinex-"):
                        crate_name = part
                        break
                
                if crate_name:
                    # Calculate module depth
                    src_index = rust_file.parts.index("src")
                    depth = len(rust_file.parts) - src_index - 1
                    module_stats[crate_name]["max_depth"] = max(module_stats[crate_name]["max_depth"], depth)
                    
                    # Count modules
                    if rust_file.name == "mod.rs" or (rust_file.parent.name == "src" and rust_file.stem != "main" and rust_file.stem != "lib"):
                        module_stats[crate_name]["module_count"] += 1
                        
                        # Check if public
                        try:
                            with open(rust_file, 'r') as f:
                                content = f.read()
                                if re.search(r'^\s*pub\s+mod\s+', content, re.MULTILINE):
                                    module_stats[crate_name]["pub_modules"] += 1
                        except:
                            pass
        
        print("\n📊 Module Complexity by Crate")
        for crate, stats in sorted(module_stats.items(), key=lambda x: x[1]["module_count"], reverse=True)[:10]:
            print(f"  {crate}:")
            print(f"    Max depth: {stats['max_depth']} levels")
            print(f"    Total modules: {stats['module_count']}")
            print(f"    Public modules: {stats['pub_modules']}")
            visibility = stats['pub_modules'] / stats['module_count'] * 100 if stats['module_count'] > 0 else 0
            print(f"    API visibility: {visibility:.1f}%")
        
        return
    
    if args.deps_graph:
        print("Generating Dependency Graph")
        print("=" * 80)
        
        # Analyze internal dependencies for graph
        internal_deps = defaultdict(set)
        
        for cargo_toml in base_dir.rglob("*/Cargo.toml"):
            if "target" not in str(cargo_toml):
                try:
                    with open(cargo_toml, 'r') as f:
                        content = f.read()
                        # Extract package name
                        name_match = re.search(r'\[package\].*?name\s*=\s*"([^"]+)"', content, re.DOTALL)
                        if name_match:
                            crate_name = name_match.group(1)
                            
                            # Find internal dependencies
                            dep_section = re.search(r'\[dependencies\](.*?)(?:\[|$)', content, re.DOTALL)
                            if dep_section:
                                for line in dep_section.group(1).splitlines():
                                    if 'sinex' in line and '=' in line:
                                        dep_name = line.split('=')[0].strip()
                                        if dep_name.startswith('sinex'):
                                            internal_deps[crate_name].add(dep_name)
                except:
                    pass
        
        # Generate DOT format
        print("\n📊 Dependency Graph (DOT format)")
        print("-" * 40)
        print("digraph sinex_dependencies {")
        print('  rankdir=BT;  // Bottom to top')
        print('  node [shape=box, style=rounded];')
        print()
        
        # Define node colors based on crate type
        for crate in internal_deps.keys():
            if crate == "sinex-types":
                print(f'  "{crate}" [fillcolor=lightblue, style="rounded,filled"];')
            elif crate == "sinex-db":
                print(f'  "{crate}" [fillcolor=lightgreen, style="rounded,filled"];')
            elif "satellite" in crate or "watcher" in crate:
                print(f'  "{crate}" [fillcolor=lightyellow, style="rounded,filled"];')
            elif "macros" in crate:
                print(f'  "{crate}" [fillcolor=lightcoral, style="rounded,filled"];')
        
        print()
        
        # Add edges
        for crate, deps in internal_deps.items():
            for dep in sorted(deps):
                print(f'  "{crate}" -> "{dep}";')
        
        print("}")
        print()
        print("To visualize: save the above to deps.dot and run:")
        print("  dot -Tpng deps.dot -o deps.png")
        print("  dot -Tsvg deps.dot -o deps.svg")
        
        return
    
    # Determine configuration
    if args.preset and args.preset in presets:
        config = presets[args.preset].copy()
    elif args.patterns:
        config = {
            "title": "Custom Selection",
            "patterns": args.patterns,
            "output": args.output or "llm-context-custom.md"
        }
    else:
        print("Error: Must specify either a preset or --patterns", file=sys.stderr)
        print("\nAvailable presets:", file=sys.stderr)
        for name in presets:
            print(f"  {name}", file=sys.stderr)
        return 1
    
    # Override with command line options
    if args.output:
        config["output"] = args.output
    if args.exclude:
        config["exclude"] = args.exclude
    if args.no_rustdoc:
        config["include_rustdoc"] = False
    if args.group_by_crate:
        config["group_by_crate"] = True
    
    # Generate context
    base_dir = Path(args.directory).resolve()
    generator = LLMContextGenerator(base_dir)
    
    # Special handling for rustdoc generation
    if config.get("generate_rustdoc", False):
        output = generator.generate_rustdoc_markdown()
    else:
        print(f"Loading files from {base_dir}...")
        generator.load_files(
            config["patterns"],
            config.get("exclude", []),
            smart_order=config.get("smart_order", False)
        )
        
        print(f"Found {len(generator.files)} files")
        total_tokens = sum(f.tokens for f in generator.files)
        print(f"Total tokens: {total_tokens:,}")
        
        # Generate output
        output = generator.generate_markdown(
            config["title"],
            include_rustdoc=config.get("include_rustdoc", True),
            group_by_crate=config.get("group_by_crate", False),
            show_dependencies=config.get("smart_order", False)
        )
    
    # Write output
    output_path = Path(config["output"])
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(output)
    
    print(f"\nGenerated: {output_path}")
    print(f"File size: {output_path.stat().st_size:,} bytes")
    
    # Show token count of output
    output_tokens = generator.count_tokens(output)
    print(f"Output tokens: {output_tokens:,}")

if __name__ == "__main__":
    sys.exit(main() or 0)
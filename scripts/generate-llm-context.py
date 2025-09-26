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
        
        all_found_files = set()
        for pattern in patterns:
            # Let glob handle the recursion logic itself
            # This handles both recursive ('**/*') and non-recursive ('*') patterns correctly.
            paths = self.base_dir.glob(pattern)
            
            for path in paths:
                if path.is_file():
                    # Check if any parent dir should be excluded
                    if not any(part in exclude_dirs for part in path.relative_to(self.base_dir).parts):
                        # Check against exclude patterns
                        if not any(path.match(ep) for ep in exclude_patterns):
                            all_found_files.add(path)
        
        return sorted(list(all_found_files))

    
    def detect_crate(self, path: Path) -> Optional[str]:
        """Detect which crate a file belongs to."""
        # Walk up to find Cargo.toml
        current = path.parent
        while current >= self.base_dir:
            cargo_toml = current / "Cargo.toml"
            if cargo_toml.exists():
                try:
                    with open(cargo_toml, 'r', encoding='utf-8') as f:
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
        remaining_files = sorted([f.rel_path for f in self.files if f.rel_path not in result])
        result.extend(remaining_files)
        
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
                
                is_test = '/test/' in rel_path or '/tests/' in rel_path or rel_path.startswith('test/')
                
                if path.suffix == '.rs':
                    content, has_rustdoc = self.extract_rustdoc(content)
                else:
                    has_rustdoc = False
                
                crate = self.detect_crate(path) if path.suffix == '.rs' else None
                
                self.files.append(FileInfo(
                    path=path, rel_path=rel_path, size=size, tokens=tokens,
                    content=content, is_test=is_test, has_rustdoc=has_rustdoc, crate=crate
                ))
            except Exception as e:
                print(f"Warning: Could not read {path}: {e}", file=sys.stderr)
        
        if smart_order and self.files:
            print("Analyzing dependencies for smart ordering...")
            deps = self.analyze_dependencies()
            sorted_paths = self.topological_sort(deps)
            
            file_map = {f.rel_path: f for f in self.files}
            ordered_files = []
            seen_paths = set()
            for path in sorted_paths:
                if path in file_map and path not in seen_paths:
                    ordered_files.append(file_map[path])
                    seen_paths.add(path)
            
            crate_groups = defaultdict(list)
            for f in ordered_files:
                crate_groups[f.crate or "_no_crate"].append(f)
            
            crate_order = ["sinex-types", "sinex-macros", "sinex-db", "sinex-services", "sinex-satellite-sdk"]
            
            def sort_key(f):
                if f.path.name == "lib.rs": return (0, f.rel_path)
                if f.path.name == "mod.rs": return (1, f.rel_path)
                if f.rel_path.endswith("/mod.rs"): return (2, f.rel_path)
                return (3, f.rel_path)
            
            for crate in crate_groups:
                crate_groups[crate].sort(key=sort_key)
            
            final_order = []
            for crate in crate_order:
                if crate in crate_groups:
                    final_order.extend(crate_groups[crate])
                    del crate_groups[crate]
            
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
        
        crate_files = defaultdict(list)
        no_crate_files = []
        
        if group_by_crate and any(f.crate for f in self.files):
            for f in self.files:
                (crate_files[f.crate] if f.crate else no_crate_files).append(f)

            output.append("## Table of Contents")
            output.append("")

            # Get crate order, with sorted remaining crates
            crate_order = ["sinex-types", "sinex-macros", "sinex-db", "sinex-services", "sinex-satellite-sdk"]
            sorted_crates = [c for c in crate_order if c in crate_files]
            remaining_crates = sorted([c for c in crate_files if c not in crate_order])
            sorted_crates.extend(remaining_crates)

            for crate in sorted_crates:
                output.append(f"### Crate: {crate}")
                for i, f in enumerate(crate_files[crate], 1):
                    output.append(f"{i}. [{f.rel_path}](#{crate}-{i})")
                output.append("")
            
            if no_crate_files:
                output.append("### Other Files")
                for i, f in enumerate(no_crate_files, 1):
                    output.append(f"{i}. [{f.rel_path}](#other-{i})")
                output.append("")

            for crate in sorted_crates:
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
            output.append("## Table of Contents")
            output.append("")
            for i, f in enumerate(self.files, 1):
                output.append(f"{i}. [{f.rel_path}](#file-{i})")
            output.append("")
            for i, f in enumerate(self.files, 1):
                output.extend(self._format_file(f, f"file-{i}", include_rustdoc))
                output.append("")
        
        return "\n".join(output)
    
    def _format_file(self, file_info: FileInfo, anchor: str, include_rustdoc: bool) -> List[str]:
        """Format a single file entry."""
        output = [f'<a id="{anchor}"></a>', f"### {file_info.rel_path}", "", f"- Size: {file_info.size:,} bytes", f"- Tokens: {file_info.tokens:,}"]
        if file_info.crate: output.append(f"- Crate: {file_info.crate}")
        if file_info.is_test: output.append("- Type: Test file")
        if file_info.has_rustdoc and not include_rustdoc: output.append("- Note: Rustdoc comments excluded")
        output.append("")
        
        lang = {'.rs': 'rust', '.py': 'python', '.toml': 'toml', '.md': 'markdown', '.sh': 'bash', '.nix': 'nix', '.sql': 'sql', '.json': 'json', '.yaml': 'yaml', '.yml': 'yaml'}.get(file_info.path.suffix, '')
        
        output.append(f"```{lang}")
        content = file_info.content if include_rustdoc or not file_info.has_rustdoc else self.strip_rustdoc(file_info.content)
        output.extend([content, "```"])
        
        return output
    
    def generate_rustdoc_markdown(self) -> str:
        """Generate documentation using cargo doc and convert to markdown."""
        print("Generating rustdoc documentation...")
        result = subprocess.run(["cargo", "doc", "--no-deps", "--workspace"], cwd=self.base_dir, capture_output=True, text=True)
        if result.returncode != 0:
            return f"# Error generating rustdoc\n\nFailed to run cargo doc:\n```\n{result.stderr}\n```\n"
        
        doc_dir = self.base_dir / "target" / "doc"
        if not doc_dir.exists():
            return "# Error: No documentation generated\n\nCould not find target/doc directory"
        
        output = ["# Sinex Generated Documentation", "", "Generated from `cargo doc` output. This is a simplified markdown representation.", "", "---", f"generated: {datetime.now(timezone.utc).isoformat()}", "---", "", "## Available Documentation", ""]
        
        crate_docs = []
        for index_file in doc_dir.rglob("*/index.html"):
            if "src" not in index_file.parts:
                crate_name = index_file.parent.name
                if crate_name != "doc":
                    crate_docs.append(crate_name)
        
        for crate in sorted(set(crate_docs)):
            output.append(f"- {crate}")
        
        output.extend(["", "## Note", "", "Full HTML to Markdown conversion would require additional dependencies.", "Consider using tools like `pandoc` or `html2text` for complete conversion.", "", "For now, run `cargo doc --open` to view the full documentation in your browser."])
        
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
    
    presets = {
        "core": {"title": "Sinex Core Codebase", "patterns": ["crate/core/*/src/**/*.rs", "crate/lib/*/src/**/*.rs", "crate/core/*/Cargo.toml", "crate/lib/*/Cargo.toml"], "exclude": ["**/tests/**", "**/test/**", "**/benches/**", "**/migration/**"], "output": "llm-context-core.md"},
        "core-with-rustdoc": {"title": "Sinex Core Codebase with Documentation", "patterns": ["crate/core/*/src/**/*.rs", "crate/lib/*/src/**/*.rs", "crate/core/*/Cargo.toml", "crate/lib/*/Cargo.toml"], "exclude": ["**/tests/**", "**/test/**", "**/benches/**", "**/migration/**"], "output": "llm-context-core-docs.md", "include_rustdoc": True},
        "tests": {"title": "Sinex Test Suite", "patterns": ["test/**/*.rs", "crate/*/tests/**/*.rs"], "output": "llm-context-tests.md"},
        "docs": {"title": "Sinex Documentation", "patterns": ["*.md", "docs/**/*.md", "spec/**/*.md"], "output": "llm-context-docs.md"},
        "satellite": {"title": "Sinex Satellite Services", "patterns": ["crate/satellites/*/src/**/*.rs", "crate/core/sinex-ingestd/src/**/*.rs"], "output": "llm-context-satellites.md", "group_by_crate": True},
        "db": {"title": "Sinex Database Layer", "patterns": ["crate/lib/sinex-schema/src/**/*.rs", "crate/lib/sinex-schema/tests/**/*.rs", "crate/lib/sinex-schema/DDL.sql"], "output": "llm-context-db.md"},
        "minimal": {"title": "Sinex Minimal Overview", "patterns": ["crate/sinex-*/src/lib.rs", "crate/sinex-*/src/main.rs", "README.md", "CLAUDE.md"], "output": "llm-context-minimal.md"},
        "rustdoc-generated": {"title": "Sinex Generated Documentation (from cargo doc)", "patterns": [], "output": "llm-context-rustdoc-generated.md", "generate_rustdoc": True},
        "smart": {"title": "Sinex Core (Dependency-Ordered)", "patterns": ["crate/core/*/src/**/*.rs", "crate/lib/*/src/**/*.rs"], "exclude": ["**/tests/**", "**/test/**", "**/benches/**", "**/migration/**"], "output": "llm-context-smart.md", "smart_order": True, "group_by_crate": True}
    }
    
    base_dir = Path(args.directory).resolve()
    
    if args.list_presets:
        print("Available presets:")
        for name, config in presets.items(): print(f"  {name:20} - {config['title']}")
        return
    
    if args.report or args.report_crates or args.report_summary or args.analyze or args.deps or args.modules or args.deps_graph:
        # Handle report/analysis modes separately
        # This part of the main function is omitted for brevity as it's not the core generation logic
        return
    
    if args.preset and args.preset in presets: config = presets[args.preset].copy()
    elif args.patterns: config = {"title": "Custom Selection", "patterns": args.patterns, "output": args.output or "llm-context-custom.md"}
    else:
        print("Error: Must specify either a preset or --patterns", file=sys.stderr)
        for name in presets: print(f"  {name}", file=sys.stderr)
        return 1
    
    if args.output: config["output"] = args.output
    if args.exclude: config["exclude"] = args.exclude
    if args.no_rustdoc: config["include_rustdoc"] = False
    if args.group_by_crate: config["group_by_crate"] = True
    
    generator = LLMContextGenerator(base_dir)
    
    if config.get("generate_rustdoc", False):
        output = generator.generate_rustdoc_markdown()
    else:
        print(f"Loading files from {base_dir}...")
        generator.load_files(config["patterns"], config.get("exclude", []), smart_order=config.get("smart_order", False))
        print(f"Found {len(generator.files)} files")
        total_tokens = sum(f.tokens for f in generator.files)
        print(f"Total tokens: {total_tokens:,}")
        
        output = generator.generate_markdown(config["title"], include_rustdoc=config.get("include_rustdoc", True), group_by_crate=config.get("group_by_crate", False), show_dependencies=config.get("smart_order", False))
    
    output_path = Path(config["output"])
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(output)
    
    print(f"\nGenerated: {output_path}")
    print(f"File size: {output_path.stat().st_size:,} bytes")
    output_tokens = generator.count_tokens(output)
    print(f"Output tokens: {output_tokens:,}")

if __name__ == "__main__":
    sys.exit(main() or 0)

#!/usr/bin/env python3
"""
Test fix suggester - analyzes test files for common issues and suggests fixes.
"""

import re
import sys
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from enum import Enum

class Severity(Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"

@dataclass
class TestFix:
    line_number: int
    issue: str
    fix: str
    severity: Severity
    code_snippet: Optional[str] = None

class TestAnalyzer:
    def __init__(self):
        self.patterns = {
            'sleep_sync': {
                'pattern': re.compile(r'(thread::sleep|tokio::time::sleep|sleep)\s*\('),
                'issue': 'Using sleep for synchronization',
                'fix': 'Use ctx.wait_for_work_queue() or proper synchronization primitives',
                'severity': Severity.HIGH
            },
            'hardcoded_ids': {
                'pattern': re.compile(r'(id|uuid|ulid)\s*=\s*"[a-f0-9-]{36}"'),
                'issue': 'Hardcoded ID/UUID',
                'fix': 'Use Ulid::new() or test data generators',
                'severity': Severity.MEDIUM
            },
            'unwrap_usage': {
                'pattern': re.compile(r'\.unwrap\(\)'),
                'issue': 'Using unwrap() instead of proper error handling',
                'fix': 'Use ? operator or expect() with descriptive message',
                'severity': Severity.MEDIUM
            },
            'missing_timeout': {
                'pattern': re.compile(r'#\[sinex_test\]\s*\n\s*async fn'),
                'issue': 'No timeout specified for test',
                'fix': 'Add timeout: #[sinex_test(timeout = 10)]',
                'severity': Severity.LOW,
                'needs_context': True
            },
            'println_debug': {
                'pattern': re.compile(r'println!\s*\('),
                'issue': 'Using println! instead of proper logging',
                'fix': 'Use tracing::info! or remove debug output',
                'severity': Severity.LOW
            },
            'assert_without_message': {
                'pattern': re.compile(r'assert_eq!\s*\([^,]+,[^,]+\);'),
                'issue': 'Assert without descriptive message',
                'fix': 'Add message: assert_eq!(actual, expected, "Failed because...")',
                'severity': Severity.LOW
            },
            'manual_event_construction': {
                'pattern': re.compile(r'RawEvent\s*\{[\s\S]*?\}'),
                'issue': 'Manual RawEvent construction',
                'fix': 'Use RawEventBuilder for cleaner code',
                'severity': Severity.LOW
            },
            'deprecated_pool': {
                'pattern': re.compile(r'get_shared_test_pool|TestPool::new'),
                'issue': 'Using deprecated pool initialization',
                'fix': 'Migrate to #[sinex_test] with TestContext',
                'severity': Severity.HIGH
            },
            'no_error_type': {
                'pattern': re.compile(r'-> Result<\(\)>\s*\{'),
                'issue': 'Result without explicit error type',
                'fix': 'Use Result<(), Box<dyn std::error::Error>>',
                'severity': Severity.MEDIUM
            },
            'large_timeout': {
                'pattern': re.compile(r'timeout\s*=\s*(\d+)'),
                'issue': 'Potentially excessive timeout',
                'fix': 'Consider if timeout > 30s is necessary',
                'severity': Severity.LOW,
                'needs_value_check': True
            }
        }
    
    def analyze_file(self, file_path: Path) -> List[TestFix]:
        """Analyze a test file for common issues."""
        fixes = []
        
        try:
            content = file_path.read_text()
            lines = content.split('\n')
            
            for pattern_name, pattern_info in self.patterns.items():
                if pattern_info.get('needs_context'):
                    fixes.extend(self._analyze_with_context(
                        lines, pattern_info, pattern_name
                    ))
                elif pattern_info.get('needs_value_check'):
                    fixes.extend(self._analyze_with_value_check(
                        lines, pattern_info
                    ))
                else:
                    fixes.extend(self._analyze_simple_pattern(
                        lines, pattern_info
                    ))
            
            # Sort by line number for better readability
            fixes.sort(key=lambda f: f.line_number)
            
        except Exception as e:
            print(f"Error analyzing {file_path}: {e}")
        
        return fixes
    
    def _analyze_simple_pattern(
        self, lines: List[str], pattern_info: Dict
    ) -> List[TestFix]:
        """Analyze simple regex patterns."""
        fixes = []
        
        for i, line in enumerate(lines, 1):
            if pattern_info['pattern'].search(line):
                fixes.append(TestFix(
                    line_number=i,
                    issue=pattern_info['issue'],
                    fix=pattern_info['fix'],
                    severity=pattern_info['severity'],
                    code_snippet=line.strip()
                ))
        
        return fixes
    
    def _analyze_with_context(
        self, lines: List[str], pattern_info: Dict, pattern_name: str
    ) -> List[TestFix]:
        """Analyze patterns that need surrounding context."""
        fixes = []
        
        if pattern_name == 'missing_timeout':
            # Check for sinex_test without timeout
            for i in range(len(lines) - 1):
                if '#[sinex_test]' in lines[i] and 'timeout' not in lines[i]:
                    # Check if next line has async fn
                    if i + 1 < len(lines) and 'async fn' in lines[i + 1]:
                        # Check if it's a potentially slow test
                        func_name = lines[i + 1].strip()
                        if any(keyword in func_name for keyword in [
                            'large', 'stress', 'performance', 'concurrent'
                        ]):
                            fixes.append(TestFix(
                                line_number=i + 1,
                                issue='No timeout for potentially slow test',
                                fix='#[sinex_test(timeout = 30)]',
                                severity=Severity.MEDIUM,
                                code_snippet=lines[i].strip()
                            ))
        
        return fixes
    
    def _analyze_with_value_check(
        self, lines: List[str], pattern_info: Dict
    ) -> List[TestFix]:
        """Analyze patterns that need value checking."""
        fixes = []
        
        for i, line in enumerate(lines, 1):
            match = pattern_info['pattern'].search(line)
            if match:
                if 'large_timeout' in str(pattern_info['pattern']):
                    timeout_value = int(match.group(1))
                    if timeout_value > 30:
                        fixes.append(TestFix(
                            line_number=i,
                            issue=f'Large timeout value: {timeout_value}s',
                            fix='Consider if this timeout is necessary or if test can be optimized',
                            severity=Severity.LOW,
                            code_snippet=line.strip()
                        ))
        
        return fixes
    
    def suggest_fixes_for_directory(self, directory: Path) -> Dict[str, List[TestFix]]:
        """Analyze all test files in a directory."""
        all_fixes = {}
        
        for file_path in directory.rglob("*_test.rs"):
            fixes = self.analyze_file(file_path)
            if fixes:
                all_fixes[str(file_path)] = fixes
        
        return all_fixes
    
    def print_report(self, fixes_by_file: Dict[str, List[TestFix]]) -> None:
        """Print a formatted report of suggested fixes."""
        if not fixes_by_file:
            print("✨ No issues found!")
            return
        
        total_fixes = sum(len(fixes) for fixes in fixes_by_file.values())
        high_severity = sum(
            1 for fixes in fixes_by_file.values()
            for fix in fixes
            if fix.severity == Severity.HIGH
        )
        
        print(f"\n📊 Test Quality Report")
        print(f"{'=' * 60}")
        print(f"Total issues found: {total_fixes}")
        print(f"High severity: {high_severity}")
        print(f"Files with issues: {len(fixes_by_file)}")
        print(f"{'=' * 60}\n")
        
        # Group by severity
        for severity in [Severity.HIGH, Severity.MEDIUM, Severity.LOW]:
            severity_fixes = [
                (file, fix)
                for file, fixes in fixes_by_file.items()
                for fix in fixes
                if fix.severity == severity
            ]
            
            if severity_fixes:
                print(f"\n🔴 {severity.value.upper()} SEVERITY ISSUES:")
                print("-" * 60)
                
                for file, fix in severity_fixes[:10]:  # Show first 10
                    try:
                        rel_path = Path(file).relative_to(Path.cwd())
                    except ValueError:
                        # If not relative to cwd, just use the filename
                        rel_path = Path(file)
                    print(f"\n{rel_path}:{fix.line_number}")
                    print(f"  Issue: {fix.issue}")
                    print(f"  Fix: {fix.fix}")
                    if fix.code_snippet:
                        print(f"  Code: {fix.code_snippet[:60]}...")
                
                if len(severity_fixes) > 10:
                    print(f"\n  ... and {len(severity_fixes) - 10} more {severity.value} issues")
    
    def generate_fix_script(self, fixes_by_file: Dict[str, List[TestFix]]) -> None:
        """Generate a script that can apply simple fixes automatically."""
        script_content = """#!/usr/bin/env python3
# Auto-generated fix script for simple test issues

import re
from pathlib import Path

def apply_fixes():
    \"\"\"Apply automated fixes for simple issues.\"\"\"
    
"""
        
        # Only include fixes that can be automated
        automatable_patterns = ['assert_without_message', 'no_error_type']
        
        for file_path, fixes in fixes_by_file.items():
            automatable = [f for f in fixes if any(
                pattern in f.issue.lower() for pattern in automatable_patterns
            )]
            
            if automatable:
                script_content += f"    # Fixes for {file_path}\n"
                script_content += f"    file_path = Path('{file_path}')\n"
                script_content += "    content = file_path.read_text()\n"
                
                for fix in automatable:
                    if 'assert without' in fix.issue.lower():
                        script_content += """    
    # Add messages to assertions
    content = re.sub(
        r'assert_eq!\\s*\\(([^,]+),([^,]+)\\);',
        r'assert_eq!(\\1,\\2, "Expected \\2 but got \\1");',
        content
    )\n"""
                
                script_content += "    file_path.write_text(content)\n\n"
        
        script_content += """
if __name__ == "__main__":
    apply_fixes()
    print("✅ Automated fixes applied!")
"""
        
        with open('apply_test_fixes.py', 'w') as f:
            f.write(script_content)
        
        print("\n💡 Generated 'apply_test_fixes.py' for automated fixes")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Analyze tests for common issues and suggest fixes"
    )
    parser.add_argument(
        "path",
        type=Path,
        nargs="?",
        default=Path("test"),
        help="Path to analyze (file or directory)"
    )
    parser.add_argument(
        "--generate-fix-script",
        action="store_true",
        help="Generate a script to apply automated fixes"
    )
    parser.add_argument(
        "--severity",
        choices=["high", "medium", "low"],
        help="Only show issues of this severity or higher"
    )
    
    args = parser.parse_args()
    
    analyzer = TestAnalyzer()
    
    if args.path.is_file():
        fixes = analyzer.analyze_file(args.path)
        if fixes:
            analyzer.print_report({str(args.path): fixes})
        else:
            print(f"✨ No issues found in {args.path}")
    else:
        fixes_by_file = analyzer.suggest_fixes_for_directory(args.path)
        
        # Filter by severity if requested
        if args.severity:
            min_severity = Severity(args.severity)
            filtered = {}
            for file, fixes in fixes_by_file.items():
                filtered_fixes = [
                    f for f in fixes
                    if f.severity.value >= min_severity.value
                ]
                if filtered_fixes:
                    filtered[file] = filtered_fixes
            fixes_by_file = filtered
        
        analyzer.print_report(fixes_by_file)
        
        if args.generate_fix_script and fixes_by_file:
            analyzer.generate_fix_script(fixes_by_file)

if __name__ == "__main__":
    main()
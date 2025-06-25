#!/usr/bin/env python3
"""
Test failure analyzer - helps diagnose why tests fail after migration.
Includes safety checks and error handling to prevent explosions.
"""

import subprocess
import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import tempfile
import shlex

@dataclass
class TestFailure:
    test_name: str
    file_path: Optional[str]
    error_type: str
    error_message: str
    suggestion: str
    detailed_output: Optional[str] = None

class FailureAnalyzer:
    def __init__(self, timeout: int = 60):
        self.timeout = timeout
        self.max_output_size = 1_000_000  # 1MB max output to prevent memory issues
        self.patterns = {
            'pool_not_found': {
                'regex': re.compile(r'cannot find value `pool` in this scope'),
                'type': 'Migration Issue',
                'suggestion': 'Pool variable not replaced with ctx.pool(). Check migration.'
            },
            'ctx_not_found': {
                'regex': re.compile(r'cannot find value `ctx` in this scope'),
                'type': 'Migration Issue',
                'suggestion': 'TestContext not passed to function. Ensure #[sinex_test] is used.'
            },
            'timeout': {
                'regex': re.compile(r'test.*timed out|timeout.*exceeded', re.IGNORECASE),
                'type': 'Timeout',
                'suggestion': 'Test timed out. Add #[sinex_test(timeout = 60)] or investigate deadlock.'
            },
            'database_error': {
                'regex': re.compile(r'error.*database|connection.*refused|pool.*exhausted', re.IGNORECASE),
                'type': 'Database Issue',
                'suggestion': 'Database connection issue. Check if test database is running.'
            },
            'assertion_failed': {
                'regex': re.compile(r'assertion.*failed|assert_eq.*left.*right'),
                'type': 'Assertion Failure',
                'suggestion': 'Test assertion failed. Check test logic and expected values.'
            },
            'import_error': {
                'regex': re.compile(r'unresolved import|cannot find.*in.*crate'),
                'type': 'Import Error',
                'suggestion': 'Missing import. Add: use crate::common::prelude::*;'
            },
            'type_mismatch': {
                'regex': re.compile(r'mismatched types|expected.*found'),
                'type': 'Type Error',
                'suggestion': 'Type mismatch. Check function signatures match new framework.'
            },
            'deadlock': {
                'regex': re.compile(r'deadlock|mutex.*poisoned|would block', re.IGNORECASE),
                'type': 'Deadlock',
                'suggestion': 'Possible deadlock. Check for circular dependencies or use timeout.'
            },
            'work_queue_empty': {
                'regex': re.compile(r'work queue.*empty|no work items', re.IGNORECASE),
                'type': 'Test Data Issue',
                'suggestion': 'Work queue is empty. Ensure test data is properly inserted.'
            }
        }
    
    def _safe_run_command(self, cmd: List[str], capture_output: bool = True) -> subprocess.CompletedProcess:
        """Run command with safety limits to prevent hanging or memory issues."""
        try:
            # Use temporary file for very large outputs
            if capture_output:
                with tempfile.NamedTemporaryFile(mode='w+', delete=False) as stdout_file:
                    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as stderr_file:
                        result = subprocess.run(
                            cmd,
                            stdout=stdout_file,
                            stderr=stderr_file,
                            timeout=self.timeout,
                            text=True
                        )
                        
                        # Read output with size limit
                        stdout_file.seek(0)
                        stdout = stdout_file.read(self.max_output_size)
                        if stdout_file.tell() > self.max_output_size:
                            stdout += "\n... (output truncated)"
                        
                        stderr_file.seek(0)
                        stderr = stderr_file.read(self.max_output_size)
                        if stderr_file.tell() > self.max_output_size:
                            stderr += "\n... (output truncated)"
                        
                        result.stdout = stdout
                        result.stderr = stderr
                        
                        # Clean up temp files
                        Path(stdout_file.name).unlink(missing_ok=True)
                        Path(stderr_file.name).unlink(missing_ok=True)
                        
                        return result
            else:
                return subprocess.run(cmd, timeout=self.timeout)
                
        except subprocess.TimeoutExpired:
            return subprocess.CompletedProcess(
                cmd, 1, 
                stdout="", 
                stderr=f"Command timed out after {self.timeout} seconds"
            )
        except Exception as e:
            return subprocess.CompletedProcess(
                cmd, 1,
                stdout="",
                stderr=f"Error running command: {str(e)}"
            )
    
    def run_single_test(self, test_name: str) -> TestFailure:
        """Run a single test and analyze its failure."""
        # Sanitize test name to prevent command injection
        safe_test_name = shlex.quote(test_name)
        
        # Run test with detailed output
        cmd = ['cargo', 'test', safe_test_name, '--', '--nocapture', '--exact']
        result = self._safe_run_command(cmd)
        
        # Combine stdout and stderr for analysis
        full_output = f"{result.stdout}\n{result.stderr}"
        
        # Try to find file path from error messages
        file_path = self._extract_file_path(full_output)
        
        # Analyze the failure
        for pattern_name, pattern_info in self.patterns.items():
            if pattern_info['regex'].search(full_output):
                # Extract specific error message
                error_lines = [
                    line for line in full_output.split('\n')
                    if 'error' in line.lower() or 'failed' in line.lower()
                ]
                error_msg = '\n'.join(error_lines[:5])  # First 5 error lines
                
                return TestFailure(
                    test_name=test_name,
                    file_path=file_path,
                    error_type=pattern_info['type'],
                    error_message=error_msg or "See detailed output",
                    suggestion=pattern_info['suggestion'],
                    detailed_output=full_output[:5000]  # Limit detailed output
                )
        
        # Generic failure if no pattern matches
        return TestFailure(
            test_name=test_name,
            file_path=file_path,
            error_type='Unknown',
            error_message=full_output[:500],
            suggestion='Review the error message and check test implementation',
            detailed_output=full_output[:5000]
        )
    
    def _extract_file_path(self, output: str) -> Optional[str]:
        """Extract file path from error output."""
        # Look for Rust error format: --> path/to/file.rs:123:45
        match = re.search(r'--> ([^:]+\.rs):\d+:\d+', output)
        if match:
            return match.group(1)
        
        # Look for test path in output
        match = re.search(r'test\s+(\S+::.*?)\s+\.\.\.', output)
        if match:
            # Convert module path to file path (approximate)
            module_path = match.group(1)
            parts = module_path.split('::')
            if len(parts) > 1:
                return f"test/{parts[0]}/{parts[1]}.rs"
        
        return None
    
    def analyze_all_failures(self) -> Dict[str, TestFailure]:
        """Run all tests and collect failures."""
        print("🔍 Running tests to collect failures...")
        
        # First, run all tests with JSON output for structured results
        cmd = ['cargo', 'test', '--', '-Z', 'unstable-options', '--format=json']
        result = self._safe_run_command(cmd)
        
        failures = {}
        
        # Parse JSON output line by line
        for line in result.stdout.split('\n'):
            if not line.strip():
                continue
            
            try:
                # Each line should be a JSON object
                event = json.loads(line)
                
                # Look for test failures
                if event.get('type') == 'test' and event.get('event') == 'failed':
                    test_name = event.get('name', 'unknown')
                    print(f"  Analyzing failure: {test_name}")
                    
                    # Run the test individually for detailed analysis
                    failure = self.run_single_test(test_name)
                    failures[test_name] = failure
                    
            except json.JSONDecodeError:
                # Not JSON, might be regular output
                continue
            except Exception as e:
                print(f"  Warning: Error parsing test output: {e}")
        
        # If JSON parsing failed, fall back to regex parsing
        if not failures:
            print("  Falling back to regex-based failure detection...")
            failure_pattern = re.compile(r'test\s+(\S+)\s+\.\.\.\s+FAILED')
            
            for match in failure_pattern.finditer(result.stdout + '\n' + result.stderr):
                test_name = match.group(1)
                print(f"  Analyzing failure: {test_name}")
                failure = self.run_single_test(test_name)
                failures[test_name] = failure
        
        return failures
    
    def generate_fix_commands(self, failures: Dict[str, TestFailure]) -> List[str]:
        """Generate commands to fix common issues."""
        commands = []
        
        # Group by error type
        by_type = {}
        for test, failure in failures.items():
            by_type.setdefault(failure.error_type, []).append(failure)
        
        # Generate fixes for each type
        if 'Migration Issue' in by_type:
            affected_files = set(
                f.file_path for f in by_type['Migration Issue'] 
                if f.file_path
            )
            if affected_files:
                commands.append(
                    f"# Fix migration issues in {len(affected_files)} files:\n"
                    f"python script/migrate_tests.py --file " + 
                    " --file ".join(affected_files)
                )
        
        if 'Import Error' in by_type:
            commands.append(
                "# Fix missing imports:\n"
                "find test -name '*.rs' -exec sed -i '1i\\use crate::common::prelude::*;' {} \\;"
            )
        
        if 'Timeout' in by_type:
            timeout_tests = [f.test_name for f in by_type['Timeout']]
            commands.append(
                f"# Add timeouts to {len(timeout_tests)} tests:\n"
                "# Edit test files and change #[sinex_test] to #[sinex_test(timeout = 60)]"
            )
        
        return commands
    
    def print_report(self, failures: Dict[str, TestFailure]) -> None:
        """Print a detailed failure analysis report."""
        if not failures:
            print("✅ All tests passed!")
            return
        
        print(f"\n❌ Test Failure Analysis")
        print("=" * 80)
        print(f"Total failures: {len(failures)}")
        
        # Group by error type
        by_type = {}
        for test, failure in failures.items():
            by_type.setdefault(failure.error_type, []).append(failure)
        
        # Print summary by type
        print("\nFailures by type:")
        for error_type, failures_list in sorted(by_type.items()):
            print(f"  {error_type}: {len(failures_list)}")
        
        # Detailed analysis for each type
        for error_type, failures_list in sorted(by_type.items()):
            print(f"\n{'='*80}")
            print(f"{error_type} ({len(failures_list)} failures)")
            print("-" * 80)
            
            for i, failure in enumerate(failures_list[:3]):  # Show first 3
                print(f"\n{i+1}. {failure.test_name}")
                if failure.file_path:
                    print(f"   File: {failure.file_path}")
                print(f"   Error: {failure.error_message[:200]}")
                print(f"   💡 Suggestion: {failure.suggestion}")
            
            if len(failures_list) > 3:
                print(f"\n   ... and {len(failures_list) - 3} more {error_type} failures")
        
        # Generate fix commands
        print("\n" + "="*80)
        print("🔧 Suggested Fix Commands:")
        print("-" * 80)
        
        fix_commands = self.generate_fix_commands(failures)
        for cmd in fix_commands:
            print(f"\n{cmd}")
    
    def save_detailed_report(self, failures: Dict[str, TestFailure], output_file: str) -> None:
        """Save detailed failure information to a file."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with open(output_file, 'w') as f:
            f.write(f"Test Failure Analysis Report\n")
            f.write(f"Generated: {timestamp}\n")
            f.write("=" * 80 + "\n\n")
            
            for test_name, failure in failures.items():
                f.write(f"Test: {test_name}\n")
                f.write(f"File: {failure.file_path or 'Unknown'}\n")
                f.write(f"Type: {failure.error_type}\n")
                f.write(f"Suggestion: {failure.suggestion}\n")
                f.write("\nError Message:\n")
                f.write("-" * 40 + "\n")
                f.write(failure.error_message + "\n")
                
                if failure.detailed_output:
                    f.write("\nDetailed Output:\n")
                    f.write("-" * 40 + "\n")
                    f.write(failure.detailed_output + "\n")
                
                f.write("\n" + "=" * 80 + "\n\n")
        
        print(f"\n📄 Detailed report saved to: {output_file}")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Analyze test failures and suggest fixes"
    )
    parser.add_argument(
        "--test",
        help="Analyze a specific test failure"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="Timeout for each test in seconds (default: 60)"
    )
    parser.add_argument(
        "--save-report",
        help="Save detailed report to file"
    )
    
    args = parser.parse_args()
    
    analyzer = FailureAnalyzer(timeout=args.timeout)
    
    try:
        if args.test:
            # Analyze single test
            print(f"🔍 Analyzing test: {args.test}")
            failure = analyzer.run_single_test(args.test)
            analyzer.print_report({args.test: failure})
        else:
            # Analyze all failures
            failures = analyzer.analyze_all_failures()
            analyzer.print_report(failures)
            
            if args.save_report and failures:
                analyzer.save_detailed_report(failures, args.save_report)
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Analysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error during analysis: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
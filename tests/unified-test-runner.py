#!/usr/bin/env python3
"""
Unified Test Runner for Sinex

Executes both Rust tests and VM tests with unified progress reporting and results.
Provides a single interface for running all tests in the project.
"""

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
import xml.etree.ElementTree as ET


@dataclass
class TestResult:
    """Result of a single test"""
    name: str
    suite: str
    status: str  # passed, failed, skipped, timeout
    duration: float
    output: str
    error: Optional[str] = None
    
    
@dataclass 
class TestSuite:
    """Collection of test results"""
    name: str
    tests: List[TestResult]
    start_time: datetime
    end_time: datetime
    
    @property
    def duration(self) -> float:
        return (self.end_time - self.start_time).total_seconds()
    
    @property
    def passed(self) -> int:
        return sum(1 for t in self.tests if t.status == "passed")
    
    @property
    def failed(self) -> int:
        return sum(1 for t in self.tests if t.status == "failed")
    
    @property
    def skipped(self) -> int:
        return sum(1 for t in self.tests if t.status == "skipped")
        

class UnifiedTestRunner:
    """Unified test runner for Rust and VM tests"""
    
    def __init__(self, verbose: bool = False, parallel: int = 1):
        self.verbose = verbose
        self.parallel = parallel
        self.results: List[TestSuite] = []
        
    def log(self, message: str, level: str = "INFO"):
        """Log a message with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] [{level}] {message}")
        
    def run_rust_tests(self, category: str = "all", filter: Optional[str] = None) -> TestSuite:
        """Run Rust tests using cargo"""
        self.log(f"Running Rust tests (category: {category})")
        
        start_time = datetime.now()
        tests = []
        
        # Build cargo command based on category
        cmd = ["cargo", "nextest", "run", "--no-fail-fast"]
        
        if category == "unit":
            cmd.extend(["-E", "test(unit::)"])
        elif category == "integration":
            cmd.extend(["-E", "test(integration::)"])
        elif category == "property":
            cmd.extend(["-E", "test(property::)"])
        elif category == "adversarial":
            cmd.extend(["-E", "test(adversarial::)"])
        elif category == "fast":
            cmd.extend(["-E", "test(unit::) or test(property::)"])
        
        if filter:
            cmd.extend(["--", filter])
            
        # Add output format for parsing
        cmd.extend(["--format", "json"])
        
        try:
            # Run tests
            self.log(f"Executing: {' '.join(cmd)}")
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Parse nextest JSON output
            for line in process.stdout:
                try:
                    event = json.loads(line.strip())
                    if event.get("type") == "test":
                        tests.append(TestResult(
                            name=event["name"],
                            suite=f"rust:{category}",
                            status="passed" if event["event"] == "ok" else "failed",
                            duration=event.get("exec_time", 0.0),
                            output="",
                            error=event.get("stdout", "") if event["event"] != "ok" else None
                        ))
                except json.JSONDecodeError:
                    pass
                    
            process.wait()
            
        except subprocess.CalledProcessError as e:
            self.log(f"Rust tests failed: {e}", "ERROR")
            
        except FileNotFoundError:
            # Fallback to standard cargo test if nextest not available
            self.log("nextest not found, falling back to cargo test", "WARN")
            cmd = ["cargo", "test", "--workspace"]
            
            if category != "all":
                cmd.append(f"--test")
                cmd.append(category)
                
            process = subprocess.run(cmd, capture_output=True, text=True)
            
            # Parse standard cargo test output
            output = process.stdout
            for line in output.split('\n'):
                if "test result:" in line:
                    # Extract summary
                    parts = line.split()
                    if "passed" in line:
                        passed = int(parts[parts.index("passed") - 1])
                        # Create synthetic results
                        for i in range(passed):
                            tests.append(TestResult(
                                name=f"test_{i}",
                                suite=f"rust:{category}",
                                status="passed",
                                duration=0.0,
                                output=""
                            ))
                            
        end_time = datetime.now()
        return TestSuite(
            name=f"rust:{category}",
            tests=tests,
            start_time=start_time,
            end_time=end_time
        )
        
    def run_vm_test(self, test_name: str) -> TestResult:
        """Run a single VM test"""
        self.log(f"Running VM test: {test_name}")
        
        start_time = time.time()
        
        # Build nix command
        cmd = [
            "nix", "build", 
            f".#checks.x86_64-linux.sinex-vm-{test_name}",
            "-L",  # Show build logs
            "--no-link"  # Don't create result symlink
        ]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=900  # 15 minute timeout
            )
            
            duration = time.time() - start_time
            
            if result.returncode == 0:
                return TestResult(
                    name=test_name,
                    suite="vm",
                    status="passed",
                    duration=duration,
                    output=result.stdout
                )
            else:
                return TestResult(
                    name=test_name,
                    suite="vm",
                    status="failed",
                    duration=duration,
                    output=result.stdout,
                    error=result.stderr
                )
                
        except subprocess.TimeoutExpired:
            return TestResult(
                name=test_name,
                suite="vm",
                status="timeout",
                duration=900.0,
                output="",
                error="Test timed out after 15 minutes"
            )
            
    def run_vm_tests(self, tests: List[str]) -> TestSuite:
        """Run VM tests in parallel"""
        self.log(f"Running {len(tests)} VM tests (parallel: {self.parallel})")
        
        start_time = datetime.now()
        results = []
        
        with ThreadPoolExecutor(max_workers=self.parallel) as executor:
            # Submit all tests
            future_to_test = {
                executor.submit(self.run_vm_test, test): test 
                for test in tests
            }
            
            # Process results as they complete
            for future in as_completed(future_to_test):
                test_name = future_to_test[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    # Progress update
                    status_icon = "✓" if result.status == "passed" else "✗"
                    self.log(f"{status_icon} {test_name} ({result.duration:.1f}s)")
                    
                except Exception as e:
                    self.log(f"Error running {test_name}: {e}", "ERROR")
                    results.append(TestResult(
                        name=test_name,
                        suite="vm",
                        status="failed",
                        duration=0.0,
                        output="",
                        error=str(e)
                    ))
                    
        end_time = datetime.now()
        return TestSuite(
            name="vm",
            tests=results,
            start_time=start_time,
            end_time=end_time
        )
        
    def generate_report(self, output_format: str = "text", output_file: Optional[str] = None):
        """Generate test report in various formats"""
        
        # Calculate totals
        total_tests = sum(len(suite.tests) for suite in self.results)
        total_passed = sum(suite.passed for suite in self.results)
        total_failed = sum(suite.failed for suite in self.results)
        total_skipped = sum(suite.skipped for suite in self.results)
        total_duration = sum(suite.duration for suite in self.results)
        
        if output_format == "text":
            report = self._generate_text_report(
                total_tests, total_passed, total_failed, total_skipped, total_duration
            )
        elif output_format == "json":
            report = self._generate_json_report()
        elif output_format == "junit":
            report = self._generate_junit_report()
        else:
            raise ValueError(f"Unknown output format: {output_format}")
            
        # Write or print report
        if output_file:
            with open(output_file, 'w') as f:
                f.write(report)
            self.log(f"Report written to {output_file}")
        else:
            print(report)
            
    def _generate_text_report(self, total: int, passed: int, failed: int, 
                              skipped: int, duration: float) -> str:
        """Generate human-readable text report"""
        lines = [
            "=" * 80,
            "UNIFIED TEST REPORT",
            "=" * 80,
            f"Total Tests: {total}",
            f"Passed:      {passed} ({passed/total*100:.1f}%)" if total > 0 else "Passed: 0",
            f"Failed:      {failed}",
            f"Skipped:     {skipped}",
            f"Duration:    {duration:.1f}s",
            "",
        ]
        
        # Details by suite
        for suite in self.results:
            lines.extend([
                f"\n{suite.name.upper()} TESTS:",
                "-" * 40,
                f"Tests:    {len(suite.tests)}",
                f"Passed:   {suite.passed}",
                f"Failed:   {suite.failed}",
                f"Duration: {suite.duration:.1f}s",
            ])
            
            # Show failed tests
            failed_tests = [t for t in suite.tests if t.status == "failed"]
            if failed_tests:
                lines.append("\nFailed tests:")
                for test in failed_tests:
                    lines.append(f"  - {test.name} ({test.duration:.1f}s)")
                    if test.error and self.verbose:
                        lines.append(f"    Error: {test.error[:200]}...")
                        
        lines.append("\n" + "=" * 80)
        return "\n".join(lines)
        
    def _generate_json_report(self) -> str:
        """Generate JSON report"""
        report = {
            "summary": {
                "total_tests": sum(len(suite.tests) for suite in self.results),
                "passed": sum(suite.passed for suite in self.results),
                "failed": sum(suite.failed for suite in self.results),
                "skipped": sum(suite.skipped for suite in self.results),
                "duration": sum(suite.duration for suite in self.results),
                "timestamp": datetime.now().isoformat(),
            },
            "suites": [
                {
                    "name": suite.name,
                    "duration": suite.duration,
                    "tests": [asdict(test) for test in suite.tests]
                }
                for suite in self.results
            ]
        }
        return json.dumps(report, indent=2)
        
    def _generate_junit_report(self) -> str:
        """Generate JUnit XML report"""
        testsuites = ET.Element("testsuites")
        
        for suite in self.results:
            testsuite = ET.SubElement(testsuites, "testsuite")
            testsuite.set("name", suite.name)
            testsuite.set("tests", str(len(suite.tests)))
            testsuite.set("failures", str(suite.failed))
            testsuite.set("skipped", str(suite.skipped))
            testsuite.set("time", f"{suite.duration:.3f}")
            
            for test in suite.tests:
                testcase = ET.SubElement(testsuite, "testcase")
                testcase.set("name", test.name)
                testcase.set("classname", suite.name)
                testcase.set("time", f"{test.duration:.3f}")
                
                if test.status == "failed":
                    failure = ET.SubElement(testcase, "failure")
                    failure.set("message", test.error or "Test failed")
                    failure.text = test.output
                elif test.status == "skipped":
                    ET.SubElement(testcase, "skipped")
                    
        return ET.tostring(testsuites, encoding="unicode")
        
    def run_all(self, rust_categories: List[str], vm_tests: List[str]):
        """Run all specified tests"""
        self.log("Starting unified test execution")
        
        # Run Rust tests
        for category in rust_categories:
            suite = self.run_rust_tests(category)
            self.results.append(suite)
            self.log(f"Rust {category} tests: {suite.passed}/{len(suite.tests)} passed")
            
        # Run VM tests
        if vm_tests:
            suite = self.run_vm_tests(vm_tests)
            self.results.append(suite)
            self.log(f"VM tests: {suite.passed}/{len(suite.tests)} passed")
            
        # Summary
        total_passed = sum(s.passed for s in self.results)
        total_tests = sum(len(s.tests) for s in self.results)
        self.log(f"All tests complete: {total_passed}/{total_tests} passed")
        

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Unified test runner for Sinex project"
    )
    
    # Test selection
    parser.add_argument(
        "--rust",
        nargs="*",
        choices=["all", "unit", "integration", "property", "adversarial", "fast"],
        default=["fast"],
        help="Rust test categories to run"
    )
    parser.add_argument(
        "--vm",
        nargs="*",
        help="VM test names to run (e.g., basic-flow, multi-source)"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all tests (equivalent to --rust all --vm all)"
    )
    
    # Execution options
    parser.add_argument(
        "-p", "--parallel",
        type=int,
        default=1,
        help="Number of parallel VM tests to run"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output"
    )
    
    # Output options
    parser.add_argument(
        "-f", "--format",
        choices=["text", "json", "junit"],
        default="text",
        help="Output format for test report"
    )
    parser.add_argument(
        "-o", "--output",
        help="Output file for test report (default: stdout)"
    )
    
    args = parser.parse_args()
    
    # Handle --all flag
    if args.all:
        args.rust = ["all"]
        args.vm = ["basic-flow", "multi-source", "failure-recovery", "performance"]
        
    # Create runner
    runner = UnifiedTestRunner(verbose=args.verbose, parallel=args.parallel)
    
    # Run tests
    rust_categories = args.rust if args.rust else []
    vm_tests = args.vm if args.vm else []
    
    if not rust_categories and not vm_tests:
        parser.print_help()
        sys.exit(1)
        
    try:
        runner.run_all(rust_categories, vm_tests)
        runner.generate_report(args.format, args.output)
        
        # Exit with error if any tests failed
        total_failed = sum(s.failed for s in runner.results)
        sys.exit(1 if total_failed > 0 else 0)
        
    except KeyboardInterrupt:
        runner.log("Test execution interrupted", "WARN")
        sys.exit(1)
    except Exception as e:
        runner.log(f"Fatal error: {e}", "ERROR")
        sys.exit(1)


if __name__ == "__main__":
    main()
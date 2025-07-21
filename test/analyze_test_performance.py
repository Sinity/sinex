#!/usr/bin/env python3
"""
Test performance analysis script for Sinex test suite.

This script analyzes cargo test output to identify slow tests,
suggest parallelization opportunities, and generate performance reports.
"""

import re
import sys
import json
import argparse
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import subprocess


class TestResult:
    """Represents a single test result with timing information."""
    
    def __init__(self, name: str, duration_ms: float, status: str, module: str = ""):
        self.name = name
        self.duration_ms = duration_ms
        self.duration_s = duration_ms / 1000.0
        self.status = status
        self.module = module or self._extract_module(name)
    
    def _extract_module(self, name: str) -> str:
        """Extract module from test name."""
        parts = name.split("::")
        if len(parts) > 1:
            return "::".join(parts[:-1])
        return "unknown"
    
    def __repr__(self):
        return f"TestResult({self.name}, {self.duration_ms}ms, {self.status})"


class TestPerformanceAnalyzer:
    """Analyzes test performance from cargo test output."""
    
    # Regex patterns for parsing cargo test output
    TEST_RESULT_PATTERN = re.compile(
        r"test ([\w:]+(?:::[\w]+)*) \.\.\. (\w+)(?: \((\d+\.\d+)s\))?"
    )
    
    # Alternative pattern for millisecond timing
    TEST_RESULT_MS_PATTERN = re.compile(
        r"test ([\w:]+(?:::[\w]+)*) \.\.\. (\w+) \[(\d+)ms\]"
    )
    
    def __init__(self):
        self.test_results: List[TestResult] = []
        self.module_stats: Dict[str, Dict] = defaultdict(lambda: {
            "count": 0,
            "total_time": 0.0,
            "tests": []
        })
    
    def parse_cargo_output(self, output: str) -> None:
        """Parse cargo test output and extract test results."""
        lines = output.split('\n')
        
        for line in lines:
            # Try standard seconds pattern first
            match = self.TEST_RESULT_PATTERN.match(line.strip())
            if match:
                test_name = match.group(1)
                status = match.group(2)
                duration_str = match.group(3)
                
                if duration_str:
                    duration_ms = float(duration_str) * 1000
                else:
                    # If no duration, check for millisecond pattern
                    ms_match = self.TEST_RESULT_MS_PATTERN.match(line.strip())
                    if ms_match:
                        duration_ms = float(ms_match.group(3))
                    else:
                        duration_ms = 0.0
                
                result = TestResult(test_name, duration_ms, status)
                self.test_results.append(result)
                
                # Update module statistics
                module = result.module
                self.module_stats[module]["count"] += 1
                self.module_stats[module]["total_time"] += result.duration_s
                self.module_stats[module]["tests"].append(result)
    
    def analyze_slow_tests(self, threshold_ms: float = 1000) -> List[TestResult]:
        """Find tests slower than threshold."""
        slow_tests = [t for t in self.test_results if t.duration_ms > threshold_ms]
        return sorted(slow_tests, key=lambda t: t.duration_ms, reverse=True)
    
    def find_parallelization_opportunities(self) -> Dict[str, List[str]]:
        """Identify tests that could benefit from parallelization."""
        opportunities = defaultdict(list)
        
        # Find modules with many slow tests
        for module, stats in self.module_stats.items():
            if stats["count"] > 5 and stats["total_time"] > 5.0:
                slow_tests = [t for t in stats["tests"] if t.duration_ms > 500]
                if len(slow_tests) > 3:
                    opportunities["high_priority_modules"].append(module)
                    opportunities["reasons"].append(
                        f"{module}: {len(slow_tests)} slow tests, "
                        f"total time: {stats['total_time']:.2f}s"
                    )
        
        # Find test patterns that might benefit from batching
        db_tests = [t for t in self.test_results if "database" in t.name.lower()]
        if len(db_tests) > 10:
            avg_db_time = sum(t.duration_ms for t in db_tests) / len(db_tests)
            if avg_db_time > 200:
                opportunities["batch_candidates"].append("database_tests")
                opportunities["reasons"].append(
                    f"Database tests: {len(db_tests)} tests, "
                    f"avg time: {avg_db_time:.0f}ms"
                )
        
        # Find integration tests that could run in parallel
        integration_tests = [
            t for t in self.test_results 
            if "integration" in t.module or "system" in t.module
        ]
        if len(integration_tests) > 5:
            total_time = sum(t.duration_s for t in integration_tests)
            if total_time > 10.0:
                opportunities["parallel_candidates"].extend([
                    t.name for t in integration_tests if t.duration_ms > 2000
                ])
                opportunities["reasons"].append(
                    f"Integration tests: {len(integration_tests)} tests, "
                    f"total time: {total_time:.2f}s"
                )
        
        return dict(opportunities)
    
    def generate_report(self, output_format: str = "text") -> str:
        """Generate performance report."""
        if output_format == "json":
            return self._generate_json_report()
        else:
            return self._generate_text_report()
    
    def _generate_text_report(self) -> str:
        """Generate human-readable text report."""
        report = []
        report.append("=" * 80)
        report.append("TEST PERFORMANCE ANALYSIS REPORT")
        report.append("=" * 80)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # Overall statistics
        total_tests = len(self.test_results)
        total_time = sum(t.duration_s for t in self.test_results)
        avg_time = total_time / total_tests if total_tests > 0 else 0
        
        report.append("OVERALL STATISTICS:")
        report.append(f"  Total tests: {total_tests}")
        report.append(f"  Total time: {total_time:.2f}s")
        report.append(f"  Average time per test: {avg_time*1000:.0f}ms")
        report.append("")
        
        # Slowest tests
        slow_tests = self.analyze_slow_tests(threshold_ms=1000)
        if slow_tests:
            report.append("SLOWEST TESTS (>1s):")
            for i, test in enumerate(slow_tests[:10], 1):
                report.append(
                    f"  {i}. {test.name} - {test.duration_s:.2f}s"
                )
            report.append("")
        
        # Module performance
        report.append("MODULE PERFORMANCE:")
        sorted_modules = sorted(
            self.module_stats.items(),
            key=lambda x: x[1]["total_time"],
            reverse=True
        )
        for module, stats in sorted_modules[:10]:
            if stats["count"] > 0:
                avg_module_time = stats["total_time"] / stats["count"]
                report.append(
                    f"  {module}: {stats['count']} tests, "
                    f"total: {stats['total_time']:.2f}s, "
                    f"avg: {avg_module_time*1000:.0f}ms"
                )
        report.append("")
        
        # Parallelization opportunities
        opportunities = self.find_parallelization_opportunities()
        if opportunities.get("reasons"):
            report.append("PARALLELIZATION OPPORTUNITIES:")
            for reason in opportunities["reasons"]:
                report.append(f"  - {reason}")
            report.append("")
        
        # Recommendations
        report.append("RECOMMENDATIONS:")
        
        # Database pooling recommendation
        db_tests = [t for t in self.test_results if "database" in t.name.lower()]
        if db_tests and len(db_tests) > 20:
            report.append(
                "  1. Consider using shared database pools for test groups "
                "to reduce connection overhead"
            )
        
        # Slow test optimization
        very_slow = [t for t in self.test_results if t.duration_ms > 5000]
        if very_slow:
            report.append(
                f"  2. Optimize {len(very_slow)} tests taking >5s each - "
                "consider mocking external dependencies"
            )
        
        # Parallel execution
        if total_time > 60:
            potential_speedup = total_time / 4  # Assume 4 cores
            report.append(
                f"  3. Enable parallel test execution could reduce time "
                f"from {total_time:.0f}s to ~{potential_speedup:.0f}s"
            )
        
        report.append("")
        report.append("=" * 80)
        
        return "\n".join(report)
    
    def _generate_json_report(self) -> str:
        """Generate JSON report for programmatic consumption."""
        slow_tests = self.analyze_slow_tests(threshold_ms=1000)
        opportunities = self.find_parallelization_opportunities()
        
        report = {
            "metadata": {
                "generated": datetime.now().isoformat(),
                "total_tests": len(self.test_results),
                "total_time_seconds": sum(t.duration_s for t in self.test_results),
            },
            "slow_tests": [
                {
                    "name": t.name,
                    "duration_ms": t.duration_ms,
                    "module": t.module
                }
                for t in slow_tests[:20]
            ],
            "module_stats": {
                module: {
                    "test_count": stats["count"],
                    "total_time_seconds": stats["total_time"],
                    "average_time_ms": (stats["total_time"] / stats["count"] * 1000)
                    if stats["count"] > 0 else 0
                }
                for module, stats in self.module_stats.items()
            },
            "parallelization_opportunities": opportunities,
            "recommendations": self._generate_recommendations()
        }
        
        return json.dumps(report, indent=2)
    
    def _generate_recommendations(self) -> List[Dict[str, str]]:
        """Generate actionable recommendations."""
        recommendations = []
        
        # Check for database test optimization
        db_tests = [t for t in self.test_results if "database" in t.name.lower()]
        if db_tests and len(db_tests) > 20:
            avg_db_time = sum(t.duration_ms for t in db_tests) / len(db_tests)
            recommendations.append({
                "type": "database_pooling",
                "description": "Use shared database pools for test groups",
                "impact": f"Could save ~{avg_db_time * len(db_tests) * 0.3 / 1000:.1f}s",
                "priority": "high"
            })
        
        # Check for very slow tests
        very_slow = [t for t in self.test_results if t.duration_ms > 5000]
        if very_slow:
            recommendations.append({
                "type": "optimize_slow_tests",
                "description": f"Optimize {len(very_slow)} tests taking >5s",
                "impact": f"Could save ~{sum(t.duration_s for t in very_slow) * 0.5:.1f}s",
                "priority": "high"
            })
        
        # Check for parallelization potential
        total_time = sum(t.duration_s for t in self.test_results)
        if total_time > 30:
            recommendations.append({
                "type": "enable_parallel_execution",
                "description": "Enable parallel test execution with --test-threads",
                "impact": f"Could reduce time from {total_time:.0f}s to ~{total_time/4:.0f}s",
                "priority": "medium"
            })
        
        return recommendations


def run_cargo_test(args: List[str]) -> str:
    """Run cargo test and capture output."""
    cmd = ["cargo", "test"] + args + ["--", "--nocapture", "--test-threads=1"]
    
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    return result.stdout + result.stderr


def main():
    parser = argparse.ArgumentParser(
        description="Analyze test performance from cargo test output"
    )
    parser.add_argument(
        "--input", "-i",
        type=str,
        help="Input file with cargo test output (if not provided, runs cargo test)"
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        help="Output file for report"
    )
    parser.add_argument(
        "--format", "-f",
        choices=["text", "json"],
        default="text",
        help="Output format"
    )
    parser.add_argument(
        "--threshold", "-t",
        type=float,
        default=1000,
        help="Slow test threshold in milliseconds"
    )
    parser.add_argument(
        "cargo_args",
        nargs="*",
        help="Additional arguments to pass to cargo test"
    )
    
    args = parser.parse_args()
    
    # Get test output
    if args.input:
        with open(args.input, 'r') as f:
            output = f.read()
    else:
        output = run_cargo_test(args.cargo_args)
    
    # Analyze output
    analyzer = TestPerformanceAnalyzer()
    analyzer.parse_cargo_output(output)
    
    # Generate report
    report = analyzer.generate_report(args.format)
    
    # Output report
    if args.output:
        with open(args.output, 'w') as f:
            f.write(report)
        print(f"Report written to {args.output}")
    else:
        print(report)
    
    # Exit with non-zero if there are very slow tests
    slow_tests = analyzer.analyze_slow_tests(threshold_ms=5000)
    if slow_tests:
        sys.exit(1)


if __name__ == "__main__":
    main()
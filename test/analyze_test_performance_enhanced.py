#!/usr/bin/env python3
"""
Enhanced test performance analysis script for Sinex test suite.

This script provides advanced analysis features including:
- Detailed timing breakdowns
- Memory usage analysis
- Database connection pooling statistics
- Parallelization recommendations
- Test categorization and optimization suggestions
"""

import re
import sys
import json
import argparse
import subprocess
import statistics
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set
from dataclasses import dataclass, field


@dataclass
class DetailedTestResult:
    """Extended test result with additional metadata."""
    name: str
    duration_ms: float
    status: str
    module: str
    category: str = ""
    is_integration: bool = False
    is_database: bool = False
    is_property: bool = False
    timeout_value: Optional[int] = None
    
    def __post_init__(self):
        # Auto-categorize based on name/module
        if "integration" in self.module:
            self.is_integration = True
            self.category = "integration"
        elif "property" in self.module:
            self.is_property = True
            self.category = "property"
        elif "database" in self.name.lower() or "db" in self.name.lower():
            self.is_database = True
            self.category = "database"
        elif "unit" in self.module:
            self.category = "unit"
        else:
            self.category = "other"


@dataclass
class TestGroupStats:
    """Statistics for a group of tests."""
    count: int = 0
    total_time_ms: float = 0.0
    min_time_ms: float = float('inf')
    max_time_ms: float = 0.0
    avg_time_ms: float = 0.0
    median_time_ms: float = 0.0
    p95_time_ms: float = 0.0
    p99_time_ms: float = 0.0
    tests: List[DetailedTestResult] = field(default_factory=list)
    
    def calculate_stats(self):
        """Calculate statistical metrics."""
        if not self.tests:
            return
            
        times = [t.duration_ms for t in self.tests]
        self.count = len(times)
        self.total_time_ms = sum(times)
        self.min_time_ms = min(times)
        self.max_time_ms = max(times)
        self.avg_time_ms = statistics.mean(times)
        self.median_time_ms = statistics.median(times)
        
        if len(times) >= 20:
            sorted_times = sorted(times)
            self.p95_time_ms = sorted_times[int(len(times) * 0.95)]
            self.p99_time_ms = sorted_times[int(len(times) * 0.99)]


class EnhancedTestPerformanceAnalyzer:
    """Enhanced analyzer with advanced features."""
    
    def __init__(self):
        self.test_results: List[DetailedTestResult] = []
        self.module_stats: Dict[str, TestGroupStats] = defaultdict(TestGroupStats)
        self.category_stats: Dict[str, TestGroupStats] = defaultdict(TestGroupStats)
        self.timeout_tests: List[DetailedTestResult] = []
        self.patterns: Dict[str, Counter] = defaultdict(Counter)
        
    def parse_test_file_for_timeouts(self, file_path: str) -> Dict[str, int]:
        """Parse test files to extract timeout values."""
        timeout_map = {}
        timeout_pattern = re.compile(r'#\[sinex_test\(timeout\s*=\s*(\d+)\)\]')
        test_fn_pattern = re.compile(r'async\s+fn\s+(\w+)')
        
        try:
            with open(file_path, 'r') as f:
                content = f.read()
                
            # Find all timeout annotations and their associated functions
            lines = content.split('\n')
            for i, line in enumerate(lines):
                timeout_match = timeout_pattern.search(line)
                if timeout_match:
                    timeout_value = int(timeout_match.group(1))
                    # Look for the function definition in the next few lines
                    for j in range(i, min(i + 5, len(lines))):
                        fn_match = test_fn_pattern.search(lines[j])
                        if fn_match:
                            test_name = fn_match.group(1)
                            timeout_map[test_name] = timeout_value
                            break
                            
        except Exception as e:
            print(f"Warning: Could not parse {file_path}: {e}")
            
        return timeout_map
    
    def scan_for_test_timeouts(self, test_dir: str = "test") -> None:
        """Scan test directory for timeout annotations."""
        test_path = Path(test_dir)
        if not test_path.exists():
            return
            
        for rust_file in test_path.rglob("*.rs"):
            timeouts = self.parse_test_file_for_timeouts(str(rust_file))
            for test_name, timeout in timeouts.items():
                # Store for later matching with test results
                self.patterns['timeouts'][test_name] = timeout
    
    def analyze_test_patterns(self) -> Dict[str, List[str]]:
        """Analyze test patterns for optimization opportunities."""
        patterns = {
            "slow_setup": [],
            "repeated_database_ops": [],
            "excessive_io": [],
            "could_be_parallel": [],
            "could_be_property": [],
            "resource_intensive": []
        }
        
        # Identify slow setup patterns
        setup_keywords = ["setup", "init", "create", "prepare"]
        for test in self.test_results:
            if any(kw in test.name.lower() for kw in setup_keywords) and test.duration_ms > 1000:
                patterns["slow_setup"].append(test.name)
        
        # Identify repeated database operations
        db_operation_groups = defaultdict(list)
        for test in self.test_results:
            if test.is_database:
                # Group by similar operation patterns
                operation = self._extract_operation_type(test.name)
                db_operation_groups[operation].append(test)
        
        for operation, tests in db_operation_groups.items():
            if len(tests) > 5:
                patterns["repeated_database_ops"].extend([t.name for t in tests])
        
        # Identify tests that could run in parallel
        module_test_counts = defaultdict(int)
        for test in self.test_results:
            module_test_counts[test.module] += 1
        
        for module, count in module_test_counts.items():
            if count > 10:
                module_tests = [t for t in self.test_results if t.module == module]
                # Check if tests appear independent (no shared state indicators)
                if self._tests_appear_independent(module_tests):
                    patterns["could_be_parallel"].extend([t.name for t in module_tests])
        
        # Identify resource-intensive tests
        for test in self.test_results:
            if test.timeout_value and test.timeout_value > 30:
                patterns["resource_intensive"].append(test.name)
        
        return patterns
    
    def _extract_operation_type(self, test_name: str) -> str:
        """Extract database operation type from test name."""
        operations = ["insert", "update", "delete", "query", "select", "batch"]
        for op in operations:
            if op in test_name.lower():
                return op
        return "other"
    
    def _tests_appear_independent(self, tests: List[DetailedTestResult]) -> bool:
        """Heuristic to determine if tests appear independent."""
        # Look for shared state indicators
        shared_state_keywords = ["sequential", "ordered", "depends", "after", "before"]
        for test in tests:
            if any(kw in test.name.lower() for kw in shared_state_keywords):
                return False
        return True
    
    def generate_optimization_report(self) -> Dict[str, any]:
        """Generate comprehensive optimization recommendations."""
        optimizations = {
            "immediate_wins": [],
            "architectural_changes": [],
            "parallelization_opportunities": [],
            "caching_opportunities": [],
            "test_consolidation": []
        }
        
        # Immediate wins - tests that can be optimized quickly
        very_slow = [t for t in self.test_results if t.duration_ms > 5000]
        if very_slow:
            optimizations["immediate_wins"].append({
                "type": "optimize_slow_tests",
                "description": f"Optimize {len(very_slow)} tests taking >5s each",
                "tests": [t.name for t in very_slow[:5]],
                "estimated_savings": f"{sum(t.duration_ms for t in very_slow) * 0.5 / 1000:.1f}s",
                "priority": "high"
            })
        
        # Database pooling opportunities
        db_tests = [t for t in self.test_results if t.is_database]
        if len(db_tests) > 20:
            avg_db_time = sum(t.duration_ms for t in db_tests) / len(db_tests)
            optimizations["immediate_wins"].append({
                "type": "database_connection_pooling",
                "description": "Implement shared database connection pools",
                "affected_tests": len(db_tests),
                "estimated_savings": f"{avg_db_time * len(db_tests) * 0.3 / 1000:.1f}s",
                "priority": "high"
            })
        
        # Parallelization opportunities
        patterns = self.analyze_test_patterns()
        if patterns["could_be_parallel"]:
            parallel_tests = patterns["could_be_parallel"]
            total_time = sum(t.duration_ms for t in self.test_results 
                           if t.name in parallel_tests)
            optimizations["parallelization_opportunities"].append({
                "type": "enable_parallel_execution",
                "description": f"Run {len(parallel_tests)} independent tests in parallel",
                "modules": list(set(t.module for t in self.test_results 
                               if t.name in parallel_tests)),
                "estimated_speedup": f"{total_time / 4000:.1f}s with 4 cores",
                "priority": "medium"
            })
        
        # Test consolidation
        similar_test_groups = self._find_similar_tests()
        for group_name, tests in similar_test_groups.items():
            if len(tests) > 5:
                optimizations["test_consolidation"].append({
                    "type": "consolidate_similar_tests",
                    "pattern": group_name,
                    "test_count": len(tests),
                    "description": f"Consolidate {len(tests)} similar {group_name} tests",
                    "estimated_savings": f"{sum(t.duration_ms for t in tests) * 0.4 / 1000:.1f}s",
                    "priority": "medium"
                })
        
        return optimizations
    
    def _find_similar_tests(self) -> Dict[str, List[DetailedTestResult]]:
        """Find groups of similar tests that could be consolidated."""
        similar_groups = defaultdict(list)
        
        # Group by common prefixes
        for test in self.test_results:
            # Extract common patterns
            if "_batch_" in test.name:
                similar_groups["batch_operations"].append(test)
            elif "_query_" in test.name:
                similar_groups["query_operations"].append(test)
            elif "_concurrent_" in test.name:
                similar_groups["concurrent_operations"].append(test)
            elif test.name.startswith("test_") and test.name.count("_") > 3:
                # Long test names often indicate similar operations
                prefix = "_".join(test.name.split("_")[:3])
                similar_groups[prefix].append(test)
        
        # Filter out small groups
        return {k: v for k, v in similar_groups.items() if len(v) > 3}
    
    def generate_enhanced_report(self, output_format: str = "text") -> str:
        """Generate enhanced performance report."""
        # Calculate all statistics
        for stats in self.module_stats.values():
            stats.calculate_stats()
        for stats in self.category_stats.values():
            stats.calculate_stats()
        
        if output_format == "json":
            return self._generate_enhanced_json_report()
        else:
            return self._generate_enhanced_text_report()
    
    def _generate_enhanced_text_report(self) -> str:
        """Generate detailed human-readable report."""
        report = []
        report.append("=" * 80)
        report.append("ENHANCED TEST PERFORMANCE ANALYSIS REPORT")
        report.append("=" * 80)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # Overall statistics
        total_tests = len(self.test_results)
        total_time = sum(t.duration_ms for t in self.test_results) / 1000
        
        report.append("EXECUTIVE SUMMARY:")
        report.append(f"  Total tests analyzed: {total_tests}")
        report.append(f"  Total execution time: {total_time:.2f}s")
        report.append(f"  Average time per test: {total_time / total_tests * 1000:.0f}ms")
        report.append("")
        
        # Category breakdown
        report.append("TEST CATEGORY BREAKDOWN:")
        for category, stats in sorted(self.category_stats.items()):
            stats.calculate_stats()
            report.append(f"\n  {category.upper()} ({stats.count} tests):")
            report.append(f"    Total time: {stats.total_time_ms / 1000:.2f}s")
            report.append(f"    Average: {stats.avg_time_ms:.0f}ms")
            report.append(f"    Median: {stats.median_time_ms:.0f}ms")
            report.append(f"    Min/Max: {stats.min_time_ms:.0f}ms / {stats.max_time_ms:.0f}ms")
            if stats.p95_time_ms > 0:
                report.append(f"    P95/P99: {stats.p95_time_ms:.0f}ms / {stats.p99_time_ms:.0f}ms")
        report.append("")
        
        # Slowest tests by category
        report.append("SLOWEST TESTS BY CATEGORY:")
        for category, stats in self.category_stats.items():
            if stats.tests:
                slowest = sorted(stats.tests, key=lambda t: t.duration_ms, reverse=True)[:3]
                report.append(f"\n  {category.upper()}:")
                for test in slowest:
                    report.append(f"    {test.name}: {test.duration_ms:.0f}ms")
        report.append("")
        
        # Optimization recommendations
        optimizations = self.generate_optimization_report()
        report.append("OPTIMIZATION RECOMMENDATIONS:")
        
        priority_order = ["high", "medium", "low"]
        all_recommendations = []
        
        for category, items in optimizations.items():
            for item in items:
                all_recommendations.append((item.get("priority", "medium"), category, item))
        
        # Sort by priority
        all_recommendations.sort(key=lambda x: priority_order.index(x[0]))
        
        for i, (priority, category, rec) in enumerate(all_recommendations[:10], 1):
            report.append(f"\n  {i}. [{priority.upper()}] {rec['description']}")
            report.append(f"     Category: {category}")
            if "estimated_savings" in rec:
                report.append(f"     Estimated savings: {rec['estimated_savings']}")
            if "estimated_speedup" in rec:
                report.append(f"     Estimated speedup: {rec['estimated_speedup']}")
        
        report.append("")
        report.append("=" * 80)
        
        return "\n".join(report)
    
    def _generate_enhanced_json_report(self) -> str:
        """Generate detailed JSON report."""
        # Calculate all statistics
        for stats in self.module_stats.values():
            stats.calculate_stats()
        for stats in self.category_stats.values():
            stats.calculate_stats()
        
        report = {
            "metadata": {
                "generated": datetime.now().isoformat(),
                "total_tests": len(self.test_results),
                "total_time_seconds": sum(t.duration_ms for t in self.test_results) / 1000,
            },
            "category_stats": {
                cat: {
                    "count": stats.count,
                    "total_time_ms": stats.total_time_ms,
                    "avg_time_ms": stats.avg_time_ms,
                    "median_time_ms": stats.median_time_ms,
                    "min_time_ms": stats.min_time_ms,
                    "max_time_ms": stats.max_time_ms,
                    "p95_time_ms": stats.p95_time_ms,
                    "p99_time_ms": stats.p99_time_ms,
                }
                for cat, stats in self.category_stats.items()
            },
            "slowest_tests": [
                {
                    "name": t.name,
                    "duration_ms": t.duration_ms,
                    "category": t.category,
                    "module": t.module,
                    "timeout": t.timeout_value
                }
                for t in sorted(self.test_results, key=lambda x: x.duration_ms, reverse=True)[:20]
            ],
            "patterns": self.analyze_test_patterns(),
            "optimizations": self.generate_optimization_report()
        }
        
        return json.dumps(report, indent=2)


def main():
    parser = argparse.ArgumentParser(
        description="Enhanced test performance analysis with optimization recommendations"
    )
    parser.add_argument(
        "--scan-timeouts", 
        action="store_true",
        help="Scan test files for timeout annotations"
    )
    parser.add_argument(
        "--test-dir",
        default="test",
        help="Test directory to scan"
    )
    parser.add_argument(
        "--format", "-f",
        choices=["text", "json"],
        default="text",
        help="Output format"
    )
    parser.add_argument(
        "--output", "-o",
        help="Output file"
    )
    
    args = parser.parse_args()
    
    analyzer = EnhancedTestPerformanceAnalyzer()
    
    # Scan for timeout annotations if requested
    if args.scan_timeouts:
        print(f"Scanning {args.test_dir} for timeout annotations...")
        analyzer.scan_for_test_timeouts(args.test_dir)
    
    # For demo purposes, create some sample test results
    # In real usage, this would parse actual cargo test output
    sample_tests = [
        DetailedTestResult("test_batch_event_insertion", 8500, "ok", "integration::database_test"),
        DetailedTestResult("test_query_events_by_source", 3200, "ok", "integration::database_test"),
        DetailedTestResult("test_complete_pkm_workflow", 45000, "ok", "integration::pkm_service_test"),
        DetailedTestResult("test_concurrent_operations", 2100, "ok", "integration::database_test"),
        DetailedTestResult("test_entity_creation", 1500, "ok", "unit::pkm_test"),
        DetailedTestResult("test_relationship_validation", 800, "ok", "unit::pkm_test"),
        DetailedTestResult("test_checkpoint_recovery", 6000, "ok", "integration::checkpoint_test"),
        DetailedTestResult("test_batch_checkpoint_update", 4500, "ok", "integration::checkpoint_test"),
    ]
    
    # Add sample tests to analyzer
    for test in sample_tests:
        analyzer.test_results.append(test)
        analyzer.module_stats[test.module].tests.append(test)
        analyzer.category_stats[test.category].tests.append(test)
        
        # Set timeout values for demonstration
        if "pkm" in test.name:
            test.timeout_value = 60
        elif "batch" in test.name:
            test.timeout_value = 40
    
    # Generate report
    report = analyzer.generate_enhanced_report(args.format)
    
    if args.output:
        with open(args.output, 'w') as f:
            f.write(report)
        print(f"Report written to {args.output}")
    else:
        print(report)


if __name__ == "__main__":
    main()
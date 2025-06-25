#!/usr/bin/env python3
"""
Sinex Test Suite Health & Improvement Analyzer

This script uses `ast-grep` as a powerful code analysis engine to gather
structured data about the test suite. It then analyzes this data to generate
a high-level report on performance, reliability, maintainability, and quality,
complete with actionable insights.
"""
import subprocess
import json
import sys
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Any

class TestSuiteAnalyzer:
    def __init__(self, test_dir: Path):
        self.project_root = self._find_project_root(test_dir)
        self.test_dir = test_dir
        self.rules_file = self.project_root / "rules" / "test_analysis.yml"
        self.results: Dict[str, List[Dict]] = defaultdict(list)

    def _find_project_root(self, start_path: Path) -> Path:
        current = start_path.resolve()
        while not (current / "Cargo.toml").exists():
            if current.parent == current:
                raise FileNotFoundError("Could not find project root (Cargo.toml).")
            current = current.parent
        return current

    def run_analysis_pass(self, rule_id: str):
        """Run ast-grep for a specific rule and store the JSON results."""
        print(f"  Analyzing: {rule_id}...")
        command = [
            "ast-grep", "scan", "-r", str(self.rules_file),
            "--filter", rule_id, str(self.test_dir),
            "--json=stream"
        ]
        try:
            process = subprocess.run(command, capture_output=True, text=True, check=False)
            if process.returncode > 1: # 0=no match, 1=match found. >1 is error.
                print(f"    ⚠️  Warning: ast-grep exited with code {process.returncode} for rule '{rule_id}'")
                print(process.stderr)
                return

            for line in process.stdout.strip().split('\n'):
                if line:
                    try:
                        self.results[rule_id].append(json.loads(line))
                    except json.JSONDecodeError:
                        print(f"    ⚠️  Warning: Could not parse JSON line for rule '{rule_id}': {line[:100]}")
        except FileNotFoundError:
            print("❌ Error: `ast-grep` not found. Please install it and ensure it's in your PATH.")
            sys.exit(1)

    def run_all_analyses(self):
        """Run all analysis passes defined in the rules file."""
        print("🚀 Starting Test Suite Analysis...")
        if not self.rules_file.exists():
            print(f"❌ Error: Analysis rule file not found at {self.rules_file}")
            sys.exit(1)

        rule_ids = [
            "find-sleep-calls", "find-unwraps", "find-expects", "find-test-functions",
            "find-long-test-functions", "find-tests-without-assertions",
            "find-assertions-without-messages", "find-total-assertions",
            "find-test-categories"
        ]

        for rule_id in rule_ids:
            self.run_analysis_pass(rule_id)
        
        print("✅ Analysis data collection complete.")

    def generate_report(self):
        """Generate a structured, actionable report from the analysis results."""
        print("\n" + "="*80)
        print("🔬 Sinex Test Suite Health & Improvement Report")
        print("="*80)

        # 1. Performance Analysis
        print("\n🚀 1. Performance & Reliability Analysis")
        print("-" * 40)
        sleeps = self.results["find-sleep-calls"]
        unwraps = self.results["find-unwraps"]
        expects = self.results["find-expects"]
        print(f"  - 🔴 Critical `sleep` calls found: {len(sleeps)} (Major source of flakiness and slowness)")
        print(f"  - 🟠 `.unwrap()` calls found: {len(unwraps)} (Risk of test panics)")
        print(f"  - 🟡 `.expect()` calls found: {len(expects)} (Better, but check messages)")
        if sleeps:
            print("    Top 3 files with sleeps:")
            self._print_top_files("find-sleep-calls", 3)

        # 2. Maintainability Analysis
        print("\n🛠️ 2. Maintainability & Complexity Analysis")
        print("-" * 40)
        all_tests = self.results["find-test-functions"]
        long_tests = self.results["find-long-test-functions"]
        total_lines = self._get_total_loc(self.test_dir)
        print(f"  - Total test files found: {len(self._get_file_set('find-test-functions'))}")
        print(f"  - Total test functions: {len(all_tests)}")
        print(f"  - Total lines of test code: ~{total_lines}")
        print(f"  - 🔴 Long test functions (>10 statements): {len(long_tests)}")
        if long_tests:
            print("    Top 3 longest/most complex tests (by statement count):")
            self._print_top_files("find-long-test-functions", 3)

        # 3. Quality & Coverage Analysis
        print("\n✅ 3. Test Quality & Coverage Analysis")
        print("-" * 40)
        no_assert_tests = self.results["find-tests-without-assertions"]
        no_msg_asserts = self.results["find-assertions-without-messages"]
        total_asserts = self.results["find-total-assertions"]
        print(f"  - Total assertions found: {len(total_asserts)}")
        print(f"  - 🟠 Tests with NO assertions: {len(no_assert_tests)} (Only check for panics)")
        print(f"  - 🟡 Assertions without descriptive messages: {len(no_msg_asserts)}")
        if no_assert_tests:
            print("    Top 3 files with assertion-less tests:")
            self._print_top_files("find-tests-without-assertions", 3)

        # 4. Architectural Analysis
        print("\n🏛️ 4. Architectural Analysis (Test Pyramid)")
        print("-" * 40)
        category_counts = self._get_category_counts()
        total_files = sum(category_counts.values())
        print(f"  - Total test files analyzed: {total_files}")
        for category, count in sorted(category_counts.items()):
            percentage = (count / total_files) * 100 if total_files > 0 else 0
            print(f"    - {category:<15}: {count:>3} files ({percentage:.1f}%)")
        if category_counts.get("integration", 0) > category_counts.get("unit", 0):
            print("  - ⚠️  Architectural Concern: Inverted test pyramid detected (more integration than unit tests).")

        # 5. Actionable Summary
        self._print_actionable_summary(sleeps, unwraps, long_tests, no_assert_tests)


    def _print_top_files(self, rule_id: str, top_n: int):
        file_counts = defaultdict(int)
        for match in self.results[rule_id]:
            file_counts[match.get('file', 'unknown')] += 1
        
        sorted_files = sorted(file_counts.items(), key=lambda item: item[1], reverse=True)
        for file, count in sorted_files[:top_n]:
            print(f"      - {Path(file).name}: {count} instance(s)")

    def _get_file_set(self, rule_id: str) -> set:
        return {match.get('file') for match in self.results[rule_id]}

    def _get_category_counts(self) -> Dict[str, int]:
        counts = defaultdict(int)
        for match in self.results["find-test-categories"]:
            path = match.get('text', '')
            if 'unit/' in path: counts['unit'] += 1
            elif 'integration/' in path: counts['integration'] += 1
            elif 'system/' in path: counts['system'] += 1
            elif 'property/' in path: counts['property'] += 1
            elif 'adversarial/' in path: counts['adversarial'] += 1
        return counts

    def _get_total_loc(self, path: Path) -> int:
        total = 0
        for p in path.rglob("*.rs"):
            if p.is_file():
                with open(p, 'r', errors='ignore') as f:
                    total += len(f.readlines())
        return total

    def _print_actionable_summary(self, sleeps, unwraps, long_tests, no_asserts):
        print("\n" + "="*80)
        print("🎯 Actionable Improvement Summary")
        print("="*80)
        print("Based on this analysis, here are the highest-impact areas for refactoring:")
        print(f"\n1. 🚀 **Performance & Reliability (Highest Priority):**")
        print(f"   - **Action:** Systematically replace {len(sleeps)} `sleep` calls with deterministic waits.")
        print(f"   - **Impact:** Drastically reduces test suite runtime and eliminates major source of flakiness.")
        
        print(f"\n2. 🛡️ **Error Handling Robustness:**")
        print(f"   - **Action:** Address {len(unwraps)} `.unwrap()` calls to prevent test panics.")
        print(f"   - **Impact:** Increases test reliability and forces more explicit testing of error paths.")

        print(f"\n3. ⚙️ **Maintainability & Readability:**")
        print(f"   - **Action:** Refactor {len(long_tests)} identified long/complex tests into smaller, focused units.")
        print(f"   - **Impact:** Lowers the cognitive load for maintaining tests and makes them easier to understand.")

        print(f"\n4. ✅ **Test Quality:**")
        print(f"   - **Action:** Add assertions to {len(no_asserts)} tests that currently only check for panics.")
        print(f"   - **Impact:** Ensures tests are actually verifying behavior, not just execution.")

if __name__ == "__main__":
    analyzer = TestSuiteAnalyzer(Path("test"))
    analyzer.run_all_analyses()
    analyzer.generate_report()

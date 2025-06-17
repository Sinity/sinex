#!/usr/bin/env python3
"""
Enhanced Test Anti-Pattern Analysis Script

Systematically identifies low-value tests and provides nuanced recommendations:
- REMOVE: Tests with no business value 
- IMPROVE: Tests that can be made more valuable with assertions
- MOVE: Tests that belong in different test suites
- DOCUMENT: Tests with exploratory/documentary value

Uses both regex patterns and ast-grep for sophisticated analysis.
"""

import os
import re
import subprocess
import json
from pathlib import Path
from typing import List, Dict, Set, Tuple
from dataclasses import dataclass
from collections import defaultdict

@dataclass
class TestIssue:
    file_path: str
    test_name: str
    line_number: int
    issue_type: str
    description: str
    confidence: str  # "high", "medium", "low"
    action: str  # "remove", "improve", "move", "document"
    suggestion: str
    test_content: str = ""  # Full test content
    improvement_suggestion: str = ""  # Specific improvement ideas

class TestAntiPatternAnalyzer:
    def __init__(self, test_dir: str):
        self.test_dir = Path(test_dir)
        self.issues: List[TestIssue] = []
        
    def analyze_all(self) -> List[TestIssue]:
        """Run all analysis methods and return found issues"""
        print("🔍 Starting comprehensive test anti-pattern analysis...")
        print("🎯 Looking for: removal candidates, improvement opportunities, and relocation needs")
        
        # Get all test files
        test_files = list(self.test_dir.rglob("*.rs"))
        print(f"📁 Found {len(test_files)} test files to analyze")
        
        # Run all analysis methods
        self.find_mock_in_assert_mock_out(test_files)
        self.find_constant_assertions(test_files)
        self.find_trivial_constructors(test_files)
        self.find_basic_crud_tests(test_files)
        self.find_library_tests(test_files)
        self.find_serialize_deserialize_round_trips(test_files)
        self.find_no_assertion_tests(test_files)
        self.find_duplicate_test_logic(test_files)
        
        # Sort issues by action priority, then file
        action_priority = {"remove": 0, "improve": 1, "move": 2, "document": 3}
        self.issues.sort(key=lambda x: (action_priority.get(x.action, 4), x.file_path, x.line_number))
        
        return self.issues
    
    def find_mock_in_assert_mock_out(self, test_files: List[Path]):
        """Find tests that create data, pass it through a function, assert it comes back unchanged"""
        print("🔎 Looking for mock-in/assert-mock-out patterns...")
        
        for file_path in test_files:
            content = file_path.read_text()
            test_functions = self._extract_test_functions(content)
            
            for test_name, test_body, start_line in test_functions:
                # Check for various mock-in/assert-mock-out patterns
                is_mock_pattern = False
                description = ""
                
                # Pattern 1: Config assignment
                if (re.search(r'let\s+\w+\s*=.*json!\(', test_body) and
                    re.search(r'\.new\([^)]*\.clone\(\)\)', test_body) and
                    re.search(r'assert_eq!\([^,]+\.config,', test_body)):
                    is_mock_pattern = True
                    description = "Creates JSON config, passes to constructor, asserts config field equals original"
                
                # Pattern 2: Builder setter verification
                elif (re.search(r'\.with_\w+\([^)]+\)', test_body) and
                      len(re.findall(r'assert_eq!\([^,]+\.\w+,', test_body)) >= 2):
                    is_mock_pattern = True
                    description = "Builder pattern test that mainly verifies setters set fields"
                
                # Pattern 3: Simple field assignment
                elif (re.search(r'let\s+\w+\s*=.*\.new\(', test_body) and
                      re.search(r'assert_eq!\(\w+\.\w+,\s*\w+\)', test_body) and
                      len(test_body.split('\n')) < 15):  # Short test
                    is_mock_pattern = True
                    description = "Simple test that just verifies field assignment works"
                
                if is_mock_pattern:
                    self.issues.append(TestIssue(
                        file_path=str(file_path.relative_to(self.test_dir)),
                        test_name=test_name,
                        line_number=start_line,
                        issue_type="mock_in_assert_mock_out",
                        description=description,
                        confidence="high",
                        action="remove",
                        suggestion="Remove test - it's testing that assignment works, not business logic",
                        test_content=test_body.strip(),
                        improvement_suggestion="N/A - test has no business value"
                    ))
    
    def find_constant_assertions(self, test_files: List[Path]):
        """Find tests that assert constants equal their literal values"""
        print("🔎 Looking for constant assertion patterns...")
        
        for file_path in test_files:
            content = file_path.read_text()
            test_functions = self._extract_test_functions(content)
            
            for test_name, test_body, start_line in test_functions:
                # Look for constant assertion patterns
                constant_patterns = [
                    r'assert_eq!\([A-Z_]+::[A-Z_]+,\s*"[^"]+"\)',
                    r'assert_eq!\("([^"]+)",\s*[A-Z_]+::[A-Z_]+\)',
                    r'assert_eq!\((\w+),\s*\1\)',
                ]
                
                for pattern in constant_patterns:
                    if re.search(pattern, test_body):
                        self.issues.append(TestIssue(
                            file_path=str(file_path.relative_to(self.test_dir)),
                            test_name=test_name,
                            line_number=start_line,
                            issue_type="constant_assertion",
                            description="Test asserts that constants equal their literal values",
                            confidence="high",
                            action="remove",
                            suggestion="Remove test - constants don't need to be tested",
                            test_content=test_body.strip(),
                            improvement_suggestion="N/A - constants are compile-time checked"
                        ))
                        break  # One pattern match is enough
    
    def find_trivial_constructors(self, test_files: List[Path]):
        """Find tests that just verify constructors don't panic"""
        print("🔎 Looking for trivial constructor tests...")
        
        for file_path in test_files:
            content = file_path.read_text()
            test_functions = self._extract_test_functions(content)
            
            for test_name, test_body, start_line in test_functions:
                # Check if test just calls new() without meaningful assertions
                if (re.search(r'\.new\(\)', test_body) and 
                    len(re.findall(r'assert', test_body)) <= 1 and
                    len(test_body.strip().split('\n')) <= 8 and
                    not re.search(r'(complex|integration|setup|config)', test_name.lower())):
                    
                    self.issues.append(TestIssue(
                        file_path=str(file_path.relative_to(self.test_dir)),
                        test_name=test_name,
                        line_number=start_line,
                        issue_type="trivial_constructor",
                        description="Test just verifies constructor doesn't panic",
                        confidence="high",
                        action="remove",
                        suggestion="Remove test - constructor panics are caught by other tests",
                        test_content=test_body.strip(),
                        improvement_suggestion="N/A - basic constructor functionality doesn't need testing"
                    ))
    
    def find_basic_crud_tests(self, test_files: List[Path]):
        """Find tests that just verify basic database CRUD operations work"""
        print("🔎 Looking for basic CRUD test patterns...")
        
        for file_path in test_files:
            content = file_path.read_text()
            test_functions = self._extract_test_functions(content)
            
            for test_name, test_body, start_line in test_functions:
                # Look for simple insert/retrieve patterns
                if (re.search(r'insert.*\(.*\)', test_body) and
                    re.search(r'get.*by_id', test_body) and
                    re.search(r'assert_eq!\([^,]+\.id,', test_body) and
                    len(test_body.split('\n')) < 20):
                    
                    self.issues.append(TestIssue(
                        file_path=str(file_path.relative_to(self.test_dir)),
                        test_name=test_name,
                        line_number=start_line,
                        issue_type="basic_crud",
                        description="Test just verifies basic database insert/retrieve works",
                        confidence="medium",
                        action="remove",
                        suggestion="Remove test - tests database functionality, not business logic",
                        test_content=test_body.strip(),
                        improvement_suggestion="Focus on testing business rules and complex queries instead"
                    ))
    
    def find_library_tests(self, test_files: List[Path]):
        """Find tests that verify third-party library functionality"""
        print("🔎 Looking for library functionality tests...")
        
        library_indicators = [
            ("Ulid::", "ULID library"),
            ("serde_json::", "Serde JSON library"),
            ("chrono::", "Chrono datetime library"),
        ]
        
        for file_path in test_files:
            content = file_path.read_text()
            test_functions = self._extract_test_functions(content)
            
            for test_name, test_body, start_line in test_functions:
                for indicator, lib_name in library_indicators:
                    # If test body is mostly about the library and not our business logic
                    if (indicator in test_body and 
                        test_body.count(indicator) > 1 and
                        not re.search(r'sinex_|RawEvent|EventSource', test_body)):
                        
                        self.issues.append(TestIssue(
                            file_path=str(file_path.relative_to(self.test_dir)),
                            test_name=test_name,
                            line_number=start_line,
                            issue_type="library_test",
                            description=f"Tests {lib_name} functionality rather than Sinex logic",
                            confidence="medium",
                            action="remove",
                            suggestion=f"Remove test - {lib_name} has its own tests",
                            test_content=test_body.strip(),
                            improvement_suggestion=f"Focus on testing how Sinex uses {lib_name}, not {lib_name} itself"
                        ))
                        break
    
    def find_serialize_deserialize_round_trips(self, test_files: List[Path]):
        """Find tests that serialize then deserialize and assert equality"""
        print("🔎 Looking for serialize/deserialize round-trip tests...")
        
        for file_path in test_files:
            content = file_path.read_text()
            test_functions = self._extract_test_functions(content)
            
            for test_name, test_body, start_line in test_functions:
                # Look for round-trip patterns
                round_trip_patterns = [
                    r'to_string.*from_str.*assert_eq!',
                    r'serde_json::to_.*serde_json::from_.*assert_eq!',
                    r'\.clone\(\).*assert_eq!\(.*,.*\.clone\(\)\)',
                ]
                
                for pattern in round_trip_patterns:
                    if re.search(pattern, test_body, re.DOTALL):
                        self.issues.append(TestIssue(
                            file_path=str(file_path.relative_to(self.test_dir)),
                            test_name=test_name,
                            line_number=start_line,
                            issue_type="round_trip",
                            description="Test serializes then deserializes and asserts equality",
                            confidence="medium",
                            action="improve",
                            suggestion="Consider removing or improving to test business logic",
                            test_content=test_body.strip(),
                            improvement_suggestion="Test business constraints during serialization, not just round-trip equality"
                        ))
                        break
    
    def find_no_assertion_tests(self, test_files: List[Path]):
        """Find tests with no assertions and categorize them by value"""
        print("🔎 Looking for tests without assertions...")
        
        for file_path in test_files:
            content = file_path.read_text()
            test_functions = self._extract_test_functions(content)
            
            for test_name, test_body, start_line in test_functions:
                # Count assertions
                assertion_count = len(re.findall(r'assert[_!]', test_body))
                
                if assertion_count == 0:
                    # Analyze the test to determine its value and what action to take
                    action, confidence, suggestion, improvement = self._analyze_no_assertion_test(
                        test_name, test_body, str(file_path)
                    )
                    
                    self.issues.append(TestIssue(
                        file_path=str(file_path.relative_to(self.test_dir)),
                        test_name=test_name,
                        line_number=start_line,
                        issue_type="no_assertions",
                        description="Test has no assertions - only checks that code doesn't panic",
                        confidence=confidence,
                        action=action,
                        suggestion=suggestion,
                        test_content=test_body.strip(),
                        improvement_suggestion=improvement
                    ))
    
    def _analyze_no_assertion_test(self, test_name: str, test_body: str, file_path: str) -> Tuple[str, str, str, str]:
        """Analyze a test with no assertions to determine appropriate action"""
        
        # Check for trivial tests that should be removed
        if (len(test_body.strip().split('\n')) <= 5 and
            re.search(r'\.new\(\)', test_body) and
            not re.search(r'(complex|integration|setup|initialize)', test_name.lower())):
            return ("remove", "high", "Remove - trivial constructor test with no value", 
                   "N/A - test has no business value")
        
        # Check for potentially valuable exploratory/documentation tests
        if (re.search(r'(dst|timezone|edge|boundary|explore|attack|stress)', test_name.lower()) or
            "println!" in test_body or "eprintln!" in test_body or
            re.search(r'(time|clock|date)', test_body.lower())):
            
            improvement = self._suggest_assertions_for_exploratory_test(test_name, test_body)
            return ("improve", "medium", "Add assertions to make test meaningful", improvement)
        
        # Check for integration/setup tests
        if (re.search(r'(integration|setup|initialize|connection|config)', test_name.lower()) or
            re.search(r'(pool|database|connect)', test_body.lower())):
            return ("improve", "medium", "Add assertions to verify setup succeeded", 
                   "Add assertions like: assert!(connection.is_ok()), assert!(!result.is_empty())")
        
        # Check for performance/stress tests
        if (re.search(r'(performance|stress|load|concurrent|parallel)', test_name.lower()) or
            "thread::" in test_body or "tokio::" in test_body):
            return ("move", "low", "Consider moving to performance test suite", 
                   "Move to performance tests where 'doesn't crash' is the main property")
        
        # Default: likely should be improved
        return ("improve", "medium", "Add meaningful assertions or document why none are needed",
               "Add assertions to verify expected behavior, or mark as #[ignore] if exploratory")
    
    def _suggest_assertions_for_exploratory_test(self, test_name: str, test_body: str) -> str:
        """Suggest specific assertions for exploratory tests"""
        suggestions = []
        
        if "ulid" in test_body.lower():
            suggestions.append("assert!(ulid.to_string().len() == 26)")
            suggestions.append("assert!(ulid.timestamp() <= Utc::now())")
        
        if "time" in test_body.lower() or "duration" in test_body.lower():
            suggestions.append("assert!(time_diff.abs() < Duration::hours(2))")
            suggestions.append("assert!(result_time.is_some())")
        
        if "event" in test_body.lower():
            suggestions.append("assert!(event.id.is_valid())")
            suggestions.append("assert!(!event.source.is_empty())")
        
        if "config" in test_body.lower():
            suggestions.append("assert!(config.is_valid())")
            suggestions.append("assert!(!config.enabled_events.is_empty())")
        
        if suggestions:
            return "Consider adding: " + "; ".join(suggestions)
        else:
            return "Add assertions that verify the expected behavior or document edge cases found"
    
    def find_duplicate_test_logic(self, test_files: List[Path]):
        """Find tests with very similar logic that might be redundant"""
        print("🔎 Looking for duplicate test logic...")
        
        # This is a simplified approach - could be enhanced with more sophisticated similarity detection
        test_signatures = defaultdict(list)
        
        for file_path in test_files:
            content = file_path.read_text()
            test_functions = self._extract_test_functions(content)
            
            for test_name, test_body, start_line in test_functions:
                # Create a simplified signature of the test
                simplified = re.sub(r'\s+', ' ', test_body)
                simplified = re.sub(r'"[^"]*"', '""', simplified)  # Replace string literals
                simplified = re.sub(r'\d+', '0', simplified)  # Replace numbers
                
                test_signatures[simplified].append((str(file_path.relative_to(self.test_dir)), test_name, test_body, start_line))
        
        # Find duplicates
        for signature, tests in test_signatures.items():
            if len(tests) > 1 and len(signature.strip()) > 50:  # Only flag substantial duplicates
                for file_path, test_name, test_body, start_line in tests[1:]:  # Skip first one
                    self.issues.append(TestIssue(
                        file_path=file_path,
                        test_name=test_name,
                        line_number=start_line,
                        issue_type="duplicate_logic",
                        description=f"Test logic very similar to {tests[0][1]} in {tests[0][0]}",
                        confidence="low",
                        action="improve",
                        suggestion="Consider consolidating duplicate tests",
                        test_content=test_body.strip(),
                        improvement_suggestion="Merge with similar test or differentiate by testing different aspects"
                    ))
    
    def _extract_test_functions(self, content: str) -> List[Tuple[str, str, int]]:
        """Extract all test functions from file content"""
        functions = []
        
        # Find all test functions with their full bodies - improved regex
        lines = content.split('\n')
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            # Look for #[test] annotation
            if line.startswith('#[test]') or line == '#[test]':
                # Find the function declaration
                j = i + 1
                while j < len(lines) and not re.search(r'fn\s+\w+', lines[j]):
                    j += 1
                
                if j < len(lines):
                    fn_match = re.search(r'fn\s+(\w+)', lines[j])
                    if fn_match:
                        test_name = fn_match.group(1)
                        
                        # Find the opening brace
                        brace_line = j
                        while brace_line < len(lines) and '{' not in lines[brace_line]:
                            brace_line += 1
                        
                        if brace_line < len(lines):
                            # Find matching closing brace
                            brace_count = 0
                            start_line = i + 1  # Line number (1-indexed)
                            body_lines = []
                            
                            for k in range(brace_line, len(lines)):
                                line_content = lines[k]
                                body_lines.append(line_content)
                                
                                # Count braces
                                brace_count += line_content.count('{') - line_content.count('}')
                                
                                # If we've closed all braces, we're done
                                if brace_count == 0:
                                    # Extract just the function body (inside the braces)
                                    full_body = '\n'.join(body_lines)
                                    if '{' in full_body and '}' in full_body:
                                        # Extract content between first { and last }
                                        start_idx = full_body.find('{') + 1
                                        end_idx = full_body.rfind('}')
                                        test_body = full_body[start_idx:end_idx].strip()
                                        functions.append((test_name, test_body, start_line))
                                    break
                            
                            i = k + 1
                        else:
                            i = j + 1
                    else:
                        i = j + 1
                else:
                    i += 1
            else:
                i += 1
        
        return functions
    
    def generate_report(self) -> str:
        """Generate a comprehensive, verbose report of all issues found"""
        if not self.issues:
            return "✅ No test anti-patterns found!"
        
        report = [
            "🧪 COMPREHENSIVE TEST ANTI-PATTERN ANALYSIS REPORT",
            "=" * 60,
            f"Found {len(self.issues)} potential issues in {len(set(i.file_path for i in self.issues))} files\n"
        ]
        
        # Group by action type for better organization
        by_action = defaultdict(list)
        for issue in self.issues:
            by_action[issue.action].append(issue)
        
        # 1. REMOVAL CANDIDATES (show full content)
        if by_action["remove"]:
            report.extend([
                "\n🗑️  REMOVAL CANDIDATES",
                "=" * 40,
                f"These {len(by_action['remove'])} tests should be removed entirely:\n"
            ])
            
            for i, issue in enumerate(by_action["remove"], 1):
                report.extend([
                    f"#{i}. 📁 {issue.file_path}:{issue.line_number}",
                    f"    🧪 Test: {issue.test_name}",
                    f"    📝 Issue: {issue.description}",
                    f"    🎯 Reason: {issue.suggestion}",
                    f"    📊 Confidence: {issue.confidence.upper()}",
                    "",
                    "    💻 FULL TEST CONTENT:",
                    "    " + "─" * 50
                ])
                
                # Show full test content with line numbers
                content_lines = issue.test_content.split('\n')
                for line_num, line in enumerate(content_lines, 1):
                    report.append(f"    {line_num:3d} │ {line}")
                
                report.extend([
                    "    " + "─" * 50,
                    ""
                ])
        
        # 2. IMPROVEMENT CANDIDATES (with specific suggestions)
        if by_action["improve"]:
            report.extend([
                "\n🔧 IMPROVEMENT CANDIDATES", 
                "=" * 40,
                f"These {len(by_action['improve'])} tests can be made more valuable:\n"
            ])
            
            for i, issue in enumerate(by_action["improve"], 1):
                report.extend([
                    f"#{i}. 📁 {issue.file_path}:{issue.line_number}",
                    f"    🧪 Test: {issue.test_name}",
                    f"    📝 Issue: {issue.description}",
                    f"    💡 Suggestion: {issue.improvement_suggestion}",
                    f"    📊 Confidence: {issue.confidence.upper()}",
                ])
                
                # Show test snippet for context (first 10 lines)
                content_lines = issue.test_content.split('\n')[:10]
                if content_lines:
                    report.append("    📄 Test Preview:")
                    for line_num, line in enumerate(content_lines, 1):
                        report.append(f"    {line_num:3d} │ {line[:80]}{'...' if len(line) > 80 else ''}")
                    if len(issue.test_content.split('\n')) > 10:
                        report.append(f"    ... (and {len(issue.test_content.split('\n')) - 10} more lines)")
                
                report.append("")
        
        # 3. RELOCATION CANDIDATES  
        if by_action["move"]:
            report.extend([
                "\n📦 RELOCATION CANDIDATES",
                "=" * 40, 
                f"These {len(by_action['move'])} tests might belong elsewhere:\n"
            ])
            
            for i, issue in enumerate(by_action["move"], 1):
                report.extend([
                    f"#{i}. 📁 {issue.file_path}:{issue.line_number}",
                    f"    🧪 Test: {issue.test_name}",
                    f"    📝 Issue: {issue.description}",
                    f"    🎯 Suggestion: {issue.improvement_suggestion}",
                    f"    📊 Confidence: {issue.confidence.upper()}",
                    ""
                ])
        
        # 4. DOCUMENTATION CANDIDATES
        if by_action["document"]:
            report.extend([
                "\n📚 DOCUMENTATION CANDIDATES",
                "=" * 40,
                f"These {len(by_action['document'])} tests have documentary value:\n"
            ])
            
            for i, issue in enumerate(by_action["document"], 1):
                report.extend([
                    f"#{i}. 📁 {issue.file_path}:{issue.line_number}",
                    f"    🧪 Test: {issue.test_name}",
                    f"    📝 Value: {issue.description}",
                    f"    💡 Suggestion: {issue.improvement_suggestion}",
                    ""
                ])
        
        # Statistics by issue type
        report.extend([
            "\n📊 ANALYSIS STATISTICS",
            "=" * 30
        ])
        
        by_type = defaultdict(list)
        for issue in self.issues:
            by_type[issue.issue_type].append(issue)
        
        for issue_type, issues in sorted(by_type.items()):
            by_action_type = defaultdict(int)
            for issue in issues:
                by_action_type[issue.action] += 1
            
            action_summary = ", ".join([f"{action}: {count}" for action, count in sorted(by_action_type.items())])
            report.append(f"• {issue_type.replace('_', ' ').title()}: {len(issues)} ({action_summary})")
        
        # Overall recommendations
        report.extend([
            "\n🎯 SUMMARY RECOMMENDATIONS",
            "=" * 35,
            f"🗑️  REMOVE: {len(by_action['remove'])} tests with no business value",
            f"🔧 IMPROVE: {len(by_action['improve'])} tests that can be made more valuable", 
            f"📦 RELOCATE: {len(by_action['move'])} tests that belong in different test suites",
            f"📚 DOCUMENT: {len(by_action['document'])} tests with documentary value",
            "",
            "Priority order:",
            "1. Remove trivial tests (high confidence removals)",
            "2. Improve valuable tests by adding meaningful assertions", 
            "3. Consider relocating tests to appropriate test suites",
            "4. Document exploratory tests or mark them as #[ignore]",
            "",
            f"Total cleanup potential: {len(by_action['remove'])} removals + {len(by_action['improve'])} improvements",
            f"This will improve test quality while preserving {len(by_action['improve']) + len(by_action['move']) + len(by_action['document'])} potentially valuable tests."
        ])
        
        return "\n".join(report)

def main():
    analyzer = TestAntiPatternAnalyzer("/realm/project/sinex/test")
    issues = analyzer.analyze_all()
    
    print(f"\n{analyzer.generate_report()}")
    
    # Save detailed results to JSON
    output_file = "/realm/project/sinex/test_antipattern_analysis_detailed.json"
    with open(output_file, 'w') as f:
        json.dump([{
            'file_path': i.file_path,
            'test_name': i.test_name, 
            'line_number': i.line_number,
            'issue_type': i.issue_type,
            'description': i.description,
            'confidence': i.confidence,
            'action': i.action,
            'suggestion': i.suggestion,
            'test_content': i.test_content,
            'improvement_suggestion': i.improvement_suggestion
        } for i in issues], f, indent=2)
    
    print(f"\n💾 Detailed analysis saved to: {output_file}")
    print(f"📈 Found {len([i for i in issues if i.action == 'remove'])} removal candidates and {len([i for i in issues if i.action == 'improve'])} improvement opportunities")

if __name__ == "__main__":
    main()
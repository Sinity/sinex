//! Coverage assurance utilities to ensure test streamlining doesn't reduce scope

use crate::common::prelude::*;
use std::collections::{HashSet, HashMap};
use std::sync::{Arc, Mutex};
use once_cell::sync::Lazy;

/// Global test coverage tracker
static COVERAGE_TRACKER: Lazy<Arc<Mutex<CoverageTracker>>> = Lazy::new(|| {
    Arc::new(Mutex::new(CoverageTracker::new()))
});

/// Track what aspects of the system are being tested
#[derive(Debug, Default)]
pub struct CoverageTracker {
    /// Track which event types have been tested
    tested_event_types: HashSet<(String, String)>, // (source, event_type)
    
    /// Track which validation rules have been exercised
    validation_rules_tested: HashSet<String>,
    
    /// Track which error conditions have been tested
    error_conditions_tested: HashSet<String>,
    
    /// Track which concurrent scenarios have been tested
    concurrency_scenarios: HashSet<String>,
    
    /// Track database operations tested
    db_operations: HashSet<String>,
    
    /// Track edge cases covered
    edge_cases: HashMap<String, Vec<String>>,
    
    /// Track performance scenarios
    performance_scenarios: HashSet<String>,
    
    /// Track integration points tested
    integration_points: HashSet<String>,
}

impl CoverageTracker {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn record_event_type_tested(source: &str, event_type: &str) {
        let mut tracker = COVERAGE_TRACKER.lock().unwrap();
        tracker.tested_event_types.insert((source.to_string(), event_type.to_string()));
    }
    
    pub fn record_validation_rule(rule: &str) {
        let mut tracker = COVERAGE_TRACKER.lock().unwrap();
        tracker.validation_rules_tested.insert(rule.to_string());
    }
    
    pub fn record_error_condition(condition: &str) {
        let mut tracker = COVERAGE_TRACKER.lock().unwrap();
        tracker.error_conditions_tested.insert(condition.to_string());
    }
    
    pub fn record_concurrency_scenario(scenario: &str) {
        let mut tracker = COVERAGE_TRACKER.lock().unwrap();
        tracker.concurrency_scenarios.insert(scenario.to_string());
    }
    
    pub fn record_edge_case(category: &str, case: &str) {
        let mut tracker = COVERAGE_TRACKER.lock().unwrap();
        tracker.edge_cases
            .entry(category.to_string())
            .or_insert_with(Vec::new)
            .push(case.to_string());
    }
    
    pub fn record_db_operation(operation: &str) {
        let mut tracker = COVERAGE_TRACKER.lock().unwrap();
        tracker.db_operations.insert(operation.to_string());
    }
    
    pub fn get_coverage_report() -> CoverageReport {
        let tracker = COVERAGE_TRACKER.lock().unwrap();
        CoverageReport {
            event_types_count: tracker.tested_event_types.len(),
            validation_rules_count: tracker.validation_rules_tested.len(),
            error_conditions_count: tracker.error_conditions_tested.len(),
            concurrency_scenarios_count: tracker.concurrency_scenarios.len(),
            db_operations_count: tracker.db_operations.len(),
            edge_case_categories: tracker.edge_cases.len(),
            total_edge_cases: tracker.edge_cases.values().map(|v| v.len()).sum(),
            details: CoverageDetails {
                event_types: tracker.tested_event_types.clone(),
                validation_rules: tracker.validation_rules_tested.clone(),
                error_conditions: tracker.error_conditions_tested.clone(),
                concurrency_scenarios: tracker.concurrency_scenarios.clone(),
                edge_cases: tracker.edge_cases.clone(),
            }
        }
    }
    
    pub fn reset() {
        let mut tracker = COVERAGE_TRACKER.lock().unwrap();
        *tracker = CoverageTracker::new();
    }
}

#[derive(Debug)]
pub struct CoverageReport {
    pub event_types_count: usize,
    pub validation_rules_count: usize,
    pub error_conditions_count: usize,
    pub concurrency_scenarios_count: usize,
    pub db_operations_count: usize,
    pub edge_case_categories: usize,
    pub total_edge_cases: usize,
    pub details: CoverageDetails,
}

#[derive(Debug)]
pub struct CoverageDetails {
    pub event_types: HashSet<(String, String)>,
    pub validation_rules: HashSet<String>,
    pub error_conditions: HashSet<String>,
    pub concurrency_scenarios: HashSet<String>,
    pub edge_cases: HashMap<String, Vec<String>>,
}

/// Ensure that streamlined tests cover at least the same scope as original tests
pub struct CoverageAssertion {
    expected_minimums: CoverageMinimums,
}

#[derive(Default)]
pub struct CoverageMinimums {
    pub event_types: usize,
    pub validation_rules: usize,
    pub error_conditions: usize,
    pub concurrency_scenarios: usize,
    pub edge_cases: usize,
}

impl CoverageAssertion {
    pub fn new() -> Self {
        Self {
            expected_minimums: CoverageMinimums::default(),
        }
    }
    
    pub fn expect_event_types(mut self, min: usize) -> Self {
        self.expected_minimums.event_types = min;
        self
    }
    
    pub fn expect_validation_rules(mut self, min: usize) -> Self {
        self.expected_minimums.validation_rules = min;
        self
    }
    
    pub fn expect_error_conditions(mut self, min: usize) -> Self {
        self.expected_minimums.error_conditions = min;
        self
    }
    
    pub fn expect_concurrency_scenarios(mut self, min: usize) -> Self {
        self.expected_minimums.concurrency_scenarios = min;
        self
    }
    
    pub fn expect_edge_cases(mut self, min: usize) -> Self {
        self.expected_minimums.edge_cases = min;
        self
    }
    
    pub fn assert_coverage_maintained(&self) {
        let report = CoverageTracker::get_coverage_report();
        
        assert!(
            report.event_types_count >= self.expected_minimums.event_types,
            "Event type coverage decreased: {} < {} expected",
            report.event_types_count,
            self.expected_minimums.event_types
        );
        
        assert!(
            report.validation_rules_count >= self.expected_minimums.validation_rules,
            "Validation rule coverage decreased: {} < {} expected",
            report.validation_rules_count,
            self.expected_minimums.validation_rules
        );
        
        assert!(
            report.error_conditions_count >= self.expected_minimums.error_conditions,
            "Error condition coverage decreased: {} < {} expected",
            report.error_conditions_count,
            self.expected_minimums.error_conditions
        );
        
        assert!(
            report.concurrency_scenarios_count >= self.expected_minimums.concurrency_scenarios,
            "Concurrency scenario coverage decreased: {} < {} expected",
            report.concurrency_scenarios_count,
            self.expected_minimums.concurrency_scenarios
        );
        
        assert!(
            report.total_edge_cases >= self.expected_minimums.edge_cases,
            "Edge case coverage decreased: {} < {} expected",
            report.total_edge_cases,
            self.expected_minimums.edge_cases
        );
    }
}

/// Macro to ensure test coverage is tracked
#[macro_export]
macro_rules! track_test_coverage {
    (event_type: $source:expr, $event_type:expr) => {
        $crate::common::coverage_assurance::CoverageTracker::record_event_type_tested($source, $event_type);
    };
    
    (validation_rule: $rule:expr) => {
        $crate::common::coverage_assurance::CoverageTracker::record_validation_rule($rule);
    };
    
    (error_condition: $condition:expr) => {
        $crate::common::coverage_assurance::CoverageTracker::record_error_condition($condition);
    };
    
    (concurrency: $scenario:expr) => {
        $crate::common::coverage_assurance::CoverageTracker::record_concurrency_scenario($scenario);
    };
    
    (edge_case: $category:expr, $case:expr) => {
        $crate::common::coverage_assurance::CoverageTracker::record_edge_case($category, $case);
    };
}

/// Test coverage comparison tool
pub struct CoverageComparison {
    before: CoverageSnapshot,
    after: CoverageSnapshot,
}

#[derive(Debug, Clone)]
pub struct CoverageSnapshot {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub test_count: usize,
    pub line_count: usize,
    pub scenarios_covered: HashSet<String>,
    pub assertions_made: usize,
}

impl CoverageComparison {
    pub fn compare(before: CoverageSnapshot, after: CoverageSnapshot) -> ComparisonResult {
        ComparisonResult {
            test_count_change: after.test_count as i32 - before.test_count as i32,
            line_count_change: after.line_count as i32 - before.line_count as i32,
            scenarios_added: after.scenarios_covered.difference(&before.scenarios_covered).cloned().collect(),
            scenarios_removed: before.scenarios_covered.difference(&after.scenarios_covered).cloned().collect(),
            assertion_density_before: before.assertions_made as f64 / before.line_count as f64,
            assertion_density_after: after.assertions_made as f64 / after.line_count as f64,
        }
    }
}

pub struct ComparisonResult {
    pub test_count_change: i32,
    pub line_count_change: i32,
    pub scenarios_added: HashSet<String>,
    pub scenarios_removed: HashSet<String>,
    pub assertion_density_before: f64,
    pub assertion_density_after: f64,
}

impl ComparisonResult {
    pub fn print_summary(&self) {
        println!("=== Test Coverage Comparison ===");
        println!("Test count change: {:+}", self.test_count_change);
        println!("Line count change: {:+} ({:.1}% reduction)", 
                 self.line_count_change,
                 (self.line_count_change as f64 / 100.0).abs());
        println!("Assertion density: {:.2} → {:.2} ({:+.1}%)",
                 self.assertion_density_before,
                 self.assertion_density_after,
                 ((self.assertion_density_after - self.assertion_density_before) / self.assertion_density_before * 100.0));
        
        if !self.scenarios_removed.is_empty() {
            println!("⚠️  Scenarios removed: {:?}", self.scenarios_removed);
        }
        
        if !self.scenarios_added.is_empty() {
            println!("✅ Scenarios added: {:?}", self.scenarios_added);
        }
    }
}

/// Property-based test coverage analyzer
pub struct PropertyCoverage {
    properties_tested: HashMap<String, usize>, // property name -> number of cases
}

impl PropertyCoverage {
    pub fn new() -> Self {
        Self {
            properties_tested: HashMap::new(),
        }
    }
    
    pub fn record_property(&mut self, property: &str, cases: usize) {
        *self.properties_tested.entry(property.to_string()).or_insert(0) += cases;
    }
    
    pub fn ensure_minimum_cases(&self, property: &str, min_cases: usize) -> bool {
        self.properties_tested.get(property).copied().unwrap_or(0) >= min_cases
    }
}
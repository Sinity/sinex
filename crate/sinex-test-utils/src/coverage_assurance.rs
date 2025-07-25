// Coverage assurance utilities to ensure test streamlining doesn't reduce scope
//
// This module provides comprehensive test coverage tracking to ensure that
// test migrations and streamlining efforts don't inadvertently reduce the
// scope of testing. It tracks various dimensions of test coverage including
// event types, validation rules, error conditions, and concurrency scenarios.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};

/// Global test coverage tracker
static COVERAGE_TRACKER: OnceLock<Arc<Mutex<CoverageTracker>>> = OnceLock::new();

fn get_coverage_tracker() -> &'static Arc<Mutex<CoverageTracker>> {
    COVERAGE_TRACKER.get_or_init(|| Arc::new(Mutex::new(CoverageTracker::new())))
}

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
        let mut tracker = get_coverage_tracker().lock().unwrap();
        tracker
            .tested_event_types
            .insert((source.to_string(), event_type.to_string()));
    }

    pub fn record_validation_rule(rule: &str) {
        let mut tracker = get_coverage_tracker().lock().unwrap();
        tracker.validation_rules_tested.insert(rule.to_string());
    }

    pub fn record_error_condition(condition: &str) {
        let mut tracker = get_coverage_tracker().lock().unwrap();
        tracker
            .error_conditions_tested
            .insert(condition.to_string());
    }

    pub fn record_concurrency_scenario(scenario: &str) {
        let mut tracker = get_coverage_tracker().lock().unwrap();
        tracker.concurrency_scenarios.insert(scenario.to_string());
    }

    pub fn record_edge_case(category: &str, case: &str) {
        let mut tracker = get_coverage_tracker().lock().unwrap();
        tracker
            .edge_cases
            .entry(category.to_string())
            .or_default()
            .push(case.to_string());
    }

    pub fn record_db_operation(operation: &str) {
        let mut tracker = get_coverage_tracker().lock().unwrap();
        tracker.db_operations.insert(operation.to_string());
    }

    pub fn get_coverage_report() -> CoverageReport {
        let tracker = get_coverage_tracker().lock().unwrap();
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
            },
        }
    }

    pub fn reset() {
        let mut tracker = get_coverage_tracker().lock().unwrap();
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
        $crate::coverage_assurance::CoverageTracker::record_event_type_tested(
            $source,
            $event_type,
        );
    };

    (validation_rule: $rule:expr) => {
        $crate::coverage_assurance::CoverageTracker::record_validation_rule($rule);
    };

    (error_condition: $condition:expr) => {
        $crate::coverage_assurance::CoverageTracker::record_error_condition($condition);
    };

    (concurrency: $scenario:expr) => {
        $crate::coverage_assurance::CoverageTracker::record_concurrency_scenario($scenario);
    };

    (edge_case: $category:expr, $case:expr) => {
        $crate::coverage_assurance::CoverageTracker::record_edge_case($category, $case);
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
            scenarios_added: after
                .scenarios_covered
                .difference(&before.scenarios_covered)
                .cloned()
                .collect(),
            scenarios_removed: before
                .scenarios_covered
                .difference(&after.scenarios_covered)
                .cloned()
                .collect(),
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

        let reduction_pct = if self.line_count_change < 0 {
            (self.line_count_change.abs() as f64 / 100.0) * 100.0
        } else {
            0.0
        };
        println!(
            "Line count change: {:+} ({:.1}% reduction)",
            self.line_count_change, reduction_pct
        );

        let density_change_pct = if self.assertion_density_before != 0.0 {
            (self.assertion_density_after - self.assertion_density_before)
                / self.assertion_density_before
                * 100.0
        } else {
            0.0
        };
        println!(
            "Assertion density: {:.2} → {:.2} ({:+.1}%)",
            self.assertion_density_before, self.assertion_density_after, density_change_pct
        );

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
        *self
            .properties_tested
            .entry(property.to_string())
            .or_insert(0) += cases;
    }

    /// Get all tested properties
    pub fn get_tested_properties(&self) -> &HashMap<String, usize> {
        &self.properties_tested
    }

    /// Get total test cases across all properties
    pub fn get_total_cases(&self) -> usize {
        self.properties_tested.values().sum()
    }

    pub fn ensure_minimum_cases(&self, property: &str, min_cases: usize) -> bool {
        self.properties_tested.get(property).copied().unwrap_or(0) >= min_cases
    }
}

// Comprehensive coverage assurance tests
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    
    #[test]
    fn test_coverage_tracker_creation() {
        let tracker = CoverageTracker::new();
        
        assert!(tracker.tested_event_types.is_empty());
        assert!(tracker.validation_rules_tested.is_empty());
        assert!(tracker.error_conditions_tested.is_empty());
        assert!(tracker.concurrency_scenarios.is_empty());
        assert!(tracker.db_operations.is_empty());
        assert!(tracker.edge_cases.is_empty());
        assert!(tracker.performance_scenarios.is_empty());
        assert!(tracker.integration_points.is_empty());
    }
    
    #[test]
    fn test_event_type_tracking() {
        CoverageTracker::record_event_type_tested("filesystem", "file.created");
        CoverageTracker::record_event_type_tested("terminal", "command.executed");
        CoverageTracker::record_event_type_tested("filesystem", "file.created"); // Duplicate
        
        let report = CoverageTracker::get_coverage_report();
        assert!(report.event_types_count >= 2);
        assert!(report.details.event_types.contains(&("filesystem".to_string(), "file.created".to_string())));
    }
    
    #[test]
    fn test_validation_rule_tracking() {
        CoverageTracker::record_validation_rule("non_empty_source");
        CoverageTracker::record_validation_rule("valid_event_type");
        CoverageTracker::record_validation_rule("json_schema_valid");
        
        let report = CoverageTracker::get_coverage_report();
        assert!(report.validation_rules_count >= 3);
        assert!(report.details.validation_rules.contains("non_empty_source"));
    }
    
    #[test]
    fn test_error_condition_tracking() {
        CoverageTracker::record_error_condition("database_connection_failed");
        CoverageTracker::record_error_condition("redis_timeout");
        CoverageTracker::record_error_condition("permission_denied");
        
        let report = CoverageTracker::get_coverage_report();
        assert!(report.error_conditions_count >= 3);
        assert!(report.details.error_conditions.contains("redis_timeout"));
    }
    
    #[test]
    fn test_concurrency_scenario_tracking() {
        CoverageTracker::record_concurrency_scenario("multiple_writers");
        CoverageTracker::record_concurrency_scenario("reader_writer_lock");
        CoverageTracker::record_concurrency_scenario("thundering_herd");
        
        let report = CoverageTracker::get_coverage_report();
        assert!(report.concurrency_scenarios_count >= 3);
        assert!(report.details.concurrency_scenarios.contains("thundering_herd"));
    }
    
    #[test]
    fn test_edge_case_tracking() {
        CoverageTracker::record_edge_case("string_handling", "empty_string");
        CoverageTracker::record_edge_case("string_handling", "unicode_characters");
        CoverageTracker::record_edge_case("numeric", "max_value");
        CoverageTracker::record_edge_case("numeric", "min_value");
        
        let report = CoverageTracker::get_coverage_report();
        assert!(report.edge_case_categories >= 2);
        assert!(report.total_edge_cases >= 4);
        
        let string_cases = report.details.edge_cases.get("string_handling").unwrap();
        assert!(string_cases.contains(&"empty_string".to_string()));
        assert!(string_cases.contains(&"unicode_characters".to_string()));
    }
    
    #[test]
    fn test_db_operation_tracking() {
        CoverageTracker::record_db_operation("insert_event");
        CoverageTracker::record_db_operation("batch_insert");
        CoverageTracker::record_db_operation("query_by_time_range");
        
        let report = CoverageTracker::get_coverage_report();
        assert!(report.db_operations_count >= 3);
    }
    
    #[test]
    fn test_performance_scenario_tracking() {
        CoverageTracker::record_performance_scenario("high_throughput_ingestion");
        CoverageTracker::record_performance_scenario("concurrent_queries");
        
        let report = CoverageTracker::get_coverage_report();
        assert!(report.performance_scenarios_count >= 2);
    }
    
    #[test]
    fn test_integration_point_tracking() {
        CoverageTracker::record_integration_point("grpc_interface");
        CoverageTracker::record_integration_point("redis_pubsub");
        
        let report = CoverageTracker::get_coverage_report();
        assert!(report.integration_points_count >= 2);
    }
    
    #[test]
    fn test_coverage_report_generation() {
        // Clear and add specific test data
        CoverageTracker::record_event_type_tested("test", "test.event");
        CoverageTracker::record_validation_rule("test_rule");
        CoverageTracker::record_error_condition("test_error");
        CoverageTracker::record_concurrency_scenario("test_scenario");
        CoverageTracker::record_edge_case("test_category", "test_case");
        CoverageTracker::record_db_operation("test_operation");
        CoverageTracker::record_performance_scenario("test_perf");
        CoverageTracker::record_integration_point("test_integration");
        
        let report = CoverageTracker::get_coverage_report();
        
        // Verify all counts are at least 1
        assert!(report.event_types_count >= 1);
        assert!(report.validation_rules_count >= 1);
        assert!(report.error_conditions_count >= 1);
        assert!(report.concurrency_scenarios_count >= 1);
        assert!(report.edge_case_categories >= 1);
        assert!(report.total_edge_cases >= 1);
        assert!(report.db_operations_count >= 1);
        assert!(report.performance_scenarios_count >= 1);
        assert!(report.integration_points_count >= 1);
    }
    
    #[test]
    fn test_coverage_assertions() {
        // Set up minimum coverage requirements
        let min_coverage = TestCoverageRequirements {
            event_types: 10,
            validation_rules: 5,
            error_conditions: 8,
            concurrency_scenarios: 3,
            edge_cases: 15,
            db_operations: 10,
            performance_scenarios: 2,
            integration_points: 5,
        };
        
        // Add some test coverage
        for i in 0..12 {
            CoverageTracker::record_event_type_tested(&format!("source_{}", i), "test.event");
        }
        
        for i in 0..6 {
            CoverageTracker::record_validation_rule(&format!("rule_{}", i));
        }
        
        // Check assertions
        let report = CoverageTracker::get_coverage_report();
        assert!(report.event_types_count >= min_coverage.event_types);
        assert!(report.validation_rules_count >= min_coverage.validation_rules);
    }
    
    #[test]
    fn test_coverage_tracker_singleton() {
        // Multiple calls should use the same global instance
        CoverageTracker::record_event_type_tested("singleton", "test");
        CoverageTracker::record_event_type_tested("singleton", "test2");
        
        let report1 = CoverageTracker::get_coverage_report();
        let report2 = CoverageTracker::get_coverage_report();
        
        // Both reports should reflect the same data
        assert_eq!(report1.event_types_count, report2.event_types_count);
    }
    
    #[test]
    fn test_category_group_assurance() {
        let mut assurance = CategoryGroupAssurance::new();
        
        // Track different categories
        assurance.record_category_coverage("authentication", &["login", "logout", "refresh"]);
        assurance.record_category_coverage("authorization", &["rbac", "permissions"]);
        assurance.record_category_coverage("data_validation", &["schema", "constraints"]);
        
        // Check coverage
        assert!(assurance.ensure_category_covered("authentication"));
        assert!(assurance.ensure_category_covered("authorization"));
        assert!(!assurance.ensure_category_covered("non_existent"));
        
        // Check minimum subcategories
        assert!(assurance.ensure_minimum_subcategories("authentication", 3));
        assert!(!assurance.ensure_minimum_subcategories("authorization", 3));
        
        // Get uncovered categories
        let required = vec!["authentication", "authorization", "encryption", "audit"];
        let uncovered = assurance.get_uncovered_categories(&required);
        assert_eq!(uncovered, vec!["encryption", "audit"]);
    }
    
    #[test]
    fn test_concurrency_test_coverage() {
        let mut coverage = ConcurrencyTestCoverage::new();
        
        // Record various concurrency patterns
        coverage.record_concurrency_pattern("reader_writer", 5);
        coverage.record_concurrency_pattern("producer_consumer", 3);
        coverage.record_concurrency_pattern("worker_pool", 10);
        
        // Record race conditions
        coverage.record_race_condition_test("counter_increment");
        coverage.record_race_condition_test("cache_update");
        
        // Record stress test
        coverage.record_stress_test(1000, Duration::from_secs(60));
        
        // Verify tracking
        assert!(coverage.patterns_tested.contains_key("reader_writer"));
        assert_eq!(*coverage.patterns_tested.get("worker_pool").unwrap(), 10);
        assert_eq!(coverage.race_conditions_tested.len(), 2);
        assert!(coverage.stress_test_performed);
    }
    
    #[test]
    fn test_property_test_coverage() {
        let mut coverage = PropertyTestCoverage::new();
        
        // Record property tests
        coverage.record_property_tested("event_id_uniqueness", 1000);
        coverage.record_property_tested("timestamp_ordering", 500);
        coverage.record_property_tested("payload_validation", 2000);
        
        // Check coverage
        assert_eq!(coverage.get_total_cases(), 3500);
        assert!(coverage.ensure_minimum_cases("event_id_uniqueness", 900));
        assert!(!coverage.ensure_minimum_cases("timestamp_ordering", 1000));
        
        let properties = coverage.get_tested_properties();
        assert_eq!(properties.len(), 3);
        assert_eq!(*properties.get("payload_validation").unwrap(), 2000);
    }
    
    #[test]
    fn test_edge_case_tracking_detail() {
        // Test multiple edge cases per category
        CoverageTracker::record_edge_case("time", "epoch_start");
        CoverageTracker::record_edge_case("time", "year_2038");
        CoverageTracker::record_edge_case("time", "leap_second");
        CoverageTracker::record_edge_case("time", "dst_transition");
        
        let report = CoverageTracker::get_coverage_report();
        let time_cases = report.details.edge_cases.get("time").unwrap();
        assert_eq!(time_cases.len(), 4);
        assert!(time_cases.contains(&"leap_second".to_string()));
    }
    
    #[test]
    fn test_coverage_completeness_check() {
        // Simulate comprehensive test coverage
        let sources = vec!["filesystem", "terminal", "clipboard", "window"];
        let event_types = vec!["created", "updated", "deleted"];
        
        for source in &sources {
            for event_type in &event_types {
                CoverageTracker::record_event_type_tested(source, &format!("{}.{}", source, event_type));
            }
        }
        
        let report = CoverageTracker::get_coverage_report();
        assert!(report.event_types_count >= sources.len() * event_types.len());
    }
}

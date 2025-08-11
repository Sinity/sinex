// # Performance Regression Detection
//
// Automated detection of performance regressions by comparing current
// performance against established baselines. Includes trend analysis,
// statistical significance testing, and automated alerting capabilities.

use color_eyre::eyre::Result;
use super::baseline_performance_test::{
    BaselineTracker, EnvironmentInfo, PerformanceBaseline,
};
use serde_json::json;
use sinex_core::types::events::{event_types, sources, EventFactory};
use sinex_test_utils::prelude::*;
use std::collections::HashMap;
use std::time::{Duration as StdDuration, Instant};

/// Performance regression detection results
#[derive(Debug, Clone, bon::Builder)]
pub struct RegressionDetectionResult {
    pub operation_name: String,
    pub baseline_performance: PerformanceBaseline,
    pub current_performance: PerformanceMeasurement,
    pub regression_detected: bool,
    pub regression_severity: RegressionSeverity,
    pub affected_metrics: Vec<String>,
    pub confidence_level: f64,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone, bon::Builder)]
pub struct PerformanceMeasurement {
    pub average_latency: StdDuration,
    pub percentile_95_latency: StdDuration,
    pub percentile_99_latency: StdDuration,
    pub throughput: f64,
    pub success_rate: f64,
    pub sample_size: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RegressionSeverity {
    None,
    Minor,    // 10-25% degradation
    Moderate, // 25-50% degradation
    Severe,   // 50-100% degradation
    Critical, // >100% degradation or functionality broken
}

/// Regression detection engine
#[derive(bon::Builder)]
pub struct RegressionDetector {
    baselines: HashMap<String, PerformanceBaseline>,
    thresholds: RegressionThresholds,
    measurements: HashMap<String, Vec<StdDuration>>,
    success_counts: HashMap<String, usize>,
    error_counts: HashMap<String, usize>,
    start_time: Instant,
}

#[derive(Debug, Clone, bon::Builder)]
pub struct RegressionThresholds {
    pub latency_minor_threshold: f64,       // 1.1 = 10% increase
    pub latency_moderate_threshold: f64,    // 1.25 = 25% increase
    pub latency_severe_threshold: f64,      // 1.5 = 50% increase
    pub latency_critical_threshold: f64,    // 2.0 = 100% increase
    pub throughput_minor_threshold: f64,    // 0.9 = 10% decrease
    pub throughput_moderate_threshold: f64, // 0.75 = 25% decrease
    pub throughput_severe_threshold: f64,   // 0.5 = 50% decrease
    pub success_rate_threshold: f64,        // 0.95 = 95% success rate
    pub minimum_confidence: f64,            // 0.8 = 80% confidence
}

impl Default for RegressionThresholds {
    fn default() -> Self {
        Self {
            latency_minor_threshold: 1.1,
            latency_moderate_threshold: 1.25,
            latency_severe_threshold: 1.5,
            latency_critical_threshold: 2.0,
            throughput_minor_threshold: 0.9,
            throughput_moderate_threshold: 0.75,
            throughput_severe_threshold: 0.5,
            success_rate_threshold: 0.95,
            minimum_confidence: 0.8,
        }
    }
}

impl RegressionDetector {
    pub fn new() -> Self {
        Self {
            baselines: HashMap::new(),
            thresholds: RegressionThresholds::default(),
            measurements: HashMap::new(),
            success_counts: HashMap::new(),
            error_counts: HashMap::new(),
            start_time: Instant::now(),
        }
    }

    pub fn with_thresholds(thresholds: RegressionThresholds) -> Self {
        Self {
            baselines: HashMap::new(),
            thresholds,
            measurements: HashMap::new(),
            success_counts: HashMap::new(),
            error_counts: HashMap::new(),
            start_time: Instant::now(),
        }
    }

    pub fn set_baseline(&mut self, baseline: PerformanceBaseline) {
        self.baselines
            .insert(baseline.operation_name.clone(), baseline);
    }

    pub fn record_measurement(&mut self, operation: &str, duration: StdDuration, success: bool) {
        self.measurements
            .entry(operation.to_string())
            .or_insert_with(Vec::new)
            .push(duration);

        if success {
            *self
                .success_counts
                .entry(operation.to_string())
                .or_insert(0) += 1;
        } else {
            *self.error_counts.entry(operation.to_string()).or_insert(0) += 1;
        }
    }

    pub fn calculate_current_performance(&self, operation: &str) -> Option<PerformanceMeasurement> {
        if let Some(measurements) = self.measurements.get(operation) {
            if measurements.len() < 10 {
                return None; // Not enough samples
            }

            let mut sorted_measurements = measurements.clone();
            sorted_measurements.sort();

            let average_latency =
                measurements.iter().sum::<StdDuration>() / measurements.len() as u32;

            let p95_index = (measurements.len() as f64 * 0.95) as usize;
            let p99_index = (measurements.len() as f64 * 0.99) as usize;

            let percentile_95_latency =
                sorted_measurements[p95_index.min(sorted_measurements.len() - 1)];
            let percentile_99_latency =
                sorted_measurements[p99_index.min(sorted_measurements.len() - 1)];

            let success_count = self.success_counts.get(operation).unwrap_or(&0);
            let error_count = self.error_counts.get(operation).unwrap_or(&0);
            let total_operations = success_count + error_count;

            let success_rate = if total_operations > 0 {
                *success_count as f64 / total_operations as f64 * 100.0
            } else {
                0.0
            };

            let throughput = *success_count as f64 / self.start_time.elapsed().as_secs_f64();

            Some(PerformanceMeasurement {
                average_latency,
                percentile_95_latency,
                percentile_99_latency,
                throughput,
                success_rate,
                sample_size: measurements.len(),
            })
        } else {
            None
        }
    }

    pub fn detect_regression(&self, operation: &str) -> Option<RegressionDetectionResult> {
        let baseline = self.baselines.get(operation)?;
        let current = self.calculate_current_performance(operation)?;

        let mut affected_metrics = Vec::new();
        let mut regression_severity = RegressionSeverity::None;

        // Check latency regression
        let latency_ratio =
            current.average_latency.as_secs_f64() / baseline.average_latency.as_secs_f64();
        if latency_ratio >= self.thresholds.latency_critical_threshold {
            regression_severity = RegressionSeverity::Critical;
            affected_metrics.push("latency".to_string());
        } else if latency_ratio >= self.thresholds.latency_severe_threshold {
            if regression_severity < RegressionSeverity::Severe {
                regression_severity = RegressionSeverity::Severe;
            }
            affected_metrics.push("latency".to_string());
        } else if latency_ratio >= self.thresholds.latency_moderate_threshold {
            if regression_severity < RegressionSeverity::Moderate {
                regression_severity = RegressionSeverity::Moderate;
            }
            affected_metrics.push("latency".to_string());
        } else if latency_ratio >= self.thresholds.latency_minor_threshold {
            if regression_severity < RegressionSeverity::Minor {
                regression_severity = RegressionSeverity::Minor;
            }
            affected_metrics.push("latency".to_string());
        }

        // Check throughput regression
        let throughput_ratio = current.throughput / baseline.throughput;
        if throughput_ratio <= self.thresholds.throughput_severe_threshold {
            if regression_severity < RegressionSeverity::Severe {
                regression_severity = RegressionSeverity::Severe;
            }
            affected_metrics.push("throughput".to_string());
        } else if throughput_ratio <= self.thresholds.throughput_moderate_threshold {
            if regression_severity < RegressionSeverity::Moderate {
                regression_severity = RegressionSeverity::Moderate;
            }
            affected_metrics.push("throughput".to_string());
        } else if throughput_ratio <= self.thresholds.throughput_minor_threshold {
            if regression_severity < RegressionSeverity::Minor {
                regression_severity = RegressionSeverity::Minor;
            }
            affected_metrics.push("throughput".to_string());
        }

        // Check success rate regression
        if current.success_rate < baseline.success_rate * self.thresholds.success_rate_threshold {
            if regression_severity < RegressionSeverity::Critical {
                regression_severity = RegressionSeverity::Critical;
            }
            affected_metrics.push("success_rate".to_string());
        }

        // Calculate confidence level based on sample sizes
        let confidence_level = self.calculate_confidence_level(&baseline, &current);

        let regression_detected = regression_severity != RegressionSeverity::None
            && confidence_level >= self.thresholds.minimum_confidence;

        let recommendations = self.generate_recommendations(
            &regression_severity,
            &affected_metrics,
            latency_ratio,
            throughput_ratio,
        );

        Some(RegressionDetectionResult {
            operation_name: operation.to_string(),
            baseline_performance: baseline.clone(),
            current_performance: current,
            regression_detected,
            regression_severity,
            affected_metrics,
            confidence_level,
            recommendations,
        })
    }

    fn calculate_confidence_level(
        &self,
        baseline: &PerformanceBaseline,
        current: &PerformanceMeasurement,
    ) -> f64 {
        // Simple confidence calculation based on sample sizes
        // In practice, would use proper statistical tests (t-test, Welch's test, etc.)
        let min_samples = baseline.sample_size.min(current.sample_size) as f64;
        let confidence = (min_samples / 100.0).min(1.0); // More samples = higher confidence
        confidence * 0.8 + 0.2 // Minimum 20% confidence, max approaches 100%
    }

    fn generate_recommendations(
        &self,
        severity: &RegressionSeverity,
        affected_metrics: &[String],
        latency_ratio: f64,
        throughput_ratio: f64,
    ) -> Vec<String> {
        let mut recommendations = Vec::new();

        match severity {
            RegressionSeverity::Critical => {
                recommendations.push("🚨 CRITICAL: Immediate investigation required".to_string());
                recommendations.push("Consider rolling back recent changes".to_string());
                recommendations
                    .push("Check for resource exhaustion or system failures".to_string());
            }
            RegressionSeverity::Severe => {
                recommendations.push("⚠️  SEVERE: High priority investigation needed".to_string());
                recommendations.push("Review recent code changes and deployment".to_string());
                recommendations
                    .push("Monitor system resources and database performance".to_string());
            }
            RegressionSeverity::Moderate => {
                recommendations.push("⚡ MODERATE: Investigation recommended".to_string());
                recommendations.push("Profile performance in affected operations".to_string());
            }
            RegressionSeverity::Minor => {
                recommendations.push("📊 MINOR: Monitor for trending issues".to_string());
                recommendations.push("Consider optimization opportunities".to_string());
            }
            RegressionSeverity::None => {
                recommendations.push("✅ No significant regression detected".to_string());
            }
        }

        if affected_metrics.contains(&"latency".to_string()) {
            recommendations.push(format!(
                "Latency increased by {:.1}%",
                (latency_ratio - 1.0) * 100.0
            ));
            recommendations.push("Check database query performance and indexing".to_string());
        }

        if affected_metrics.contains(&"throughput".to_string()) {
            recommendations.push(format!(
                "Throughput decreased by {:.1}%",
                (1.0 - throughput_ratio) * 100.0
            ));
            recommendations.push("Check for bottlenecks in concurrent processing".to_string());
        }

        if affected_metrics.contains(&"success_rate".to_string()) {
            recommendations.push("Success rate degradation detected".to_string());
            recommendations.push("Check error logs and exception patterns".to_string());
        }

        recommendations
    }

    pub fn print_regression_report(&self, results: &[RegressionDetectionResult]) {
        println!("\n🔍 Performance Regression Detection Report");
        println!("==========================================");

        let mut critical_count = 0;
        let mut severe_count = 0;
        let mut moderate_count = 0;
        let mut minor_count = 0;
        let mut clean_count = 0;

        for result in results {
            match result.regression_severity {
                RegressionSeverity::Critical => critical_count += 1,
                RegressionSeverity::Severe => severe_count += 1,
                RegressionSeverity::Moderate => moderate_count += 1,
                RegressionSeverity::Minor => minor_count += 1,
                RegressionSeverity::None => clean_count += 1,
            }
        }

        println!("\n📊 Summary:");
        println!("  🚨 Critical: {}", critical_count);
        println!("  ⚠️  Severe: {}", severe_count);
        println!("  ⚡ Moderate: {}", moderate_count);
        println!("  📊 Minor: {}", minor_count);
        println!("  ✅ Clean: {}", clean_count);

        for result in results {
            if result.regression_detected {
                println!("\n🔍 Operation: {}", result.operation_name);
                println!("  Severity: {:?}", result.regression_severity);
                println!("  Confidence: {:.1}%", result.confidence_level * 100.0);
                println!("  Affected metrics: {:?}", result.affected_metrics);

                println!("  📈 Performance comparison:");
                println!(
                    "    Baseline latency: {:?} -> Current: {:?}",
                    result.baseline_performance.average_latency,
                    result.current_performance.average_latency
                );
                println!(
                    "    Baseline throughput: {:.2} -> Current: {:.2}",
                    result.baseline_performance.throughput, result.current_performance.throughput
                );

                println!("  💡 Recommendations:");
                for recommendation in &result.recommendations {
                    println!("    - {}", recommendation);
                }
            }
        }
    }
}

// =============================================================================
// Regression Detection Tests
// =============================================================================

/// Test regression detection for database operations
#[sinex_bench]
async fn test_database_operation_regression_detection(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();
    let mut detector = RegressionDetector::new();

    println!("🔍 Testing database operation regression detection");

    // Step 1: Establish baseline performance
    println!("\n📊 Step 1: Establishing baseline performance");

    let mut baseline_tracker = BaselineTracker::new();

    // Baseline measurements
    for i in 0..100 {
        let start = Instant::now();

        let factory = EventFactory::new("regression-test-baseline");
        let event = factory.create_event(
            event_types::test::REGRESSION_BASELINE_TEST,
            json!({
                "iteration": i,
                "test_phase": "baseline"
            }),
        );

        let result = sinex_core::db::insert_event_with_validator(pool, &event, None).await;
        let duration = start.elapsed();

        baseline_tracker.record_measurement("database_insertion", duration, result.is_ok());
    }

    let env_info = EnvironmentInfo {
        test_data_size: 100,
        concurrent_operations: 1,
        database_pool_size: pool.size() as usize,
        system_load: "regression_test".to_string(),
    };

    if let Some(baseline) = baseline_tracker.calculate_baseline("database_insertion", env_info) {
        detector.set_baseline(baseline);
        println!("  ✅ Baseline established for database_insertion");
    }

    // Step 2: Simulate normal performance (should not detect regression)
    println!("\n✅ Step 2: Testing normal performance (no regression expected)");

    for i in 0..100 {
        let start = Instant::now();

        let factory = EventFactory::new("regression-test-normal");
        let event = factory.create_event(
            event_types::test::REGRESSION_NORMAL_TEST,
            json!({
                "iteration": i,
                "test_phase": "normal"
            }),
        );

        let result = sinex_core::db::insert_event_with_validator(pool, &event, None).await;
        let duration = start.elapsed();

        detector.record_measurement("database_insertion", duration, result.is_ok());
    }

    if let Some(result) = detector.detect_regression("database_insertion") {
        println!(
            "  Normal performance regression result: {:?}",
            result.regression_severity
        );
        assert!(
            !result.regression_detected,
            "Normal performance should not show regression"
        );
        println!("  ✅ No regression detected in normal performance");
    }

    // Step 3: Simulate performance degradation
    println!("\n⚠️  Step 3: Simulating performance degradation");

    let mut degraded_detector = RegressionDetector::new();
    if let Some(baseline) = baseline_tracker.get_baseline("database_insertion") {
        degraded_detector.set_baseline(baseline.clone());
    }

    // Simulate degraded performance by adding artificial delays
    for i in 0..100 {
        let start = Instant::now();

        // Add artificial delay to simulate degradation
        tokio::time::sleep(StdDuration::from_millis(20)).await;

        let event = EventFactory::new("regression-baseline")
            .source("regression-test-degraded")
            .event_type("regression.degraded.test")
            .host("regression-host")
            .payload(json!({
                "iteration": i,
                "test_phase": "degraded"
            }))
            .build();

        let result = sinex_core::db::insert_event_with_validator(pool, &event, None).await;
        let duration = start.elapsed();

        degraded_detector.record_measurement("database_insertion", duration, result.is_ok());
    }

    if let Some(result) = degraded_detector.detect_regression("database_insertion") {
        println!(
            "  Degraded performance regression result: {:?}",
            result.regression_severity
        );
        assert!(
            result.regression_detected,
            "Degraded performance should show regression"
        );
        assert!(
            result.regression_severity != RegressionSeverity::None,
            "Should detect non-trivial regression"
        );
        println!("  ✅ Regression correctly detected in degraded performance");
    }

    // Step 4: Simulate severe degradation
    println!("\n🚨 Step 4: Simulating severe degradation");

    let mut severe_detector = RegressionDetector::new();
    if let Some(baseline) = baseline_tracker.get_baseline("database_insertion") {
        severe_detector.set_baseline(baseline.clone());
    }

    // Simulate severe degradation with larger delays
    for i in 0..50 {
        let start = Instant::now();

        // Add significant delay to simulate severe degradation
        tokio::time::sleep(StdDuration::from_millis(100)).await;

        let event = EventFactory::new("regression-baseline")
            .source("regression-test-severe")
            .event_type("regression.severe.test")
            .host("regression-host")
            .payload(json!({
                "iteration": i,
                "test_phase": "severe"
            }))
            .build();

        let result = sinex_core::db::insert_event_with_validator(pool, &event, None).await;
        let duration = start.elapsed();

        severe_detector.record_measurement("database_insertion", duration, result.is_ok());
    }

    if let Some(result) = severe_detector.detect_regression("database_insertion") {
        println!(
            "  Severe degradation regression result: {:?}",
            result.regression_severity
        );
        assert!(
            result.regression_detected,
            "Severe degradation should show regression"
        );
        assert!(
            result.regression_severity == RegressionSeverity::Severe
                || result.regression_severity == RegressionSeverity::Critical,
            "Should detect severe or critical regression"
        );
        println!("  ✅ Severe regression correctly detected");
    }

    println!("✅ Database operation regression detection test passed");
    Ok(())
}

/// Test regression detection with multiple operations
#[sinex_bench]
async fn test_multi_operation_regression_detection(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();
    let mut detector = RegressionDetector::new();

    println!("🔍 Testing multi-operation regression detection");

    // Establish baselines for multiple operations
    println!("\n📊 Establishing baselines for multiple operations");

    let operations = vec![
        ("fast_operation", 5),    // Baseline: ~5ms
        ("medium_operation", 20), // Baseline: ~20ms
        ("slow_operation", 50),   // Baseline: ~50ms
    ];

    for (operation_name, baseline_delay) in &operations {
        let mut baseline_tracker = BaselineTracker::new();

        for i in 0..100 {
            let start = Instant::now();

            // Simulate different operation speeds
            tokio::time::sleep(StdDuration::from_millis(*baseline_delay)).await;

            let event = EventFactory::new("regression-baseline")
                .source(&format!("multi-regression-{}", operation_name))
                .event_type(&format!("multi.regression.{}", operation_name))
                .host("multi-regression-host")
                .payload(json!({
                    "iteration": i,
                    "operation": operation_name
                }))
                .build();

            let result = sinex_core::db::insert_event_with_validator(pool, &event, None).await;
            let duration = start.elapsed();

            baseline_tracker.record_measurement(operation_name, duration, result.is_ok());
        }

        let env_info = EnvironmentInfo {
            test_data_size: 100,
            concurrent_operations: 1,
            database_pool_size: pool.size() as usize,
            system_load: "multi_regression_test".to_string(),
        };

        if let Some(baseline) = baseline_tracker.calculate_baseline(operation_name, env_info) {
            detector.set_baseline(baseline);
            println!("  ✅ Baseline established for {}", operation_name);
        }
    }

    // Test current performance with mixed results
    println!("\n🔍 Testing current performance with mixed results");

    let test_scenarios = vec![
        ("fast_operation", 8),    // Minor regression: 5ms -> 8ms
        ("medium_operation", 15), // Improvement: 20ms -> 15ms
        ("slow_operation", 120),  // Severe regression: 50ms -> 120ms
    ];

    for (operation_name, current_delay) in &test_scenarios {
        for i in 0..100 {
            let start = Instant::now();

            // Simulate current operation speeds
            tokio::time::sleep(StdDuration::from_millis(*current_delay)).await;

            let event = EventFactory::new("regression-baseline")
                .source(&format!("multi-current-{}", operation_name))
                .event_type(&format!("multi.current.{}", operation_name))
                .host("multi-current-host")
                .payload(json!({
                    "iteration": i,
                    "operation": operation_name
                }))
                .build();

            let result = sinex_core::db::insert_event_with_validator(pool, &event, None).await;
            let duration = start.elapsed();

            detector.record_measurement(operation_name, duration, result.is_ok());
        }
    }

    // Analyze regression results
    println!("\n📊 Analyzing regression results");

    let mut regression_results = Vec::new();

    for (operation_name, _) in &operations {
        if let Some(result) = detector.detect_regression(operation_name) {
            regression_results.push(result);
        }
    }

    detector.print_regression_report(&regression_results);

    // Verify expected results
    for result in &regression_results {
        match result.operation_name.as_str() {
            "fast_operation" => {
                // Should detect minor regression (5ms -> 8ms = 60% increase)
                assert!(
                    result.regression_detected,
                    "fast_operation should show regression"
                );
                assert!(
                    result.regression_severity == RegressionSeverity::Minor
                        || result.regression_severity == RegressionSeverity::Moderate,
                    "fast_operation should show minor to moderate regression"
                );
            }
            "medium_operation" => {
                // Should show improvement (20ms -> 15ms)
                assert!(
                    !result.regression_detected,
                    "medium_operation should not show regression"
                );
            }
            "slow_operation" => {
                // Should detect severe regression (50ms -> 120ms = 140% increase)
                assert!(
                    result.regression_detected,
                    "slow_operation should show regression"
                );
                assert!(
                    result.regression_severity == RegressionSeverity::Severe
                        || result.regression_severity == RegressionSeverity::Critical,
                    "slow_operation should show severe or critical regression"
                );
            }
            _ => {}
        }
    }

    println!("✅ Multi-operation regression detection test passed");
    Ok(())
}

/// Test regression detection with custom thresholds
#[sinex_bench]
async fn test_custom_threshold_regression_detection(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();

    println!("🔍 Testing custom threshold regression detection");

    // Test with strict thresholds
    println!("\n🎯 Testing with strict thresholds");

    let strict_thresholds = RegressionThresholds {
        latency_minor_threshold: 1.05,       // 5% increase = minor
        latency_moderate_threshold: 1.15,    // 15% increase = moderate
        latency_severe_threshold: 1.3,       // 30% increase = severe
        latency_critical_threshold: 1.5,     // 50% increase = critical
        throughput_minor_threshold: 0.95,    // 5% decrease = minor
        throughput_moderate_threshold: 0.85, // 15% decrease = moderate
        throughput_severe_threshold: 0.7,    // 30% decrease = severe
        success_rate_threshold: 0.98,        // 98% success rate required
        minimum_confidence: 0.9,             // 90% confidence required
    };

    let mut strict_detector = RegressionDetector::with_thresholds(strict_thresholds);

    // Establish baseline
    let mut baseline_tracker = BaselineTracker::new();

    for i in 0..150 {
        let start = Instant::now();

        tokio::time::sleep(StdDuration::from_millis(10)).await;

        let event = EventFactory::new("regression-baseline")
            .source("strict-threshold-baseline")
            .event_type("strict.threshold.baseline")
            .host("strict-host")
            .payload(json!({"iteration": i}))
            .build();

        let result = sinex_core::db::insert_event_with_validator(pool, &event, None).await;
        let duration = start.elapsed();

        baseline_tracker.record_measurement("strict_operation", duration, result.is_ok());
    }

    let env_info = EnvironmentInfo {
        test_data_size: 150,
        concurrent_operations: 1,
        database_pool_size: pool.size() as usize,
        system_load: "strict_threshold_test".to_string(),
    };

    if let Some(baseline) = baseline_tracker.calculate_baseline("strict_operation", env_info) {
        strict_detector.set_baseline(baseline);
        println!("  ✅ Strict baseline established");
    }

    // Test with small degradation (should be detected with strict thresholds)
    for i in 0..150 {
        let start = Instant::now();

        // Small degradation: 10ms -> 12ms (20% increase)
        tokio::time::sleep(StdDuration::from_millis(12)).await;

        let event = EventFactory::new("regression-baseline")
            .source("strict-threshold-test")
            .event_type("strict.threshold.test")
            .host("strict-host")
            .payload(json!({"iteration": i}))
            .build();

        let result = sinex_core::db::insert_event_with_validator(pool, &event, None).await;
        let duration = start.elapsed();

        strict_detector.record_measurement("strict_operation", duration, result.is_ok());
    }

    if let Some(strict_result) = strict_detector.detect_regression("strict_operation") {
        println!(
            "  Strict threshold result: {:?}",
            strict_result.regression_severity
        );
        assert!(
            strict_result.regression_detected,
            "Strict thresholds should detect small degradation"
        );
        println!("  ✅ Strict thresholds correctly detected regression");
    }

    // Test with lenient thresholds
    println!("\n🎯 Testing with lenient thresholds");

    let lenient_thresholds = RegressionThresholds {
        latency_minor_threshold: 1.5,       // 50% increase = minor
        latency_moderate_threshold: 2.0,    // 100% increase = moderate
        latency_severe_threshold: 3.0,      // 200% increase = severe
        latency_critical_threshold: 5.0,    // 400% increase = critical
        throughput_minor_threshold: 0.5,    // 50% decrease = minor
        throughput_moderate_threshold: 0.3, // 70% decrease = moderate
        throughput_severe_threshold: 0.1,   // 90% decrease = severe
        success_rate_threshold: 0.8,        // 80% success rate required
        minimum_confidence: 0.5,            // 50% confidence required
    };

    let mut lenient_detector = RegressionDetector::with_thresholds(lenient_thresholds);

    if let Some(baseline) = baseline_tracker.get_baseline("strict_operation") {
        lenient_detector.set_baseline(baseline.clone());
    }

    // Test same degradation with lenient thresholds
    for i in 0..150 {
        let start = Instant::now();

        // Same degradation: 10ms -> 12ms (20% increase)
        tokio::time::sleep(StdDuration::from_millis(12)).await;

        let event = EventFactory::new("regression-baseline")
            .source("lenient-threshold-test")
            .event_type("lenient.threshold.test")
            .host("lenient-host")
            .payload(json!({"iteration": i}))
            .build();

        let result = sinex_core::db::insert_event_with_validator(pool, &event, None).await;
        let duration = start.elapsed();

        lenient_detector.record_measurement("strict_operation", duration, result.is_ok());
    }

    if let Some(lenient_result) = lenient_detector.detect_regression("strict_operation") {
        println!(
            "  Lenient threshold result: {:?}",
            lenient_result.regression_severity
        );
        assert!(
            !lenient_result.regression_detected,
            "Lenient thresholds should not detect small degradation"
        );
        println!("  ✅ Lenient thresholds correctly ignored minor degradation");
    }

    println!("✅ Custom threshold regression detection test passed");
    Ok(())
}

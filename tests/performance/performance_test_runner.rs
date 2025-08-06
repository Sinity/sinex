// # Performance Test Runner
//
// Orchestrates comprehensive performance testing suites including baseline
// establishment, regression detection, and bottleneck identification.
// Provides unified reporting and performance tracking capabilities.

use sinex_test_utils::prelude::*;
use super::baseline_performance_test::{BaselineTracker, EnvironmentInfo};
use super::regression_detection_test::RegressionDetector;
use super::bottleneck_identification_test::BottleneckDetector;
use redis::cmd;
use serde_json::json;
use sinex_db::queries::{EventQueries, CheckpointQueries};
use sinex_db::query_builder::{QueryBuilder, QueryParam};
use sinex_db::models::{EventFactory, services, event_types};
use sinex_satellite_sdk::RedisStreamClient;
use std::collections::HashMap;
use std::time::{Duration as StdDuration, Instant};

/// Comprehensive performance test results
#[derive(Debug, Clone)]
pub struct PerformanceTestSuite {
    pub suite_name: String,
    pub test_duration: StdDuration,
    pub baseline_results: HashMap<String, BaselineResult>,
    pub regression_results: Vec<RegressionResult>,
    pub bottleneck_results: Vec<BottleneckResult>,
    pub overall_health: PerformanceHealth,
    pub recommendations: Vec<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
pub struct BaselineResult {
    pub operation: String,
    pub average_latency: StdDuration,
    pub throughput: f64,
    pub success_rate: f64,
    pub sample_size: usize,
}

#[derive(Debug, Clone)]
pub struct RegressionResult {
    pub operation: String,
    pub regression_detected: bool,
    pub severity: String,
    pub latency_change: f64,
    pub throughput_change: f64,
}

#[derive(Debug, Clone)]
pub struct BottleneckResult {
    pub bottleneck_type: String,
    pub severity: String,
    pub affected_operations: Vec<String>,
    pub resource_utilization: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PerformanceHealth {
    Excellent,  // No issues, performance above baseline
    Good,       // Minor issues, performance meets expectations
    Warning,    // Some regressions or bottlenecks detected
    Critical,   // Significant performance issues
    Failure,    // System failing to meet basic performance requirements
}

/// Performance test orchestrator
pub struct PerformanceTestRunner {
    baseline_tracker: BaselineTracker,
    regression_detector: RegressionDetector,
    bottleneck_detector: BottleneckDetector,
    test_start: Instant,
    environment_info: EnvironmentInfo,
}

impl PerformanceTestRunner {
    pub fn new(env_info: EnvironmentInfo) -> Self {
        Self {
            baseline_tracker: BaselineTracker::new(),
            regression_detector: RegressionDetector::new(),
            bottleneck_detector: BottleneckDetector::new(),
            test_start: Instant::now(),
            environment_info: env_info,
        }
    }

    pub fn record_operation(&mut self, operation: &str, duration: StdDuration, success: bool) {
        self.baseline_tracker.record_measurement(operation, duration, success);
        self.regression_detector.record_measurement(operation, duration, success);
        self.bottleneck_detector.record_operation(operation, duration, success);
    }

    pub fn record_resource_utilization(&mut self, resource: &str, utilization: f64) {
        self.bottleneck_detector.record_resource_utilization(resource, utilization);
    }

    pub fn generate_comprehensive_report(&mut self, suite_name: &str) -> PerformanceTestSuite {
        let test_duration = self.test_start.elapsed();

        // Generate baseline results
        let mut baseline_results = HashMap::new();
        let operations = vec![
            "database_insert",
            "database_query",
            "stream_write",
            "stream_read",
            "checkpoint_save",
            "checkpoint_load"
        ];

        for operation in operations {
            if let Some(baseline) = self.baseline_tracker.calculate_baseline(operation, self.environment_info.clone()) {
                baseline_results.insert(operation.to_string(), BaselineResult {
                    operation: operation.to_string(),
                    average_latency: baseline.average_latency,
                    throughput: baseline.throughput,
                    success_rate: baseline.success_rate,
                    sample_size: baseline.sample_size,
                });
            }
        }

        // Generate regression results
        let mut regression_results = Vec::new();
        for operation in baseline_results.keys() {
            if let Some(regression) = self.regression_detector.detect_regression(operation) {
                regression_results.push(RegressionResult {
                    operation: operation.clone(),
                    regression_detected: regression.regression_detected,
                    severity: format!("{:?}", regression.regression_severity),
                    latency_change: regression.current_performance.average_latency.as_secs_f64() /
                                  regression.baseline_performance.average_latency.as_secs_f64(),
                    throughput_change: regression.current_performance.throughput /
                                     regression.baseline_performance.throughput,
                });
            }
        }

        // Generate bottleneck results
        let bottleneck_analyses = self.bottleneck_detector.analyze_bottlenecks();
        let bottleneck_results = bottleneck_analyses.iter().map(|analysis| {
            BottleneckResult {
                bottleneck_type: format!("{:?}", analysis.bottleneck_type),
                severity: format!("{:?}", analysis.severity),
                affected_operations: analysis.affected_operations.clone(),
                resource_utilization: analysis.metrics.resource_utilization,
            }
        }).collect();

        // Determine overall health
        let overall_health = self.calculate_overall_health(&regression_results, &bottleneck_results);

        // Generate recommendations
        let recommendations = self.generate_recommendations(&regression_results, &bottleneck_results, &overall_health);

        PerformanceTestSuite {
            suite_name: suite_name.to_string(),
            test_duration,
            baseline_results,
            regression_results,
            bottleneck_results,
            overall_health,
            recommendations,
            timestamp: chrono::Utc::now(),
        }
    }

    fn calculate_overall_health(&self, regressions: &[RegressionResult], bottlenecks: &[BottleneckResult]) -> PerformanceHealth {
        let critical_regressions = regressions.iter()
            .filter(|r| r.regression_detected && r.severity == "Critical")
            .count();

        let severe_regressions = regressions.iter()
            .filter(|r| r.regression_detected && r.severity == "Severe")
            .count();

        let critical_bottlenecks = bottlenecks.iter()
            .filter(|b| b.severity == "Critical")
            .count();

        let high_bottlenecks = bottlenecks.iter()
            .filter(|b| b.severity == "High")
            .count();

        if critical_regressions > 0 || critical_bottlenecks > 0 {
            PerformanceHealth::Critical
        } else if severe_regressions > 2 || high_bottlenecks > 1 {
            PerformanceHealth::Failure
        } else if severe_regressions > 0 || high_bottlenecks > 0 {
            PerformanceHealth::Warning
        } else if regressions.iter().any(|r| r.regression_detected) {
            PerformanceHealth::Good
        } else {
            PerformanceHealth::Excellent
        }
    }

    fn generate_recommendations(&self, regressions: &[RegressionResult], bottlenecks: &[BottleneckResult], health: &PerformanceHealth) -> Vec<String> {
        let mut recommendations = Vec::new();

        match health {
            PerformanceHealth::Critical => {
                recommendations.push("🚨 IMMEDIATE ACTION REQUIRED: Critical performance issues detected".to_string());
                recommendations.push("Consider system rollback or emergency maintenance".to_string());
            }
            PerformanceHealth::Failure => {
                recommendations.push("⚠️  URGENT: System failing to meet performance requirements".to_string());
                recommendations.push("Schedule immediate performance investigation".to_string());
            }
            PerformanceHealth::Warning => {
                recommendations.push("⚡ WARNING: Performance degradation detected".to_string());
                recommendations.push("Schedule performance review and optimization".to_string());
            }
            PerformanceHealth::Good => {
                recommendations.push("✅ GOOD: Minor performance issues detected".to_string());
                recommendations.push("Monitor for trending issues and consider preventive optimization".to_string());
            }
            PerformanceHealth::Excellent => {
                recommendations.push("🎉 EXCELLENT: No significant performance issues detected".to_string());
                recommendations.push("Continue monitoring and maintain current practices".to_string());
            }
        }

        // Specific regression recommendations
        for regression in regressions {
            if regression.regression_detected {
                if regression.latency_change > 1.5 {
                    recommendations.push(format!("Investigate latency increase in {}: {:.1}x slower",
                                               regression.operation, regression.latency_change));
                }
                if regression.throughput_change < 0.7 {
                    recommendations.push(format!("Investigate throughput decrease in {}: {:.1}% reduction",
                                               regression.operation, (1.0 - regression.throughput_change) * 100.0));
                }
            }
        }

        // Specific bottleneck recommendations
        for bottleneck in bottlenecks {
            match bottleneck.bottleneck_type.as_str() {
                "DatabaseConnections" => {
                    recommendations.push("Consider increasing database connection pool size".to_string());
                    recommendations.push("Review database query optimization opportunities".to_string());
                }
                "Memory" => {
                    recommendations.push("Monitor memory usage patterns and consider optimization".to_string());
                    recommendations.push("Review large object allocation patterns".to_string());
                }
                "RedisMemory" => {
                    recommendations.push("Check Redis memory usage and eviction policies".to_string());
                    recommendations.push("Consider Redis clustering for horizontal scaling".to_string());
                }
                _ => {
                    recommendations.push(format!("Investigate {} bottleneck", bottleneck.bottleneck_type));
                }
            }
        }

        recommendations
    }

    pub fn print_comprehensive_report(&self, suite: &PerformanceTestSuite) {
        println!("\n📊 COMPREHENSIVE PERFORMANCE REPORT");
        println!("=====================================");
        println!("Suite: {}", suite.suite_name);
        println!("Duration: {:?}", suite.test_duration);
        println!("Timestamp: {}", suite.timestamp.format("%Y-%m-%d %H:%M:%S UTC"));
        println!("Overall Health: {:?}", suite.overall_health);

        // Health indicator
        let health_emoji = match suite.overall_health {
            PerformanceHealth::Excellent => "🟢",
            PerformanceHealth::Good => "🟡",
            PerformanceHealth::Warning => "🟠",
            PerformanceHealth::Critical => "🔴",
            PerformanceHealth::Failure => "💀",
        };
        println!("Health Status: {} {:?}", health_emoji, suite.overall_health);

        // Baseline results
        if !suite.baseline_results.is_empty() {
            println!("\n📈 BASELINE PERFORMANCE:");
            for (operation, result) in &suite.baseline_results {
                println!("  {} - Latency: {:?}, Throughput: {:.2} ops/sec, Success: {:.1}%",
                         operation, result.average_latency, result.throughput, result.success_rate);
            }
        }

        // Regression results
        if !suite.regression_results.is_empty() {
            println!("\n⚡ REGRESSION ANALYSIS:");
            for regression in &suite.regression_results {
                if regression.regression_detected {
                    println!("  🔍 {} - Severity: {}, Latency: {:.2}x, Throughput: {:.2}x",
                             regression.operation, regression.severity,
                             regression.latency_change, regression.throughput_change);
                }
            }
        }

        // Bottleneck results
        if !suite.bottleneck_results.is_empty() {
            println!("\n🚧 BOTTLENECK ANALYSIS:");
            for bottleneck in &suite.bottleneck_results {
                println!("  {} - Severity: {}, Resource Util: {:.1}%, Affected: {:?}",
                         bottleneck.bottleneck_type, bottleneck.severity,
                         bottleneck.resource_utilization * 100.0, bottleneck.affected_operations);
            }
        }

        // Recommendations
        if !suite.recommendations.is_empty() {
            println!("\n💡 RECOMMENDATIONS:");
            for (i, recommendation) in suite.recommendations.iter().enumerate() {
                println!("  {}. {}", i + 1, recommendation);
            }
        }

        println!("\n=====================================");
    }

    pub fn export_results_json(&self, suite: &PerformanceTestSuite) -> String {
        serde_json::to_string_pretty(&json!({
            "suite_name": suite.suite_name,
            "test_duration_ms": suite.test_duration.as_millis(),
            "timestamp": suite.timestamp,
            "overall_health": format!("{:?}", suite.overall_health),
            "baseline_results": suite.baseline_results,
            "regression_results": suite.regression_results,
            "bottleneck_results": suite.bottleneck_results,
            "recommendations": suite.recommendations,
            "environment": {
                "test_data_size": self.environment_info.test_data_size,
                "concurrent_operations": self.environment_info.concurrent_operations,
                "database_pool_size": self.environment_info.database_pool_size,
                "system_load": self.environment_info.system_load
            }
        })).unwrap_or_else(|_| "{}".to_string())
    }
}

// =============================================================================
// Comprehensive Performance Test Suites
// =============================================================================

/// Run complete performance test suite
#[sinex_test]
async fn test_comprehensive_performance_suite(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();

    println!("🚀 Running comprehensive performance test suite");

    let env_info = EnvironmentInfo {
        test_data_size: 500,
        concurrent_operations: 10,
        database_pool_size: pool.size() as usize,
        system_load: "comprehensive_test".to_string(),
    };

    let mut runner = PerformanceTestRunner::new(env_info);

    // Phase 1: Database operations
    println!("\n📊 Phase 1: Database performance testing");

    for i in 0..100 {
        let start = Instant::now();

        let factory = EventFactory::new("comprehensive-test");
        let event = factory.create_event(
            event_types::test::COMPREHENSIVE_PERFORMANCE_TEST,
            json!({
                "iteration": i,
                "phase": "database",
                "test_type": "comprehensive"
            })
        );

        let result = sinex_db::insert_event_with_validator(pool, &event, None).await;
        let duration = start.elapsed();

        runner.record_operation("database_insert", duration, result.is_ok());

        // Also test queries
        if i % 10 == 0 {
            let query_start = Instant::now();
            // Keep as raw SQL for timing measurement
            let query_result = sqlx::query!(
                "SELECT COUNT(*) as count FROM core.events WHERE source = 'comprehensive-test'"
            ).fetch_one(pool).await;
            let query_duration = query_start.elapsed();

            runner.record_operation("database_query", query_duration, query_result.is_ok());
        }
    }

    // Phase 2: Stream operations
    println!("\n📡 Phase 2: Stream performance testing");

    if let Ok(redis_client) = RedisStreamClient::new("redis://localhost:6379")?.await {
        let stream_key = "sinex:comprehensive:test-stream";

        // Stream writes
        for i in 0..100 {
            let start = Instant::now();

            let message_data = json!({
                "message_id": i,
                "phase": "stream",
                "test_type": "comprehensive"
            });

            let result = redis_client.xadd(stream_key, "*", &message_data).await;
            let duration = start.elapsed();

            runner.record_operation("stream_write", duration, result.is_ok());
        }

        // Stream reads
        let consumer_group = "comprehensive-group";
        match redis_client.xgroup_create(stream_key, consumer_group, "0", true).await {
            Ok(_) => {
                for i in 0..20 {
                    let start = Instant::now();

                    let result = cmd("XREADGROUP")
            .arg("GROUP")
            .arg(consumer_group)
            .arg("comprehensive-consumer")
            .arg("COUNT")
            .arg(10)
            .arg("STREAMS")
            .arg(stream_key)
            .arg(">")
            .query_async::<_, redis::streams::StreamReadReply>(&mut redis_client).await;

                    let duration = start.elapsed();
                    runner.record_operation("stream_read", duration, result.is_ok());

                    if let Ok(messages) = &result {
                        for stream in &messages.keys {
                            for message in &stream.ids {
                                let _ = redis_client.xack(stream_key, consumer_group, &message.id).await;
                            }
                        }
                    }
                }
            }
            Err(e) => println!("    Consumer group creation failed: {}", e),
        }

        // Cleanup
        let _ = redis_client.del(stream_key).await;
    }

    // Phase 3: Concurrent operations
    println!("\n🔄 Phase 3: Concurrent performance testing");

    let concurrent_handles = (0..10)
        .map(|worker_id| {
            let pool_clone = pool.clone();

            tokio::spawn(async move {
                let mut operations = Vec::new();

                for op_id in 0..20 {
                    let start = Instant::now();

                    let factory = EventFactory::new(&format!("comprehensive-concurrent-{}", worker_id));
                    let event = factory.create_event(
                        event_types::test::COMPREHENSIVE_CONCURRENT_TEST,
                        json!({
                            "worker_id": worker_id,
                            "operation_id": op_id,
                            "phase": "concurrent"
                        })
                    );

                    let result = sinex_db::insert_event_with_validator(&pool_clone, &event, None).await;
                    let duration = start.elapsed();

                    operations.push((duration, result.is_ok()));
                }

                operations
            })
        })
        .collect::<Vec<_>>();

    let concurrent_results = futures::future::join_all(concurrent_handles).await;

    for result in concurrent_results {
        if let Ok(operations) = result {
            for (duration, success) in operations {
                runner.record_operation("concurrent_insert", duration, success);
            }
        }
    }

    // Phase 4: Resource utilization simulation
    println!("\n🔧 Phase 4: Resource utilization testing");

    // Simulate varying resource utilization
    let utilization_levels = vec![0.3, 0.5, 0.7, 0.9];

    for utilization in utilization_levels {
        runner.record_resource_utilization("memory", utilization);
        runner.record_resource_utilization("cpu", utilization * 0.8);

        // Simulate operations under load
        for i in 0..10 {
            let start = Instant::now();

            // Simulate delay proportional to resource pressure
            let delay = StdDuration::from_millis((utilization * 50.0) as u64);
            tokio::time::sleep(delay).await;

            let factory = EventFactory::new("comprehensive-resource-test");
            let event = factory.create_event(
                event_types::test::COMPREHENSIVE_RESOURCE_TEST,
                json!({
                    "iteration": i,
                    "utilization": utilization,
                    "phase": "resource"
                })
            );

            let result = sinex_db::insert_event_with_validator(pool, &event, None).await;
            let duration = start.elapsed();

            runner.record_operation("resource_operation", duration, result.is_ok());
        }
    }

    // Generate comprehensive report
    println!("\n📊 Generating comprehensive performance report");

    let suite = runner.generate_comprehensive_report("Comprehensive Performance Suite");
    runner.print_comprehensive_report(&suite);

    // Export JSON results
    let json_results = runner.export_results_json(&suite);
    println!("\n📄 JSON Export (first 500 chars):");
    println!("{}", &json_results[..json_results.len().min(500)]);
    if json_results.len() > 500 {
        println!("... (truncated)");
    }

    // Performance assertions
    assert!(suite.overall_health != PerformanceHealth::Failure,
        "Overall performance health should not be Failure");
    assert!(!suite.baseline_results.is_empty(),
        "Should have baseline results");
    assert!(!suite.recommendations.is_empty(),
        "Should have recommendations");

    // Log final metrics using centralized query system
    let total_events = EventQueries::count_by_source_pattern(&pool, "comprehensive%").await?;

    println!("\n📈 Final metrics:");
    println!("  - Total events created: {}", total_events);
    println!("  - Test duration: {:?}", suite.test_duration);
    println!("  - Overall health: {:?}", suite.overall_health);

    println!("✅ Comprehensive performance test suite completed");
    Ok(())
}

/// Run focused performance regression test
#[sinex_test]
async fn test_focused_performance_regression_suite(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();

    println!("🎯 Running focused performance regression test suite");

    let env_info = EnvironmentInfo {
        test_data_size: 200,
        concurrent_operations: 5,
        database_pool_size: pool.size() as usize,
        system_load: "regression_focused_test".to_string(),
    };

    let mut runner = PerformanceTestRunner::new(env_info);

    // Establish baseline
    println!("\n📊 Establishing performance baseline");

    for i in 0..100 {
        let start = Instant::now();

        // Normal operations
        tokio::time::sleep(StdDuration::from_millis(5)).await;

        let factory = EventFactory::new("regression-baseline");
        let event = factory.create_event(
            event_types::test::REGRESSION_BASELINE_TEST,
            json!({
                "iteration": i,
                "phase": "baseline"
            })
        );

        let result = sinex_db::insert_event_with_validator(pool, &event, None).await;
        let duration = start.elapsed();

        runner.record_operation("regression_operation", duration, result.is_ok());
    }

    // Simulate regression
    println!("\n⚠️  Simulating performance regression");

    for i in 0..100 {
        let start = Instant::now();

        // Degraded operations (3x slower)
        tokio::time::sleep(StdDuration::from_millis(15)).await;

        let factory = EventFactory::new("regression-degraded");
        let event = factory.create_event(
            event_types::test::REGRESSION_DEGRADED_TEST,
            json!({
                "iteration": i,
                "phase": "regression"
            })
        );

        let result = sinex_db::insert_event_with_validator(pool, &event, None).await;
        let duration = start.elapsed();

        runner.record_operation("regression_operation", duration, result.is_ok());
    }

    // Generate regression-focused report
    let suite = runner.generate_comprehensive_report("Focused Regression Suite");
    runner.print_comprehensive_report(&suite);

    // Should detect regression
    let has_regression = suite.regression_results.iter()
        .any(|r| r.regression_detected);

    assert!(has_regression, "Should detect performance regression");
    assert!(suite.overall_health == PerformanceHealth::Warning ||
            suite.overall_health == PerformanceHealth::Critical,
            "Should indicate performance issues");

    println!("✅ Focused performance regression test completed");
    Ok(())
}


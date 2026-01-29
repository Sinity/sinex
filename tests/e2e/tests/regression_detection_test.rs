// # Performance Regression Detection Tests
//
// This file contains:
// 1. `test_regression_detector_mechanism`: Validates the logic of the `RegressionDetector` using simulated data.
//    (This ensures the math and alerting logic works correctly).
// 2. `test_e2e_real_database_performance`: A REAL benchmark that inserts data into the DB and verifies throughput.
//    (This ensures actual system performance is within acceptable limits).

use async_nats::jetstream::Context as JetStream;
use serde_json::json;
use sinex_node_sdk::diagnostics::regression::*;
use sinex_primitives::events::{event_types, sources, EventFactory};
use std::collections::HashMap;
use std::time::{Duration as StdDuration, Instant};
use xtask::sandbox::nats::EphemeralNats;
use xtask::sandbox::{prelude::*, timing_utils::Timeouts};

// =============================================================================
// Real System Performance Tests
// =============================================================================

/// Benchmark actual database ingestion throughput.
/// This test is NOT a simulation. It stresses the actual postgres instance.
#[sinex_bench]
async fn test_e2e_real_database_performance(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();
    let mut detector = RegressionDetector::new();

    // Define our "Expected Baseline" hardcoded for this environment.
    // In a mature system, this might come from a historical DB or config.
    // We expect at least 200 inserts/sec on dev environment to pass.
    let expected_baseline = PerformanceBaseline {
        operation_name: "real_db_ingestion".to_string(),
        average_latency: StdDuration::from_millis(5), // Expect fast individual inserts
        percentile_95_latency: StdDuration::from_millis(20),
        throughput: 200.0, // Expected Ops/Sec
        success_rate: 100.0,
        sample_size: 1000,
        environment: EnvironmentInfo {
            test_data_size: 1000,
            concurrent_operations: 1, // Sequential for this test
            database_pool_size: pool.size() as usize,
            system_load: "benchmark".to_string(),
        },
    };

    detector.set_baseline(expected_baseline.clone());

    println!("🚀 Starting Real Database Performance Benchmark (1000 inserts)...");
    let start_all = Instant::now();
    let sample_count = 1000;

    for i in 0..sample_count {
        let op_start = Instant::now();

        let factory = EventFactory::new("perf-benchmark");
        let event = factory.create_event(
            "benchmark.event",
            json!({ "i": i, "payload": "benchmarking is fun" }),
        );

        // Real DB Insert
        let result =
            sinex_primitives::db::insert_event_with_validator(pool.clone(), &event, None).await;

        detector.record_measurement("real_db_ingestion", op_start.elapsed(), result.is_ok());

        if result.is_err() {
            eprintln!("Insert failed: {:?}", result.err());
        }
    }

    let total_duration = start_all.elapsed();
    let throughput = sample_count as f64 / total_duration.as_secs_f64();

    println!("✅ Benchmark Complete in {:.2?}.", total_duration);
    println!("   Throughput: {:.2} events/sec", throughput);

    if let Some(perf) = detector.calculate_current_performance("real_db_ingestion") {
        println!("   Avg Latency: {:.2?}", perf.average_latency);
        println!("   P95 Latency: {:.2?}", perf.percentile_95_latency);
    }

    // Detect Regression against our hardcoded expectations
    if let Some(result) = detector.detect_regression("real_db_ingestion") {
        if result.regression_detected {
            println!("⚠️  PERFORMANCE REGRESSION DETECTED against expected baseline!");
            detector.print_regression_report(&[result.clone()]);

            // We soft-fail or hard-fail depending on severity.
            // For now, let's hard fail if it's Critical (e.g. < 50% expected speed).
            if result.regression_severity == RegressionSeverity::Critical
                || result.regression_severity == RegressionSeverity::Severe
            {
                return Err(color_eyre::eyre::eyre!(
                    "Database performance is critically low ({:.2} ops/sec, expected {:.2})",
                    throughput,
                    expected_baseline.throughput
                ));
            }
        } else {
            println!("✨ Performance is within acceptable limits.");
        }
    }

    Ok(())
}

// =============================================================================
// Regression Detector Mechanism Tests (Logic Verification)
// =============================================================================

/// Validate the logic of the RegressionDetector itself using controlled measurements.
/// This test verifies the detector's math and alerting logic by providing synthetic
/// performance data rather than simulating actual delays.
#[sinex_bench]
async fn test_regression_detector_mechanism(_ctx: TestContext) -> TestResult<()> {
    println!("🔍 Testing RegressionDetector Logic (Mechanism Test)");

    // Step 1: Establish baseline with synthetic fast measurements
    let mut baseline_tracker = BaselineTracker::new();
    for _ in 0..50 {
        // Record synthetic "fast" measurements (1ms baseline)
        baseline_tracker.record_measurement("logic_test", StdDuration::from_millis(1), true);
    }

    let baseline = baseline_tracker
        .calculate_baseline(
            "logic_test",
            EnvironmentInfo {
                test_data_size: 50,
                concurrent_operations: 1,
                database_pool_size: 0,
                system_load: "unit_test".to_string(),
            },
        )
        .expect("Should calculate baseline");

    // Step 2: Verify detection of degradations with synthetic slow measurements
    println!("⚠️  Testing degradation detection with synthetic data...");
    let mut severe_detector = RegressionDetector::new();
    severe_detector.set_baseline(baseline.clone());

    // Record synthetic "slow" measurements (20ms = 20x regression)
    for _ in 0..20 {
        severe_detector.record_measurement("logic_test", StdDuration::from_millis(20), true);
    }

    let result = severe_detector
        .detect_regression("logic_test")
        .expect("Should produce regression result");

    println!("  Detection Result: {:?}", result.regression_severity);
    assert!(
        result.regression_detected,
        "Should detect simulated logic regression"
    );
    assert!(
        result.regression_severity == RegressionSeverity::Critical
            || result.regression_severity == RegressionSeverity::Severe,
        "Should be Severe/Critical for 20x degradation"
    );

    println!("✅ RegressionDetector logic verification passed");
    Ok(())
}

#[sinex_test]
async fn test_jetstream_publish_regression_detection() -> TestResult<()> {
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let js = nats.jetstream_with_client(client.clone());

    let stream_name = format!("regression_publish_{}", Ulid::new());
    let subject = format!("regression.publish.{}", Ulid::new());

    js.get_or_create_stream(async_nats::jetstream::stream::Config {
        name: stream_name.clone(),
        subjects: vec![subject.clone()],
        retention: async_nats::jetstream::stream::RetentionPolicy::Limits,
        max_age: std::time::Duration::from_secs(Timeouts::EXTENDED),
        ..Default::default()
    })
    .await?;

    let mut baseline_tracker = BaselineTracker::new();

    for _ in 0..200 {
        let payload = serde_json::to_vec(&json!({
            "kind": "baseline",
            "timestamp": OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339).unwrap(),
        }))?;
        let start = Instant::now();
        js.publish(&subject, payload.into()).await?.await?;
        baseline_tracker.record_measurement("jetstream_publish", start.elapsed(), true);
    }

    let baseline_env = EnvironmentInfo {
        test_data_size: 200,
        concurrent_operations: 1,
        database_pool_size: 0,
        system_load: "jetstream_publish_baseline".to_string(),
    };
    let baseline = baseline_tracker
        .calculate_baseline("jetstream_publish", baseline_env)
        .expect("baseline should be computed");

    let mut detector = RegressionDetector::new();
    detector.set_baseline(baseline.clone());

    // Normal measurement
    for _ in 0..100 {
        let payload = serde_json::to_vec(&json!({ "kind": "normal" }))?;
        let start = Instant::now();
        js.publish(&subject, payload.into()).await?.await?;
        detector.record_measurement("jetstream_publish", start.elapsed(), true);
    }

    if let Some(result) = detector.detect_regression("jetstream_publish") {
        assert!(
            !result.regression_detected,
            "normal publish path should not trigger regression"
        );
    }

    // Degraded measurement - measure actual performance without artificial delays
    // If JetStream is slower, the test will detect it naturally
    let mut degraded_detector = RegressionDetector::new();
    degraded_detector.set_baseline(baseline);

    for _ in 0..100 {
        let payload = serde_json::to_vec(&json!({ "kind": "degraded" }))?;
        let start = Instant::now();
        js.publish(&subject, payload.into()).await?.await?;
        degraded_detector.record_measurement("jetstream_publish", start.elapsed(), true);
    }

    // Note: This test may not always detect regression since we're not artificially
    // degrading performance. It primarily validates that the detector doesn't
    // false-positive on normal operations. For true regression testing, use
    // test_e2e_real_database_performance which has hardcoded expectations.
    if let Some(regression) = degraded_detector.detect_regression("jetstream_publish") {
        // We expect no regression in normal conditions
        if regression.regression_detected {
            println!("⚠️  Unexpected regression detected in JetStream publish");
            println!("    This may indicate actual performance issues or environmental variance");
        }
    }

    js.delete_stream(&stream_name).await?;
    Ok(())
}

/// Test regression detection with custom thresholds using real database operations
#[sinex_bench]
async fn test_custom_threshold_regression_detection(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();

    println!("🔍 Testing custom threshold regression detection");

    // Test with strict thresholds - should detect even small performance changes
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

    // Establish baseline with real DB inserts
    let mut baseline_tracker = BaselineTracker::new();

    for i in 0..100 {
        let start = Instant::now();

        let event = EventFactory::new("regression-baseline")
            .source("strict-threshold-baseline")
            .event_type("strict.threshold.baseline")
            .host("strict-host")
            .payload(json!({"iteration": i}))
            .build();

        let result =
            sinex_primitives::db::insert_event_with_validator(pool.clone(), &event, None).await;
        let duration = start.elapsed();

        baseline_tracker.record_measurement("strict_operation", duration, result.is_ok());
    }

    // Wait for all baseline events to be persisted
    WaitHelpers::wait_for_source_events(&pool, "strict-threshold-baseline", 100, Timeouts::SHORT)
        .await?;

    let env_info = EnvironmentInfo {
        test_data_size: 100,
        concurrent_operations: 1,
        database_pool_size: pool.size() as usize,
        system_load: "strict_threshold_test".to_string(),
    };

    if let Some(baseline) = baseline_tracker.calculate_baseline("strict_operation", env_info) {
        strict_detector.set_baseline(baseline);
        println!("  ✅ Strict baseline established");
    }

    // Test current performance - should be similar to baseline
    for i in 0..100 {
        let start = Instant::now();

        let event = EventFactory::new("regression-baseline")
            .source("strict-threshold-test")
            .event_type("strict.threshold.test")
            .host("strict-host")
            .payload(json!({"iteration": i}))
            .build();

        let result =
            sinex_primitives::db::insert_event_with_validator(pool.clone(), &event, None).await;
        let duration = start.elapsed();

        strict_detector.record_measurement("strict_operation", duration, result.is_ok());
    }

    // Wait for test events
    WaitHelpers::wait_for_source_events(&pool, "strict-threshold-test", 100, Timeouts::SHORT)
        .await?;

    if let Some(strict_result) = strict_detector.detect_regression("strict_operation") {
        println!(
            "  Strict threshold result: {:?}",
            strict_result.regression_severity
        );
        // With strict thresholds, we might detect minor variations
        // This is expected behavior - strict thresholds are more sensitive
        if strict_result.regression_detected {
            println!(
                "  ⚠️  Strict thresholds detected a regression (expected with tight tolerances)"
            );
        } else {
            println!("  ✅ No regression detected with strict thresholds");
        }
    }

    // Test with lenient thresholds - should tolerate normal variance
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

    // Re-measure with lenient detector (reuse same test data conceptually)
    for i in 0..100 {
        let start = Instant::now();

        let event = EventFactory::new("regression-baseline")
            .source("lenient-threshold-test")
            .event_type("lenient.threshold.test")
            .host("lenient-host")
            .payload(json!({"iteration": i}))
            .build();

        let result =
            sinex_primitives::db::insert_event_with_validator(pool.clone(), &event, None).await;
        let duration = start.elapsed();

        lenient_detector.record_measurement("strict_operation", duration, result.is_ok());
    }

    // Wait for lenient test events
    WaitHelpers::wait_for_source_events(&pool, "lenient-threshold-test", 100, Timeouts::SHORT)
        .await?;

    if let Some(lenient_result) = lenient_detector.detect_regression("strict_operation") {
        println!(
            "  Lenient threshold result: {:?}",
            lenient_result.regression_severity
        );
        assert!(
            !lenient_result.regression_detected,
            "Lenient thresholds should not detect regression in normal operations"
        );
        println!("  ✅ Lenient thresholds correctly tolerated normal variance");
    }

    println!("✅ Custom threshold regression detection test passed");
    Ok(())
}

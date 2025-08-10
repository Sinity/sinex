// # Checkpoint Performance Tests
//
// Tests checkpoint system performance including persistence speed,
// recovery time, and checkpoint consistency under load.
// Critical for automaton reliability and system recovery.

use color_eyre::eyre::Result;
use serde_json::json;
use sinex_satellite_sdk::checkpoint::{CheckpointManager, CheckpointState};
use sinex_satellite_sdk::stream_processor::Checkpoint;
use sinex_satellite_sdk::RedisStreamClient;
use sinex_test_utils::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::Mutex;

/// Checkpoint performance metrics
struct CheckpointMetrics {
    operation_times: HashMap<String, Vec<StdDuration>>,
    checkpoint_sizes: Vec<usize>,
    error_counts: HashMap<String, usize>,
    success_counts: HashMap<String, usize>,
    recovery_times: Vec<StdDuration>,
    consistency_checks: Vec<bool>,
    start_time: Instant,
}

impl CheckpointMetrics {
    fn new() -> Self {
        Self {
            operation_times: HashMap::new(),
            checkpoint_sizes: Vec::new(),
            error_counts: HashMap::new(),
            success_counts: HashMap::new(),
            recovery_times: Vec::new(),
            consistency_checks: Vec::new(),
            start_time: Instant::now(),
        }
    }

    fn record_operation(&mut self, operation: &str, duration: StdDuration, success: bool) {
        if success {
            *self
                .success_counts
                .entry(operation.to_string())
                .or_insert(0) += 1;
        } else {
            *self.error_counts.entry(operation.to_string()).or_insert(0) += 1;
        }

        self.operation_times
            .entry(operation.to_string())
            .or_insert_with(Vec::new)
            .push(duration);
    }

    fn record_checkpoint_size(&mut self, size: usize) {
        self.checkpoint_sizes.push(size);
    }

    fn record_recovery_time(&mut self, duration: StdDuration) {
        self.recovery_times.push(duration);
    }

    fn record_consistency_check(&mut self, consistent: bool) {
        self.consistency_checks.push(consistent);
    }

    fn average_latency(&self, operation: &str) -> StdDuration {
        if let Some(times) = self.operation_times.get(operation) {
            if !times.is_empty() {
                return times.iter().sum::<StdDuration>() / times.len() as u32;
            }
        }
        StdDuration::from_millis(0)
    }

    fn percentile_latency(&self, operation: &str, percentile: f64) -> StdDuration {
        if let Some(times) = self.operation_times.get(operation) {
            if !times.is_empty() {
                let mut sorted = times.clone();
                sorted.sort();
                let index =
                    ((sorted.len() as f64 * percentile / 100.0) as usize).min(sorted.len() - 1);
                return sorted[index];
            }
        }
        StdDuration::from_millis(0)
    }

    fn success_rate(&self, operation: &str) -> f64 {
        let success = self.success_counts.get(operation).unwrap_or(&0);
        let errors = self.error_counts.get(operation).unwrap_or(&0);
        let total = success + errors;
        if total > 0 {
            *success as f64 / total as f64 * 100.0
        } else {
            0.0
        }
    }

    fn average_checkpoint_size(&self) -> f64 {
        if self.checkpoint_sizes.is_empty() {
            0.0
        } else {
            self.checkpoint_sizes.iter().sum::<usize>() as f64 / self.checkpoint_sizes.len() as f64
        }
    }

    fn average_recovery_time(&self) -> StdDuration {
        if self.recovery_times.is_empty() {
            StdDuration::from_millis(0)
        } else {
            self.recovery_times.iter().sum::<StdDuration>() / self.recovery_times.len() as u32
        }
    }

    fn consistency_rate(&self) -> f64 {
        if self.consistency_checks.is_empty() {
            100.0
        } else {
            let consistent_count = self.consistency_checks.iter().filter(|&&x| x).count();
            consistent_count as f64 / self.consistency_checks.len() as f64 * 100.0
        }
    }

    fn print_summary(&self) {
        println!("\n📊 Checkpoint Performance Summary:");
        println!("Total test duration: {:?}", self.start_time.elapsed());
        println!(
            "Average checkpoint size: {:.1} bytes",
            self.average_checkpoint_size()
        );
        println!("Average recovery time: {:?}", self.average_recovery_time());
        println!("Consistency rate: {:.2}%", self.consistency_rate());

        for operation in self.operation_times.keys() {
            println!("\n🔍 Operation: {}", operation);
            println!(
                "  - Success count: {}",
                self.success_counts.get(operation).unwrap_or(&0)
            );
            println!(
                "  - Error count: {}",
                self.error_counts.get(operation).unwrap_or(&0)
            );
            println!("  - Success rate: {:.2}%", self.success_rate(operation));
            println!("  - Average latency: {:?}", self.average_latency(operation));
            println!(
                "  - P95 latency: {:?}",
                self.percentile_latency(operation, 95.0)
            );
            println!(
                "  - P99 latency: {:?}",
                self.percentile_latency(operation, 99.0)
            );
        }
    }
}

// =============================================================================
// Basic Checkpoint Performance Tests
// =============================================================================

/// Test basic checkpoint save and load performance
#[sinex_bench]
async fn test_checkpoint_save_load_performance(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();
    let mut metrics = CheckpointMetrics::new();

    let processor_name = "performance-test-automaton";
    let consumer_group = "performance-test-group";

    println!("💾 Testing checkpoint save/load performance");

    // Test various checkpoint sizes
    let checkpoint_tests = vec![
        (
            100,
            "Small",
            json!({"processed": 100, "data": "x".repeat(100)}),
        ),
        (
            1_000,
            "Medium",
            json!({"processed": 1000, "data": "x".repeat(1000), "counters": (0..50).collect::<Vec<_>>()}),
        ),
        (
            10_000,
            "Large",
            json!({"processed": 10000, "data": "x".repeat(10000), "counters": (0..500).collect::<Vec<_>>()}),
        ),
        (
            50_000,
            "Extra Large",
            json!({"processed": 50000, "data": "x".repeat(50000), "detailed_state": (0..1000).map(|i| format!("item-{}", i)).collect::<Vec<_>>()}),
        ),
    ];

    for (expected_size, size_label, checkpoint_data) in checkpoint_tests {
        println!("\n📦 Testing {} checkpoints", size_label);

        let checkpoint_iterations = 50;

        for i in 0..checkpoint_iterations {
            // Create checkpoint state
            let checkpoint_state = CheckpointState {
                checkpoint: sinex_satellite_sdk::stream_processor::Checkpoint::Stream {
                    message_id: format!("test-id-{}", i),
                    event_id: None,
                },
                processed_count: i as u64,
                last_activity: chrono::Utc::now(),
                data: Some(checkpoint_data.clone()),
                version: 2,
            };

            let serialized = serde_json::to_string(&checkpoint_state).unwrap_or_default();
            metrics.record_checkpoint_size(serialized.len());

            // Test save performance
            let save_start = Instant::now();

            let save_result = sqlx::query!(
                r#"
                INSERT INTO core.processor_checkpoints
                (processor_name, consumer_group, last_processed_id, state_data)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (processor_name, consumer_group)
                DO UPDATE SET
                    last_processed_id = EXCLUDED.last_processed_id,
                    state_data = EXCLUDED.state_data,
                    updated_at = CURRENT_TIMESTAMP
                "#,
                processor_name,
                consumer_group,
                checkpoint_state.last_processed_id(),
                checkpoint_state.data
            )
            .execute(pool)
            .await;

            let save_duration = save_start.elapsed();
            let operation_key = format!("save_{}", size_label);
            metrics.record_operation(&operation_key, save_duration, save_result.is_ok());

            if save_result.is_err() {
                println!("  Save {} failed: {:?}", i, save_result.err());
                continue;
            }

            // Test load performance
            let load_start = Instant::now();

            let load_result = sqlx::query!(
                "SELECT last_processed_id, state_data FROM core.processor_checkpoints WHERE processor_name = $1 AND consumer_group = $2",
                processor_name,
                consumer_group
            ).fetch_optional(pool).await;

            let load_duration = load_start.elapsed();
            let load_operation_key = format!("load_{}", size_label);

            match load_result {
                Ok(Some(row)) => {
                    metrics.record_operation(&load_operation_key, load_duration, true);

                    // Verify data consistency
                    let consistent = row.last_processed_id == checkpoint_state.last_processed_id()
                        && row.state_data == checkpoint_state.data;
                    metrics.record_consistency_check(consistent);

                    if !consistent {
                        println!("  Consistency check failed for iteration {}", i);
                    }
                }
                Ok(None) => {
                    metrics.record_operation(&load_operation_key, load_duration, false);
                    println!("  Load {} returned no data", i);
                }
                Err(e) => {
                    metrics.record_operation(&load_operation_key, load_duration, false);
                    println!("  Load {} failed: {}", i, e);
                }
            }

            if i % 10 == 0 {
                println!("    Completed {} iterations for {}", i + 1, size_label);
            }
        }

        println!("  {} checkpoint tests completed", size_label);
        println!(
            "    Save avg latency: {:?}",
            metrics.average_latency(&format!("save_{}", size_label))
        );
        println!(
            "    Load avg latency: {:?}",
            metrics.average_latency(&format!("load_{}", size_label))
        );
    }

    metrics.print_summary();

    // Performance assertions
    for (_, size_label, _) in &checkpoint_tests {
        let save_key = format!("save_{}", size_label);
        let load_key = format!("load_{}", size_label);

        assert!(
            metrics.success_rate(&save_key) > 95.0,
            "{} save success rate should be > 95%",
            size_label
        );
        assert!(
            metrics.success_rate(&load_key) > 95.0,
            "{} load success rate should be > 95%",
            size_label
        );

        // Performance thresholds scale with size
        let max_save_latency = match size_label.as_str() {
            "Small" => StdDuration::from_millis(20),
            "Medium" => StdDuration::from_millis(50),
            "Large" => StdDuration::from_millis(100),
            "Extra Large" => StdDuration::from_millis(200),
            _ => StdDuration::from_millis(100),
        };

        assert!(
            metrics.average_latency(&save_key) < max_save_latency,
            "{} save latency should be < {:?}",
            size_label,
            max_save_latency
        );
        assert!(
            metrics.average_latency(&load_key) < StdDuration::from_millis(50),
            "{} load latency should be < 50ms",
            size_label
        );
    }

    assert!(
        metrics.consistency_rate() > 99.0,
        "Checkpoint consistency rate should be > 99%"
    );

    println!("✅ Checkpoint save/load performance test passed");
    Ok(())
}

/// Test checkpoint recovery performance
#[sinex_bench]
async fn test_checkpoint_recovery_performance(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();
    let mut metrics = CheckpointMetrics::new();

    println!("🔄 Testing checkpoint recovery performance");

    // Setup: Create multiple automatons with checkpoints
    let automaton_count = 20;
    let consumer_group = "recovery-test-group";

    println!(
        "  Setting up {} automatons with checkpoints",
        automaton_count
    );

    for i in 0..automaton_count {
        let processor_name = format!("recovery-test-automaton-{}", i);
        let checkpoint_data = json!({
            "processed_count": i * 100,
            "last_event_time": chrono::Utc::now().to_rfc3339(),
            "state": format!("state-data-{}", i),
            "counters": (0..i).collect::<Vec<_>>(),
            "metadata": {
                "created": chrono::Utc::now().to_rfc3339(),
                "version": "1.0",
                "type": "recovery-test"
            }
        });

        sqlx::query!(
            r#"
            INSERT INTO core.processor_checkpoints
            (processor_name, consumer_group, last_processed_id, state_data)
            VALUES ($1, $2, $3, $4)
            "#,
            processor_name,
            consumer_group,
            format!("event-id-{}", i * 100),
            checkpoint_data
        )
        .execute(pool)
        .await?;
    }

    println!("  Testing individual checkpoint recovery");

    // Test individual checkpoint recovery
    for i in 0..automaton_count {
        let processor_name = format!("recovery-test-automaton-{}", i);

        let recovery_start = Instant::now();

        let result = sqlx::query!(
            "SELECT last_processed_id, state_data FROM core.processor_checkpoints WHERE processor_name = $1 AND consumer_group = $2",
            processor_name,
            consumer_group
        ).fetch_optional(pool).await;

        let recovery_duration = recovery_start.elapsed();
        metrics.record_recovery_time(recovery_duration);

        match result {
            Ok(Some(row)) => {
                metrics.record_operation("individual_recovery", recovery_duration, true);

                // Verify recovery data
                if let Some(state_data) = &row.state_data {
                    let state_size = serde_json::to_string(state_data).unwrap_or_default().len();
                    metrics.record_checkpoint_size(state_size);

                    // Consistency check
                    let expected_processed = i * 100;
                    let consistent = state_data
                        .get("processed_count")
                        .and_then(|v| v.as_u64())
                        .map(|v| v == expected_processed as u64)
                        .unwrap_or(false);

                    metrics.record_consistency_check(consistent);
                } else {
                    metrics.record_consistency_check(false);
                }
            }
            Ok(None) => {
                metrics.record_operation("individual_recovery", recovery_duration, false);
                metrics.record_consistency_check(false);
            }
            Err(e) => {
                metrics.record_operation("individual_recovery", recovery_duration, false);
                metrics.record_consistency_check(false);
                println!("  Recovery failed for {}: {}", processor_name, e);
            }
        }

        if i % 5 == 0 {
            println!("    Recovered {} automatons", i + 1);
        }
    }

    // Test bulk checkpoint recovery
    println!("  Testing bulk checkpoint recovery");

    let bulk_recovery_start = Instant::now();

    let bulk_result = sqlx::query!(
        "SELECT processor_name, last_processed_id, state_data FROM core.processor_checkpoints WHERE consumer_group = $1",
        consumer_group
    ).fetch_all(pool).await;

    let bulk_recovery_duration = bulk_recovery_start.elapsed();

    match bulk_result {
        Ok(rows) => {
            metrics.record_operation("bulk_recovery", bulk_recovery_duration, true);
            println!("    Bulk recovery loaded {} checkpoints", rows.len());

            // Verify all expected automatons were recovered
            let recovered_count = rows.len();
            let consistency_ok = recovered_count == automaton_count;
            metrics.record_consistency_check(consistency_ok);

            // Calculate total state size
            let total_state_size: usize = rows
                .iter()
                .filter_map(|row| row.state_data.as_ref())
                .map(|state| serde_json::to_string(state).unwrap_or_default().len())
                .sum();

            println!("    Total state size: {} bytes", total_state_size);
            metrics.record_checkpoint_size(total_state_size);
        }
        Err(e) => {
            metrics.record_operation("bulk_recovery", bulk_recovery_duration, false);
            metrics.record_consistency_check(false);
            println!("    Bulk recovery failed: {}", e);
        }
    }

    // Test recovery under simulated failure conditions
    println!("  Testing recovery under failure simulation");

    // Simulate concurrent checkpoint updates during recovery
    let concurrent_recovery_handles = (0..5)
        .map(|thread_id| {
            let pool_clone = pool.clone();
            let consumer_group = consumer_group.to_string();

            tokio::spawn(async move {
                let mut thread_recoveries = 0;
                let mut thread_errors = 0;

                for i in 0..10 {
                    let processor_name = format!("concurrent-recovery-{}-{}", thread_id, i);

                    // Create checkpoint
                    let checkpoint_data = json!({
                        "thread_id": thread_id,
                        "iteration": i,
                        "timestamp": chrono::Utc::now().to_rfc3339()
                    });

                    let save_result = sqlx::query!(
                        r#"
                        INSERT INTO core.processor_checkpoints
                        (processor_name, consumer_group, last_processed_id, state_data)
                        VALUES ($1, $2, $3, $4)
                        "#,
                        processor_name,
                        consumer_group,
                        format!("concurrent-event-{}-{}", thread_id, i),
                        checkpoint_data
                    ).execute(&pool_clone).await;

                    if save_result.is_ok() {
                        // Immediately try to recover
                        let recovery_result = sqlx::query!(
                            "SELECT last_processed_id, state_data FROM core.processor_checkpoints WHERE processor_name = $1",
                            processor_name
                        ).fetch_optional(&pool_clone).await;

                        if recovery_result.is_ok() {
                            thread_recoveries += 1;
                        } else {
                            thread_errors += 1;
                        }
                    } else {
                        thread_errors += 1;
                    }

                    // Small delay
                    tokio::time::sleep(StdDuration::from_millis(10)).await;
                }

                (thread_recoveries, thread_errors)
            })
        })
        .collect::<Vec<_>>();

    let concurrent_results = futures::future::join_all(concurrent_recovery_handles).await;

    let mut total_concurrent_recoveries = 0;
    let mut total_concurrent_errors = 0;

    for result in concurrent_results {
        if let Ok((recoveries, errors)) = result {
            total_concurrent_recoveries += recoveries;
            total_concurrent_errors += errors;
        }
    }

    println!(
        "    Concurrent recovery: {} successes, {} errors",
        total_concurrent_recoveries, total_concurrent_errors
    );

    let concurrent_success_rate = if total_concurrent_recoveries + total_concurrent_errors > 0 {
        total_concurrent_recoveries as f64
            / (total_concurrent_recoveries + total_concurrent_errors) as f64
            * 100.0
    } else {
        0.0
    };

    metrics.record_consistency_check(concurrent_success_rate > 90.0);

    metrics.print_summary();

    // Performance assertions
    assert!(
        metrics.success_rate("individual_recovery") > 95.0,
        "Individual recovery success rate should be > 95%"
    );
    assert!(
        metrics.success_rate("bulk_recovery") > 95.0,
        "Bulk recovery success rate should be > 95%"
    );
    assert!(
        metrics.average_latency("individual_recovery") < StdDuration::from_millis(30),
        "Individual recovery latency should be < 30ms"
    );
    assert!(
        metrics.average_latency("bulk_recovery") < StdDuration::from_millis(100),
        "Bulk recovery latency should be < 100ms"
    );
    assert!(
        metrics.consistency_rate() > 95.0,
        "Recovery consistency rate should be > 95%"
    );
    assert!(
        concurrent_success_rate > 90.0,
        "Concurrent recovery success rate should be > 90%"
    );

    println!("✅ Checkpoint recovery performance test passed");
    Ok(())
}

/// Test checkpoint performance under high frequency updates
#[sinex_bench]
async fn test_high_frequency_checkpoint_updates(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();
    let shared_metrics = Arc::new(Mutex::new(CheckpointMetrics::new()));

    println!("⚡ Testing high-frequency checkpoint updates");

    let automaton_count = 10;
    let updates_per_automaton = 100;
    let update_frequency = StdDuration::from_millis(10); // 100 updates/sec per automaton

    println!(
        "  Configuration: {} automatons, {} updates each, every {:?}",
        automaton_count, updates_per_automaton, update_frequency
    );

    let worker_handles = (0..automaton_count)
        .map(|automaton_id| {
            let pool_clone = pool.clone();
            let metrics = shared_metrics.clone();

            tokio::spawn(async move {
                let processor_name = format!("high-freq-automaton-{}", automaton_id);
                let consumer_group = "high-frequency-group";

                let mut successes = 0;
                let mut errors = 0;

                for update_id in 0..updates_per_automaton {
                    let update_start = Instant::now();

                    let checkpoint_data = json!({
                        "automaton_id": automaton_id,
                        "update_id": update_id,
                        "processed_count": update_id + 1,
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                        "state_snapshot": format!("state-{}-{}", automaton_id, update_id),
                        "performance_data": {
                            "operations_completed": update_id * 10,
                            "last_operation_time": chrono::Utc::now().to_rfc3339(),
                            "metrics": (0..10).collect::<Vec<_>>()
                        }
                    });

                    let result = sqlx::query!(
                        r#"
                        INSERT INTO core.processor_checkpoints
                        (processor_name, consumer_group, last_processed_id, state_data)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (processor_name, consumer_group)
                        DO UPDATE SET
                            last_processed_id = EXCLUDED.last_processed_id,
                            state_data = EXCLUDED.state_data,
                            updated_at = CURRENT_TIMESTAMP
                        "#,
                        processor_name,
                        consumer_group,
                        format!("event-{}-{}", automaton_id, update_id),
                        checkpoint_data
                    )
                    .execute(&pool_clone)
                    .await;

                    let update_duration = update_start.elapsed();

                    if result.is_ok() {
                        successes += 1;
                        let mut metrics_lock = metrics.lock().await;
                        metrics_lock.record_operation(
                            "high_frequency_update",
                            update_duration,
                            true,
                        );

                        let state_size = serde_json::to_string(&checkpoint_data)
                            .unwrap_or_default()
                            .len();
                        metrics_lock.record_checkpoint_size(state_size);
                    } else {
                        errors += 1;
                        let mut metrics_lock = metrics.lock().await;
                        metrics_lock.record_operation(
                            "high_frequency_update",
                            update_duration,
                            false,
                        );

                        if errors <= 3 {
                            println!(
                                "  Automaton {} update {} failed: {:?}",
                                automaton_id,
                                update_id,
                                result.err()
                            );
                        }
                    }

                    // Maintain update frequency
                    tokio::time::sleep(update_frequency).await;

                    if update_id % 20 == 0 {
                        println!(
                            "    Automaton {} completed {} updates",
                            automaton_id,
                            update_id + 1
                        );
                    }
                }

                println!(
                    "  Automaton {} finished: {} successes, {} errors",
                    automaton_id, successes, errors
                );
                (successes, errors)
            })
        })
        .collect::<Vec<_>>();

    // Wait for all automatons to complete
    let results = futures::future::join_all(worker_handles).await;

    let mut total_successes = 0;
    let mut total_errors = 0;

    for result in results {
        if let Ok((successes, errors)) = result {
            total_successes += successes;
            total_errors += errors;
        }
    }

    println!(
        "  High-frequency updates completed: {} successes, {} errors",
        total_successes, total_errors
    );

    // Test recovery performance after high-frequency updates
    println!("  Testing recovery after high-frequency updates");

    let recovery_start = Instant::now();

    let all_checkpoints = sqlx::query!(
        "SELECT processor_name, last_processed_id, state_data FROM core.processor_checkpoints WHERE consumer_group = 'high-frequency-group'"
    ).fetch_all(pool).await?;

    let recovery_duration = recovery_start.elapsed();

    {
        let mut metrics_lock = shared_metrics.lock().await;
        metrics_lock.record_operation("post_highfreq_recovery", recovery_duration, true);
        metrics_lock.record_recovery_time(recovery_duration);

        // Verify all automatons have latest checkpoints
        let expected_automatons = automaton_count;
        let recovered_automatons = all_checkpoints.len();
        let consistency_ok = recovered_automatons == expected_automatons;
        metrics_lock.record_consistency_check(consistency_ok);

        println!(
            "    Recovered {}/{} automatons in {:?}",
            recovered_automatons, expected_automatons, recovery_duration
        );
    }

    let final_metrics = shared_metrics.lock().await;
    final_metrics.print_summary();

    // Performance assertions
    let success_rate = total_successes as f64 / (total_successes + total_errors) as f64 * 100.0;
    assert!(
        success_rate > 95.0,
        "High-frequency update success rate should be > 95%"
    );
    assert!(
        final_metrics.average_latency("high_frequency_update") < StdDuration::from_millis(50),
        "High-frequency update latency should be < 50ms"
    );
    assert!(
        final_metrics.percentile_latency("high_frequency_update", 95.0)
            < StdDuration::from_millis(100),
        "High-frequency update P95 latency should be < 100ms"
    );
    assert!(
        final_metrics.average_latency("post_highfreq_recovery") < StdDuration::from_millis(100),
        "Recovery after high-frequency updates should be < 100ms"
    );
    assert!(
        final_metrics.consistency_rate() > 95.0,
        "High-frequency checkpoint consistency should be > 95%"
    );

    println!("✅ High-frequency checkpoint update test passed");
    Ok(())
}

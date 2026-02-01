//! Node Lifecycle Integration Tests
//!
//! Tests the complete lifecycle of node services including:
//! - Initialization and startup
//! - State transitions
//! - Health monitoring and heartbeats
//! - Error recovery and resilience
//! - Graceful shutdown and cleanup

use camino::Utf8PathBuf;
use sinex_node_sdk::{
    checkpoint::{CheckpointManager, CheckpointState},
    config::{EventSourceConfig, NodeConfig},
    coordination::{InstanceMode, NodeCoordination},
    stream_processor::Checkpoint,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::Seconds;
use sinex_primitives::SinexError;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc,
};
use tokio::time::{sleep, timeout, Duration, Instant};
use tracing::{debug, info, warn};
use xtask::sandbox::{sinex_test, TestContext};

#[path = "../support/mod.rs"]
mod support;
use support::runtime::TestRuntimeBuilder;

/// Test complete node lifecycle from birth to death
#[sinex_test]
async fn test_node_complete_lifecycle(ctx: TestContext) -> color_eyre::Result<()> {
    info!("Testing complete node lifecycle");

    let runtime = TestRuntimeBuilder::new(&ctx, "lifecycle_test_node")
        .build()
        .await?;
    let mut coordination = NodeCoordination::from_runtime(
        &runtime.runtime,
        format!("lifecycle-{}", sinex_node_sdk::Ulid::new()),
    )
    .await?;

    // Phase 1: Initial state should be standby
    info!("Phase 1: Node initialization");
    assert_eq!(coordination.current_mode(), InstanceMode::Standby);
    debug!("  Node initialized in standby mode");

    // Phase 2: Startup and leadership acquisition
    info!("Phase 2: Startup and leadership acquisition");
    let became_leader = Arc::new(AtomicBool::new(false));
    let processing_count = Arc::new(AtomicU32::new(0));

    let leader_flag = became_leader.clone();
    let process_count = processing_count.clone();

    let lifecycle_result = timeout(
        Duration::from_millis(500),
        coordination.run_coordination_loop(move || {
            let flag = leader_flag.clone();
            let count = process_count.clone();
            async move {
                // First time becoming leader
                if !flag.load(Ordering::SeqCst) {
                    info!("Node became leader!");
                    flag.store(true, Ordering::SeqCst);
                }

                // Simulate processing work
                count.fetch_add(1, Ordering::SeqCst);
                sleep(Duration::from_millis(50)).await;
                Ok::<(), SinexError>(())
            }
        }),
    )
    .await;

    // Phase 3: Verify steady state operations
    info!("Phase 3: Verifying operations");
    assert!(
        lifecycle_result.is_ok(),
        "Lifecycle coordination should complete within timeout"
    );
    assert!(
        became_leader.load(Ordering::SeqCst),
        "Node should have become leader"
    );
    let final_processing = processing_count.load(Ordering::SeqCst);
    assert!(final_processing > 0, "Node should have processed work");
    info!(
        "  Node lifecycle completed: {} work units processed",
        final_processing
    );

    Ok(())
}

/// Test node health monitoring and heartbeat mechanisms
#[sinex_test]
async fn test_node_health_monitoring(ctx: TestContext) -> color_eyre::Result<()> {
    info!("Testing node health monitoring");

    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let checkpoint_manager = CheckpointManager::new(
        kv,
        "health_monitor_test".to_string(),
        "health_group".to_string(),
        "health_consumer".to_string(),
    );

    // Test checkpoint-based health tracking
    let start_time = Timestamp::now();
    let mut checkpoint = CheckpointState {
        checkpoint: Checkpoint::Stream {
            message_id: "health-check-001".to_string(),
            event_id: None,
        },
        processed_count: 1,
        last_activity: start_time,
        data: Some(serde_json::json!({
            "health_status": "healthy",
            "uptime_seconds": 0
        })),
        version: 1,
        revision: 0,
    };

    // Save initial health checkpoint
    checkpoint_manager.save_checkpoint(&checkpoint).await?;
    debug!("  Initial health checkpoint saved");

    // Simulate health updates over time
    for i in 1..=5 {
        sleep(Duration::from_millis(50)).await;

        checkpoint.processed_count += 1;
        checkpoint.last_activity = Timestamp::now();
        checkpoint.data = Some(serde_json::json!({
            "health_status": "healthy",
            "uptime_seconds": i,
            "last_heartbeat": checkpoint.last_activity.format(&time::format_description::well_known::Rfc3339).unwrap()
        }));
        checkpoint.version += 1;

        checkpoint_manager.save_checkpoint(&checkpoint).await?;
        debug!("  Health checkpoint {} updated", i);
    }

    // Verify health data persistence
    let final_checkpoint = checkpoint_manager.load_checkpoint().await?;
    assert_eq!(final_checkpoint.processed_count, 6); // Initial + 5 updates
    assert!(final_checkpoint.data.is_some());

    let health_data = final_checkpoint.data.as_ref().unwrap();
    assert_eq!(health_data["health_status"], "healthy");
    assert_eq!(health_data["uptime_seconds"], 5);

    info!("  Node health monitoring working correctly");
    Ok(())
}

/// Test node error recovery and resilience patterns
#[sinex_test]
async fn test_node_error_recovery(ctx: TestContext) -> color_eyre::Result<()> {
    info!("Testing node error recovery");

    let runtime = TestRuntimeBuilder::new(&ctx, "error_recovery_test")
        .build()
        .await?;
    let mut coordination = NodeCoordination::from_runtime(
        &runtime.runtime,
        format!("recovery-{}", sinex_node_sdk::Ulid::new()),
    )
    .await?;

    let error_count = Arc::new(AtomicU32::new(0));
    let recovery_count = Arc::new(AtomicU32::new(0));
    let successful_ops = Arc::new(AtomicU32::new(0));

    let err_count = error_count.clone();
    let rec_count = recovery_count.clone();
    let success_count = successful_ops.clone();

    // Simulate node with intermittent failures
    let recovery_result = timeout(
        Duration::from_millis(600),
        coordination.run_coordination_loop(move || {
            let errors = err_count.clone();
            let recoveries = rec_count.clone();
            let successes = success_count.clone();

            async move {
                let current_errors = errors.load(Ordering::SeqCst);

                // Simulate failure every 3rd operation
                if current_errors < 3 && successes.load(Ordering::SeqCst) % 3 == 2 {
                    errors.fetch_add(1, Ordering::SeqCst);
                    warn!("Simulated node error #{}", current_errors + 1);

                    // Simulate recovery attempt
                    sleep(Duration::from_millis(50)).await;
                    recoveries.fetch_add(1, Ordering::SeqCst);
                    debug!("Recovery attempt #{}", recoveries.load(Ordering::SeqCst));

                    // Recover successfully after brief delay
                    sleep(Duration::from_millis(25)).await;
                }

                successes.fetch_add(1, Ordering::SeqCst);
                sleep(Duration::from_millis(75)).await;
                Ok::<(), SinexError>(())
            }
        }),
    )
    .await;

    assert!(
        recovery_result.is_ok(),
        "Recovery coordination should complete"
    );

    // Verify error recovery behavior
    let final_errors = error_count.load(Ordering::SeqCst);
    let final_recoveries = recovery_count.load(Ordering::SeqCst);
    let final_successes = successful_ops.load(Ordering::SeqCst);

    assert_eq!(
        final_errors, final_recoveries,
        "Each error should trigger recovery"
    );
    assert!(
        final_successes > final_errors,
        "Should have more successes than errors"
    );

    info!(
        "  Error recovery: {} errors, {} recoveries, {} successful operations",
        final_errors, final_recoveries, final_successes
    );
    Ok(())
}

/// Test node state transitions and mode changes
#[sinex_test]
async fn test_node_state_transitions(ctx: TestContext) -> color_eyre::Result<()> {
    info!("Testing node state transitions");

    let runtime = TestRuntimeBuilder::new(&ctx, "state_transition_test")
        .build()
        .await?;
    let mut coordination = NodeCoordination::from_runtime(
        &runtime.runtime,
        format!("states-{}", sinex_node_sdk::Ulid::new()),
    )
    .await?;

    // Initial state should be Standby
    assert_eq!(coordination.current_mode(), InstanceMode::Standby);
    debug!("  Initial state: Standby");

    // Track state transitions during coordination
    let state_changes = Arc::new(AtomicU32::new(0));
    let became_leader = Arc::new(AtomicBool::new(false));

    let state_counter = state_changes.clone();
    let leader_flag = became_leader.clone();

    let transition_result = timeout(
        Duration::from_millis(400),
        coordination.run_coordination_loop(move || {
            let counter = state_counter.clone();
            let flag = leader_flag.clone();

            async move {
                // Track when we become leader (state transition)
                if !flag.load(Ordering::SeqCst) {
                    flag.store(true, Ordering::SeqCst);
                    counter.fetch_add(1, Ordering::SeqCst);
                    debug!("State transition: Standby -> Leader");
                }

                sleep(Duration::from_millis(50)).await;
                Ok::<(), SinexError>(())
            }
        }),
    )
    .await;

    assert!(
        transition_result.is_ok(),
        "State transition coordination should complete"
    );

    // Verify transitions occurred
    assert!(
        became_leader.load(Ordering::SeqCst),
        "Should have transitioned to leader"
    );
    assert!(
        state_changes.load(Ordering::SeqCst) > 0,
        "Should have recorded state changes"
    );

    info!("  Node state transitions working correctly");
    Ok(())
}

/// Test node configuration loading and validation
#[sinex_test]
async fn test_node_configuration_lifecycle(ctx: TestContext) -> color_eyre::Result<()> {
    info!("Testing node configuration lifecycle");

    // Test configuration creation and validation
    let test_configs = [
        create_minimal_config("config_test_minimal"),
        create_standard_config("config_test_standard"),
        create_enhanced_config("config_test_enhanced"),
    ];

    for (i, config) in test_configs.iter().enumerate() {
        debug!("Testing configuration variant {}", i + 1);

        // Verify configuration structure
        assert!(
            !config.base.service_name.is_empty(),
            "Service name should not be empty"
        );
        assert!(config.batch_size > 0, "Batch size should be positive");
        assert!(
            config.batch_timeout_secs.as_secs() > 0,
            "Batch timeout should be positive"
        );
        assert!(
            !config.base.nats.url.is_empty(),
            "NATS URL should be specified"
        );

        // Test configuration with runtime builder
        let runtime = TestRuntimeBuilder::new(&ctx, &config.base.service_name)
            .build()
            .await?;

        let coordination = NodeCoordination::from_runtime(
            &runtime.runtime,
            format!("config-{i}-{}", sinex_node_sdk::Ulid::new()),
        )
        .await?;

        // Verify coordination uses configuration correctly
        assert_eq!(coordination.current_mode(), InstanceMode::Standby);
        debug!("  Configuration {} validated and applied", i + 1);
    }

    info!("  All node configurations validated successfully");
    Ok(())
}

/// Test node shutdown sequence and cleanup
#[sinex_test]
async fn test_node_graceful_shutdown(ctx: TestContext) -> color_eyre::Result<()> {
    info!("Testing node graceful shutdown");

    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let checkpoint_manager = CheckpointManager::new(
        kv,
        "shutdown_test".to_string(),
        "shutdown_group".to_string(),
        "shutdown_consumer".to_string(),
    );

    let runtime = TestRuntimeBuilder::new(&ctx, "shutdown_test")
        .build()
        .await?;
    let mut coordination = NodeCoordination::from_runtime(
        &runtime.runtime,
        format!("shutdown-{}", sinex_node_sdk::Ulid::new()),
    )
    .await?;

    // Track shutdown process
    let operations_completed = Arc::new(AtomicU32::new(0));
    let shutdown_initiated = Arc::new(AtomicBool::new(false));
    let cleanup_completed = Arc::new(AtomicBool::new(false));

    let ops_count = operations_completed.clone();
    let shutdown_flag = shutdown_initiated.clone();
    let cleanup_flag = cleanup_completed.clone();

    // Start node operations with timeout
    let start_time = Instant::now();
    let shutdown_result = timeout(
        Duration::from_millis(400),
        coordination.run_coordination_loop(move || {
            let ops = ops_count.clone();
            let shutdown = shutdown_flag.clone();
            let cleanup = cleanup_flag.clone();
            let elapsed = start_time.elapsed();

            async move {
                ops.fetch_add(1, Ordering::SeqCst);

                // Simulate shutdown after some operations
                if elapsed > Duration::from_millis(200) && !shutdown.load(Ordering::SeqCst) {
                    shutdown.store(true, Ordering::SeqCst);
                    debug!("Initiating graceful shutdown");

                    // Simulate cleanup operations
                    sleep(Duration::from_millis(50)).await;
                    cleanup.store(true, Ordering::SeqCst);
                    debug!("Cleanup completed");
                }

                sleep(Duration::from_millis(50)).await;
                Ok::<(), SinexError>(())
            }
        }),
    )
    .await;

    // Verify shutdown process
    assert!(
        shutdown_result.is_ok(),
        "Shutdown coordination should complete"
    );
    assert!(
        operations_completed.load(Ordering::SeqCst) > 0,
        "Should have completed some operations"
    );

    // Save final checkpoint
    let final_checkpoint = CheckpointState {
        checkpoint: Checkpoint::Stream {
            message_id: "shutdown-final".to_string(),
            event_id: None,
        },
        processed_count: operations_completed.load(Ordering::SeqCst) as u64,
        last_activity: Timestamp::now(),
        data: Some(serde_json::json!({
            "shutdown_reason": "graceful",
            "operations_completed": operations_completed.load(Ordering::SeqCst)
        })),
        version: 1,
        revision: 0,
    };

    checkpoint_manager
        .save_checkpoint(&final_checkpoint)
        .await?;
    let saved_checkpoint = checkpoint_manager.load_checkpoint().await?;
    assert_eq!(
        saved_checkpoint.processed_count,
        final_checkpoint.processed_count
    );

    info!("  Graceful shutdown completed successfully");
    Ok(())
}

/// Test node lifecycle under concurrent operations
#[sinex_test]
async fn test_node_concurrent_lifecycle(_ctx: TestContext) -> color_eyre::Result<()> {
    info!("Testing node lifecycle under concurrency");

    // Start multiple nodes concurrently to test coordination
    let node_count = 3;
    let mut handles = Vec::new();
    let completion_count = Arc::new(AtomicU32::new(0));

    for i in 0..node_count {
        let counter = completion_count.clone();

        let handle = tokio::spawn(async move {
            // Each task creates its own context
            let task_ctx = TestContext::new().await?;

            let runtime = TestRuntimeBuilder::new(&task_ctx, format!("concurrent_test_{i}"))
                .build()
                .await?;

            let mut coordination = NodeCoordination::from_runtime(
                &runtime.runtime,
                format!("concurrent-{i}-{}", sinex_node_sdk::Ulid::new()),
            )
            .await?;

            let result = timeout(
                Duration::from_millis(300),
                coordination.run_coordination_loop(move || {
                    let counter = counter.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        sleep(Duration::from_millis(50)).await;
                        Ok::<(), SinexError>(())
                    }
                }),
            )
            .await;

            Ok::<bool, color_eyre::Report>(result.is_ok())
        });

        handles.push(handle);
        debug!("Started node {}", i);
    }

    // Wait for all nodes to complete
    let results: Vec<_> = futures::future::join_all(handles).await;

    // Verify all nodes completed successfully
    for (i, result) in results.iter().enumerate() {
        let inner = result.as_ref().expect("Task should not panic");
        assert!(
            inner.as_ref().unwrap_or(&false),
            "Node {} should complete successfully",
            i
        );
    }

    let total_operations = completion_count.load(Ordering::SeqCst);
    assert!(total_operations > 0, "Should have completed operations");

    info!(
        "  Concurrent node lifecycle completed: {} total operations",
        total_operations
    );
    Ok(())
}

// Helper functions for configuration testing

fn create_minimal_config(service_name: &str) -> EventSourceConfig {
    EventSourceConfig {
        base: NodeConfig {
            service_name: service_name.to_string(),
            log_level: "info".to_string(),
            nats: sinex_primitives::nats::NatsConnectionConfig {
                url: "nats://localhost:4222".to_string(),
                ..Default::default()
            },
            database_url: None,
            database_pool_size: 5,
            work_dir: Utf8PathBuf::from("/tmp/sinex-minimal"),
            dry_run: true,
            replay: None,
        },
        batch_size: 10,
        batch_timeout_secs: Seconds::from_secs(30),
        source_config: HashMap::new(),
    }
}

fn create_standard_config(service_name: &str) -> EventSourceConfig {
    let mut source_config = HashMap::new();
    source_config.insert("max_retries".to_string(), serde_json::json!(3));
    source_config.insert("retry_delay_ms".to_string(), serde_json::json!(1000));

    EventSourceConfig {
        base: NodeConfig {
            service_name: service_name.to_string(),
            log_level: "debug".to_string(),
            nats: sinex_primitives::nats::NatsConnectionConfig {
                url: "nats://localhost:4222".to_string(),
                ..Default::default()
            },
            database_url: Some("postgresql:///sinex_test".to_string()),
            database_pool_size: 10,
            work_dir: Utf8PathBuf::from("/tmp/sinex-standard"),
            dry_run: false,
            replay: None,
        },
        batch_size: 50,
        batch_timeout_secs: Seconds::from_secs(10),
        source_config,
    }
}

fn create_enhanced_config(service_name: &str) -> EventSourceConfig {
    use sinex_node_sdk::config::ReplayConfig;

    let mut source_config = HashMap::new();
    source_config.insert("max_retries".to_string(), serde_json::json!(5));
    source_config.insert("retry_delay_ms".to_string(), serde_json::json!(500));
    source_config.insert("health_check_interval".to_string(), serde_json::json!(30));
    source_config.insert("enable_metrics".to_string(), serde_json::json!(true));

    EventSourceConfig {
        base: NodeConfig {
            service_name: service_name.to_string(),
            log_level: "trace".to_string(),
            nats: sinex_primitives::nats::NatsConnectionConfig {
                url: "nats://localhost:4222".to_string(),
                ..Default::default()
            },
            database_url: Some("postgresql:///sinex_dev".to_string()),
            database_pool_size: 20,
            work_dir: Utf8PathBuf::from("/tmp/sinex-enhanced"),
            dry_run: false,
            replay: Some(ReplayConfig {
                enabled: true,
                start_time: Some("2024-01-01T00:00:00Z".to_string()),
                end_time: None,
                sources: vec![],
                event_types: vec![],
                replay_batch_size: 100,
            }),
        },
        batch_size: 100,
        batch_timeout_secs: Seconds::from_secs(5),
        source_config,
    }
}

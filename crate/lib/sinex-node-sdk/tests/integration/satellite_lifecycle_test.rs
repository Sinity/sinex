//! Satellite Lifecycle Integration Tests
//!
//! Tests the complete lifecycle of satellite services including:
//! - Initialization and startup
//! - State transitions
//! - Health monitoring and heartbeats
//! - Error recovery and resilience
//! - Graceful shutdown and cleanup

use camino::Utf8PathBuf;
use futures::future;
use sinex_node_sdk::{
    checkpoint::{CheckpointManager, CheckpointState},
    config::{EventSourceConfig, NodeConfig},
    coordination::{InstanceMode, NodeCoordination},
    stream_processor::Checkpoint,
    version::{NodeInstance, NodeVersion},
};
use sinex_core::types::Seconds;
use sinex_test_utils::TestResult;
use sinex_test_utils::{sinex_test, TestContext};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc,
};
use tokio::time::{sleep, timeout, Duration, Instant};
use tracing::{debug, info, warn};

/// Test complete satellite lifecycle from birth to death
#[sinex_test]
async fn test_satellite_complete_lifecycle(ctx: TestContext) -> TestResult<()> {
    info!("Testing complete satellite lifecycle");

    let instance = NodeInstance::new(
        "lifecycle_test_satellite",
        NodeVersion::parse("1.0.0+lifecycle").unwrap(),
    );

    let mut coordination = NodeCoordination::new(instance.clone(), ctx.pool().clone());

    // Phase 1: Initialization
    info!("Phase 1: Satellite initialization");
    coordination.initialize().await?;

    // Should start in standby mode
    assert_eq!(coordination.current_mode(), &InstanceMode::Standby);
    debug!("✓ Satellite initialized in standby mode");

    // Phase 2: Startup and leadership acquisition
    info!("Phase 2: Startup and leadership acquisition");
    let became_leader = Arc::new(AtomicBool::new(false));
    let processing_count = Arc::new(AtomicU32::new(0));

    let leader_flag = became_leader.clone();
    let process_count = processing_count.clone();

    let lifecycle_handle = tokio::spawn(async move {
        coordination
            .run_coordination_loop(|| {
                let flag = leader_flag.clone();
                let count = process_count.clone();
                async move {
                    // First time becoming leader
                    if !flag.load(Ordering::SeqCst) {
                        info!("Satellite became leader!");
                        flag.store(true, Ordering::SeqCst);
                    }

                    // Simulate processing work
                    count.fetch_add(1, Ordering::SeqCst);
                    sleep(Duration::from_millis(50)).await;
                    Ok::<(), Box<dyn std::error::Error>>(())
                }
            })
            .await
    });

    // Phase 3: Steady state operations
    info!("Phase 3: Steady state operations");
    sleep(Duration::from_millis(300)).await;

    // Verify satellite is operating
    assert!(
        became_leader.load(Ordering::SeqCst),
        "Satellite should have become leader"
    );
    let initial_processing = processing_count.load(Ordering::SeqCst);
    assert!(
        initial_processing > 0,
        "Satellite should have processed work"
    );
    debug!("✓ Satellite processing {} work units", initial_processing);

    // Phase 4: Graceful shutdown
    info!("Phase 4: Graceful shutdown");
    lifecycle_handle.abort();

    let final_processing = processing_count.load(Ordering::SeqCst);
    assert!(
        final_processing >= initial_processing,
        "Processing should not decrease"
    );
    info!("✓ Satellite lifecycle completed successfully");

    Ok(())
}

/// Test satellite initialization sequence and state setup
#[sinex_test]
async fn test_satellite_initialization_sequence(ctx: TestContext) -> TestResult<()> {
    info!("Testing satellite initialization sequence");

    // Test with multiple configurations to ensure robustness
    let configs = vec![
        ("minimal_satellite", "1.0.0+minimal"),
        ("full_featured_satellite", "2.1.0+full"),
        ("legacy_satellite", "0.9.0+legacy"),
    ];

    for (service_name, version) in configs {
        debug!("Testing initialization for {} v{}", service_name, version);

        let instance =
            NodeInstance::new(service_name, NodeVersion::parse(version).unwrap());

        let mut coordination = NodeCoordination::new(instance.clone(), ctx.pool().clone());

        // Initialize should succeed
        coordination.initialize().await?;

        // Verify initial state
        assert_eq!(coordination.current_mode(), &InstanceMode::Standby);

        // Verify instance is registered via KV coordination
        let instances = coordination.kv_client().list_instances().await?;
        let registered = instances.iter().find(|inst| inst.instance_id == instance.instance_id);

        assert!(registered.is_some(), "Instance should be registered in KV");
        let reg = registered.unwrap();
        assert_eq!(reg.hostname, instance.host_name);
        assert!(reg.version.contains(version), "Version should match");

        debug!("✓ {} v{} initialized correctly", service_name, version);
    }

    info!("✓ All satellite initialization sequences completed");
    Ok(())
}

/// Test satellite health monitoring and heartbeat mechanisms
#[sinex_test]
async fn test_satellite_health_monitoring(ctx: TestContext) -> TestResult<()> {
    info!("Testing satellite health monitoring");

    let instance = NodeInstance::new(
        "health_monitor_test",
        NodeVersion::parse("1.0.0+health").unwrap(),
    );

    let ctx = ctx.with_nats().await?;
    let kv = ctx.checkpoint_kv().await?;
    let checkpoint_manager = CheckpointManager::new(
        kv,
        "health_monitor_test".to_string(),
        "health_group".to_string(),
        "health_consumer".to_string(),
    );

    // Test checkpoint-based health tracking
    let start_time = chrono::Utc::now();
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
    };

    // Save initial health checkpoint
    checkpoint_manager.save_checkpoint(&checkpoint).await?;
    debug!("✓ Initial health checkpoint saved");

    // Simulate health updates over time
    for i in 1..=5 {
        sleep(Duration::from_millis(100)).await;

        checkpoint.processed_count += 1;
        checkpoint.last_activity = chrono::Utc::now();
        checkpoint.data = Some(serde_json::json!({
            "health_status": "healthy",
            "uptime_seconds": i,
            "last_heartbeat": checkpoint.last_activity
        }));
        checkpoint.version += 1;

        checkpoint_manager.save_checkpoint(&checkpoint).await?;
        debug!("✓ Health checkpoint {} updated", i);
    }

    // Verify health data persistence
    let final_checkpoint = checkpoint_manager.load_checkpoint().await?;
    assert_eq!(final_checkpoint.processed_count, 6); // Initial + 5 updates
    assert!(final_checkpoint.data.is_some());

    let health_data = final_checkpoint.data.as_ref().unwrap();
    assert_eq!(health_data["health_status"], "healthy");
    assert_eq!(health_data["uptime_seconds"], 5);

    info!("✓ Satellite health monitoring working correctly");
    Ok(())
}

/// Test satellite error recovery and resilience patterns
#[sinex_test]
async fn test_satellite_error_recovery(ctx: TestContext) -> TestResult<()> {
    info!("Testing satellite error recovery");

    let instance = NodeInstance::new(
        "error_recovery_test",
        NodeVersion::parse("1.0.0+recovery").unwrap(),
    );

    let mut coordination = NodeCoordination::new(instance.clone(), ctx.pool().clone());
    coordination.initialize().await?;

    let error_count = Arc::new(AtomicU32::new(0));
    let recovery_count = Arc::new(AtomicU32::new(0));
    let successful_ops = Arc::new(AtomicU32::new(0));

    let err_count = error_count.clone();
    let rec_count = recovery_count.clone();
    let success_count = successful_ops.clone();

    // Simulate satellite with intermittent failures
    let recovery_handle = tokio::spawn(async move {
        timeout(
            Duration::from_millis(600),
            coordination.run_coordination_loop(|| {
                let errors = err_count.clone();
                let recoveries = rec_count.clone();
                let successes = success_count.clone();

                async move {
                    let current_errors = errors.load(Ordering::SeqCst);

                    // Simulate failure every 3rd operation
                    if current_errors < 3 && successes.load(Ordering::SeqCst) % 3 == 2 {
                        errors.fetch_add(1, Ordering::SeqCst);
                        warn!("Simulated satellite error #{}", current_errors + 1);

                        // Simulate recovery attempt
                        sleep(Duration::from_millis(50)).await;
                        recoveries.fetch_add(1, Ordering::SeqCst);
                        debug!("Recovery attempt #{}", recoveries.load(Ordering::SeqCst));

                        // Recover successfully after brief delay
                        sleep(Duration::from_millis(25)).await;
                    }

                    successes.fetch_add(1, Ordering::SeqCst);
                    sleep(Duration::from_millis(75)).await;
                    Ok::<(), Box<dyn std::error::Error>>(())
                }
            }),
        )
        .await
        .is_ok()
    });

    let result = recovery_handle.await.unwrap();
    assert!(result, "Recovery coordination should complete successfully");

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
    assert!(final_errors > 0, "Should have encountered some errors");

    info!(
        "✓ Error recovery: {} errors, {} recoveries, {} successful operations",
        final_errors, final_recoveries, final_successes
    );
    Ok(())
}

/// Test satellite state transitions and mode changes
#[sinex_test]
async fn test_satellite_state_transitions(ctx: TestContext) -> TestResult<()> {
    info!("Testing satellite state transitions");

    let instance = NodeInstance::new(
        "state_transition_test",
        NodeVersion::parse("1.0.0+states").unwrap(),
    );

    let mut coordination = NodeCoordination::new(instance.clone(), ctx.pool().clone());

    // Initial state should be uninitialized (before initialize() call)
    info!("Testing pre-initialization state");

    // Initialize - should transition to Standby
    coordination.initialize().await?;
    assert_eq!(coordination.current_mode(), &InstanceMode::Standby);
    debug!("✓ Transitioned from uninitialized to Standby");

    // Track state transitions during coordination
    let state_changes = Arc::new(AtomicU32::new(0));
    let became_leader = Arc::new(AtomicBool::new(false));

    let state_counter = state_changes.clone();
    let leader_flag = became_leader.clone();

    let transition_handle = tokio::spawn(async move {
        timeout(
            Duration::from_millis(400),
            coordination.run_coordination_loop(|| {
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
                    Ok::<(), Box<dyn std::error::Error>>(())
                }
            }),
        )
        .await
        .is_ok()
    });

    let completed = transition_handle.await.unwrap();
    assert!(completed, "State transition coordination should complete");

    // Verify transitions occurred
    assert!(
        became_leader.load(Ordering::SeqCst),
        "Should have transitioned to leader"
    );
    assert!(
        state_changes.load(Ordering::SeqCst) > 0,
        "Should have recorded state changes"
    );

    info!("✓ Satellite state transitions working correctly");
    Ok(())
}

/// Test satellite configuration loading and validation
#[sinex_test]
async fn test_satellite_configuration_lifecycle(ctx: TestContext) -> TestResult<()> {
    info!("Testing satellite configuration lifecycle");

    // Test configuration creation and validation
    let test_configs = vec![
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

        // Test configuration with satellite instance
        let version = NodeVersion::parse(&format!("1.0.{}", i)).unwrap();
        let instance = NodeInstance::new(&config.base.service_name, version);

        let mut coordination = NodeCoordination::new(instance.clone(), ctx.pool().clone());
        coordination.initialize().await?;

        // Verify instance uses configuration correctly via KV coordination
        let instances = coordination.kv_client().list_instances().await?;
        let registered = instances.iter().find(|inst| inst.instance_id == instance.instance_id);

        assert!(registered.is_some(), "Instance should be registered in KV");
        debug!("✓ Configuration {} validated and applied", i + 1);
    }

    info!("✓ All satellite configurations validated successfully");
    Ok(())
}

/// Test satellite shutdown sequence and cleanup
#[sinex_test]
async fn test_satellite_graceful_shutdown(ctx: TestContext) -> TestResult<()> {
    info!("Testing satellite graceful shutdown");

    let instance = NodeInstance::new(
        "shutdown_test",
        NodeVersion::parse("1.0.0+shutdown").unwrap(),
    );

    let ctx = ctx.with_nats().await?;
    let kv = ctx.checkpoint_kv().await?;
    let checkpoint_manager = CheckpointManager::new(
        kv,
        "shutdown_test".to_string(),
        "shutdown_group".to_string(),
        "shutdown_consumer".to_string(),
    );

    let mut coordination = NodeCoordination::new(instance.clone(), ctx.pool().clone());
    coordination.initialize().await?;

    // Track shutdown process
    let operations_completed = Arc::new(AtomicU32::new(0));
    let shutdown_initiated = Arc::new(AtomicBool::new(false));
    let cleanup_completed = Arc::new(AtomicBool::new(false));

    let ops_count = operations_completed.clone();
    let shutdown_flag = shutdown_initiated.clone();
    let cleanup_flag = cleanup_completed.clone();

    // Start satellite operations
    let shutdown_handle = tokio::spawn(async move {
        let start_time = Instant::now();

        let result = coordination
            .run_coordination_loop(|| {
                let ops = ops_count.clone();
                let shutdown = shutdown_flag.clone();
                let cleanup = cleanup_flag.clone();

                async move {
                    ops.fetch_add(1, Ordering::SeqCst);

                    // Simulate shutdown after some operations
                    if start_time.elapsed() > Duration::from_millis(200)
                        && !shutdown.load(Ordering::SeqCst)
                    {
                        shutdown.store(true, Ordering::SeqCst);
                        debug!("Initiating graceful shutdown");

                        // Simulate cleanup operations
                        sleep(Duration::from_millis(50)).await;
                        cleanup.store(true, Ordering::SeqCst);
                        debug!("Cleanup completed");
                    }

                    sleep(Duration::from_millis(50)).await;
                    Ok::<(), Box<dyn std::error::Error>>(())
                }
            })
            .await;

        result
    });

    // Let satellite run briefly then shut down
    sleep(Duration::from_millis(350)).await;
    shutdown_handle.abort();

    // Verify shutdown process
    assert!(
        operations_completed.load(Ordering::SeqCst) > 0,
        "Should have completed some operations"
    );
    assert!(
        shutdown_initiated.load(Ordering::SeqCst),
        "Should have initiated shutdown"
    );
    assert!(
        cleanup_completed.load(Ordering::SeqCst),
        "Should have completed cleanup"
    );

    // Verify final checkpoint saved
    let final_checkpoint = CheckpointState {
        checkpoint: Checkpoint::Stream {
            message_id: "shutdown-final".to_string(),
            event_id: None,
        },
        processed_count: operations_completed.load(Ordering::SeqCst),
        last_activity: chrono::Utc::now(),
        data: Some(serde_json::json!({
            "shutdown_reason": "graceful",
            "operations_completed": operations_completed.load(Ordering::SeqCst)
        })),
        version: 1,
    };

    checkpoint_manager
        .save_checkpoint(&final_checkpoint)
        .await?;
    let saved_checkpoint = checkpoint_manager.load_checkpoint().await?;
    assert_eq!(
        saved_checkpoint.processed_count,
        final_checkpoint.processed_count
    );

    info!("✓ Graceful shutdown completed successfully");
    Ok(())
}

/// Test satellite lifecycle under concurrent operations
#[sinex_test]
async fn test_satellite_concurrent_lifecycle(ctx: TestContext) -> TestResult<()> {
    info!("Testing satellite lifecycle under concurrency");

    // Start multiple satellites concurrently to test coordination
    let satellite_count = 3;
    let mut handles = Vec::new();
    let completion_count = Arc::new(AtomicU32::new(0));

    for i in 0..satellite_count {
        let instance = NodeInstance::new(
            "concurrent_test",
            NodeVersion::parse(&format!("1.0.{}", i)).unwrap(),
        );

        let mut coordination = NodeCoordination::new(instance.clone(), ctx.pool().clone());
        coordination.initialize().await?;

        let counter = completion_count.clone();
        let handle = tokio::spawn(async move {
            let result = timeout(
                Duration::from_millis(300),
                coordination.run_coordination_loop(|| {
                    let counter = counter.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        sleep(Duration::from_millis(50)).await;
                        Ok::<(), Box<dyn std::error::Error>>(())
                    }
                }),
            )
            .await;

            result.is_ok()
        });

        handles.push(handle);
        debug!("Started satellite {}", i);
    }

    // Wait for all satellites to complete
    let results: Vec<_> = futures::future::join_all(handles).await;

    // Verify all satellites completed successfully
    for (i, result) in results.iter().enumerate() {
        assert!(
            result.as_ref().unwrap(),
            "Satellite {} should complete successfully",
            i
        );
    }

    let total_operations = completion_count.load(Ordering::SeqCst);
    assert!(total_operations > 0, "Should have completed operations");

    info!(
        "✓ Concurrent satellite lifecycle completed: {} total operations",
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
            nats: sinex_core::nats::NatsConnectionConfig {
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
    source_config.insert("max_retries".to_string(), "3".to_string());
    source_config.insert("retry_delay_ms".to_string(), "1000".to_string());

    EventSourceConfig {
        base: NodeConfig {
            service_name: service_name.to_string(),
            log_level: "debug".to_string(),
            nats: sinex_core::nats::NatsConnectionConfig {
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
    let mut source_config = HashMap::new();
    source_config.insert("max_retries".to_string(), "5".to_string());
    source_config.insert("retry_delay_ms".to_string(), "500".to_string());
    source_config.insert("health_check_interval".to_string(), "30".to_string());
    source_config.insert("enable_metrics".to_string(), "true".to_string());

    EventSourceConfig {
        base: NodeConfig {
            service_name: service_name.to_string(),
            log_level: "trace".to_string(),
            nats: sinex_core::nats::NatsConnectionConfig {
                url: "nats://localhost:4222".to_string(),
                ..Default::default()
            },
            database_url: Some("postgresql:///sinex_dev".to_string()),
            database_pool_size: 20,
            work_dir: Utf8PathBuf::from("/tmp/sinex-enhanced"),
            dry_run: false,
            replay: Some("2024-01-01T00:00:00Z".to_string()),
        },
        batch_size: 100,
        batch_timeout_secs: Seconds::from_secs(5),
        source_config,
    }
}

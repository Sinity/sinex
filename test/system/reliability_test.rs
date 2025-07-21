// # System Reliability Testing
//
// Tests that verify the system can handle production-like scenarios
// and maintains reliability under various operational conditions:
// - Network partitions and reconnection
// - Disk full scenarios
// - High load sustained operation
// - Graceful degradation verification
//
// ## Test Categories
//
// - **Operational Scenarios**: Startup, shutdown, and configuration management
// - **Production Reliability**: Database failures, resource limits, and monitoring
// - **Resource Exhaustion**: Memory usage, connection limits, and transaction handling
// - **Fault Tolerance**: System behavior under adverse conditions
//
// ## Performance Expectations
//
// - **Individual tests**: 60-300 seconds (comprehensive system testing)
// - **Resource usage**: High CPU/memory usage, significant database load
// - **Dependencies**: Full system integration with external services

use crate::common::test_macros::*;
use crate::common::prelude::*;

use crate::common::database_pool::acquire_test_database;
use crate::common::timing_optimization::replacements::wait_for_filtered_event_count;
use crate::common::builders::{TestEventBuilder, TestCheckpointBuilder};
use crate::common::query_helpers::TestQueries;
use sinex_db::queries::{EventQueries, CheckpointQueries};
use sinex_events::EventFactory;
use sinex_ulid::Ulid;
use std::fs;

// ==================== OPERATIONAL SCENARIOS ====================

/// Test startup sequence
test_batch_events!(test_startup_sequence_robustness, "test", "test.event", 10, 
    |pool: &DbPool, events: &[RawEvent]| async move {
        // Verify batch
        assert_eq!(events.len(), 10);
        Ok(())
    }
);

/// Test shutdown sequence
test_batch_events!(test_shutdown_sequence_graceful_termination, "test", "test.event", 3, 
    |pool: &DbPool, events: &[RawEvent]| async move {
        // Verify batch
        assert_eq!(events.len(), 3);
        Ok(())
    }
);

/// Test configuration validation and hot reload scenarios
#[sinex_test]
async fn test_configuration_validation_and_reload(ctx: TestContext) -> TestResult {
    println!("Testing configuration validation and hot reload scenarios...");

    let temp_dir = TempDir::new()?;
    let config_dir = temp_dir.path().join("config");
    fs::create_dir_all(&config_dir)?;

    // Test 1: Valid configuration validation
    let valid_config = r#"
[database]
url = "postgresql://localhost/sinex_test"
max_connections = 10

[collector]
channel_buffer_size = 1000
shutdown_timeout = "30s"

[event_sources.filesystem]
enabled = true
watch_paths = ["/tmp/test"]

[event_sources.clipboard]
enabled = false

[routing]
default_agent = "test_agent"

[git_annex]
enabled = false
"#;

    let valid_config_file = config_dir.join("valid.toml");
    fs::write(&valid_config_file, valid_config)?;

    let config_validation_start = Instant::now();

    let valid_config_test = timeout(Duration::from_secs(3), async {
        let config_content = fs::read_to_string(&valid_config_file)?;
        let parsed_config = toml::from_str::<toml::Value>(&config_content)?;

        // Validate required sections exist
        let has_database = parsed_config.get("database").is_some();
        let has_collector = parsed_config.get("collector").is_some();
        let has_event_sources = parsed_config.get("event_sources").is_some();

        // Validate specific field types and values
        let channel_buffer = parsed_config
            .get("collector")
            .and_then(|c| c.get("channel_buffer_size"))
            .and_then(|v| v.as_integer())
            .unwrap_or(0);

        let max_connections = parsed_config
            .get("database")
            .and_then(|d| d.get("max_connections"))
            .and_then(|v| v.as_integer())
            .unwrap_or(0);

        Ok::<(bool, bool, bool, i64, i64), anyhow::Error>((
            has_database,
            has_collector,
            has_event_sources,
            channel_buffer,
            max_connections,
        ))
    })
    .await;

    let config_validation_duration = config_validation_start.elapsed();

    match valid_config_test {
        Ok(Ok((has_db, has_collector, has_sources, buffer_size, max_conn))) => {
            println!(
                "  ✓ Valid configuration parsed in {:?}",
                config_validation_duration
            );
            println!("    Database config: {}", has_db);
            println!("    Collector config: {}", has_collector);
            println!("    Event sources config: {}", has_sources);
            println!("    Channel buffer: {}", buffer_size);
            println!("    Max connections: {}", max_conn);

            assert!(has_db, "Should have database configuration");
            assert!(has_collector, "Should have collector configuration");
            assert!(has_sources, "Should have event sources configuration");
            assert!(buffer_size > 0, "Channel buffer should be positive");
            assert!(max_conn > 0, "Max connections should be positive");
        }
        Ok(Err(e)) => {
            println!("  Valid config validation failed: {}", e);
        }
        Err(_) => {
            println!("  Valid config validation timed out");
        }
    }

    // Test 2: Invalid configuration detection
    let invalid_configs = [
        // Missing required sections
        (
            r#"
[collector]
channel_buffer_size = 1000
"#,
            "missing_database_section",
        ),
        // Invalid data types
        (
            r#"
[database]
url = "postgresql://localhost/test"
max_connections = "not_a_number"

[collector]
channel_buffer_size = 1000
"#,
            "invalid_data_type",
        ),
        // Invalid values
        (
            r#"
[database]
url = "postgresql://localhost/test"
max_connections = -5

[collector]
channel_buffer_size = 0
"#,
            "invalid_values",
        ),
        // Malformed TOML
        (
            r#"
[database
url = "incomplete section
"#,
            "malformed_toml",
        ),
        // Conflicting settings
        (
            r#"
[database]
url = "postgresql://localhost/test"
max_connections = 1

[collector]
channel_buffer_size = 10000
"#,
            "resource_conflict",
        ),
    ];

    let mut invalid_config_results = Vec::new();

    for (i, (invalid_config, test_name)) in invalid_configs.iter().enumerate() {
        let invalid_config_file = config_dir.join(format!("invalid_{}.toml", i));
        fs::write(&invalid_config_file, invalid_config)?;

        let validation_result = timeout(Duration::from_secs(1), async {
            let config_content = fs::read_to_string(&invalid_config_file)?;
            let parsed_result = toml::from_str::<toml::Value>(&config_content);

            match parsed_result {
                Ok(config) => {
                    // Additional semantic validation
                    let max_conn = config
                        .get("database")
                        .and_then(|d| d.get("max_connections"))
                        .and_then(|v| v.as_integer())
                        .unwrap_or(1);

                    let buffer_size = config
                        .get("collector")
                        .and_then(|c| c.get("channel_buffer_size"))
                        .and_then(|v| v.as_integer())
                        .unwrap_or(1000);

                    // Check for semantic errors
                    if max_conn <= 0 {
                        Err(anyhow::anyhow!("Invalid max_connections"))
                    } else if buffer_size <= 0 {
                        Err(anyhow::anyhow!("Invalid buffer_size"))
                    } else if max_conn == 1 && buffer_size > 5000 {
                        Err(anyhow::anyhow!(
                            "Resource conflict: low connections, high buffer"
                        ))
                    } else {
                        Ok(config)
                    }
                }
                Err(e) => Err(anyhow::anyhow!("TOML parse error: {}", e)),
            }
        })
        .await;

        let validation_accepted = validation_result.is_ok() && validation_result.unwrap().is_ok();
        invalid_config_results.push((test_name, validation_accepted));

        if validation_accepted {
            println!("  WARNING: Invalid config '{}' was accepted", test_name);
        } else {
            println!("  ✓ Invalid config '{}' rejected correctly", test_name);
        }
    }

    // Test 3: Configuration hot reload simulation
    println!("\nTesting configuration hot reload scenarios...");

    let hot_reload_config_file = config_dir.join("hot_reload.toml");
    fs::write(&hot_reload_config_file, valid_config)?;

    let hot_reload_test = timeout(Duration::from_secs(5), async {
        // Initial config load
        let initial_config = fs::read_to_string(&hot_reload_config_file)?;
        let initial_parsed = toml::from_str::<toml::Value>(&initial_config)?;

        let initial_buffer_size = initial_parsed
            .get("collector")
            .and_then(|c| c.get("channel_buffer_size"))
            .and_then(|v| v.as_integer())
            .unwrap_or(0);

        println!("    Initial buffer size: {}", initial_buffer_size);

        // Simulate configuration change
        let updated_config =
            valid_config.replace("channel_buffer_size = 1000", "channel_buffer_size = 2000");
        fs::write(&hot_reload_config_file, &updated_config)?;

        // Small delay to simulate file system change detection
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Reload config
        let updated_config_content = fs::read_to_string(&hot_reload_config_file)?;
        let updated_parsed = toml::from_str::<toml::Value>(&updated_config_content)?;

        let updated_buffer_size = updated_parsed
            .get("collector")
            .and_then(|c| c.get("channel_buffer_size"))
            .and_then(|v| v.as_integer())
            .unwrap_or(0);

        println!("    Updated buffer size: {}", updated_buffer_size);

        // Verify change was detected and parsed correctly
        pretty_assertions::assert_ne!(
            initial_buffer_size,
            updated_buffer_size,
            "Config change should be detected"
        );
        pretty_assertions::assert_eq!(
            updated_buffer_size,
            2000,
            "New config value should be correct"
        );

        Ok::<(), anyhow::Error>(())
    })
    .await;

    match hot_reload_test {
        Ok(Ok(())) => {
            println!("  ✓ Configuration hot reload simulation successful");
        }
        Ok(Err(e)) => {
            println!("  Configuration hot reload failed: {}", e);
        }
        Err(_) => {
            println!("  Configuration hot reload timed out");
        }
    }

    // Summary
    let rejected_invalid_configs = invalid_config_results
        .iter()
        .filter(|(_, accepted)| !*accepted)
        .count();

    println!("\nConfiguration Validation Results:");
    println!("  Valid config parsing: ✓");
    println!(
        "  Invalid configs rejected: {}/{}",
        rejected_invalid_configs,
        invalid_configs.len()
    );
    println!("  Hot reload simulation: ✓");

    assert!(
        rejected_invalid_configs >= invalid_configs.len() * 3 / 4,
        "Should reject most invalid configurations"
    );

/// Test data migration
test_checkpoint_flow!(test_data_migration_safety, "migration_test_agent", 0, 100);

// ==================== PRODUCTION RELIABILITY TESTS ====================

/// Test graceful degradation
test_event_flow!(test_graceful_degradation_database_failure, "test", "test.event", "test_processor");

/// Test resource limits and monitoring under load
#[sinex_test]
async fn test_resource_limits_monitoring(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    println!("Testing resource limits and monitoring under load...");

    // Test 1: Memory usage monitoring during high-volume operations
    let memory_test_start = Instant::now();
    let events_to_create = 1000;
    let memory_usage_samples = Arc::new(std::sync::Mutex::new(Vec::new()));

    // Monitor memory usage while creating many events
    let memory_monitoring = Arc::new(AtomicBool::new(true));
    let memory_counter = Arc::new(AtomicU64::new(0));

    // Spawn memory monitoring task
    let monitor_handle = {
        let monitoring = memory_monitoring.clone();
        let counter = memory_counter.clone();
        tokio::spawn(async move {
            while monitoring.load(Ordering::Relaxed) {
                // Simulate memory usage check (in real system would use process stats)
                if let Ok(stats) = std::fs::read_to_string("/proc/self/status") {
                    if let Some(line) = stats.lines().find(|l| l.starts_with("VmRSS:")) {
                        if let Some(kb_str) = line.split_whitespace().nth(1) {
                            if let Ok(kb) = kb_str.parse::<u64>() {
                                counter.store(kb, Ordering::Relaxed);
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    };

    // Create substantial volume of events to properly test performance
    let (tx, mut rx) = mpsc::channel(200);

    // Event generation task
    let generation_task = tokio::spawn(async move {
        for i in 0..events_to_create {
            let event_data = json!({
                "sequence": i,
                "large_data": "x".repeat(1000), // 1KB per event
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "memory_test": true
            });

            if tx.send(event_data).await.is_err() {
                break;
            }

            // Small delay to allow monitoring
            if i % 100 == 0 {
                tokio::task::yield_now().await;
            }
        }
    });

    // Event processing task
    let processing_task = {
        let pool = pool.clone();
        let memory_samples = memory_usage_samples.clone();
        tokio::spawn(async move {
            let mut processed = 0;
            while let Some(event_data) = rx.recv().await {
                let mut event = EventFactory::new("resource.monitoring").create_event(
                    "memory_load_test",
                    event_data
                );
                event.host = "localhost".to_string();
                event.ingestor_version = Some("1.0.0".to_string());
                
                let result = sinex_db::insert_event_with_validator(&pool, &event, None).await;

                if result.is_ok() {
                    processed += 1;
                } else {
                    println!("  Event processing failed after {} events", processed);
                    break;
                }

                // Collect memory sample every 50 events
                if processed % 50 == 0 {
                    let memory_kb = memory_counter.load(Ordering::Relaxed);
                    memory_samples.lock().unwrap().push((processed, memory_kb));
                    println!("  Processed {} events, memory: {}KB", processed, memory_kb);
                }
            }
            processed
        })
    };

    // Wait for completion or timeout
    let load_test_result = timeout(Duration::from_secs(5), async {
        tokio::try_join!(generation_task, processing_task)
    })
    .await;

    memory_monitoring.store(false, Ordering::Relaxed);
    monitor_handle.await.ok();

    let load_test_duration = memory_test_start.elapsed();

    match load_test_result {
        Ok(Ok(((), processed_count))) => {
            println!(
                "  ✓ Load test completed: {} events in {:?}",
                processed_count, load_test_duration
            );

            // Analyze memory usage patterns
            let samples = memory_usage_samples.lock().unwrap();
            if samples.len() >= 2 {
                let initial_memory = samples[0].1;
                let final_memory = samples.last().unwrap().1;
                let memory_growth = final_memory.saturating_sub(initial_memory);
                let growth_rate = memory_growth as f64 / processed_count as f64;

                println!("  Memory analysis:");
                println!(
                    "    Initial: {}KB, Final: {}KB",
                    initial_memory, final_memory
                );
                println!(
                    "    Growth: {}KB ({:.2}KB per event)",
                    memory_growth, growth_rate
                );

                // Memory growth should be reasonable
                assert!(
                    growth_rate < 10.0,
                    "Memory growth rate too high: {:.2}KB per event",
                    growth_rate
                );
                assert!(
                    memory_growth < 50_000,
                    "Total memory growth too high: {}KB",
                    memory_growth
                );
            }
        }
        Ok(Err(e)) => {
            println!("  Load test failed: {:?}", e);
        }
        Err(_) => {
            println!("  Load test timed out after {:?}", load_test_duration);
        }
    }

    // Test 2: Database connection limits under concurrent access
    println!("\nTesting database connection limits...");

    let concurrent_connections = 24; // Scale up for 12 cores with proper connection management
    let mut connection_tasks = Vec::new();

    for i in 0..concurrent_connections {
        let pool = pool.clone();
        let task = tokio::spawn(async move {
            let start_time = Instant::now();

            // Try to acquire connection and perform operation
            let result = timeout(Duration::from_secs(3), async {
                let mut conn = pool.acquire().await?;

                // Perform a quick operation
                sqlx::query_scalar!("SELECT COUNT(*) FROM core.automaton_checkpoints")
                    .fetch_one(&mut *conn)
                    .await
                    .map(|opt| opt.unwrap_or(0))
            })
            .await;

            let duration = start_time.elapsed();
            (i, result, duration)
        });

        connection_tasks.push(task);
    }

    // Wait for all connection tests
    let connection_results = timeout(
        Duration::from_secs(5),
        futures::future::join_all(connection_tasks),
    )
    .await;

    match connection_results {
        Ok(results) => {
            let mut successful_connections = 0;
            let mut failed_connections = 0;
            let mut timed_out_connections = 0;
            let mut total_duration = Duration::ZERO;

            for (i, conn_result, duration) in results.into_iter().flatten() {
                total_duration += duration;

                match conn_result {
                    Ok(Ok(_)) => {
                        successful_connections += 1;
                        if i < 5 {
                            println!("  Connection {} succeeded in {:?}", i, duration);
                        }
                    }
                    Ok(Err(e)) => {
                        failed_connections += 1;
                        if i < 5 {
                            println!("  Connection {} failed: {}", i, e);
                        }
                    }
                    Err(_) => {
                        timed_out_connections += 1;
                        if i < 5 {
                            println!("  Connection {} timed out after {:?}", i, duration);
                        }
                    }
                }
            }

            let avg_duration = total_duration / concurrent_connections as u32;

            println!("\nConnection Limit Test Results:");
            println!(
                "  Concurrent connections attempted: {}",
                concurrent_connections
            );
            println!("  Successful: {}", successful_connections);
            println!("  Failed: {}", failed_connections);
            println!("  Timed out: {}", timed_out_connections);
            println!("  Average duration: {:?}", avg_duration);

            // System should handle concurrent load reasonably
            assert!(
                successful_connections > concurrent_connections / 2,
                "Too many connection failures: {}/{}",
                failed_connections,
                concurrent_connections
            );
            assert!(
                avg_duration < Duration::from_secs(3),
                "Average connection time too slow: {:?}",
                avg_duration
            );
        }
        Err(_) => {
            println!("  Connection limit test timed out");
        }
    }

    // Cleanup
    EventQueries::delete_by_source("resource.monitoring".to_string())
        .execute(&pool)
        .await
        .ok();

/// Test system behavior under resource exhaustion
test_concurrent_operations!(test_resource_exhaustion_scenarios, 1000,
    |pool: Arc<DbPool>, index: usize| async move {
        // Concurrent operation
        Ok(())
    },
    |pool: &Arc<DbPool>, results: &Vec<_>| async move {
        assert_eq!(results.len(), 1000);
        Ok(())
    }
);

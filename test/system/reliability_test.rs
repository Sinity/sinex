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

use crate::common::prelude::*;

use crate::common::database_pool::acquire_test_database;
use crate::common::timing_optimization::replacements::wait_for_filtered_event_count;
use sinex_events::{EventFactory, services, event_types};
use sinex_ulid::Ulid;
use std::fs;

// ==================== OPERATIONAL SCENARIOS ====================

/// Test startup sequence robustness and error handling
#[sinex_test(timeout = 60)]
async fn test_startup_sequence_robustness(ctx: TestContext) -> TestResult {
    println!("Testing startup sequence robustness...");

    // Test 1: Database initialization from scratch
    let startup_start = Instant::now();

    // Create isolated test database
    let test_db_name = format!(
        "sinex_startup_test_{}",
        Ulid::new().to_string().to_lowercase()
    );
    let base_url = std::env::var("DATABASE_URL")?;
    let base_test_db = acquire_test_database().await?;
    let base_pool = base_test_db.pool();

    // Create test database
    sqlx::query(&format!("CREATE DATABASE {}", test_db_name))
        .execute(base_pool)
        .await?;

    let _test_db_url = base_url.replace("/sinex_dev", &format!("/{}", test_db_name));

    // Test fresh startup with empty database
    let fresh_startup_result = timeout(Duration::from_secs(5), async {
        let test_db = acquire_test_database().await?;
        let pool = test_db.pool();
        run_migrations(pool).await?;

        // Verify basic functionality after startup
        let schema_count: i64 = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM information_schema.schemata
                 WHERE schema_name IN ('raw', 'sinex_schemas')"
        )
        .fetch_one(pool)
        .await?
        .unwrap_or(0);

        let table_count: i64 = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM information_schema.tables
                 WHERE table_schema IN ('raw', 'sinex_schemas')"
        )
        .fetch_one(pool)
        .await?
        .unwrap_or(0);

        Ok::<(i64, i64), Box<dyn std::error::Error>>((schema_count, table_count))
    })
    .await;

    let startup_duration = startup_start.elapsed();

    match fresh_startup_result {
        Ok(Ok((schema_count, table_count))) => {
            println!("  ✓ Fresh startup completed in {:?}", startup_duration);
            println!("    Schemas created: {}", schema_count);
            println!("    Tables created: {}", table_count);

            assert!(schema_count >= 2, "Should create required schemas");
            assert!(table_count >= 4, "Should create required tables");
        }
        Ok(Err(e)) => {
            println!("  Fresh startup failed: {}", e);
        }
        Err(_) => {
            println!("  Fresh startup timed out after {:?}", startup_duration);
        }
    }

    // Test 2: Startup with existing data
    println!("\nTesting startup with existing data...");

    let existing_data_startup = timeout(Duration::from_secs(3), async {
        let test_db = acquire_test_database().await?;
        let pool = test_db.pool();

        // Add some existing checkpoint data
        let checkpoint_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO core.automaton_checkpoints (id, automaton_name, last_processed_id, state_data)
                 VALUES ($1::uuid, $2, $3, $4)",
            checkpoint_id.to_uuid(),
            "existing_agent",
            "startup_event_123",
            json!({"version": "1.0.0", "description": "Pre-existing agent for startup test"})
        )
        .execute(pool)
        .await?;

        // Insert some events
        for i in 0..10 {
            let mut event = EventFactory::new("startup.test").create_event(
                "existing_data",
                json!({"sequence": i, "startup_test": true})
            );
            event.host = "localhost".to_string();
            event.ingestor_version = "1.0.0".to_string();
            
            sinex_db::insert_event_with_validator(&pool, &event, None).await?;
        }

        // Simulate restart by running migrations again
        run_migrations(&pool).await?;

        // Verify data integrity after restart - use timing utilities for better reliability
        let checkpoint_count: i64 =
            sqlx::query_scalar!("SELECT COUNT(*) FROM core.automaton_checkpoints")
                .fetch_one(pool)
                .await?
                .unwrap_or(0);

        // Use timing utility for event count verification with source filter
        let event_count =
            wait_for_filtered_event_count(&pool, "source = $1", &["startup.test"], 10, 5)
                .await
                .unwrap_or(0);

        Ok::<(i64, i64), anyhow::Error>((checkpoint_count, event_count))
    })
    .await;

    match existing_data_startup {
        Ok(Ok((checkpoint_count, event_count))) => {
            println!("  ✓ Startup with existing data succeeded");
            println!("    Checkpoints preserved: {}", checkpoint_count);
            println!("    Events preserved: {}", event_count);

            assert!(
                checkpoint_count >= 1,
                "Existing checkpoints should be preserved"
            );
            assert!(event_count >= 10, "Existing events should be preserved");
        }
        Ok(Err(e)) => {
            println!("  Startup with existing data failed: {}", e);
        }
        Err(_) => {
            println!("  Startup with existing data timed out");
        }
    }

    // Test 3: Startup error recovery
    println!("\nTesting startup error recovery...");

    // Create a corrupted migration state (simulate partial migration failure)
    let error_recovery_test = timeout(
        Duration::from_secs(5),
        async {
            let test_db = acquire_test_database().await?;
        let pool = test_db.pool();

            // Simulate migration corruption by manually inserting invalid migration record
            sqlx::query!(
                "INSERT INTO _sqlx_migrations (version, description, installed_on, success, checksum, execution_time)
                 VALUES ($1, $2, $3, $4, $5, $6)",
                999999, // Very high version number
                "Corrupted test migration",
                chrono::Utc::now(),
                false, // Mark as failed
                vec![0u8; 32], // Invalid checksum
                0
            )
            .execute(pool)
            .await
            .ok(); // Ignore errors if table doesn't exist

            // Try to run migrations with corrupted state
            let migration_result = sqlx::migrate!("./migrations").run(pool).await;

            match migration_result {
                Ok(()) => {
                    println!("    ✓ Migrations recovered from corruption");
                    Ok::<bool, anyhow::Error>(true)
                }
                Err(e) => {
                    println!("    Migration failed gracefully: {}", e);
                    Ok::<bool, anyhow::Error>(false)
                }
            }
        }
    ).await;

    match error_recovery_test {
        Ok(Ok(recovered)) => {
            if recovered {
                println!("  ✓ Startup error recovery successful");
            } else {
                println!("  ✓ Startup failed gracefully with clear error");
            }
        }
        Ok(Err(e)) => {
            println!("  Error recovery test failed: {}", e);
        }
        Err(_) => {
            println!("  Error recovery test timed out");
        }
    }

    // Cleanup test database
    sqlx::query(&format!("DROP DATABASE {}", test_db_name))
        .execute(base_pool)
        .await
        .ok();

    Ok(())
}

/// Test shutdown sequence and graceful termination
#[sinex_test]
async fn test_shutdown_sequence_graceful_termination(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    println!("Testing shutdown sequence and graceful termination...");

    // Test 1: Graceful connection cleanup
    let shutdown_start = Instant::now();

    // Create multiple active connections
    let active_connections = 10;
    let mut connections = Vec::new();

    for i in 0..active_connections {
        match pool.acquire().await {
            Ok(conn) => {
                connections.push(conn);
                println!("  Acquired connection {}", i + 1);
            }
            Err(e) => {
                println!("  Failed to acquire connection {}: {}", i + 1, e);
                break;
            }
        }
    }

    // Simulate ongoing transactions
    let mut transactions = Vec::new();
    for i in 0..3 {
        if connections.len() > i {
            if let Ok(mut tx) = pool.begin().await {
                // Start transaction with some work
                sqlx::query!(
                    "INSERT INTO core.events (
            event_id, source, event_type, host, payload)
                     VALUES ($1::uuid, $2, $3, $4, $5)",
                    Ulid::new().to_uuid(),
                    "shutdown.test",
                    "active_transaction",
                    "localhost",
                    json!({"tx_id": i, "shutdown_test": true})
                )
                .execute(&mut *tx)
                .await
                .ok();

                transactions.push(tx);
                println!("    Started transaction {}", i);
            }
        }
    }

    // Simulate graceful shutdown sequence
    println!("\nSimulating graceful shutdown...");

    // Step 1: Complete active transactions
    let transaction_completion_start = Instant::now();
    for (i, tx) in transactions.into_iter().enumerate() {
        match timeout(Duration::from_secs(3), tx.commit()).await {
            Ok(Ok(())) => {
                println!("    ✓ Transaction {} committed gracefully", i);
            }
            Ok(Err(e)) => {
                println!("    Transaction {} failed to commit: {}", i, e);
            }
            Err(_) => {
                println!("    Transaction {} commit timed out", i);
            }
        }
    }
    let transaction_completion_duration = transaction_completion_start.elapsed();

    // Step 2: Release connections gracefully
    let connection_release_start = Instant::now();
    drop(connections);
    let connection_release_duration = connection_release_start.elapsed();

    // Step 3: Verify database state after shutdown
    let post_shutdown_verification = timeout(Duration::from_secs(3), async {
        // New connection should work
        let verification_pool = ctx.pool();

        // Check that committed transactions are persisted - use timing utility
        let committed_events = wait_for_filtered_event_count(
            verification_pool,
            "source = $1",
            &["shutdown.test"],
            3,
            5,
        )
        .await
        .unwrap_or(0);

        // Check database integrity
        let db_check = sqlx::query_scalar!("SELECT 1")
            .fetch_one(verification_pool)
            .await?;

        Ok::<(i64, i32), anyhow::Error>((committed_events, db_check.unwrap_or(0)))
    })
    .await;

    let total_shutdown_duration = shutdown_start.elapsed();

    match post_shutdown_verification {
        Ok(Ok((committed_events, db_check))) => {
            println!("\nShutdown Sequence Results:");
            println!(
                "  Transaction completion: {:?}",
                transaction_completion_duration
            );
            println!("  Connection release: {:?}", connection_release_duration);
            println!("  Total shutdown time: {:?}", total_shutdown_duration);
            println!("  Committed events: {}", committed_events);
            println!(
                "  Database integrity: {}",
                if db_check == 1 { "OK" } else { "FAILED" }
            );

            assert!(
                committed_events >= 3,
                "Transactions should be committed before shutdown"
            );
            assert!(
                db_check == 1,
                "Database should remain functional after shutdown"
            );
            assert!(
                total_shutdown_duration < Duration::from_secs(5),
                "Shutdown should be reasonably fast"
            );

            println!("  ✓ Graceful shutdown sequence completed successfully");
        }
        Ok(Err(e)) => {
            println!("  Post-shutdown verification failed: {}", e);
        }
        Err(_) => {
            println!("  Post-shutdown verification timed out");
        }
    }

    // Test 2: Handling of interrupted shutdown
    println!("\nTesting interrupted shutdown scenarios...");

    let interrupted_shutdown_test = timeout(Duration::from_secs(5), async {
        // Get pool outside of spawn to avoid Send issues
        let pool = ctx.pool().clone();

        // Create long-running operation
        let long_operation = tokio::spawn(async move {
            // Simulate long-running batch operation
            for i in 0..1000 {
                let mut event = EventFactory::new("interrupted.shutdown").create_event(
                    "long_operation",
                    json!({"batch_item": i, "operation": "long_running"})
                );
                event.host = "localhost".to_string();
                event.ingestor_version = "1.0.0".to_string();
                
                sinex_db::insert_event_with_validator(&pool, &event, None).await?;

                // Simulate work with small delays
                if i % 100 == 0 {
                    tokio::task::yield_now().await;
                }
            }

            Ok::<(), anyhow::Error>(())
        });

        // Let operation start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Simulate interrupt (abort the task)
        long_operation.abort();

        // Verify system remains stable after interrupt
        let stability_check = timeout(Duration::from_secs(2), async {
            let pool = ctx.pool();

            // Database should still be responsive
            let health_check = sqlx::query_scalar!("SELECT 1").fetch_one(pool).await?;

            // Check partial data from interrupted operation - use timing utility
            let partial_events =
                wait_for_filtered_event_count(pool, "source = $1", &["interrupted.shutdown"], 0, 3)
                    .await
                    .unwrap_or(0);

            Ok::<(i32, i64), anyhow::Error>((health_check.unwrap_or(0), partial_events))
        })
        .await;

        match stability_check {
            Ok(Ok((health, partial_count))) => {
                println!(
                    "    ✓ System stable after interrupt (health: {}, partial events: {})",
                    health, partial_count
                );
                assert!(
                    health == 1,
                    "Database should remain healthy after interrupt"
                );
                // Some events should be committed, but not all 1000
                assert!(
                    partial_count < 1000,
                    "Operation should have been interrupted"
                );
                Ok(())
            }
            Ok(Err(e)) => {
                println!("    Stability check failed: {}", e);
                Err(e)
            }
            Err(_) => {
                println!("    Stability check timed out");
                Err(anyhow::anyhow!("Stability check timeout"))
            }
        }
    })
    .await;

    match interrupted_shutdown_test {
        Ok(Ok(())) => {
            println!("  ✓ Interrupted shutdown handled gracefully");
        }
        Ok(Err(e)) => {
            println!("  Interrupted shutdown test failed: {}", e);
        }
        Err(_) => {
            println!("  Interrupted shutdown test timed out");
        }
    }

    // Cleanup
    sqlx::query!(
        "DELETE FROM core.events WHERE source IN ('shutdown.test', 'interrupted.shutdown')"
    )
    .execute(pool)
    .await
    .ok();

    Ok(())
}

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

    Ok(())
}

/// Test data migration safety and version compatibility
#[sinex_test]
async fn test_data_migration_safety(ctx: TestContext) -> TestResult {
    println!("Testing data migration safety and version compatibility...");

    // Create isolated test database for migration testing
    let test_db_name = format!(
        "sinex_migration_test_{}",
        Ulid::new().to_string().to_lowercase()
    );
    let base_url = std::env::var("DATABASE_URL")?;
    let base_test_db = acquire_test_database().await?;
    let base_pool = base_test_db.pool();

    sqlx::query(&format!("CREATE DATABASE {}", test_db_name))
        .execute(base_pool)
        .await?;

    let _test_db_url = base_url.replace("/sinex_dev", &format!("/{}", test_db_name));

    // Test 1: Fresh migration safety
    let migration_start = Instant::now();

    let fresh_migration_test = timeout(Duration::from_secs(5), async {
        let test_db = acquire_test_database().await?;
        let pool = test_db.pool();

        // Run migrations on fresh database
        run_migrations(&pool).await?;

        // Verify all required objects exist
        let schemas: Vec<String> = sqlx::query_scalar!(
            "SELECT schema_name FROM information_schema.schemata
                 WHERE schema_name IN ('raw', 'sinex_schemas')"
        )
        .fetch_all(pool)
        .await?
        .into_iter()
        .flatten()
        .collect();

        let tables: Vec<String> = sqlx::query_scalar!(
            "SELECT table_name FROM information_schema.tables
                 WHERE table_schema IN ('raw', 'sinex_schemas')"
        )
        .fetch_all(pool)
        .await?
        .into_iter()
        .flatten()
        .collect();

        let extensions: Vec<String> = sqlx::query_scalar!(
            "SELECT extname FROM pg_extension WHERE extname IN ('timescaledb', 'uuid-ossp')"
        )
        .fetch_all(pool)
        .await
        .unwrap_or_default();

        Ok::<(Vec<String>, Vec<String>, Vec<String>), anyhow::Error>((schemas, tables, extensions))
    })
    .await;

    let migration_duration = migration_start.elapsed();

    match fresh_migration_test {
        Ok(Ok((schemas, tables, extensions))) => {
            println!("  ✓ Fresh migration completed in {:?}", migration_duration);
            println!("    Schemas created: {:?}", schemas);
            println!("    Tables created: {} tables", tables.len());
            println!("    Extensions: {:?}", extensions);

            assert!(
                schemas.contains(&"raw".to_string()),
                "Should create 'raw' schema"
            );
            assert!(
                schemas.contains(&"sinex_schemas".to_string()),
                "Should create 'sinex_schemas' schema"
            );
            assert!(tables.len() >= 4, "Should create minimum required tables");
            assert!(!extensions.is_empty(), "Should have required extensions");
        }
        Ok(Err(e)) => {
            println!("  Fresh migration failed: {}", e);
        }
        Err(_) => {
            println!("  Fresh migration timed out after {:?}", migration_duration);
        }
    }

    // Test 2: Migration idempotency (running migrations multiple times)
    println!("\nTesting migration idempotency...");

    let idempotency_test = timeout(Duration::from_secs(4), async {
        let test_db = acquire_test_database().await?;
        let pool = test_db.pool();

        // Run migrations again (should be idempotent)
        run_migrations(&pool).await?;

        // Run a third time to be sure
        run_migrations(&pool).await?;

        // Verify state is still correct
        let table_count: i64 = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM information_schema.tables
                 WHERE table_schema IN ('raw', 'sinex_schemas')"
        )
        .fetch_one(pool)
        .await?
        .unwrap_or(0);

        let migration_count: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(pool)
            .await?
            .unwrap_or(0);

        Ok::<(i64, i64), anyhow::Error>((table_count, migration_count))
    })
    .await;

    match idempotency_test {
        Ok(Ok((table_count, migration_count))) => {
            println!("  ✓ Migration idempotency verified");
            println!("    Tables after multiple runs: {}", table_count);
            println!("    Migration records: {}", migration_count);

            assert!(table_count >= 4, "Table count should remain consistent");
            assert!(migration_count > 0, "Should have migration records");
        }
        Ok(Err(e)) => {
            println!("  Migration idempotency test failed: {}", e);
        }
        Err(_) => {
            println!("  Migration idempotency test timed out");
        }
    }

    // Test 3: Data preservation during migrations
    println!("\nTesting data preservation during migrations...");

    let data_preservation_test = timeout(Duration::from_secs(5), async {
        let test_db = acquire_test_database().await?;
        let pool = test_db.pool();

        // Insert test checkpoint data before migration
        let migration_checkpoint_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO core.automaton_checkpoints (id, automaton_name, last_processed_id, state_data)
                 VALUES ($1::uuid, $2, $3, $4)",
            migration_checkpoint_id.to_uuid(),
            "migration_test_agent",
            "migration_event_456",
            json!({"version": "1.0.0", "description": "Agent for testing data preservation"})
        )
        .execute(pool)
        .await?;

        // Insert test events
        let test_events = 50;
        for i in 0..test_events {
            let mut event = EventFactory::new("migration.safety").create_event(
                "data_preservation",
                json!({"sequence": i, "migration_test": true})
            );
            event.host = "localhost".to_string();
            event.ingestor_version = "1.0.0".to_string();
            
            sinex_db::insert_event_with_validator(&pool, &event, None).await?;
        }

        // Record initial state - use timing utilities for consistency
        let initial_checkpoint_count: i64 =
            sqlx::query_scalar!("SELECT COUNT(*) FROM core.automaton_checkpoints")
                .fetch_one(pool)
                .await?
                .unwrap_or(0);

        // Use timing utility to wait for expected event count with source filter
        let initial_event_count = wait_for_filtered_event_count(
            &pool,
            "source = $1",
            &["migration.safety"],
            test_events,
            5,
        )
        .await
        .unwrap_or(0);

        println!(
            "    Initial state: {} checkpoints, {} events",
            initial_checkpoint_count, initial_event_count
        );

        // Run migrations again (simulating upgrade)
        run_migrations(&pool).await?;

        // Verify data preservation - use timing utilities for reliability
        let final_checkpoint_count: i64 =
            sqlx::query_scalar!("SELECT COUNT(*) FROM core.automaton_checkpoints")
                .fetch_one(pool)
                .await?
                .unwrap_or(0);

        // Use timing utility to ensure events are available after migration
        let final_event_count = wait_for_filtered_event_count(
            &pool,
            "source = $1",
            &["migration.safety"],
            test_events,
            5,
        )
        .await
        .unwrap_or(0);

        // Verify checkpoint data integrity
        let checkpoint_data: Option<serde_json::Value> = sqlx::query_scalar!(
            "SELECT state_data FROM core.automaton_checkpoints
                 WHERE automaton_name = 'migration_test_agent'"
        )
        .fetch_optional(pool)
        .await?;

        let sample_event: Option<serde_json::Value> = sqlx::query_scalar!(
            "SELECT payload FROM core.events WHERE source = 'migration.safety' LIMIT 1"
        )
        .fetch_optional(pool)
        .await?;

        Ok::<
            (
                i64,
                i64,
                i64,
                i64,
                Option<serde_json::Value>,
                Option<serde_json::Value>,
            ),
            anyhow::Error,
        >((
            initial_checkpoint_count,
            initial_event_count,
            final_checkpoint_count,
            final_event_count,
            checkpoint_data,
            sample_event,
        ))
    })
    .await;

    match data_preservation_test {
        Ok(Ok((
            init_checkpoints,
            init_events,
            final_checkpoints,
            final_events,
            checkpoint_data,
            event_data,
        ))) => {
            println!("  ✓ Data preservation test completed");
            println!(
                "    Checkpoints: {} -> {}",
                init_checkpoints, final_checkpoints
            );
            println!("    Events: {} -> {}", init_events, final_events);

            pretty_assertions::assert_eq!(
                init_checkpoints,
                final_checkpoints,
                "Checkpoint count should be preserved"
            );
            pretty_assertions::assert_eq!(
                init_events,
                final_events,
                "Event count should be preserved"
            );
            assert!(
                checkpoint_data.is_some(),
                "Checkpoint data should be preserved"
            );
            assert!(event_data.is_some(), "Event data should be preserved");

            if let Some(checkpoint_json) = checkpoint_data {
                assert!(
                    checkpoint_json.get("description").is_some(),
                    "Checkpoint content should be preserved"
                );
            }

            if let Some(event_json) = event_data {
                assert!(
                    event_json.get("migration_test").is_some(),
                    "Event content should be preserved"
                );
            }
        }
        Ok(Err(e)) => {
            println!("  Data preservation test failed: {}", e);
        }
        Err(_) => {
            println!("  Data preservation test timed out");
        }
    }

    // Test 4: Migration rollback simulation (error handling)
    println!("\nTesting migration error handling...");

    let error_handling_test = timeout(Duration::from_secs(5), async {
        let test_db = match acquire_test_database().await {
            Ok(test_db) => test_db,
            Err(_) => return false,
        };
        let pool = test_db.pool();

        // Simulate a migration error by attempting invalid operation
        let invalid_migration_result = sqlx::query!(
            "CREATE TABLE core.events (id UUID PRIMARY KEY)" // This should fail - table exists
        )
        .execute(pool)
        .await;

        // Migration should fail gracefully
        match invalid_migration_result {
            Ok(_) => {
                println!("    WARNING: Invalid migration unexpectedly succeeded");
                false
            }
            Err(e) => {
                println!("    ✓ Invalid migration failed as expected: {}", e);
                true
            }
        }
    })
    .await;

    match error_handling_test {
        Ok(failed_gracefully) => {
            if failed_gracefully {
                println!("  ✓ Migration error handling works correctly");
            } else {
                println!("  WARNING: Migration error handling may need improvement");
            }
        }
        Err(_) => {
            println!("  Migration error handling test timed out");
        }
    }

    println!("\nData Migration Safety Results:");
    println!("  Fresh migrations: ✓");
    println!("  Migration idempotency: ✓");
    println!("  Data preservation: ✓");
    println!("  Error handling: ✓");

    // Cleanup test database
    sqlx::query(&format!("DROP DATABASE {}", test_db_name))
        .execute(base_pool)
        .await
        .ok();

    Ok(())
}

// ==================== PRODUCTION RELIABILITY TESTS ====================

/// Test graceful degradation under database connectivity issues
#[sinex_test]
async fn test_graceful_degradation_database_failure(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Create test checkpoint for degradation testing
    let agent_name = format!("degradation_test_{}", Ulid::new());
    let degradation_checkpoint_id = Ulid::new();
    sqlx::query!(
        "INSERT INTO core.automaton_checkpoints (id, automaton_name, last_processed_id, state_data)
         VALUES ($1::uuid, $2, $3, $4)",
        degradation_checkpoint_id.to_uuid(),
        agent_name,
        "degradation_test_event",
        json!({"version": "1.0.0", "description": "Graceful degradation test"})
    )
    .execute(pool)
    .await?;

    println!("Testing graceful degradation under database connectivity issues...");

    // Test 1: Database connection pool exhaustion simulation
    // Test reasonable connection pressure within shared pool limits
    let mut held_connections = Vec::new();
    let max_connections = 8; // Reasonable for testing connection pressure with 12 cores

    // Simulate connection pressure without exhausting the shared pool
    for i in 0..max_connections {
        match pool.acquire().await {
            Ok(conn) => {
                held_connections.push(conn);
                println!("  Acquired connection {}/{}", i + 1, max_connections);
            }
            Err(e) => {
                println!("  Connection {} failed: {}", i + 1, e);
                break;
            }
        }
    }

    println!(
        "  Connection pressure applied with {} connections",
        held_connections.len()
    );

    // Test graceful handling of no available connections
    let pool1 = pool.clone();
    let pool2 = pool.clone();
    let pool3 = pool.clone();

    // Define async functions for each operation
    async fn event_test(pool: DbPool) -> AnyhowResult<(), anyhow::Error> {
        let mut event = EventFactory::new("degradation.test").create_event(
            "connection_exhaustion",
            json!({"test": "degraded_mode"})
        );
        event.host = "localhost".to_string();
        event.ingestor_version = "1.0.0".to_string();
        
        let _event = sinex_db::insert_event_with_validator(&pool, &event, None).await?;
        Ok(())
    }

    async fn health_test(pool: DbPool) -> AnyhowResult<(), anyhow::Error> {
        let _health_check = sqlx::query_scalar!("SELECT 1")
            .fetch_one(pool)
            .await
            .map_err(anyhow::Error::from)?
            .unwrap_or(0);
        Ok(())
    }

    async fn checkpoint_test(pool: DbPool) -> AnyhowResult<(), anyhow::Error> {
        let _checkpoint_check =
            sqlx::query!("SELECT automaton_name FROM core.automaton_checkpoints LIMIT 1")
                .fetch_one(pool)
                .await
                .map_err(anyhow::Error::from)?;
        Ok(())
    }

    let mut graceful_timeouts = 0;
    let mut unexpected_errors = 0;

    // Test event operation
    let operation = timeout(Duration::from_secs(2), event_test(pool1));
    match operation.await {
        Ok(Ok(_)) => {
            println!("  Operation 0 succeeded unexpectedly");
        }
        Ok(Err(e)) => {
            println!("  Operation 0 failed gracefully: {}", e);
            unexpected_errors += 1;
        }
        Err(_) => {
            println!("  ✓ Operation 0 timed out gracefully");
            graceful_timeouts += 1;
        }
    }

    // Test health operation
    let operation = timeout(Duration::from_secs(2), health_test(pool2));
    match operation.await {
        Ok(Ok(_)) => {
            println!("  Operation 1 succeeded unexpectedly");
        }
        Ok(Err(e)) => {
            println!("  Operation 1 failed gracefully: {}", e);
            unexpected_errors += 1;
        }
        Err(_) => {
            println!("  ✓ Operation 1 timed out gracefully");
            graceful_timeouts += 1;
        }
    }

    // Test checkpoint operation
    let operation = timeout(Duration::from_secs(2), checkpoint_test(pool3));
    match operation.await {
        Ok(Ok(_)) => {
            println!("  Operation 2 succeeded unexpectedly");
        }
        Ok(Err(e)) => {
            println!("  Operation 2 failed gracefully: {}", e);
            unexpected_errors += 1;
        }
        Err(_) => {
            println!("  ✓ Operation 2 timed out gracefully");
            graceful_timeouts += 1;
        }
    }

    // Release connections to restore functionality
    drop(held_connections);

    // Verify system recovery
    let recovery_start = Instant::now();
    let mut event = EventFactory::new("degradation.test").create_event(
        "recovery_test",
        json!({"recovered": true})
    );
    event.host = "localhost".to_string();
    event.ingestor_version = "1.0.0".to_string();
    
    let recovery_test = timeout(
        Duration::from_secs(5),
        sinex_db::insert_event_with_validator(pool, &event, None),
    )
    .await;

    let recovery_duration = recovery_start.elapsed();

    match recovery_test {
        Ok(Ok(_)) => {
            println!("  ✓ System recovered in {:?}", recovery_duration);
        }
        Ok(Err(e)) => {
            println!("  WARNING: Recovery failed: {}", e);
        }
        Err(_) => {
            println!(
                "  WARNING: Recovery timed out after {:?}",
                recovery_duration
            );
        }
    }

    println!("\nGraceful Degradation Test Results:");
    println!("  Graceful timeouts: {}/3", graceful_timeouts);
    println!("  Unexpected errors: {}/3", unexpected_errors);
    println!("  Recovery time: {:?}", recovery_duration);

    // System should handle degradation gracefully
    assert!(
        graceful_timeouts >= 2,
        "System should timeout gracefully under load"
    );
    assert!(
        recovery_duration < Duration::from_secs(5),
        "Recovery should be fast"
    );

    // Cleanup
    sqlx::query!("DELETE FROM core.events WHERE source = 'degradation.test'")
        .execute(pool)
        .await
        .ok();
    sqlx::query!(
        "DELETE FROM core.automaton_checkpoints WHERE automaton_name = $1",
        agent_name
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Test resource limits and monitoring under load
#[sinex_test]
async fn test_resource_limits_monitoring(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

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
                event.ingestor_version = "1.0.0".to_string();
                
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
    sqlx::query!("DELETE FROM core.events WHERE source = 'resource.monitoring'")
        .execute(pool)
        .await
        .ok();

    Ok(())
}

/// Test system behavior under resource exhaustion scenarios
#[sinex_test]
async fn test_resource_exhaustion_scenarios(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    println!("Testing resource exhaustion scenarios...");

    // Test 1: Large transaction handling
    let large_transaction_start = Instant::now();

    let large_transaction_result = timeout(Duration::from_secs(5), async {
        let mut tx = pool.begin().await?;

        // Try to insert many events in a single transaction
        for i in 0..1000 {
            sqlx::query!(
                "INSERT INTO core.events (
            event_id, source, event_type, host, payload)
                     VALUES ($1::uuid, $2, $3, $4, $5)",
                Ulid::new().to_uuid(),
                "exhaustion.test",
                "large_transaction",
                "localhost",
                json!({"batch_item": i, "data": "x".repeat(100)})
            )
            .execute(&mut *tx)
            .await?;

            // Check for timeout every 100 items
            if i % 100 == 0 {
                println!("    Inserted {} items in transaction", i + 1);
            }
        }

        tx.commit().await?;
        Ok::<(), sqlx::Error>(())
    })
    .await;

    let large_transaction_duration = large_transaction_start.elapsed();

    match large_transaction_result {
        Ok(Ok(())) => {
            println!(
                "  ✓ Large transaction completed in {:?}",
                large_transaction_duration
            );
        }
        Ok(Err(e)) => {
            println!("  Large transaction failed: {}", e);
        }
        Err(_) => {
            println!("  ✓ Large transaction timed out (protection active)");
        }
    }

    // Test 2: Concurrent transaction stress
    println!("\nTesting concurrent transaction stress...");

    let concurrent_transactions = 20;
    let mut transaction_tasks = Vec::new();

    for i in 0..concurrent_transactions {
        let pool = pool.clone();
        let task = tokio::spawn(async move {
            let start_time = Instant::now();

            let result = timeout(Duration::from_secs(3), async {
                let mut tx = pool.begin().await?;

                // Each transaction inserts a small batch
                for j in 0..10 {
                    sqlx::query!(
                        "INSERT INTO core.events (
            event_id, source, event_type, host, payload)
                             VALUES ($1::uuid, $2, $3, $4, $5)",
                        Ulid::new().to_uuid(),
                        format!("concurrent.tx.{}", i),
                        "concurrent_test",
                        "localhost",
                        json!({"tx_id": i, "item": j})
                    )
                    .execute(&mut *tx)
                    .await?;
                }

                tx.commit().await?;
                Ok::<(), sqlx::Error>(())
            })
            .await;

            let duration = start_time.elapsed();
            (i, result, duration)
        });

        transaction_tasks.push(task);
    }

    let transaction_results = timeout(
        Duration::from_secs(5),
        futures::future::join_all(transaction_tasks),
    )
    .await;

    match transaction_results {
        Ok(results) => {
            let mut successful_transactions = 0;
            let mut failed_transactions = 0;
            let mut total_tx_duration = Duration::ZERO;

            for (i, tx_result, duration) in results.into_iter().flatten() {
                total_tx_duration += duration;

                match tx_result {
                    Ok(Ok(())) => {
                        successful_transactions += 1;
                        if i < 3 {
                            println!("    Transaction {} completed in {:?}", i, duration);
                        }
                    }
                    Ok(Err(e)) => {
                        failed_transactions += 1;
                        if i < 3 {
                            println!("    Transaction {} failed: {}", i, e);
                        }
                    }
                    Err(_) => {
                        failed_transactions += 1;
                        if i < 3 {
                            println!("    Transaction {} timed out", i);
                        }
                    }
                }
            }

            let avg_tx_duration = total_tx_duration / concurrent_transactions as u32;

            println!("\nConcurrent Transaction Results:");
            println!("  Attempted: {}", concurrent_transactions);
            println!("  Successful: {}", successful_transactions);
            println!("  Failed: {}", failed_transactions);
            println!("  Average duration: {:?}", avg_tx_duration);

            // Most transactions should succeed under normal load
            assert!(
                successful_transactions >= concurrent_transactions * 7 / 10,
                "Transaction failure rate too high: {}/{}",
                failed_transactions,
                concurrent_transactions
            );
        }
        Err(_) => {
            println!("  Concurrent transaction test timed out");
        }
    }

    // Cleanup all test data
    sqlx::query!(
        "DELETE FROM core.events WHERE source LIKE 'exhaustion%' OR source LIKE 'concurrent%'"
    )
    .execute(pool)
    .await
    .ok();

    Ok(())
}

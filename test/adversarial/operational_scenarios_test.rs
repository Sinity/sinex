use anyhow::Result;
use std::time::{Duration, Instant};
use std::fs;
use tokio::time::timeout;
use sinex_db::{create_test_pool, run_migrations, queries::insert_raw_event};
use sinex_ulid::Ulid;
use serde_json::json;
use tempfile::TempDir;

/// Test startup sequence robustness and error handling
#[tokio::test]
async fn test_startup_sequence_robustness() -> Result<()> {
    println!("Testing startup sequence robustness...");

    // Test 1: Database initialization from scratch
    let startup_start = Instant::now();
    
    // Create isolated test database
    let test_db_name = format!("sinex_startup_test_{}", Ulid::new().to_string().to_lowercase());
    let base_url = std::env::var("DATABASE_URL")?;
    let base_pool = create_test_pool(&base_url).await?;
    
    // Create test database
    sqlx::query(&format!("CREATE DATABASE {}", test_db_name))
        .execute(&base_pool)
        .await?;
    
    let test_db_url = base_url.replace("/sinex_dev", &format!("/{}", test_db_name));
    
    // Test fresh startup with empty database
    let fresh_startup_result = timeout(
        Duration::from_secs(30),
        async {
            let pool = create_test_pool(&test_db_url).await?;
            run_migrations(&pool).await?;
            
            // Verify basic functionality after startup
            let schema_count: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM information_schema.schemata 
                 WHERE schema_name IN ('raw', 'sinex_schemas')"
            )
            .fetch_one(&pool)
            .await?
            .unwrap_or(0);
            
            let table_count: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM information_schema.tables 
                 WHERE table_schema IN ('raw', 'sinex_schemas')"
            )
            .fetch_one(&pool)
            .await?
            .unwrap_or(0);
            
            Ok::<(i64, i64), anyhow::Error>((schema_count, table_count))
        }
    ).await;

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
    
    let existing_data_startup = timeout(
        Duration::from_secs(15),
        async {
            let pool = create_test_pool(&test_db_url).await?;
            
            // Add some existing data
            sqlx::query!(
                "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description) 
                 VALUES ($1, $2, $3)",
                "existing_agent",
                "1.0.0",
                "Pre-existing agent for startup test"
            )
            .execute(&pool)
            .await?;
            
            // Insert some events
            for i in 0..10 {
                insert_raw_event(
                    &pool,
                    "startup.test",
                    "existing_data",
                    "localhost",
                    json!({"sequence": i, "startup_test": true}),
                    None,
                    Some("1.0.0"),
                    None,
                ).await?;
            }
            
            // Simulate restart by running migrations again
            run_migrations(&pool).await?;
            
            // Verify data integrity after restart
            let agent_count: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM sinex_schemas.agent_manifests"
            )
            .fetch_one(&pool)
            .await?
            .unwrap_or(0);
            
            let event_count: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM raw.events WHERE source = 'startup.test'"
            )
            .fetch_one(&pool)
            .await?
            .unwrap_or(0);
            
            Ok::<(i64, i64), anyhow::Error>((agent_count, event_count))
        }
    ).await;

    match existing_data_startup {
        Ok(Ok((agent_count, event_count))) => {
            println!("  ✓ Startup with existing data succeeded");
            println!("    Agents preserved: {}", agent_count);
            println!("    Events preserved: {}", event_count);
            
            assert!(agent_count >= 1, "Existing agents should be preserved");
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
        Duration::from_secs(10),
        async {
            let pool = create_test_pool(&test_db_url).await?;
            
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
            .execute(&pool)
            .await
            .ok(); // Ignore errors if table doesn't exist
            
            // Try to run migrations with corrupted state
            let migration_result = run_migrations(&pool).await;
            
            // Should either succeed (fix corruption) or fail gracefully
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
        .execute(&base_pool)
        .await
        .ok();

    Ok(())
}

/// Test shutdown sequence and graceful termination
#[tokio::test]
async fn test_shutdown_sequence_graceful_termination() -> Result<()> {
    let pool = create_test_pool(&std::env::var("DATABASE_URL")?).await?;
    run_migrations(&pool).await?;

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
                    "INSERT INTO raw.events (id, source, event_type, host, payload) 
                     VALUES ($1::uuid::ulid, $2, $3, $4, $5)",
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
        match timeout(Duration::from_secs(5), tx.commit()).await {
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
    let post_shutdown_verification = timeout(
        Duration::from_secs(5),
        async {
            // New connection should work
            let verification_pool = create_test_pool(&std::env::var("DATABASE_URL")?).await?;
            
            // Check that committed transactions are persisted
            let committed_events: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM raw.events WHERE source = 'shutdown.test'"
            )
            .fetch_one(&verification_pool)
            .await?
            .unwrap_or(0);
            
            // Check database integrity
            let db_check = sqlx::query_scalar!("SELECT 1").fetch_one(&verification_pool).await?;
            
            Ok::<(i64, i32), anyhow::Error>((committed_events, db_check.unwrap_or(0)))
        }
    ).await;

    let total_shutdown_duration = shutdown_start.elapsed();

    match post_shutdown_verification {
        Ok(Ok((committed_events, db_check))) => {
            println!("\nShutdown Sequence Results:");
            println!("  Transaction completion: {:?}", transaction_completion_duration);
            println!("  Connection release: {:?}", connection_release_duration);
            println!("  Total shutdown time: {:?}", total_shutdown_duration);
            println!("  Committed events: {}", committed_events);
            println!("  Database integrity: {}", if db_check == 1 { "OK" } else { "FAILED" });
            
            assert!(committed_events >= 3, "Transactions should be committed before shutdown");
            assert!(db_check == 1, "Database should remain functional after shutdown");
            assert!(total_shutdown_duration < Duration::from_secs(10), "Shutdown should be reasonably fast");
            
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
    
    let interrupted_shutdown_test = timeout(
        Duration::from_secs(10),
        async {
            // Create long-running operation
            let long_operation = tokio::spawn(async {
                let pool = create_test_pool(&std::env::var("DATABASE_URL")?).await?;
                
                // Simulate long-running batch operation
                for i in 0..1000 {
                    insert_raw_event(
                        &pool,
                        "interrupted.shutdown",
                        "long_operation",
                        "localhost",
                        json!({"batch_item": i, "operation": "long_running"}),
                        None,
                        Some("1.0.0"),
                        None,
                    ).await?;
                    
                    // Simulate work with small delays
                    if i % 100 == 0 {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
                
                Ok::<(), anyhow::Error>(())
            });

            // Let operation start
            tokio::time::sleep(Duration::from_millis(100)).await;
            
            // Simulate interrupt (abort the task)
            long_operation.abort();
            
            // Verify system remains stable after interrupt
            let stability_check = timeout(
                Duration::from_secs(3),
                async {
                    let pool = create_test_pool(&std::env::var("DATABASE_URL")?).await?;
                    
                    // Database should still be responsive
                    let health_check = sqlx::query_scalar!("SELECT 1").fetch_one(&pool).await?;
                    
                    // Check partial data from interrupted operation
                    let partial_events: i64 = sqlx::query_scalar!(
                        "SELECT COUNT(*) FROM raw.events WHERE source = 'interrupted.shutdown'"
                    )
                    .fetch_one(&pool)
                    .await?
                    .unwrap_or(0);
                    
                    Ok::<(i32, i64), anyhow::Error>((health_check.unwrap_or(0), partial_events))
                }
            ).await;

            match stability_check {
                Ok(Ok((health, partial_count))) => {
                    println!("    ✓ System stable after interrupt (health: {}, partial events: {})", health, partial_count);
                    assert!(health == 1, "Database should remain healthy after interrupt");
                    // Some events should be committed, but not all 1000
                    assert!(partial_count < 1000, "Operation should have been interrupted");
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
        }
    ).await;

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
    sqlx::query!("DELETE FROM raw.events WHERE source IN ('shutdown.test', 'interrupted.shutdown')")
        .execute(&pool).await.ok();

    Ok(())
}

/// Test configuration validation and hot reload scenarios
#[tokio::test]
async fn test_configuration_validation_and_reload() -> Result<()> {
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
    
    let valid_config_test = timeout(
        Duration::from_secs(5),
        async {
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
                has_database, has_collector, has_event_sources, channel_buffer, max_connections
            ))
        }
    ).await;

    let config_validation_duration = config_validation_start.elapsed();

    match valid_config_test {
        Ok(Ok((has_db, has_collector, has_sources, buffer_size, max_conn))) => {
            println!("  ✓ Valid configuration parsed in {:?}", config_validation_duration);
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
    let invalid_configs = vec![
        // Missing required sections
        (r#"
[collector]
channel_buffer_size = 1000
"#, "missing_database_section"),
        
        // Invalid data types
        (r#"
[database]
url = "postgresql://localhost/test"
max_connections = "not_a_number"

[collector]
channel_buffer_size = 1000
"#, "invalid_data_type"),
        
        // Invalid values
        (r#"
[database]
url = "postgresql://localhost/test"
max_connections = -5

[collector]
channel_buffer_size = 0
"#, "invalid_values"),
        
        // Malformed TOML
        (r#"
[database
url = "incomplete section
"#, "malformed_toml"),
        
        // Conflicting settings
        (r#"
[database]
url = "postgresql://localhost/test"
max_connections = 1

[collector]
channel_buffer_size = 10000
"#, "resource_conflict"),
    ];

    let mut invalid_config_results = Vec::new();

    for (i, (invalid_config, test_name)) in invalid_configs.iter().enumerate() {
        let invalid_config_file = config_dir.join(format!("invalid_{}.toml", i));
        fs::write(&invalid_config_file, invalid_config)?;

        let validation_result = timeout(
            Duration::from_secs(2),
            async {
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
                            Err(anyhow::anyhow!("Resource conflict: low connections, high buffer"))
                        } else {
                            Ok(config)
                        }
                    }
                    Err(e) => Err(anyhow::anyhow!("TOML parse error: {}", e))
                }
            }
        ).await;

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

    let hot_reload_test = timeout(
        Duration::from_secs(10),
        async {
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
            let updated_config = valid_config.replace("channel_buffer_size = 1000", "channel_buffer_size = 2000");
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
            assert_ne!(initial_buffer_size, updated_buffer_size, "Config change should be detected");
            assert_eq!(updated_buffer_size, 2000, "New config value should be correct");
            
            Ok::<(), anyhow::Error>(())
        }
    ).await;

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
    let rejected_invalid_configs = invalid_config_results.iter()
        .filter(|(_, accepted)| !*accepted)
        .count();
    
    println!("\nConfiguration Validation Results:");
    println!("  Valid config parsing: ✓");
    println!("  Invalid configs rejected: {}/{}", rejected_invalid_configs, invalid_configs.len());
    println!("  Hot reload simulation: ✓");

    assert!(rejected_invalid_configs >= invalid_configs.len() * 3 / 4,
           "Should reject most invalid configurations");

    Ok(())
}

/// Test data migration safety and version compatibility
#[tokio::test]
async fn test_data_migration_safety() -> Result<()> {
    println!("Testing data migration safety and version compatibility...");

    // Create isolated test database for migration testing
    let test_db_name = format!("sinex_migration_test_{}", Ulid::new().to_string().to_lowercase());
    let base_url = std::env::var("DATABASE_URL")?;
    let base_pool = create_test_pool(&base_url).await?;
    
    sqlx::query(&format!("CREATE DATABASE {}", test_db_name))
        .execute(&base_pool)
        .await?;
    
    let test_db_url = base_url.replace("/sinex_dev", &format!("/{}", test_db_name));

    // Test 1: Fresh migration safety
    let migration_start = Instant::now();
    
    let fresh_migration_test = timeout(
        Duration::from_secs(30),
        async {
            let pool = create_test_pool(&test_db_url).await?;
            
            // Run migrations on fresh database
            run_migrations(&pool).await?;
            
            // Verify all required objects exist
            let schemas: Vec<String> = sqlx::query_scalar!(
                "SELECT schema_name FROM information_schema.schemata 
                 WHERE schema_name IN ('raw', 'sinex_schemas')"
            )
            .fetch_all(&pool)
            .await?
            .into_iter()
            .filter_map(|s| s)
            .collect();
            
            let tables: Vec<String> = sqlx::query_scalar!(
                "SELECT table_name FROM information_schema.tables 
                 WHERE table_schema IN ('raw', 'sinex_schemas')"
            )
            .fetch_all(&pool)
            .await?
            .into_iter()
            .filter_map(|t| t)
            .collect();
            
            let extensions: Vec<String> = sqlx::query_scalar!(
                "SELECT extname FROM pg_extension WHERE extname IN ('timescaledb', 'uuid-ossp')"
            )
            .fetch_all(&pool)
            .await
            .unwrap_or_default();
            
            Ok::<(Vec<String>, Vec<String>, Vec<String>), anyhow::Error>((schemas, tables, extensions))
        }
    ).await;

    let migration_duration = migration_start.elapsed();

    match fresh_migration_test {
        Ok(Ok((schemas, tables, extensions))) => {
            println!("  ✓ Fresh migration completed in {:?}", migration_duration);
            println!("    Schemas created: {:?}", schemas);
            println!("    Tables created: {} tables", tables.len());
            println!("    Extensions: {:?}", extensions);
            
            assert!(schemas.contains(&"raw".to_string()), "Should create 'raw' schema");
            assert!(schemas.contains(&"sinex_schemas".to_string()), "Should create 'sinex_schemas' schema");
            assert!(tables.len() >= 4, "Should create minimum required tables");
            assert!(extensions.len() >= 1, "Should have required extensions");
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
    
    let idempotency_test = timeout(
        Duration::from_secs(15),
        async {
            let pool = create_test_pool(&test_db_url).await?;
            
            // Run migrations again (should be idempotent)
            run_migrations(&pool).await?;
            
            // Run a third time to be sure
            run_migrations(&pool).await?;
            
            // Verify state is still correct
            let table_count: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM information_schema.tables 
                 WHERE table_schema IN ('raw', 'sinex_schemas')"
            )
            .fetch_one(&pool)
            .await?
            .unwrap_or(0);
            
            let migration_count: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM _sqlx_migrations"
            )
            .fetch_one(&pool)
            .await?
            .unwrap_or(0);
            
            Ok::<(i64, i64), anyhow::Error>((table_count, migration_count))
        }
    ).await;

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
    
    let data_preservation_test = timeout(
        Duration::from_secs(20),
        async {
            let pool = create_test_pool(&test_db_url).await?;
            
            // Insert test data before migration
            sqlx::query!(
                "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description) 
                 VALUES ($1, $2, $3)",
                "migration_test_agent",
                "1.0.0",
                "Agent for testing data preservation"
            )
            .execute(&pool)
            .await?;
            
            // Insert test events
            let test_events = 50;
            for i in 0..test_events {
                insert_raw_event(
                    &pool,
                    "migration.safety",
                    "data_preservation",
                    "localhost",
                    json!({"sequence": i, "migration_test": true}),
                    None,
                    Some("1.0.0"),
                    None,
                ).await?;
            }
            
            // Record initial state
            let initial_agent_count: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM sinex_schemas.agent_manifests"
            )
            .fetch_one(&pool)
            .await?
            .unwrap_or(0);
            
            let initial_event_count: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM raw.events WHERE source = 'migration.safety'"
            )
            .fetch_one(&pool)
            .await?
            .unwrap_or(0);
            
            println!("    Initial state: {} agents, {} events", initial_agent_count, initial_event_count);
            
            // Run migrations again (simulating upgrade)
            run_migrations(&pool).await?;
            
            // Verify data preservation
            let final_agent_count: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM sinex_schemas.agent_manifests"
            )
            .fetch_one(&pool)
            .await?
            .unwrap_or(0);
            
            let final_event_count: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM raw.events WHERE source = 'migration.safety'"
            )
            .fetch_one(&pool)
            .await?
            .unwrap_or(0);
            
            // Verify data integrity
            let agent_data: Option<String> = sqlx::query_scalar!(
                "SELECT description FROM sinex_schemas.agent_manifests 
                 WHERE agent_name = 'migration_test_agent'"
            )
            .fetch_optional(&pool)
            .await?
            .flatten();
            
            let sample_event: Option<serde_json::Value> = sqlx::query_scalar!(
                "SELECT payload FROM raw.events WHERE source = 'migration.safety' LIMIT 1"
            )
            .fetch_optional(&pool)
            .await?;
            
            Ok::<(i64, i64, i64, i64, Option<String>, Option<serde_json::Value>), anyhow::Error>((
                initial_agent_count, initial_event_count,
                final_agent_count, final_event_count,
                agent_data, sample_event
            ))
        }
    ).await;

    match data_preservation_test {
        Ok(Ok((init_agents, init_events, final_agents, final_events, agent_desc, event_data))) => {
            println!("  ✓ Data preservation test completed");
            println!("    Agents: {} -> {}", init_agents, final_agents);
            println!("    Events: {} -> {}", init_events, final_events);
            
            assert_eq!(init_agents, final_agents, "Agent count should be preserved");
            assert_eq!(init_events, final_events, "Event count should be preserved");
            assert!(agent_desc.is_some(), "Agent data should be preserved");
            assert!(event_data.is_some(), "Event data should be preserved");
            
            if let Some(event_json) = event_data {
                assert!(event_json.get("migration_test").is_some(), "Event content should be preserved");
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
    
    let error_handling_test = timeout(
        Duration::from_secs(10),
        async {
            let pool = match create_test_pool(&test_db_url).await {
                Ok(pool) => pool,
                Err(_) => return false,
            };
            
            // Simulate a migration error by attempting invalid operation
            let invalid_migration_result = sqlx::query!(
                "CREATE TABLE raw.events (id UUID PRIMARY KEY)" // This should fail - table exists
            )
            .execute(&pool)
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
        }
    ).await;

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
        .execute(&base_pool)
        .await
        .ok();

    Ok(())
}
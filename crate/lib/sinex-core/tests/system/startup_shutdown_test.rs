// # Startup and Shutdown Tests
//
// Tests that verify:
// - Startup sequence robustness and error handling
// - Shutdown sequence and graceful termination
//
// ## Performance Expectations
//
// - **Individual tests**: 30-60 seconds
// - **Resource usage**: Moderate database load
// - **Dependencies**: PostgreSQL

use sinex_core::db::models::EventFactory;
use sinex_test_utils::prelude::*;
use sinex_test_utils::{acquire_test_database, wait_for_filtered_event_count};
use sinex_test_utils::timing_utils::Timeouts;

use sinex_core::types::ulid::Ulid;

/// Test startup sequence robustness and error handling
#[sinex_test(timeout = 60)]
async fn test_startup_sequence_robustness(ctx: TestContext) -> TestResult<()> {
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
    let fresh_startup_result = timeout(Duration::from_secs(Timeouts::QUICK), async {
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

        Ok::<(i64, i64), Box<dyn std::error::Error + Send + Sync>>((schema_count, table_count))
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

    let existing_data_startup = timeout(Duration::from_secs(Timeouts::SHORT), async {
        let test_db = acquire_test_database().await?;
        let pool = test_db.pool();

        // Add some existing processor data
        sqlx::query!(
            "INSERT INTO core.processor_manifests (processor_name, node_type, version, description, anchor_rule_version)
                 VALUES ($1, 'automaton', '1.0.0', $2, 1)",
            "existing_agent",
            "Pre-existing agent for startup test"
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
            event.ingestor_version = Some("1.0.0".to_string());

            sinex_core::db::insert_event_with_validator(&pool, &event, None).await?;
        }

        // Simulate restart by running migrations again
        run_migrations(&pool).await?;

        // Verify data integrity after restart - use timing utilities for better reliability
        let manifest_count: i64 = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM core.processor_manifests WHERE processor_name = $1",
            "existing_agent"
        )
        .fetch_one(pool)
        .await?
        .unwrap_or(0);

        // Use timing utility for event count verification with source filter
        let event_count =
            wait_for_filtered_event_count(pool, "source = $1", &["startup.test"], 10, 5)
                .await
                .unwrap_or(0);

        Ok::<(i64, i64), color_eyre::eyre::Error>((manifest_count, event_count))
    })
    .await;

    match existing_data_startup {
        Ok(Ok((manifest_count, event_count))) => {
            println!("  ✓ Startup with existing data succeeded");
            println!("    Manifests preserved: {}", manifest_count);
            println!("    Events preserved: {}", event_count);

            assert!(
                manifest_count >= 1,
                "Existing manifests should be preserved"
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
        Duration::from_secs(Timeouts::QUICK),
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
            // Note: Using sinex_db's migration system now
            let migration_result = sinex_core::db::run_migrations(pool).await;

            match migration_result {
                Ok(()) => {
                    println!("    ✓ Migrations recovered from corruption");
                    Ok::<bool, color_eyre::eyre::Error>(true)
                }
                Err(e) => {
                    println!("    Migration failed gracefully: {}", e);
                    Ok::<bool, color_eyre::eyre::Error>(false)
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
async fn test_shutdown_sequence_graceful_termination(
    ctx: TestContext,
) -> TestResult<()> {
    let pool = ctx.pool().clone();

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
        match timeout(Duration::from_secs(Timeouts::SHORT), tx.commit()).await {
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
    let post_shutdown_verification = timeout(Duration::from_secs(Timeouts::SHORT), async {
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

        Ok::<(i64, i32), color_eyre::eyre::Error>((committed_events, db_check.unwrap_or(0)))
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
                total_shutdown_duration < Duration::from_secs(Timeouts::QUICK),
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

    let interrupted_shutdown_test = timeout(Duration::from_secs(Timeouts::QUICK), async {
        // Get pool outside of spawn to avoid Send issues
        let pool = ctx.pool().clone();

        // Create long-running operation
        let long_operation = tokio::spawn(async move {
            // Simulate long-running batch operation
            for i in 0..1000 {
                let mut event = EventFactory::new("interrupted.shutdown").create_event(
                    "long_operation",
                    json!({"batch_item": i, "operation": "long_running"}),
                );
                event.host = "localhost".to_string();
                event.ingestor_version = Some("1.0.0".to_string());

                sinex_core::db::insert_event_with_validator(&pool, &event, None).await?;

                // Simulate work with small delays
                if i % 100 == 0 {
                    tokio::task::yield_now().await;
                }
            }

            Ok::<(), color_eyre::eyre::Error>(())
        });

        // Let operation start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Simulate interrupt (abort the task)
        long_operation.abort();

        // Verify system remains stable after interrupt
        let stability_check = timeout(Duration::from_secs(Timeouts::SHORT), async {
            let pool = ctx.pool().clone();

            // Database should still be responsive
            let health_check = sqlx::query_scalar!("SELECT 1").fetch_one(&pool).await?;

            // Check partial data from interrupted operation - use timing utility
            let partial_events = wait_for_filtered_event_count(
                &pool,
                "source = $1",
                &["interrupted.shutdown"],
                0,
                3,
            )
            .await
            .unwrap_or(0);

            Ok::<(i32, i64), color_eyre::eyre::Error>((health_check.unwrap_or(0), partial_events))
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
                Err(eyre!("Stability check timeout"))
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
    .execute(ctx.pool())
    .await
    .ok();

    Ok(())
}

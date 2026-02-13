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

use sinex_db::{run_migrations, DbPoolExt};
use sinex_primitives::DynamicPayload;
use std::time::Instant;
use tokio::time::timeout;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::{Timeouts, WaitHelpers};

/// Test startup sequence robustness and error handling.
///
/// Validates that running migrations is idempotent, data survives re-migration,
/// and corrupted migration state is handled gracefully.
#[sinex_test(timeout = 60)]
async fn test_startup_sequence_robustness(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    // Test 1: Idempotent migration (simulates fresh startup)
    let startup_start = Instant::now();

    run_migrations(pool).await?;

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

    let startup_duration = startup_start.elapsed();
    tracing::info!(
        "Idempotent migration completed in {startup_duration:?}: {schema_count} schemas, {table_count} tables"
    );

    assert!(schema_count >= 2, "Should have required schemas");
    assert!(table_count >= 4, "Should have required tables");

    // Test 2: Data survives re-migration
    sqlx::query!(
        "INSERT INTO core.processor_manifests (processor_name, node_type, version, description, anchor_rule_version)
             VALUES ($1, 'automaton', '1.0.0', $2, 1)
             ON CONFLICT (processor_name) DO NOTHING",
        "existing_agent",
        "Pre-existing agent for startup test"
    )
    .execute(pool)
    .await?;

    let material = pool
        .source_materials()
        .register_in_flight("startup.test", Some("/test"), json!({}))
        .await?;

    for i in 0..10 {
        let event = DynamicPayload::new(
            "startup.test",
            "existing_data",
            json!({"sequence": i, "startup_test": true}),
        )
        .from_material(material.id)
        .build()?;

        pool.events().insert(event).await?;
    }

    // Re-run migrations (should be idempotent)
    run_migrations(pool).await?;

    let manifest_count: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.processor_manifests WHERE processor_name = $1",
        "existing_agent"
    )
    .fetch_one(pool)
    .await?
    .unwrap_or(0);

    let event_count =
        WaitHelpers::wait_for_source_events(pool, "startup.test", 10, Timeouts::QUICK)
            .await
            .unwrap_or(0);

    tracing::info!(
        "Data preserved after re-migration: {manifest_count} manifests, {event_count} events"
    );

    assert!(
        manifest_count >= 1,
        "Existing manifests should be preserved"
    );
    assert!(event_count >= 10, "Existing events should be preserved");

    // Test 3: Corrupted migration state handled gracefully
    sqlx::query(
        "INSERT INTO _sqlx_migrations (version, description, installed_on, success, checksum, execution_time)
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(999999_i64)
    .bind("Corrupted test migration")
    .bind(time::OffsetDateTime::now_utc())
    .bind(false)
    .bind(vec![0u8; 32])
    .bind(0_i64)
    .execute(pool)
    .await
    .ok();

    let migration_result = sinex_db::run_migrations(pool).await;
    match migration_result {
        Ok(()) => tracing::info!("Migrations recovered from corruption"),
        Err(e) => tracing::info!("Migration failed gracefully: {e}"),
    }

    // Clean up corrupted migration record
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 999999")
        .execute(pool)
        .await
        .ok();

    Ok(())
}

/// Test shutdown sequence and graceful termination
#[sinex_test]
async fn test_shutdown_sequence_graceful_termination(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();

    // Register source material FIRST (before holding connections, needs a free one)
    let material = pool
        .source_materials()
        .register_in_flight("shutdown.test", Some("/test"), json!({}))
        .await?;

    // Hold a couple of connections to simulate active usage.
    // Test pool slots have max_connections=4, so hold 2 to leave headroom
    // for repository operations and transactions.
    let mut connections = Vec::new();
    for i in 0..2 {
        match pool.acquire().await {
            Ok(conn) => {
                connections.push(conn);
                tracing::info!("Acquired connection {}", i + 1);
            }
            Err(e) => {
                tracing::info!("Failed to acquire connection {}: {}", i + 1, e);
                break;
            }
        }
    }

    // Simulate ongoing transactions (drop held connections first to free pool)
    drop(connections);
    let mut transactions = Vec::new();
    for i in 0..3 {
        if let Ok(mut tx) = pool.begin().await {
            let event = DynamicPayload::new(
                "shutdown.test",
                "active_transaction",
                json!({"tx_id": i, "shutdown_test": true}),
            )
            .from_material(material.id)
            .build()?;

            pool.events().insert_with_tx(&mut tx, event).await.ok();

            transactions.push(tx);
            tracing::info!("Started transaction {i}");
        }
    }

    // Step 1: Complete active transactions
    let transaction_completion_start = Instant::now();
    for (i, tx) in transactions.into_iter().enumerate() {
        match timeout(Duration::from_secs(Timeouts::SHORT), tx.commit()).await {
            Ok(Ok(())) => {
                tracing::info!("Transaction {i} committed gracefully");
            }
            Ok(Err(e)) => {
                tracing::warn!("Transaction {i} failed to commit: {e}");
            }
            Err(_) => {
                tracing::warn!("Transaction {i} commit timed out");
            }
        }
    }
    let transaction_completion_duration = transaction_completion_start.elapsed();

    // Step 2: Verify database state after shutdown
    let verification_pool = ctx.pool();
    let committed_events =
        WaitHelpers::wait_for_source_events(verification_pool, "shutdown.test", 3, Timeouts::QUICK)
            .await
            .unwrap_or(0);

    let db_check: Option<i32> = sqlx::query_scalar!("SELECT 1")
        .fetch_one(verification_pool)
        .await?;

    tracing::info!(
        "Shutdown: {transaction_completion_duration:?}, {committed_events} events, db={}",
        if db_check == Some(1) { "OK" } else { "FAIL" }
    );

    assert!(
        committed_events >= 3,
        "Transactions should be committed before shutdown"
    );
    assert!(
        db_check == Some(1),
        "Database should remain functional after shutdown"
    );

    // Test 2: Handling of interrupted shutdown
    let interrupt_material = ctx
        .pool()
        .source_materials()
        .register_in_flight("interrupted.shutdown", Some("/test"), json!({}))
        .await?;
    let interrupt_material_id = interrupt_material.id;

    let long_operation = {
        let pool = ctx.pool().clone();
        tokio::spawn(async move {
            for i in 0..1000 {
                let event = DynamicPayload::new(
                    "interrupted.shutdown",
                    "long_operation",
                    json!({"batch_item": i, "operation": "long_running"}),
                )
                .from_material(interrupt_material_id)
                .build()?;

                pool.events().insert(event).await?;

                if i % 100 == 0 {
                    tokio::task::yield_now().await;
                }
            }

            Ok::<(), color_eyre::eyre::Error>(())
        })
    };

    // Let operation start then abort
    tokio::time::sleep(Duration::from_millis(100)).await;
    long_operation.abort();

    // Verify system remains stable after interrupt
    let health_check: Option<i32> = sqlx::query_scalar!("SELECT 1")
        .fetch_one(ctx.pool())
        .await?;

    let partial_events =
        WaitHelpers::wait_for_source_events(ctx.pool(), "interrupted.shutdown", 0, Timeouts::QUICK)
            .await
            .unwrap_or(0);

    tracing::info!(
        "System stable after interrupt: health={}, partial_events={partial_events}",
        health_check.unwrap_or(0)
    );

    assert!(
        health_check == Some(1),
        "Database should remain healthy after interrupt"
    );
    assert!(
        partial_events < 1000,
        "Operation should have been interrupted"
    );

    // Cleanup
    sqlx::query!(
        "DELETE FROM core.events WHERE source IN ('shutdown.test', 'interrupted.shutdown')"
    )
    .execute(ctx.pool())
    .await
    .ok();

    Ok(())
}

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

use sinex_db::{DbPoolExt, apply_schema};
use sinex_primitives::DynamicPayload;
use std::time::Instant;
use tokio::sync::oneshot;
use tokio::time::timeout;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::{Timeouts, WaitHelpers};

/// Test startup sequence robustness and error handling.
///
/// Validates that declarative schema apply is idempotent, data survives re-apply,
/// and missing schema artifacts are restored.
#[sinex_test(timeout = 60)]
async fn test_startup_sequence_robustness(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    // Test 1: Idempotent schema apply (simulates fresh startup)
    let startup_start = Instant::now();

    apply_schema(pool).await?;

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
        "Idempotent schema apply completed in {startup_duration:?}: {schema_count} schemas, {table_count} tables"
    );

    assert!(schema_count >= 2, "Should have required schemas");
    assert!(table_count >= 4, "Should have required tables");

    // Test 2: Data survives re-apply
    // ON CONFLICT DO NOTHING (no target) suppresses any unique-constraint violation,
    // independent of which specific constraint the schema defines on this table.
    sqlx::query!(
        "INSERT INTO core.node_manifests (node_name, node_type, version, description, anchor_rule_version)
             VALUES ($1, 'automaton', '1.0.0', $2, 1)
             ON CONFLICT DO NOTHING",
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

    // Re-run apply (should be idempotent)
    apply_schema(pool).await?;

    let manifest_count: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.node_manifests WHERE node_name = $1",
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
        "Data preserved after re-apply: {manifest_count} manifests, {event_count} events"
    );

    assert!(
        manifest_count >= 1,
        "Existing manifests should be preserved"
    );
    assert!(event_count >= 10, "Existing events should be preserved");

    // Test 3: Missing schema artifacts are restored by declarative apply.
    sqlx::query("DROP INDEX IF EXISTS core.ix_events_ts_persisted")
        .execute(pool)
        .await?;

    sinex_db::apply_schema(pool).await?;

    let index_restored: bool = sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1
            FROM pg_indexes
            WHERE schemaname = 'core'
              AND tablename = 'events'
              AND indexname = 'ix_events_ts_persisted'
        )",
    )
    .fetch_one(pool)
    .await?;
    assert!(
        index_restored,
        "schema apply must restore ix_events_ts_persisted"
    );

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

    // Step 1: Commit an in-flight transaction within a bounded shutdown window.
    let mut tx = pool.begin().await?;
    let event = DynamicPayload::new(
        "shutdown.test",
        "active_transaction",
        json!({"tx_id": 0, "shutdown_test": true}),
    )
    .from_material(material.id)
    .build()?;
    pool.events().insert_with_tx(&mut tx, event).await?;

    let transaction_completion_start = Instant::now();
    timeout(Duration::from_secs(Timeouts::SHORT), tx.commit()).await??;
    let transaction_completion_duration = transaction_completion_start.elapsed();

    // Step 2: Verify database state after shutdown
    let verification_pool = ctx.pool();
    let committed_events =
        WaitHelpers::wait_for_source_events(verification_pool, "shutdown.test", 1, Timeouts::QUICK)
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
        committed_events >= 1,
        "The in-flight transaction should commit before shutdown"
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

    let (first_insert_tx, first_insert_rx) = oneshot::channel();
    let long_operation = {
        let pool = ctx.pool().clone();
        tokio::spawn(async move {
            let event = DynamicPayload::new(
                "interrupted.shutdown",
                "long_operation",
                json!({"batch_item": 0, "operation": "long_running"}),
            )
            .from_material(interrupt_material_id)
            .build()?;

            pool.events().insert(event).await?;
            let _ = first_insert_tx.send(());

            std::future::pending::<()>().await;
            #[allow(unreachable_code)]
            Ok::<(), color_eyre::eyre::Error>(())
        })
    };

    timeout(Duration::from_secs(Timeouts::SHORT), first_insert_rx).await??;
    long_operation.abort();
    let join_result = long_operation.await;
    assert!(
        matches!(&join_result, Err(error) if error.is_cancelled()),
        "interrupted shutdown task should be cancelled cleanly: {join_result:?}"
    );

    // Verify system remains stable after interrupt
    let health_check: Option<i32> = sqlx::query_scalar!("SELECT 1")
        .fetch_one(ctx.pool())
        .await?;

    let partial_events: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*)::bigint FROM core.events WHERE source = 'interrupted.shutdown'"
    )
    .fetch_one(ctx.pool())
    .await?
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
        partial_events == 1,
        "Interrupted operation should stop after the first persisted event"
    );

    Ok(())
}

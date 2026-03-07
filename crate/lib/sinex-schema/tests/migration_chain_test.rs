//! Declarative schema invariants and operation-id safety gate tests.

use xtask::sandbox::prelude::*;

#[sinex_test]
async fn declarative_apply_is_idempotent(ctx: TestContext) -> TestResult<()> {
    sinex_schema::apply::apply(&ctx.pool).await?;
    sinex_schema::apply::apply(&ctx.pool).await?;

    let drift = sinex_schema::apply::diff(&ctx.pool).await?;
    assert!(
        drift.is_empty(),
        "schema drift must be empty after repeated apply(): {drift:?}"
    );

    Ok(())
}

#[sinex_test]
async fn declarative_table_registry_is_non_empty(_ctx: TestContext) -> TestResult<()> {
    let tables = sinex_schema::schema::all_tables();
    assert!(
        !tables.is_empty(),
        "schema table metadata must not be empty"
    );
    assert!(
        tables.iter().any(|t| t.qualified_name == "core.events"),
        "core.events must be in declarative table metadata"
    );
    Ok(())
}

/// Helper: insert a test event directly via SQL, bypassing the NATS pipeline.
async fn insert_test_event(
    pool: &sqlx::PgPool,
    ctx: &TestContext,
    source: &str,
) -> TestResult<sinex_primitives::Id<sinex_primitives::Event<serde_json::Value>>> {
    let event_id = sinex_primitives::Id::<sinex_primitives::Event<serde_json::Value>>::new();
    let material_id = ctx.create_source_material(Some(source)).await?;

    sqlx::query(
        r"
        INSERT INTO core.events (id, source, event_type, payload, ts_orig, host, node_version, source_material_id, anchor_byte)
        VALUES ($1::uuid, $2, $3, $4::jsonb, NOW(), $5, $6, $7::uuid, $8)
        ",
    )
    .bind(event_id.to_uuid())
    .bind(source)
    .bind("test.security")
    .bind(serde_json::json!({"test": "operation_id_guard"}))
    .bind("test-host")
    .bind("test-v1")
    .bind(material_id.to_uuid())
    .bind(0_i64)
    .execute(pool)
    .await?;

    Ok(event_id)
}

#[sinex_test]
async fn delete_without_operation_id_is_rejected(ctx: TestContext) -> TestResult<()> {
    let pool = &ctx.pool;
    let event_id = insert_test_event(pool, &ctx, "migration-test-guard").await?;

    // Attempt DELETE without setting sinex.operation_id — trigger should reject.
    let result = sqlx::query("DELETE FROM core.events WHERE id = $1::uuid")
        .bind(event_id.to_uuid())
        .execute(pool)
        .await;

    assert!(
        result.is_err(),
        "DELETE without sinex.operation_id should be rejected by the archive trigger"
    );

    let err_msg = result.expect_err("expected delete rejection").to_string();
    assert!(
        err_msg.contains("sinex.operation_id"),
        "Error message should mention sinex.operation_id, got: {err_msg}"
    );

    // Verify the event still exists.
    let count: (i64,) =
        sqlx::query_as("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(event_id.to_uuid())
            .fetch_one(pool)
            .await?;
    assert_eq!(count.0, 1, "Event should still exist after rejected delete");

    Ok(())
}

#[sinex_test]
async fn delete_with_operation_id_succeeds(ctx: TestContext) -> TestResult<()> {
    let pool = &ctx.pool;
    let event_id = insert_test_event(pool, &ctx, "migration-test-allowed").await?;

    // Set sinex.operation_id and delete — should succeed.
    let mut tx = pool.begin().await?;

    sqlx::query("SELECT set_config('sinex.operation_id', $1, true)")
        .bind("test-schema-delete")
        .execute(&mut *tx)
        .await?;

    sqlx::query("DELETE FROM core.events WHERE id = $1::uuid")
        .bind(event_id.to_uuid())
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;

    // Verify the event is gone from core.events.
    let count_live: (i64,) =
        sqlx::query_as("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(event_id.to_uuid())
            .fetch_one(pool)
            .await?;
    assert_eq!(count_live.0, 0, "Event should be deleted from live table");

    // Verify it was archived.
    let count_archived: (i64,) =
        sqlx::query_as("SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid")
            .bind(event_id.to_uuid())
            .fetch_one(pool)
            .await?;
    assert_eq!(count_archived.0, 1, "Event should be moved to archive");

    Ok(())
}

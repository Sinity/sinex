//! Declarative schema invariants and operation-id safety gate tests.

use std::time::Duration;

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
async fn declarative_apply_rebuilds_telemetry_read_models(ctx: TestContext) -> TestResult<()> {
    let pool = &ctx.pool;

    sqlx::query("DROP VIEW IF EXISTS sinex_telemetry.recent_activity_summary")
        .execute(pool)
        .await?;
    sqlx::query("DROP MATERIALIZED VIEW IF EXISTS sinex_telemetry.command_frequency_hourly")
        .execute(pool)
        .await?;
    sqlx::query(
        r#"
        CREATE MATERIALIZED VIEW sinex_telemetry.command_frequency_hourly AS
        SELECT
            NOW() AS bucket,
            'broken'::text AS command,
            NULL::text AS shell,
            0::bigint AS total_executions,
            0::bigint AS successful_executions,
            0::bigint AS failed_executions,
            NULL::float8 AS avg_duration_ms
        WITH NO DATA
        "#,
    )
    .execute(pool)
    .await?;

    sinex_schema::apply::apply(pool).await?;

    let relation_state = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT
            c.relkind::text,
            pg_get_viewdef(c.oid, true)
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'sinex_telemetry'
          AND c.relname = 'command_frequency_hourly'
        "#,
    )
    .fetch_one(pool)
    .await?;
    assert_eq!(
        relation_state.0, "v",
        "command_frequency_hourly must be restored as the Timescale continuous aggregate view surface, got relkind={} definition={}",
        relation_state.0, relation_state.1
    );

    let definition = sqlx::query_scalar::<_, String>(
        r#"
        SELECT view_definition
        FROM timescaledb_information.continuous_aggregates
        WHERE view_schema = 'sinex_telemetry'
          AND view_name = 'command_frequency_hourly'
        "#,
    )
    .fetch_one(pool)
    .await?;
    assert!(
        definition.contains("shell.command") && definition.contains("shell.command.canonical"),
        "schema apply must restore the live command_frequency_hourly definition, got: {definition}"
    );

    let summary_exists = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM pg_views
            WHERE schemaname = 'sinex_telemetry'
              AND viewname = 'recent_activity_summary'
        )
        "#,
    )
    .fetch_one(pool)
    .await?;
    assert!(
        summary_exists,
        "schema apply must recreate recent_activity_summary after rebuilding telemetry dependencies"
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
        INSERT INTO core.events (id, source, event_type, payload, ts_orig, host, node_run_id, source_material_id, anchor_byte)
        VALUES ($1::uuid, $2, $3, $4::jsonb, NOW(), $5, $6, $7::uuid, $8)
        ",
    )
    .bind(event_id.to_uuid())
    .bind(source)
    .bind("test.security")
    .bind(serde_json::json!({"test": "operation_id_guard"}))
    .bind("test-host")
    .bind(Option::<Uuid>::None)
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

#[sinex_test]
async fn current_health_tracks_latest_status_per_component(ctx: TestContext) -> TestResult<()> {
    let pool = &ctx.pool;

    async fn insert_health_status(
        ctx: &TestContext,
        component: &str,
        status: &str,
        reason: &str,
    ) -> TestResult<()> {
        let material_id = ctx.create_source_material(Some("sinex")).await?;
        sqlx::query!(
            r#"
            INSERT INTO core.events (
                id,
                source,
                event_type,
                host,
                payload,
                ts_orig,
                source_material_id,
                anchor_byte
            )
            VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)
            "#,
            uuid::Uuid::now_v7(),
            "sinex",
            "health.status",
            "test-host",
            serde_json::json!({
                "component": component,
                "current_status": status,
                "reason": reason,
            }),
            *sinex_primitives::temporal::now(),
            material_id.to_uuid(),
            0_i64,
        )
        .execute(ctx.pool())
        .await?;
        Ok(())
    }

    insert_health_status(&ctx, "ingestd", "healthy", "fresh heartbeat").await?;
    tokio::time::sleep(Duration::from_millis(5)).await;
    insert_health_status(&ctx, "gateway", "degraded", "warming caches").await?;
    tokio::time::sleep(Duration::from_millis(5)).await;
    insert_health_status(&ctx, "ingestd", "failed", "lost database").await?;

    let rows = sqlx::query!(
        r#"
        SELECT component, status, reason
        FROM sinex_telemetry.current_health
        ORDER BY component ASC
        "#,
    )
    .fetch_all(pool)
    .await?;

    assert_eq!(rows.len(), 2, "current_health must retain one row per component");
    assert_eq!(rows[0].component.as_deref(), Some("gateway"));
    assert_eq!(rows[0].status.as_deref(), Some("degraded"));
    assert_eq!(rows[0].reason.as_deref(), Some("warming caches"));
    assert_eq!(rows[1].component.as_deref(), Some("ingestd"));
    assert_eq!(rows[1].status.as_deref(), Some("failed"));
    assert_eq!(rows[1].reason.as_deref(), Some("lost database"));

    Ok(())
}

//! Declarative schema invariants and operation-id safety gate tests.

use std::collections::BTreeSet;
use std::time::Duration;

use xtask::sandbox::prelude::*;

async fn drop_telemetry_relation(
    pool: &sqlx::PgPool,
    relation_name: &str,
) -> Result<(), sqlx::Error> {
    let relation_kind = sqlx::query_scalar::<_, Option<String>>(
        r"
        SELECT c.relkind::text
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'sinex_telemetry'
          AND c.relname = $1
        ",
    )
    .bind(relation_name)
    .fetch_optional(pool)
    .await?
    .flatten();

    match relation_kind.as_deref() {
        Some("m") => {
            sqlx::query(&format!(
                "DROP MATERIALIZED VIEW sinex_telemetry.{relation_name}"
            ))
            .execute(pool)
            .await?;
        }
        Some("v") => {
            sqlx::query(&format!("DROP VIEW sinex_telemetry.{relation_name}"))
                .execute(pool)
                .await?;
        }
        Some(_) | None => {}
    }

    Ok(())
}

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
async fn shared_access_role_bootstrap_is_idempotent(ctx: TestContext) -> TestResult<()> {
    sinex_schema::apply::ensure_shared_access_roles(&ctx.pool).await?;

    for role in sinex_schema::apply::SHARED_ACCESS_ROLES {
        let can_login = sqlx::query_scalar::<_, bool>(
            r"
            SELECT rolcanlogin
            FROM pg_roles
            WHERE rolname = $1
            ",
        )
        .bind(*role)
        .fetch_one(&ctx.pool)
        .await?;

        assert!(!can_login, "bootstrap role {role} must remain NOLOGIN");
    }

    Ok(())
}

#[sinex_test]
async fn declarative_diff_accepts_normalized_source_material_status_constraint(
    ctx: TestContext,
) -> TestResult<()> {
    sqlx::query(
        r"
        ALTER TABLE raw.source_material_registry
            DROP CONSTRAINT IF EXISTS source_material_registry_status_check,
            ADD CONSTRAINT source_material_registry_status_check
            CHECK (status IN ('sensing', 'completed', 'cancelled', 'recovered_partial', 'failed'))
        ",
    )
    .execute(&ctx.pool)
    .await?;

    let drift = sinex_schema::apply::diff(&ctx.pool).await?;
    assert!(
        !drift
            .iter()
            .any(|entry| entry.contains("source_material_registry_status_check")),
        "normalized Postgres CHECK definition must not be reported as drift: {drift:?}"
    );

    Ok(())
}

#[sinex_test]
async fn declarative_apply_rebuilds_telemetry_read_models(ctx: TestContext) -> TestResult<()> {
    let pool = &ctx.pool;

    drop_telemetry_relation(pool, "recent_activity_summary").await?;
    drop_telemetry_relation(pool, "command_frequency_hourly").await?;
    sqlx::query(
        r"
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
        ",
    )
    .execute(pool)
    .await?;

    sinex_schema::apply::apply(pool).await?;

    let relation_state = sqlx::query_as::<_, (String, String)>(
        r"
        SELECT
            c.relkind::text,
            pg_get_viewdef(c.oid, true)
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'sinex_telemetry'
          AND c.relname = 'command_frequency_hourly'
        ",
    )
    .fetch_one(pool)
    .await?;
    assert_eq!(
        relation_state.0, "v",
        "command_frequency_hourly must be restored as the live event-time view surface, got relkind={} definition={}",
        relation_state.0, relation_state.1
    );

    let definition = sqlx::query_scalar::<_, String>(
        r"
        SELECT pg_get_viewdef(c.oid, true)
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'sinex_telemetry'
          AND c.relname = 'command_frequency_hourly'
        ",
    )
    .fetch_one(pool)
    .await?;
    assert!(
        definition.contains("command.executed")
            && definition.contains("shell.kitty")
            && definition.contains("shell.history.%")
            && definition.contains("time_bucket(")
            && definition.contains("ts_orig"),
        "schema apply must restore the live command_frequency_hourly definition, got: {definition}"
    );

    let ca_exists = sqlx::query_scalar::<_, bool>(
        r"
        SELECT EXISTS (
            SELECT 1
            FROM timescaledb_information.continuous_aggregates
            WHERE view_schema = 'sinex_telemetry'
              AND view_name = 'command_frequency_hourly'
        )
        ",
    )
    .fetch_one(pool)
    .await?;
    assert!(
        !ca_exists,
        "command_frequency_hourly must no longer be registered as a Timescale continuous aggregate"
    );

    let summary_exists = sqlx::query_scalar::<_, bool>(
        r"
        SELECT EXISTS (
            SELECT 1
            FROM pg_views
            WHERE schemaname = 'sinex_telemetry'
              AND viewname = 'recent_activity_summary'
        )
        ",
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
async fn declarative_diff_detects_polluted_telemetry_view_kind(ctx: TestContext) -> TestResult<()> {
    drop_telemetry_relation(&ctx.pool, "recent_activity_summary").await?;
    drop_telemetry_relation(&ctx.pool, "command_frequency_hourly").await?;
    sqlx::query(
        r"
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
        ",
    )
    .execute(&ctx.pool)
    .await?;

    let drift = sinex_schema::apply::diff(&ctx.pool).await?;
    assert!(
        drift.iter().any(|entry| {
            entry.contains("command_frequency_hourly") && entry.contains("expected a view")
        }),
        "diff must report command_frequency_hourly kind drift, got: {drift:?}"
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

    assert_eq!(
        rows.len(),
        2,
        "current_health must retain one row per component"
    );
    assert_eq!(rows[0].component.as_deref(), Some("gateway"));
    assert_eq!(rows[0].status.as_deref(), Some("degraded"));
    assert_eq!(rows[0].reason.as_deref(), Some("warming caches"));
    assert_eq!(rows[1].component.as_deref(), Some("ingestd"));
    assert_eq!(rows[1].status.as_deref(), Some("failed"));
    assert_eq!(rows[1].reason.as_deref(), Some("lost database"));

    Ok(())
}

async fn relation_columns(
    pool: &sqlx::PgPool,
    schema: &str,
    relation: &str,
) -> TestResult<Vec<String>> {
    let columns = sqlx::query_scalar::<_, String>(
        r"
        SELECT a.attname
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1
          AND c.relname = $2
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum
        ",
    )
    .bind(schema)
    .bind(relation)
    .fetch_all(pool)
    .await?;
    Ok(columns)
}

#[sinex_test]
async fn telemetry_relations_expose_expected_contract_columns(ctx: TestContext) -> TestResult<()> {
    let pool = &ctx.pool;

    let expected_contracts = [
        (
            "current_health",
            &[
                "source",
                "event_type",
                "component",
                "status",
                "reason",
                "last_update",
            ][..],
        ),
        (
            "current_device_state",
            &[
                "unit_name",
                "unit_type",
                "state",
                "sub_state",
                "last_update",
            ][..],
        ),
        (
            "gateway_stats_1h",
            &[
                "bucket",
                "source",
                "stat_events",
                "avg_total_requests",
                "total_rate_limited",
                "avg_latency_ms",
                "max_p99_latency_ms",
            ][..],
        ),
        (
            "stream_stats_1h",
            &[
                "bucket",
                "stream_name",
                "avg_fill_pct",
                "max_fill_pct",
                "avg_messages",
                "max_messages",
                "sample_count",
            ][..],
        ),
        (
            "assembly_stats_1h",
            &[
                "bucket",
                "max_active_assemblies",
                "total_completed",
                "total_cancelled",
                "total_failed",
                "total_timed_out",
                "avg_duration_ms",
                "sample_count",
            ][..],
        ),
        (
            "node_stats_1h",
            &[
                "bucket",
                "node_type",
                "total_events_processed",
                "total_events_dropped",
                "avg_latency_ms",
                "max_queue_depth",
                "total_errors",
                "sample_count",
            ][..],
        ),
        (
            "metric_counters_1h",
            &[
                "bucket",
                "component",
                "metric_name",
                "total_value",
                "max_value",
                "sample_count",
            ][..],
        ),
        (
            "current_window_focus",
            &[
                "bucket",
                "workspace",
                "window_class",
                "window_title",
                "window_id",
                "last_focus_time",
                "focus_event_count",
            ][..],
        ),
        (
            "command_frequency_hourly",
            &[
                "bucket",
                "command",
                "shell",
                "total_executions",
                "successful_executions",
                "failed_executions",
                "avg_duration_ms",
            ][..],
        ),
        (
            "file_activity_summary",
            &[
                "bucket",
                "directory",
                "event_type",
                "total_events",
                "unique_files",
            ][..],
        ),
        (
            "current_system_state",
            &[
                "bucket",
                "avg_cpu_percent",
                "max_cpu_percent",
                "avg_memory_percent",
                "max_memory_percent",
                "avg_disk_percent",
                "current_active_units",
                "sample_count",
            ][..],
        ),
        (
            "ingestd_batch_stats_1h",
            &[
                "bucket",
                "avg_batch_size",
                "max_batch_size",
                "avg_latency_ms",
                "max_latency_ms",
                "total_deferred",
                "total_failed",
                "synthesis_batches",
                "batch_count",
                "validation_valid",
                "validation_skipped",
                "validation_no_schema",
                "validation_schema_not_found",
                "validation_invalid",
                "avg_validation_coverage_pct",
            ][..],
        ),
        (
            "recent_activity_summary",
            &["activity_type", "context", "detail", "timestamp"][..],
        ),
    ];

    for (relation, expected_columns) in expected_contracts {
        let actual_columns = relation_columns(pool, "sinex_telemetry", relation).await?;
        let expected_columns = expected_columns
            .iter()
            .map(|column| (*column).to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            actual_columns, expected_columns,
            "sinex_telemetry.{relation} column contract drifted"
        );
    }

    Ok(())
}

#[sinex_test]
async fn operator_telemetry_does_not_register_continuous_aggregates(
    ctx: TestContext,
) -> TestResult<()> {
    sinex_schema::apply::apply(&ctx.pool).await?;

    let actual = sqlx::query_scalar::<_, String>(
        r"
        SELECT view_name
        FROM timescaledb_information.continuous_aggregates
        WHERE view_schema = 'sinex_telemetry'
        ORDER BY view_name
        ",
    )
    .fetch_all(&ctx.pool)
    .await?
    .into_iter()
    .collect::<BTreeSet<_>>();

    let expected = BTreeSet::new();

    assert_eq!(
        actual, expected,
        "operator telemetry should no longer rely on Timescale continuous aggregates"
    );

    Ok(())
}

#[sinex_test]
async fn operator_telemetry_views_include_live_rows(
    ctx: TestContext,
) -> TestResult<()> {
    sinex_schema::apply::apply(&ctx.pool).await?;

    let material_id = ctx.create_source_material(Some("sinex.ingestd")).await?;
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
        "sinex.ingestd",
        "batch.stats",
        "test-host",
        serde_json::json!({
            "batch_size": 8,
            "fetch_to_ack_ms": 42,
            "events_deferred": 1,
            "events_failed": 0,
            "had_synthesis": true,
            "insert_path": "copy",
            "validation_valid": 20,
            "validation_skipped": 0,
            "validation_no_schema": 2,
            "validation_schema_not_found": 1,
            "validation_invalid": 3,
            "validation_coverage_pct": 87.5,
            "suspicious_future_ts_orig": 4
        }),
        *sinex_primitives::temporal::now(),
        material_id.to_uuid(),
        0_i64,
    )
    .execute(&ctx.pool)
    .await?;

    let rows = sqlx::query_as::<_, (Option<f64>, Option<i64>, Option<i64>)>(
        r#"
        SELECT
            AVG(avg_batch_size::float8) AS avg_batch_size,
            MAX(batch_count) AS batch_count,
            MAX(total_deferred) AS total_deferred
        FROM sinex_telemetry.ingestd_batch_stats_1h
        WHERE batch_count > 0
        "#,
    )
    .fetch_all(&ctx.pool)
    .await?;

    assert_eq!(rows.len(), 1, "real-time aggregate should expose the fresh batch");
    assert_eq!(rows[0].0, Some(8.0));
    assert_eq!(rows[0].1, Some(1));
    assert_eq!(rows[0].2, Some(1));

    Ok(())
}

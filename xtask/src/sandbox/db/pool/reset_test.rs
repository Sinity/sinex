use super::{
    force_event_material_cleanup, force_event_material_cleanup_with_tables, log_remaining_rows,
    seed_test_fixtures,
};
use crate::sandbox::sinex_test;
use sinex_primitives::Uuid;

#[sinex_test]
async fn log_remaining_rows_reports_extra_source_materials(
    ctx: crate::sandbox::Sandbox,
) -> ::xtask::sandbox::TestResult<()> {
    let source_identifier = format!("force-cleanup-test-{}", Uuid::now_v7());
    sqlx::query(
        "INSERT INTO raw.source_material_registry \
            (id, material_kind, source_identifier, status, timing_info_type) \
         VALUES ($1, 'annex', $2, 'completed', 'realtime')",
    )
    .bind(Uuid::now_v7())
    .bind(&source_identifier)
    .execute(ctx.pool())
    .await?;

    let residuals = log_remaining_rows(ctx.pool()).await?;

    assert!(
        residuals
            .iter()
            .any(|(table, count)| table == "raw.source_material_registry" && *count >= 2)
    );
    Ok(())
}

#[sinex_test]
async fn force_event_material_cleanup_clears_extra_source_materials(
    ctx: crate::sandbox::Sandbox,
) -> ::xtask::sandbox::TestResult<()> {
    let source_identifier = format!("force-cleanup-test-{}", Uuid::now_v7());
    sqlx::query(
        "INSERT INTO raw.source_material_registry \
            (id, material_kind, source_identifier, status, timing_info_type) \
         VALUES ($1, 'annex', $2, 'completed', 'realtime')",
    )
    .bind(Uuid::now_v7())
    .bind(&source_identifier)
    .execute(ctx.pool())
    .await?;

    force_event_material_cleanup(ctx.pool()).await?;

    let counts = crate::sandbox::db::common::get_row_counts(ctx.pool()).await?;
    assert_eq!(counts.get("core.events").copied().unwrap_or_default(), 0);
    assert!(
        counts
            .get("raw.source_material_registry")
            .copied()
            .unwrap_or_default()
            <= 1
    );

    seed_test_fixtures(ctx.pool()).await?;
    Ok(())
}

#[sinex_test]
async fn force_event_material_cleanup_surfaces_delete_failures(
    ctx: crate::sandbox::Sandbox,
) -> ::xtask::sandbox::TestResult<()> {
    let err = force_event_material_cleanup_with_tables(
        ctx.pool(),
        vec!["missing_schema.missing_table".to_string()],
    )
    .await
    .expect_err("invalid cleanup table should fail honestly");

    assert!(
        err.to_string()
            .contains("forced cleanup delete failed for missing_schema.missing_table"),
        "unexpected error: {err:#}"
    );
    Ok(())
}

#[sinex_test]
async fn ensure_default_session_state_conn_resets_multiple_dirty_settings(
    ctx: crate::sandbox::Sandbox,
) -> ::xtask::sandbox::TestResult<()> {
    let mut conn = ctx.pool().acquire().await?;

    // Dirty all three settings at once so the reset path must issue more
    // than one SET statement in a single pass.
    sqlx::query("SET session_replication_role = 'replica'")
        .execute(conn.as_mut())
        .await?;
    sqlx::query("SET row_security = off")
        .execute(conn.as_mut())
        .await?;
    sqlx::query("SET synchronous_commit = on")
        .execute(conn.as_mut())
        .await?;

    // Regression test for sinex-96fg: the reset path used to join every
    // dirty SET into one "; "-separated string and run it through
    // sqlx::query(), which uses the extended (prepared-statement) protocol
    // and rejects multi-statement strings ("cannot insert multiple commands
    // into a prepared statement"). This runs on every pool `before_acquire`,
    // so dirtying 2+ settings at once reliably triggered it.
    super::ensure_default_session_state_conn(conn.as_mut()).await?;

    let (role, row_sec, sync_commit): (String, String, String) = sqlx::query_as(
        "SELECT current_setting('session_replication_role'),
                current_setting('row_security'),
                current_setting('synchronous_commit')",
    )
    .fetch_one(conn.as_mut())
    .await?;

    assert_eq!(role, "origin");
    assert_eq!(row_sec.to_lowercase(), "on");
    assert_eq!(sync_commit, "off");
    Ok(())
}

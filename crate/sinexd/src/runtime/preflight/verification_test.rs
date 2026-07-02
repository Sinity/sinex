use super::{EVENTS_ACCESS_PROBE_SQL, test_schema_access};
use crate::runtime::preflight::{
    PREFLIGHT_MAX_PARALLEL_WORKERS_PER_GATHER, configure_preflight_database_session,
};
use serde_json::Value;

use xtask::sandbox::sinex_test;

#[sinex_test]
async fn event_access_probe_is_metadata_only() -> TestResult<()> {
    assert!(EVENTS_ACCESS_PROBE_SQL.contains("LIMIT 0"));
    assert!(!EVENTS_ACCESS_PROBE_SQL.contains("COUNT(*)"));
    Ok(())
}

#[sinex_test]
async fn test_schema_access_uses_source_material_registry_contract(
    ctx: TestContext,
) -> TestResult<()> {
    let mut messages = Vec::new();
    let details = test_schema_access(ctx.pool(), &mut messages).await?;

    assert_eq!(
        details.get("source_material_registry_exists"),
        Some(&Value::Bool(true))
    );
    assert_eq!(
        details.get("source_material_registry_select_works"),
        Some(&Value::Bool(true))
    );
    assert_eq!(
        details.get("unbounded_event_count_skipped"),
        Some(&Value::Bool(true))
    );
    assert_eq!(details.get("current_event_count"), None);
    assert_eq!(details.get("source_materials_exists"), None);
    assert_eq!(details.get("blobs_exists"), Some(&Value::Bool(true)));
    assert_eq!(details.get("blobs_select_works"), Some(&Value::Bool(true)));
    assert_eq!(details.get("all_checks_passed"), Some(&Value::Bool(true)));

    Ok(())
}

#[sinex_test]
async fn preflight_database_session_disables_parallel_workers(
    ctx: TestContext,
) -> TestResult<()> {
    let mut conn = ctx.pool().acquire().await?;
    configure_preflight_database_session(&mut conn).await?;

    let parallel_workers =
        sqlx::query_scalar::<_, String>("SHOW max_parallel_workers_per_gather")
            .fetch_one(&mut *conn)
            .await?;
    let statement_timeout = sqlx::query_scalar::<_, String>("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await?;
    let lock_timeout = sqlx::query_scalar::<_, String>("SHOW lock_timeout")
        .fetch_one(&mut *conn)
        .await?;

    assert_eq!(parallel_workers, PREFLIGHT_MAX_PARALLEL_WORKERS_PER_GATHER);
    assert_eq!(statement_timeout, "5s");
    assert_eq!(lock_timeout, "1s");

    Ok(())
}

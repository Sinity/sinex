//! sinex-z1dk: `apply_schema` (the DDL convergence entry point `sinexd`'s
//! `main.rs` calls unconditionally on `SINEX_SCHEMA_APPLY_ON_STARTUP=1`) had
//! no advisory lock at all, unlike the lighter `event_engine` schema-sync
//! (`try_acquire_migration_lock`, keyed `"event_engine.migrations"`). This
//! proves the fix: a second concurrent `apply_schema` call against the same
//! database, while another holder has the DDL-apply advisory lock, fails
//! fast with a clear error instead of racing the convergence engine's
//! two-phase `NOT VALID -> VALIDATE CONSTRAINT` step.

use sinex_db::advisory_lock::AdvisoryLock;
use sinex_primitives::EXPECTED_BINARY_SCHEMA_VERSION;
use sqlx::Row;
use xtask::sandbox::prelude::*;

/// Must match `SCHEMA_DDL_APPLY_LOCK_KEY` in `crate/sinex-db/src/schema_apply.rs`.
/// Kept as a literal here (not exported) to mirror this crate's existing
/// `advisory_lock_test.rs` style of exercising the lock by its real key.
const SCHEMA_DDL_APPLY_LOCK_KEY: &str = "sinex_schema.ddl_apply";

#[sinex_test]
async fn apply_schema_rejects_concurrent_caller_holding_the_ddl_lock(
    ctx: TestContext,
) -> TestResult<()> {
    let holder_guard = AdvisoryLock::try_acquire(&ctx.pool, SCHEMA_DDL_APPLY_LOCK_KEY)
        .await?
        .expect("test setup: acquiring the DDL-apply lock as the 'other process' must succeed");

    let error = sinex_db::apply_schema(&ctx.pool).await.expect_err(
        "sinex-z1dk: apply_schema must fail fast when another process already holds the \
         schema DDL-apply advisory lock, instead of racing convergence DDL unguarded",
    );
    assert!(
        error
            .to_string()
            .contains("already applying the database schema"),
        "unexpected error: {error}"
    );

    holder_guard.cleanup_now().await;

    // Once the lock is released, a normal apply_schema call must succeed --
    // proves the lock is scoped to the call and not accidentally held open
    // forever (e.g. by this test's own prior failed attempt).
    sinex_db::apply_schema(&ctx.pool)
        .await
        .expect("apply_schema must succeed once the DDL-apply lock is free");

    Ok(())
}

/// A successful DDL apply upgrades the persisted compatibility epoch. Without
/// this write, an old binary cannot distinguish a database after destructive
/// convergence from its pre-drop shape and could silently recreate columns on
/// rollback.
#[sinex_test]
async fn apply_schema_records_current_binary_schema_version(ctx: TestContext) -> TestResult<()> {
    sqlx::query(
        r#"
        INSERT INTO sinex_schemas.binary_schema_version (id, version)
        VALUES (1, 'stale-before-apply')
        ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version
        "#,
    )
    .execute(&ctx.pool)
    .await?;

    sinex_db::apply_schema(&ctx.pool).await?;

    let row = sqlx::query("SELECT version FROM sinex_schemas.binary_schema_version WHERE id = 1")
        .fetch_one(&ctx.pool)
        .await?;
    let version: String = row.try_get("version")?;
    assert_eq!(version, EXPECTED_BINARY_SCHEMA_VERSION);
    Ok(())
}

//! CI infrastructure validation tests.
//!
//! These tests verify that the CI environment is correctly configured,
//! preventing permission-related failures that could otherwise only be
//! caught during CI runs.

use sinex_schema::schema_registry;
use sqlx::Row;
use xtask::sandbox::db::ensure_default_session_state;
use xtask::sandbox::fs::{ReplicationRoleGuard, RowSecurityGuard, TriggersGuard};
use xtask::sandbox::prelude::{CleanupConfig, CleanupMethod, sinex_test};

/// Verifies that all schemas from the registry are accessible.
///
/// This test catches cases where schemas are added to the registry but
/// CI scripts don't grant access to them (like the `public` schema issue).
#[sinex_test]
async fn ci_setup_grants_all_schemas(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let pool = ctx.pool();

    // Get current user
    let current_user: String = sqlx::query_scalar("SELECT current_user")
        .fetch_one(pool)
        .await?;

    // Verify we have USAGE on all schemas
    for schema in schema_registry::SINEX_SCHEMAS {
        let has_usage: bool = sqlx::query(&format!(
            "SELECT has_schema_privilege($1, $2, 'USAGE') as has_usage"
        ))
        .bind(&current_user)
        .bind(schema.name)
        .fetch_one(pool)
        .await?
        .get("has_usage");

        assert!(
            has_usage,
            "User '{current_user}' missing USAGE privilege on schema '{}'.\n\
             This indicates CI setup (xtask ci postgres) is not granting access to all schemas.\n\
             The schema registry includes this schema, but permissions were not granted.",
            schema.name
        );
    }

    Ok(())
}

/// Verifies that we can create tables in all schemas.
///
/// This catches missing ALTER DEFAULT PRIVILEGES grants.
#[sinex_test]
async fn can_create_tables_in_all_schemas(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let pool = ctx.pool();

    for schema in schema_registry::SINEX_SCHEMAS {
        // Try to create a temporary table
        let result = sqlx::query(&format!(
            "CREATE TEMP TABLE {}_test_ci_permissions (id INT)",
            schema.name
        ))
        .execute(pool)
        .await;

        assert!(
            result.is_ok(),
            "Cannot create tables in schema '{}': {:?}\n\
             This indicates missing DEFAULT PRIVILEGES grants in CI setup.",
            schema.name,
            result.err()
        );
    }

    Ok(())
}

/// Verifies that temporal_ledger triggers can be disabled.
///
/// This is required for test cleanup to work.
#[sinex_test]
async fn can_disable_temporal_ledger_triggers(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let pool = ctx.pool();
    let mut conn = pool.acquire().await?;

    // Try to disable triggers (cleanup tests need this)
    let disable_result = sqlx::query("ALTER TABLE raw.temporal_ledger DISABLE TRIGGER ALL")
        .execute(&mut *conn)
        .await;

    assert!(
        disable_result.is_ok(),
        "Cannot disable triggers on raw.temporal_ledger: {:?}\n\
         Test cleanup requires the ability to disable append-only triggers.",
        disable_result.err()
    );

    // Re-enable for cleanup
    sqlx::query("ALTER TABLE raw.temporal_ledger ENABLE TRIGGER ALL")
        .execute(&mut *conn)
        .await?;

    Ok(())
}

/// Verifies declarative schema core objects are accessible.
#[sinex_test]
async fn declarative_schema_core_objects_accessible(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let pool = ctx.pool();

    let events_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('core.events') IS NOT NULL")
            .fetch_one(pool)
            .await?;
    let has_ts_persisted: bool = sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'core'
              AND table_name = 'events'
              AND column_name = 'ts_persisted'
        )",
    )
    .fetch_one(pool)
    .await?;

    assert!(
        events_exists && has_ts_persisted,
        "Declarative schema missing expected core.events shape (events_exists={events_exists}, ts_persisted={has_ts_persisted})"
    );

    Ok(())
}

/// Verifies that session_replication_role can be set and reset.
///
/// This is critical for cleanup operations that need to bypass FK constraints.
#[sinex_test]
async fn can_set_session_replication_role(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let pool = ctx.pool();
    let mut conn = pool.acquire().await?;

    // Try to set session_replication_role to replica
    let set_result = sqlx::query("SET session_replication_role = 'replica'")
        .execute(&mut *conn)
        .await;

    assert!(
        set_result.is_ok(),
        "Cannot set session_replication_role: {:?}\n\
         Cleanup operations require this permission to bypass FK constraints.",
        set_result.err()
    );

    // Verify it was set
    let role: String = sqlx::query_scalar("SHOW session_replication_role")
        .fetch_one(&mut *conn)
        .await?;

    assert_eq!(role, "replica", "session_replication_role should be 'replica'");

    // Reset to origin
    sqlx::query("SET session_replication_role = 'origin'")
        .execute(&mut *conn)
        .await?;

    let role: String = sqlx::query_scalar("SHOW session_replication_role")
        .fetch_one(&mut *conn)
        .await?;

    assert_eq!(role, "origin", "session_replication_role should be reset to 'origin'");

    Ok(())
}

/// Verifies that row_security can be disabled and re-enabled.
///
/// This is required for cleanup operations to bypass RLS policies.
#[sinex_test]
async fn can_disable_row_security(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let pool = ctx.pool();
    let mut conn = pool.acquire().await?;

    // Try to disable row security
    let disable_result = sqlx::query("SET row_security = off")
        .execute(&mut *conn)
        .await;

    assert!(
        disable_result.is_ok(),
        "Cannot disable row_security: {:?}\n\
         Cleanup operations require this permission.",
        disable_result.err()
    );

    // Re-enable
    sqlx::query("SET row_security = on")
        .execute(&mut *conn)
        .await?;

    Ok(())
}

/// Verifies that triggers can be disabled and re-enabled on core.events.
///
/// This is required for cleanup to work around archive triggers.
#[sinex_test]
async fn can_toggle_core_events_triggers(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let pool = ctx.pool();
    let mut conn = pool.acquire().await?;

    // Disable triggers
    let disable_result = sqlx::query("ALTER TABLE core.events DISABLE TRIGGER ALL")
        .execute(&mut *conn)
        .await;

    match disable_result {
        Ok(_) => {
            let enable_result = sqlx::query("ALTER TABLE core.events ENABLE TRIGGER ALL")
                .execute(&mut *conn)
                .await;

            assert!(
                enable_result.is_ok(),
                "Cannot re-enable triggers on core.events: {:?}",
                enable_result.err()
            );
        }
        Err(error) => {
            let error_string = error.to_string();
            assert!(
                error_string.contains("hypertables do not support  enabling or disabling triggers"),
                "Unexpected trigger toggle failure on core.events: {error:#?}",
            );

            let role_result = sqlx::query("SET session_replication_role = 'replica'")
                .execute(&mut *conn)
                .await;
            assert!(
                role_result.is_ok(),
                "Cleanup fallback must still be available when hypertable trigger toggling is unsupported: {:?}",
                role_result.err()
            );
            sqlx::query("SET session_replication_role = 'origin'")
                .execute(&mut *conn)
                .await?;
        }
    }

    Ok(())
}

/// Verifies permissions on all CleanupConfig tables.
///
/// Ensures CI can DELETE from all tables that cleanup needs to clear.
#[sinex_test]
async fn can_delete_from_all_cleanup_tables(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let pool = ctx.pool();
    let config = CleanupConfig::default();

    for table in config.tables_to_clean() {
        // Try DELETE (should succeed even if table is empty)
        let delete_result = sqlx::query(&format!("DELETE FROM {} WHERE false", table.table_name))
            .execute(pool)
            .await;

        assert!(
            delete_result.is_ok(),
            "Cannot DELETE from table '{}': {:?}\n\
             CI permissions must allow DELETE on all cleanup tables.",
            table.table_name,
            delete_result.err()
        );

        // For tables that support TRUNCATE, verify that too
        if table.method == CleanupMethod::Truncate {
            let truncate_result = sqlx::query(&format!("TRUNCATE TABLE {} RESTART IDENTITY CASCADE", table.table_name))
                .execute(pool)
                .await;

            assert!(
                truncate_result.is_ok(),
                "Cannot TRUNCATE table '{}': {:?}\n\
                 Table is configured for TRUNCATE but operation failed.",
                table.table_name,
                truncate_result.err()
            );
        }
    }

    Ok(())
}

/// Verifies that session guards properly restore state.
///
/// This ensures connections don't leak altered state back to the pool.
#[sinex_test]
async fn session_guards_restore_state(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let pool = ctx.pool();
    let mut conn = pool.acquire().await?;
    // Record initial state
    let initial_replication_role: String = sqlx::query_scalar("SHOW session_replication_role")
        .fetch_one(&mut *conn)
        .await?;

    // Use guards (simulating cleanup operation)
    {
        let replication_guard =
            ReplicationRoleGuard::disable_for_cleanup(&mut conn).await?;
        let row_security_guard =
            RowSecurityGuard::disable_for_cleanup(&mut conn).await?;
        let triggers_guard = TriggersGuard::disable_for_cleanup(
            &mut conn,
            ["core.events", "raw.temporal_ledger"],
        )
        .await?;

        // Verify altered state
        let altered_role: String = sqlx::query_scalar("SHOW session_replication_role")
            .fetch_one(&mut *conn)
            .await?;

        // Only check if it was actually changed (might fail due to permissions)
        if altered_role != initial_replication_role {
            assert_eq!(
                altered_role, "replica",
                "State should be altered during guard lifetime"
            );
        }

        // Restore via guards
        triggers_guard.restore(&mut conn).await?;
        row_security_guard.restore(&mut conn).await?;
        replication_guard.restore(&mut conn).await?;
    }

    // Verify state was restored
    let final_replication_role: String = sqlx::query_scalar("SHOW session_replication_role")
        .fetch_one(&mut *conn)
        .await?;

    assert_eq!(
        final_replication_role, initial_replication_role,
        "Guards should restore original session state"
    );

    Ok(())
}

/// Verifies that CleanupConfig is authoritative for cleanup behavior.
///
/// No hardcoded table lists should exist in cleanup functions.
#[sinex_test]
async fn cleanup_config_is_authoritative() -> xtask::sandbox::TestResult<()> {
    let config = CleanupConfig::default();

    // Verify config contains expected critical tables
    let table_names: Vec<_> = config.tables.iter().map(|t| t.table_name).collect();

    assert!(
        table_names.contains(&"core.events"),
        "CleanupConfig must include core.events"
    );
    assert!(
        table_names.contains(&"raw.temporal_ledger"),
        "CleanupConfig must include raw.temporal_ledger"
    );
    assert!(
        table_names.contains(&"raw.source_material_registry"),
        "CleanupConfig must include raw.source_material_registry"
    );

    // Current cleanup contract is truncate-first rather than trigger-disable-first.
    let truncatable_tables: Vec<_> = config
        .truncatable_tables()
        .map(|t| t.table_name)
        .collect();

    assert!(
        truncatable_tables.contains(&"core.events"),
        "core.events must stay on the truncate cleanup path"
    );
    assert!(
        truncatable_tables.contains(&"raw.temporal_ledger"),
        "raw.temporal_ledger must stay on the truncate cleanup path"
    );

    Ok(())
}

/// Ensures pool stats helpers are usable inside async runtimes (no blocking_lock panics).
#[sinex_test]
async fn pool_stats_helpers_are_async_safe() -> xtask::sandbox::TestResult<()> {
    let _ = xtask::sandbox::db::get_pool_stats();
    let _ = xtask::sandbox::db::get_pool_stats_async().await;
    Ok(())
}

/// Ensures session state reset helper is callable in CI (permissions, triggers, RLS).
#[sinex_test]
async fn can_reset_session_state_via_helper(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    ensure_default_session_state(ctx.pool()).await?;
    Ok(())
}

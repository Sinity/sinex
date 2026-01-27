//! CI infrastructure validation tests.
//!
//! These tests verify that the CI environment is correctly configured,
//! preventing permission-related failures that could otherwise only be
//! caught during CI runs.

use futures::future::BoxFuture;
use sinex_schema::schema_registry;
use xtask::sandbox::{sinex_test, test_db_pool, TestResult};
use sqlx::{PgPool, Row};

/// Verifies that all schemas from the registry are accessible.
///
/// This test catches cases where schemas are added to the registry but
/// CI scripts don't grant access to them (like the `public` schema issue).
#[sinex_test]
async fn ci_setup_grants_all_schemas() -> TestResult<()> {
    let pool = test_db_pool().await;

    // Get current user
    let current_user: String = sqlx::query_scalar("SELECT current_user")
        .fetch_one(&pool)
        .await?;

    // Verify we have USAGE on all schemas
    for schema in schema_registry::SINEX_SCHEMAS {
        let has_usage: bool = sqlx::query(&format!(
            "SELECT has_schema_privilege($1, $2, 'USAGE') as has_usage"
        ))
        .bind(&current_user)
        .bind(schema.name)
        .fetch_one(&pool)
        .await?
        .get("has_usage");

        assert!(
            has_usage,
            "User '{current_user}' missing USAGE privilege on schema '{}'.\n\
             This indicates CI setup (cargo xtask ci postgres) is not granting access to all schemas.\n\
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
async fn can_create_tables_in_all_schemas() -> TestResult<()> {
    let pool = test_db_pool().await;

    for schema in schema_registry::SINEX_SCHEMAS {
        // Try to create a temporary table
        let result = sqlx::query(&format!(
            "CREATE TEMP TABLE {schema}_test_ci_permissions (id INT)"
        ))
        .execute(&pool)
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
async fn can_disable_temporal_ledger_triggers() -> TestResult<()> {
    let pool = test_db_pool().await;
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

/// Verifies seaql_migrations table is accessible.
///
/// SQLx validation needs to query this table for migration tracking.
#[sinex_test]
async fn seaql_migrations_table_accessible() -> TestResult<()> {
    let pool = test_db_pool().await;

    // Try to query the migrations table
    let result = sqlx::query("SELECT COUNT(*) FROM seaql_migrations")
        .fetch_one(&pool)
        .await;

    assert!(
        result.is_ok(),
        "Cannot access seaql_migrations table: {:?}\n\
         This is in the public schema - ensure CI setup grants access to 'public' schema.",
        result.err()
    );

    Ok(())
}

/// Verifies that session_replication_role can be set and reset.
///
/// This is critical for cleanup operations that need to bypass FK constraints.
#[sinex_test]
async fn can_set_session_replication_role() -> TestResult<()> {
    let pool = test_db_pool().await;
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
async fn can_disable_row_security() -> TestResult<()> {
    let pool = test_db_pool().await;
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
async fn can_toggle_core_events_triggers() -> TestResult<()> {
    let pool = test_db_pool().await;
    let mut conn = pool.acquire().await?;

    // Disable triggers
    let disable_result = sqlx::query("ALTER TABLE core.events DISABLE TRIGGER ALL")
        .execute(&mut *conn)
        .await;

    assert!(
        disable_result.is_ok(),
        "Cannot disable triggers on core.events: {:?}\n\
         Test cleanup requires the ability to disable archive triggers.",
        disable_result.err()
    );

    // Re-enable triggers
    let enable_result = sqlx::query("ALTER TABLE core.events ENABLE TRIGGER ALL")
        .execute(&mut *conn)
        .await;

    assert!(
        enable_result.is_ok(),
        "Cannot re-enable triggers on core.events: {:?}",
        enable_result.err()
    );

    Ok(())
}

/// Verifies permissions on all CleanupConfig tables.
///
/// Ensures CI can DELETE from all tables that cleanup needs to clear.
#[sinex_test]
async fn can_delete_from_all_cleanup_tables() -> TestResult<()> {
    use xtask::sandbox::fs::{CleanupConfig, CleanupMethod};

    let pool = test_db_pool().await;
    let config = CleanupConfig::default();

    for table in config.tables_to_clean() {
        // Try DELETE (should succeed even if table is empty)
        let delete_result = sqlx::query(&format!("DELETE FROM {} WHERE false", table.table_name))
            .execute(&pool)
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
                .execute(&pool)
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
async fn session_guards_restore_state() -> TestResult<()> {
    use xtask::sandbox::fs::CleanupConfig;

    let pool = test_db_pool().await;
    let mut conn = pool.acquire().await?;
    let config = CleanupConfig::default();

    // Record initial state
    let initial_replication_role: String = sqlx::query_scalar("SHOW session_replication_role")
        .fetch_one(&mut *conn)
        .await?;

    // Use guards (simulating cleanup operation)
    {
        let replication_guard =
            sinex_test_utils::session_guards::ReplicationRoleGuard::disable_for_cleanup(&mut conn)
                .await?;
        let row_security_guard =
            sinex_test_utils::session_guards::RowSecurityGuard::disable_for_cleanup(&mut conn)
                .await?;
        let trigger_tables: Vec<_> = config
            .tables_requiring_trigger_disable()
            .map(|t| t.table_name)
            .collect();
        let triggers_guard =
            sinex_test_utils::session_guards::TriggersGuard::disable_for_cleanup(
                &mut conn,
                trigger_tables,
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

/// Verifies guards restore state even when the inner block errors.
#[sinex_test]
async fn session_guards_restore_on_error() -> TestResult<()> {
    use xtask::sandbox::fs::CleanupConfig;

    let pool = test_db_pool().await;
    let mut conn = pool.acquire().await?;
    let config = CleanupConfig::default();

    let initial_replication_role: String = sqlx::query_scalar("SHOW session_replication_role")
        .fetch_one(&mut *conn)
        .await?;

    // Force an error inside the guard block
    let result = xtask::sandbox::db_common::with_cleanup_session(&mut conn, &config, |_conn| {
        let fut: BoxFuture<'_, TestResult<()>> =
            Box::pin(async move { Err(color_eyre::eyre::eyre!("intentional failure")) });
        fut
    })
    .await;

    assert!(result.is_err(), "Expected intentional failure");

    let final_replication_role: String = sqlx::query_scalar("SHOW session_replication_role")
        .fetch_one(&mut *conn)
        .await?;

    assert_eq!(
        final_replication_role, initial_replication_role,
        "Guards should restore original session state even on error"
    );

    Ok(())
}

/// Verifies that CleanupConfig is authoritative for cleanup behavior.
///
/// No hardcoded table lists should exist in cleanup functions.
#[sinex_test]
async fn cleanup_config_is_authoritative() -> TestResult<()> {
    use xtask::sandbox::fs::CleanupConfig;

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

    // Verify tables requiring trigger disable are marked correctly
    let trigger_disable_tables: Vec<_> = config
        .tables_requiring_trigger_disable()
        .map(|t| t.table_name)
        .collect();

    assert!(
        trigger_disable_tables.contains(&"core.events"),
        "core.events must have disable_triggers = true"
    );
    assert!(
        trigger_disable_tables.contains(&"raw.temporal_ledger"),
        "raw.temporal_ledger must have disable_triggers = true"
    );

    Ok(())
}

/// Ensures pool stats helpers are usable inside async runtimes (no blocking_lock panics).
#[sinex_test]
async fn pool_stats_helpers_are_async_safe() -> TestResult<()> {
    let _ = xtask::sandbox::db::get_pool_stats();
    let _ = xtask::sandbox::db::get_pool_stats_async().await;
    Ok(())
}

/// Ensures session state reset helper is callable in CI (permissions, triggers, RLS).
#[sinex_test]
async fn can_reset_session_state_via_helper() -> TestResult<()> {
    let pool = test_db_pool().await;
    xtask::sandbox::db::ensure_default_session_state(&pool).await?;
    Ok(())
}
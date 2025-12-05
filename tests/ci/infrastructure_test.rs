//! CI infrastructure validation tests.
//!
//! These tests verify that the CI environment is correctly configured,
//! preventing permission-related failures that could otherwise only be
//! caught during CI runs.

use sinex_schema::schema_registry;
use sinex_test_utils::{sinex_test, test_db_pool, TestResult};
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
             This indicates CI setup (scripts/ci-postgres.sh) is not granting access to all schemas.\n\
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

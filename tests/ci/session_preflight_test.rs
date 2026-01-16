//! Tests for session preflight reset helpers.
use sinex_test_utils::{sinex_test, test_db_pool, TestResult};

/// Intentionally corrupt session state and ensure ensure_default_session_state fixes it.
#[sinex_test]
async fn preflight_resets_session_state() -> TestResult<()> {
    use sinex_test_utils::database_pool::ensure_default_session_state;
    use sinex_test_utils::cleanup_config::CleanupConfig;

    let pool = test_db_pool().await;
    let mut conn = pool.acquire().await?;

    // Corrupt replication_role and row_security
    let _ = sqlx::query("SET session_replication_role = 'replica'")
        .execute(&mut *conn)
        .await;
    let _ = sqlx::query("SET row_security = off")
        .execute(&mut *conn)
        .await;

    // Disable triggers on required tables
    let config = CleanupConfig::default();
    for table in config.tables_requiring_trigger_disable() {
        let _ = sqlx::query(&format!("ALTER TABLE {} DISABLE TRIGGER ALL", table.table_name))
            .execute(&mut *conn)
            .await;
    }

    drop(conn);
    ensure_default_session_state(&pool).await?;

    let mut check = pool.acquire().await?;
    let role: String = sqlx::query_scalar("SHOW session_replication_role")
        .fetch_one(&mut *check)
        .await?;
    assert_eq!(role, "origin");
    let row_sec: String = sqlx::query_scalar("SHOW row_security")
        .fetch_one(&mut *check)
        .await?;
    assert_eq!(row_sec.to_lowercase(), "on");

    for table in config.tables_requiring_trigger_disable() {
        let query = format!(
            "SELECT NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = '{}'::regclass AND NOT tgenabled IN ('O','D')) AS enabled",
            table.table_name
        );
        let enabled: Option<bool> = sqlx::query_scalar(&query)
            .fetch_one(&mut *check)
            .await?;
        assert_ne!(enabled, Some(false), "Triggers should be enabled on {}", table.table_name);
    }

    Ok(())
}
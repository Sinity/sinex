//! Tests for session preflight reset helpers.
use xtask::sandbox::db::ensure_default_session_state;
use xtask::sandbox::prelude::sinex_test;

/// Intentionally corrupt session state and ensure ensure_default_session_state fixes it.
#[sinex_test]
async fn preflight_resets_session_state(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let pool = ctx.pool();
    let mut conn = pool.acquire().await?;

    // Corrupt replication_role and row_security
    let _ = sqlx::query("SET session_replication_role = 'replica'")
        .execute(&mut *conn)
        .await;
    let _ = sqlx::query("SET row_security = off")
        .execute(&mut *conn)
        .await;

    drop(conn);
    ensure_default_session_state(pool).await?;

    let mut check = pool.acquire().await?;
    let role: String = sqlx::query_scalar("SHOW session_replication_role")
        .fetch_one(&mut *check)
        .await?;
    assert_eq!(role, "origin");
    let row_sec: String = sqlx::query_scalar("SHOW row_security")
        .fetch_one(&mut *check)
        .await?;
    assert_eq!(row_sec.to_lowercase(), "on");

    Ok(())
}

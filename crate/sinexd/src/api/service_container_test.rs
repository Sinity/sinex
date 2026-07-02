use super::recover_stale_replay_operations;
use sqlx::postgres::PgPoolOptions;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn stale_replay_recovery_accepts_clean_state(ctx: TestContext) -> TestResult<()> {
    let replay = sinex_db::replay::state_machine::ReplayStateMachine::new(ctx.pool.clone());
    recover_stale_replay_operations(&replay).await?;
    Ok(())
}

#[sinex_test]
async fn stale_replay_recovery_surfaces_startup_failures() -> TestResult<()> {
    let pool = PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_millis(10))
        .connect_lazy("postgresql://127.0.0.1:1/sinex_test")?;
    let replay = sinex_db::replay::state_machine::ReplayStateMachine::new(pool);

    let error = recover_stale_replay_operations(&replay)
        .await
        .expect_err("startup recovery should fail honestly when the pool is unusable");

    let message = error.to_string();
    assert!(message.contains("Failed to recover stale replay operations on startup"));
    assert!(message.contains("gateway.recover_stale_replay_operations"));
    Ok(())
}

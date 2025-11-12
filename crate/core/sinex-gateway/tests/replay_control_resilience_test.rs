use std::env;

use sinex_gateway::ServiceContainer;
use sinex_test_utils::{sinex_test, TestContext, TestResult};
use tempfile::TempDir;

#[sinex_test]
async fn service_container_should_fail_when_replay_control_unavailable(
    ctx: TestContext,
) -> TestResult<()> {
    let annex_dir = TempDir::new()?;
    env::set_var("SINEX_ANNEX_PATH", annex_dir.path().to_str().unwrap());
    env::set_var("SINEX_NATS_URL", "nats://127.0.0.1:59999");

    let result = ServiceContainer::new(Some(ctx.database_url().to_string())).await;

    assert!(
        result.is_err(),
        "ServiceContainer should error instead of silently disabling replay control when NATS is unreachable"
    );

    Ok(())
}

use sinex_gateway::ServiceContainer;
use tempfile::TempDir;
use xtask::sandbox::{sinex_test, EnvGuard};

#[sinex_test]
async fn service_container_should_fail_when_replay_control_unavailable(
    ctx: TestContext,
) -> TestResult<()> {
    let annex_dir = TempDir::new()?;
    let mut env = EnvGuard::new();
    // Ensure replay control is NOT optional so failures surface as errors
    env.clear("SINEX_REPLAY_CONTROL_OPTIONAL");
    env.set(
        "SINEX_ANNEX_PATH",
        annex_dir
            .path()
            .to_str()
            .expect("path should be valid UTF-8"),
    );
    // Point at a port where nothing is listening
    env.set("SINEX_NATS_URL", "nats://127.0.0.1:59999");

    let result = ServiceContainer::new(Some(ctx.database_url().to_string())).await;

    assert!(
        result.is_err(),
        "ServiceContainer should error instead of silently disabling replay control when NATS is unreachable"
    );

    Ok(())
}

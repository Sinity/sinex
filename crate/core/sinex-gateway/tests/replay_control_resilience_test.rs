use sinex_gateway::ServiceContainer;
use tempfile::TempDir;
use xtask::sandbox::{EnvGuard, sinex_test};

#[sinex_test]
async fn service_container_should_fail_when_replay_control_unavailable(
    ctx: TestContext,
) -> TestResult<()> {
    let content_store_dir = TempDir::new()?;
    let mut env = EnvGuard::new();
    env.set(
        "SINEX_CONTENT_STORE_PATH",
        content_store_dir
            .path()
            .to_str()
            .expect("path should be valid UTF-8"),
    );
    // Point at a port where nothing is listening
    env.set("SINEX_NATS_URL", "nats://127.0.0.1:59999");

    let result = ServiceContainer::from_database_url(ctx.database_url()).await;

    assert!(
        result.is_err(),
        "ServiceContainer should error instead of silently disabling replay control when NATS is unreachable"
    );

    Ok(())
}

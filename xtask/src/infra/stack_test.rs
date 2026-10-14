use super::StackConfig;
use crate::sandbox::{EnvGuard, TestResult};
use xtask::sandbox::sinex_test;

#[test]
fn persisted_nats_port_overrides_the_shell_default() {
    let temp = tempfile::tempdir().expect("temporary state directory");
    std::fs::write(temp.path().join("nats.port"), "44309\n").expect("persisted NATS port");
    let mut env = EnvGuard::with_keys(&["SINEX_DEV_NATS_PORT"]);
    env.set("SINEX_DEV_NATS_PORT", "44308");
    let config = StackConfig::from_state_dir(temp.path().to_path_buf());
    assert_eq!(config.nats.port, 44_309);
}

#[sinex_test]
async fn stack_config_uses_explicit_environment_ports_without_an_assignment() -> TestResult<()> {
    let state = tempfile::tempdir()?;
    let mut env = EnvGuard::with_keys(&["SINEX_DEV_POSTGRES_PORT", "SINEX_DEV_NATS_PORT"]);
    env.set("SINEX_DEV_POSTGRES_PORT", "45432");
    env.set("SINEX_DEV_NATS_PORT", "44308");
    let config = StackConfig::from_state_dir(state.path().to_path_buf());
    assert_eq!(config.postgres.port, 45_432);
    assert_eq!(config.nats.port, 44_308);
    assert!(
        config.database_url().contains("port=45432"),
        "database consumers must use the configured PostgreSQL port"
    );
    assert_eq!(config.nats_url(), "nats://localhost:44308");
    Ok(())
}

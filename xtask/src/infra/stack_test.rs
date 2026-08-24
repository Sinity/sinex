use super::StackConfig;
use crate::sandbox::{EnvGuard, TestResult};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn stack_config_uses_agentctl_lease_ports() -> TestResult<()> {
    let mut env = EnvGuard::with_keys(&["SINEX_DEV_POSTGRES_PORT", "SINEX_DEV_NATS_PORT"]);
    env.set("SINEX_DEV_POSTGRES_PORT", "45432");
    env.set("SINEX_DEV_NATS_PORT", "44308");
    let config = StackConfig::for_current_checkout()?;
    assert_eq!(config.postgres.port, 45_432);
    assert_eq!(config.nats.port, 44_308);
    assert!(
        config.database_url().contains("port=45432"),
        "database consumers must use the leased PostgreSQL port"
    );
    assert_eq!(config.nats_url(), "nats://localhost:44308");
    Ok(())
}

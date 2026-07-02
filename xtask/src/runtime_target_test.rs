use super::*;
use crate::config::Config;
use xtask::sandbox::{EnvGuard, sinex_test};

#[sinex_test]
async fn checkout_runtime_target_uses_checkout_stack_without_descriptor() -> TestResult<()> {
    let mut env = EnvGuard::with_keys(&[
        "DATABASE_URL",
        "SINEX_NATS_URL",
        "SINEX_API_URL",
        "SINEX_API_URL",
        "SINEX_API_TCP_LISTEN",
        "SINEX_RUNTIME_TARGET_CONFIG",
    ]);
    env.clear("DATABASE_URL");
    env.clear("SINEX_NATS_URL");
    env.clear("SINEX_API_URL");
    env.clear("SINEX_API_URL");
    env.clear("SINEX_API_TCP_LISTEN");
    env.set("SINEX_RUNTIME_TARGET_CONFIG", "/definitely/not/loaded.json");

    let cfg = Config::from_env();
    let target = checkout_runtime_target(&cfg)?;

    assert_eq!(target.name, "checkout-local");
    assert_eq!(target.kind, RuntimeTargetKind::DevCheckout);
    assert_eq!(target.source.as_deref(), Some("xtask checkout config"));
    assert!(
        target
            .database
            .url
            .as_deref()
            .is_some_and(|url| url.contains("sinex_dev"))
    );
    assert_eq!(target.nats.servers.len(), 1);
    assert_eq!(
        target.gateway.base_url.as_deref(),
        Some(CHECKOUT_DEV_GATEWAY_URL)
    );
    Ok(())
}

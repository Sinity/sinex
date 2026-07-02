// Inline because this covers local env/default and report helper semantics.
use super::{module_heartbeat_stale_after, probe_health, probe_health_bool};
use sinex_primitives::error::SinexError;
use xtask::sandbox::{EnvGuard, sinex_serial_test, sinex_test};

#[sinex_serial_test]
async fn module_heartbeat_stale_after_defaults_invalid_override()
-> xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_MODULE_HEARTBEAT_STALE_SECS", "bogus");

    let error = module_heartbeat_stale_after().expect_err("invalid override should fail");
    assert!(
        error
            .to_string()
            .contains("SINEX_MODULE_HEARTBEAT_STALE_SECS must be a positive integer")
    );
    Ok(())
}

#[sinex_serial_test]
async fn module_heartbeat_stale_after_defaults_zero_override() -> xtask::sandbox::TestResult<()>
{
    let mut env = EnvGuard::new();
    env.set("SINEX_MODULE_HEARTBEAT_STALE_SECS", "0");

    let error = module_heartbeat_stale_after().expect_err("zero override should fail");
    assert!(
        error
            .to_string()
            .contains("SINEX_MODULE_HEARTBEAT_STALE_SECS must be greater than zero")
    );
    Ok(())
}

#[sinex_test]
async fn probe_health_preserves_error_text() -> xtask::sandbox::TestResult<()> {
    let (_value, error) = probe_health::<()>(Err(SinexError::configuration("probe failed")));
    assert_eq!(error.as_deref(), Some("Configuration error: probe failed"));
    Ok(())
}

#[sinex_test]
async fn probe_health_bool_preserves_error_text() -> xtask::sandbox::TestResult<()> {
    let (value, error) = probe_health_bool(Err(SinexError::configuration("probe failed")));
    assert!(!value);
    assert_eq!(error.as_deref(), Some("Configuration error: probe failed"));
    Ok(())
}

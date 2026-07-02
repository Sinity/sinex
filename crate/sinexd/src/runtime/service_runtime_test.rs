use super::*;
use xtask::sandbox::EnvGuard;
use xtask::sandbox::prelude::*;

#[sinex_serial_test]
async fn load_env_filter_defaults_when_rust_log_is_missing() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.clear("RUST_LOG");

    load_env_filter("test_service=info")?;
    Ok(())
}

#[sinex_serial_test]
async fn load_env_filter_rejects_invalid_directive() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("RUST_LOG", "test_service=wat");

    let error = load_env_filter("test_service=info")
        .expect_err("invalid directives must fail honestly");
    let message = error.to_string();
    assert!(message.contains("RUST_LOG"));
    assert!(message.contains("test_service=wat"));
    Ok(())
}

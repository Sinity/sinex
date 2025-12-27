use sinex_rpc_dispatcher::RpcDispatcherConfig;
use sinex_test_utils::{sinex_test, TestResult};
use validator::Validate;

#[sinex_test]
fn rpc_dispatcher_default_config_validates() -> TestResult<()> {
    let config = RpcDispatcherConfig::default();
    assert!(config.validate().is_ok());
    Ok(())
}

#[sinex_test]
fn rpc_dispatcher_config_rejects_invalid_max_connections() -> TestResult<()> {
    let mut config = RpcDispatcherConfig::default();
    config.max_connections = Some(0);
    assert!(config.validate().is_err());
    Ok(())
}

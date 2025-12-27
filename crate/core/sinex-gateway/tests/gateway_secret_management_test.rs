use once_cell::sync::Lazy;
use sinex_gateway::rpc_server::test_support::{
    gateway_auth_mode_from_env, GatewayAuthModeSnapshot,
};
use sinex_test_utils::sinex_test;
use std::sync::Mutex;

static ENV_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

fn clear_auth_env() {
    std::env::remove_var("SINEX_GATEWAY_ALLOW_INSECURE");
    std::env::remove_var("SINEX_RPC_TOKEN");
    std::env::remove_var("SINEX_RPC_TOKEN_FILE");
}

#[sinex_test]
fn gateway_auth_requires_token_by_default() -> color_eyre::eyre::Result<()> {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_auth_env();

    let result = gateway_auth_mode_from_env();
    assert!(result.is_err(), "expected missing token to error");

    Ok(())
}

#[sinex_test]
fn gateway_auth_accepts_env_token() -> color_eyre::eyre::Result<()> {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_auth_env();
    std::env::set_var("SINEX_RPC_TOKEN", "secret-token");

    let mode = gateway_auth_mode_from_env()?;
    assert_eq!(mode, GatewayAuthModeSnapshot::StaticToken);

    clear_auth_env();
    Ok(())
}

#[sinex_test]
fn gateway_auth_accepts_file_token() -> color_eyre::eyre::Result<()> {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_auth_env();

    let mut temp = tempfile::NamedTempFile::new()?;
    std::io::Write::write_all(&mut temp, b"file-token\n")?;
    std::env::set_var("SINEX_RPC_TOKEN_FILE", temp.path());

    let mode = gateway_auth_mode_from_env()?;
    assert_eq!(mode, GatewayAuthModeSnapshot::StaticToken);

    clear_auth_env();
    Ok(())
}

#[sinex_test]
fn gateway_auth_allows_insecure_opt_in() -> color_eyre::eyre::Result<()> {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_auth_env();
    std::env::set_var("SINEX_GATEWAY_ALLOW_INSECURE", "1");

    let mode = gateway_auth_mode_from_env()?;
    assert_eq!(mode, GatewayAuthModeSnapshot::Disabled);

    clear_auth_env();
    Ok(())
}

use std::io::Write;

use sinex_gateway::rpc_server_test_support::{
    GatewayAuthModeSnapshot, gateway_auth_mode_from_env, read_token_from_env,
};
use xtask::sandbox::{EnvGuard, sinex_test};

fn reset_auth_env(env: &mut EnvGuard) {
    env.clear("SINEX_GATEWAY_ADMIN_TOKEN_FILE");
    env.clear("SINEX_RPC_TOKEN");
    env.clear("SINEX_RPC_TOKEN_FILE");
}

#[sinex_test]
fn gateway_auth_requires_token_by_default() -> TestResult<()> {
    let mut env = EnvGuard::new();
    reset_auth_env(&mut env);

    let result = gateway_auth_mode_from_env();
    assert!(result.is_err(), "expected missing token to error");

    Ok(())
}

#[sinex_test]
fn gateway_auth_accepts_env_token() -> TestResult<()> {
    let mut env = EnvGuard::new();
    reset_auth_env(&mut env);
    env.set("SINEX_RPC_TOKEN", "secret-token");

    let mode = gateway_auth_mode_from_env()?;
    assert_eq!(mode, GatewayAuthModeSnapshot::StaticToken);

    Ok(())
}

#[sinex_test]
fn gateway_auth_accepts_file_token() -> TestResult<()> {
    let mut env = EnvGuard::new();
    reset_auth_env(&mut env);

    let mut temp = tempfile::NamedTempFile::new()?;
    temp.write_all(
        b"file-token
",
    )?;
    env.set(
        "SINEX_RPC_TOKEN_FILE",
        temp.path().to_str().expect("temp path utf8"),
    );

    let mode = gateway_auth_mode_from_env()?;
    assert_eq!(mode, GatewayAuthModeSnapshot::StaticToken);

    Ok(())
}

#[sinex_test]
fn gateway_auth_accepts_admin_token_file() -> TestResult<()> {
    let mut env = EnvGuard::new();
    reset_auth_env(&mut env);

    let mut temp = tempfile::NamedTempFile::new()?;
    temp.write_all(
        b"admin-token
",
    )?;
    env.set(
        "SINEX_GATEWAY_ADMIN_TOKEN_FILE",
        temp.path().to_str().expect("temp path utf8"),
    );

    let mode = gateway_auth_mode_from_env()?;
    assert_eq!(mode, GatewayAuthModeSnapshot::StaticToken);

    Ok(())
}

#[sinex_test]
fn gateway_token_file_rotation_reads_latest() -> TestResult<()> {
    let mut env = EnvGuard::new();
    reset_auth_env(&mut env);

    let temp = tempfile::NamedTempFile::new()?;
    std::fs::write(
        temp.path(),
        "token-a
",
    )?;
    env.set(
        "SINEX_RPC_TOKEN_FILE",
        temp.path().to_str().expect("temp path utf8"),
    );

    let token = read_token_from_env()?.expect("token file should be readable");
    assert_eq!(token, "token-a");

    std::fs::write(
        temp.path(),
        "token-b
",
    )?;
    let token = read_token_from_env()?.expect("token file should be readable");
    assert_eq!(token, "token-b");

    Ok(())
}

use super::*;
use xtask::sandbox::prelude::*;

fn trusted_message(secret: &str) -> NativeMessage {
    NativeMessage {
        msg_type: "request".to_string(),
        method: None,
        params: None,
        id: None,
        extension_id: Some("ext-1".to_string()),
        extension_secret: Some(secret.to_string()),
        host: None,
        protocol_version: None,
    }
}

#[sinex_test]
async fn secret_comparison_is_routed_through_constant_time_helper() -> TestResult<()> {
    SECRET_COMPARE_CALLS.store(0, Ordering::Relaxed);

    let config = NativeMessagingConfig {
        trusted_extensions: vec![TrustedExtension {
            id: "ext-1".to_string(),
            secret: Some("topsecret".to_string()),
        }],
        trusted_extensions_config_error: None,
        trusted_hosts: Vec::new(),
        trusted_hosts_config_error: None,
        expected_protocol_version: None,
        capabilities: std::collections::HashMap::new(),
        capabilities_config_error: None,
        rate_limiter: None,
        extension_roles: std::collections::HashMap::new(),
        extension_roles_config_error: None,
        max_message_size: 1024 * 1024,
        read_timeout: std::time::Duration::from_secs(DEFAULT_READ_TIMEOUT_SECS),
    };

    // Successful path still calls the constant-time helper
    config
        .enforce_extension(&trusted_message("topsecret"))
        .expect("trusted secret should pass");

    // Failure path also uses the same helper
    assert!(
        config
            .enforce_extension(&trusted_message("wrongsecret"))
            .is_err()
    );

    assert!(SECRET_COMPARE_CALLS.load(Ordering::Relaxed) >= 2);
    Ok(())
}

#[sinex_test]
async fn extension_roles_env_uses_typed_role_values() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set(
        EXTENSION_ROLES_ENV,
        r#"{"ext-read":"readonly","ext-write":"write","ext-admin":"admin"}"#,
    );

    let config = NativeMessagingConfig::from_env()?;

    assert_eq!(
        config.resolve_extension_role(Some("ext-read"))?,
        crate::api::auth::Role::ReadOnly
    );
    assert_eq!(
        config.resolve_extension_role(Some("ext-write"))?,
        crate::api::auth::Role::Write
    );
    assert_eq!(
        config.resolve_extension_role(Some("ext-admin"))?,
        crate::api::auth::Role::Admin
    );
    Ok(())
}

#[sinex_test]
async fn invalid_extension_role_env_entry_surfaces_parse_error() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set(EXTENSION_ROLES_ENV, r#"{"ext-write":"superuser"}"#);

    let config = NativeMessagingConfig::from_env()?;

    let error = config
        .resolve_extension_role(Some("ext-write"))
        .expect_err("invalid role config should be surfaced");
    assert!(error.to_string().contains(EXTENSION_ROLES_ENV));
    Ok(())
}

#[sinex_test]
async fn invalid_capabilities_env_entry_surfaces_parse_error() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set(
        CAPABILITIES_ENV,
        r#"{"ext-1":{"allowed_methods":"system.health","rate_limit_per_minute":null}}"#,
    );

    let config = NativeMessagingConfig::from_env()?;
    let message = NativeMessage {
        msg_type: "rpc".to_string(),
        method: Some("system.health".to_string()),
        params: Some(serde_json::json!({})),
        id: None,
        extension_id: Some("ext-1".to_string()),
        extension_secret: None,
        host: None,
        protocol_version: None,
    };

    let error = config
        .enforce_capabilities(&message)
        .expect_err("invalid capabilities config should be surfaced");
    assert!(error.to_string().contains(CAPABILITIES_ENV));
    Ok(())
}

#[sinex_test]
async fn invalid_trusted_extensions_env_entry_surfaces_parse_error() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set(TRUSTED_EXTENSION_ENV, "#missing-id");

    let config = NativeMessagingConfig::from_env()?;
    let error = config
        .enforce_extension(&trusted_message("anything"))
        .expect_err("malformed trusted extension config should be surfaced");
    assert!(error.to_string().contains(TRUSTED_EXTENSION_ENV));
    assert!(error.to_string().contains("missing an extension id"));
    Ok(())
}

#[sinex_test]
async fn duplicate_trusted_extensions_env_entry_surfaces_parse_error() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set(TRUSTED_EXTENSION_ENV, "ext-1#alpha,ext-1#beta");

    let config = NativeMessagingConfig::from_env()?;
    let error = config
        .enforce_extension(&trusted_message("alpha"))
        .expect_err("duplicate trusted extension ids should be surfaced");
    assert!(error.to_string().contains(TRUSTED_EXTENSION_ENV));
    assert!(error.to_string().contains("duplicate trusted extension id"));
    Ok(())
}

#[sinex_test]
async fn invalid_trusted_hosts_env_entry_surfaces_parse_error() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set(TRUSTED_HOSTS_ENV, " , ");

    let config = NativeMessagingConfig::from_env()?;
    let error = config
        .enforce_host(&NativeMessage {
            msg_type: "request".to_string(),
            method: None,
            params: None,
            id: None,
            extension_id: None,
            extension_secret: None,
            host: Some("localhost".to_string()),
            protocol_version: None,
        })
        .expect_err("malformed trusted hosts config should be surfaced");
    assert!(error.to_string().contains(TRUSTED_HOSTS_ENV));
    assert!(
        error
            .to_string()
            .contains("no host entries could be parsed")
    );
    Ok(())
}

#[sinex_test]
async fn native_messaging_numeric_env_overrides_apply_valid_values() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set(MAX_MESSAGE_SIZE_ENV, "2048");
    env.set(READ_TIMEOUT_ENV, "12");

    let config = NativeMessagingConfig::from_env()?;

    assert_eq!(config.max_message_size, 2048);
    assert_eq!(config.read_timeout, std::time::Duration::from_secs(12));
    Ok(())
}

#[sinex_test]
async fn native_messaging_numeric_env_overrides_reject_invalid_values() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set(MAX_MESSAGE_SIZE_ENV, "0");
    env.set(READ_TIMEOUT_ENV, "forever");

    let config = NativeMessagingConfig::from_env()?;

    assert_eq!(config.max_message_size, DEFAULT_MAX_MESSAGE_SIZE_BYTES);
    assert_eq!(
        config.read_timeout,
        std::time::Duration::from_secs(DEFAULT_READ_TIMEOUT_SECS)
    );
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn native_messaging_env_rejects_non_utf8_trusted_extensions() -> TestResult<()> {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    let mut env = EnvGuard::new();
    env.set(
        TRUSTED_EXTENSION_ENV,
        OsString::from_vec(vec![0x66, 0x6f, 0x80, 0x6f]),
    );

    let error = NativeMessagingConfig::from_env()
        .expect_err("non-UTF-8 native messaging env should be rejected");
    assert!(error.to_string().contains(
        "Environment variable SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS is not valid UTF-8"
    ));
    Ok(())
}

#[sinex_test]
async fn native_messaging_read_rejects_header_timeout() -> TestResult<()> {
    let (_writer, mut reader) = tokio::io::duplex(64);

    let error = read_message_from(
        &mut reader,
        DEFAULT_MAX_MESSAGE_SIZE_BYTES,
        std::time::Duration::from_millis(10),
    )
    .await
    .expect_err("header timeout should be surfaced");

    let message = error.to_string();
    assert!(message.contains("header"));
    assert!(message.contains("timed out"));
    Ok(())
}

#[sinex_test]
async fn native_messaging_read_rejects_body_timeout() -> TestResult<()> {
    let (mut writer, mut reader) = tokio::io::duplex(64);
    writer.write_all(&(4_u32).to_le_bytes()).await?;
    writer.write_all(&[0x7b, 0x7d]).await?;

    let error = read_message_from(
        &mut reader,
        DEFAULT_MAX_MESSAGE_SIZE_BYTES,
        std::time::Duration::from_millis(10),
    )
    .await
    .expect_err("body timeout should be surfaced");

    let message = error.to_string();
    assert!(message.contains("body"));
    assert!(message.contains("expected_bytes"));
    Ok(())
}

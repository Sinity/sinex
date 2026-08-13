use camino::Utf8PathBuf;
use sinexd::api::config::GatewayConfig;
use sinexd::event_engine::EventEngineConfig;
use sinexd::runtime::content_store::{ContentStoreConfig, MaterialContentStore};
#[cfg(unix)]
use std::ffi::OsString;
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;
use xtask::sandbox::prelude::*;

#[sinex_serial_test]
async fn gateway_config_load_namespaces_database_url_from_env() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("DATABASE_URL", "postgresql://gateway-config/sinex");

    let config = GatewayConfig::load()?;
    assert_eq!(config.database_url, "postgresql://gateway-config/sinex");
    Ok(())
}

#[sinex_serial_test]
async fn gateway_config_load_requires_database_url() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.clear("DATABASE_URL");

    let error =
        GatewayConfig::load().expect_err("missing database url should fail gateway config load");
    let message = error.to_string();

    assert!(message.contains("Database URL not provided"));
    Ok(())
}

#[sinex_serial_test]
async fn gateway_config_load_rejects_malformed_database_url() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("DATABASE_URL", "not-a-database-url");

    let error =
        GatewayConfig::load().expect_err("malformed database url should fail gateway config load");
    let message = error.to_string();

    assert!(message.contains("failed to parse DATABASE_URL"));
    Ok(())
}

#[sinex_test]
async fn gateway_cli_database_override_uses_effective_database_url() -> TestResult<()> {
    let config = GatewayConfig::default().with_cli_overrides(
        Some("postgresql://gateway-cli/sinex".to_string()),
        None,
        None,
    );
    assert_eq!(config.database_url, "postgresql://gateway-cli/sinex");
    Ok(())
}

#[sinex_serial_test]
async fn gateway_config_rejects_invalid_numeric_env_overrides() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_API_MAX_CONCURRENCY", "many");

    let error = GatewayConfig::load().expect_err("invalid env should fail gateway config load");
    let message = error.to_string();

    assert!(message.contains("SINEX_API_MAX_CONCURRENCY"));
    assert!(message.contains("many"));
    Ok(())
}

#[sinex_serial_test]
async fn gateway_config_propagates_invalid_nested_nats_configuration() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("DATABASE_URL", "postgresql://gateway-config/sinex");
    env.set("SINEX_NATS_REQUIRE_TLS", "definitely");

    let error = GatewayConfig::load()
        .expect_err("invalid nested NATS TLS setting must reject gateway startup configuration");
    assert!(error.to_string().contains("SINEX_NATS_REQUIRE_TLS"));
    Ok(())
}

#[sinex_serial_test]
async fn gateway_config_load_with_database_url_keeps_manual_env_overrides() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_NATS_URL", "nats://127.0.0.1:4555");
    env.set(
        "SINEX_API_CONTENT_STORE_PATH",
        "/tmp/sinex-content-store-test",
    );

    let config = GatewayConfig::load_with_database_url("postgresql://gateway-helper/sinex")?;

    assert_eq!(config.database_url, "postgresql://gateway-helper/sinex");
    assert_eq!(config.nats.url, "nats://127.0.0.1:4555");
    assert_eq!(config.content_store_path, "/tmp/sinex-content-store-test");
    Ok(())
}

/// Regression coverage for sinex-w334: the event-engine writer and gateway
/// reader must resolve the same default CAS root when no path override exists.
/// The byte round-trip proves this is a real shared-store check, rather than
/// merely comparing two default strings.
#[sinex_serial_test]
async fn writer_and_reader_defaults_share_cas_root() -> TestResult<()> {
    let workspace = tempfile::tempdir()?;
    let workspace_path = Utf8PathBuf::from_path_buf(workspace.path().to_path_buf())
        .map_err(|_| color_eyre::eyre::eyre!("temporary workspace path must be UTF-8"))?;
    let home = workspace_path.join("home");
    let event_engine_work_dir = workspace_path.join("event-engine-work");

    let mut env = EnvGuard::new();
    env.clear("SINEX_CONTENT_STORE_PATH");
    env.clear("SINEX_API_CONTENT_STORE_PATH");
    env.set("HOME", home.as_str());
    env.set(
        "SINEX_EVENT_ENGINE_WORK_DIR",
        event_engine_work_dir.as_str(),
    );

    let writer_config = EventEngineConfig::from_args(
        Some("postgresql://writer-reader-defaults/sinex".to_string()),
        "nats://writer-reader-defaults:4222".to_string(),
        false,
        16,
        None,
        None,
        None,
        None,
        false,
        None,
        None,
        None,
    )?;
    let reader_config =
        GatewayConfig::load_with_database_url("postgresql://writer-reader-defaults/sinex")?;

    assert_eq!(
        writer_config.content_store_path.as_str(),
        reader_config.content_store_path,
        "writer and reader must resolve one canonical default CAS root"
    );
    assert_ne!(
        writer_config.content_store_path, event_engine_work_dir,
        "the canonical default must not silently follow the event-engine work directory"
    );

    let writer_store = MaterialContentStore::new(ContentStoreConfig {
        root_path: writer_config.content_store_path.clone(),
        ..Default::default()
    })?;
    let reader_root = Utf8PathBuf::from(reader_config.content_store_path.clone());
    let reader_store = MaterialContentStore::new(ContentStoreConfig {
        root_path: reader_root,
        ..Default::default()
    })?;

    let source_path = workspace_path.join("writer-payload.txt");
    let payload = b"writer and reader share one CAS root";
    tokio::fs::write(&source_path, payload).await?;
    let key = writer_store.store_file(&source_path).await?;
    let reader_path = reader_store
        .path_if_local(&key.key)?
        .expect("local CAS key must resolve for the reader");

    assert_eq!(
        reader_path,
        writer_store
            .path_if_local(&key.key)?
            .expect("local CAS key must resolve for the writer")
    );
    assert_eq!(tokio::fs::read(reader_path).await?, payload);
    Ok(())
}

#[sinex_serial_test]
async fn gateway_config_prefers_gateway_specific_content_store_override() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("DATABASE_URL", "postgresql://gateway-config/sinex");
    env.set(
        "SINEX_CONTENT_STORE_PATH",
        "/tmp/sinex-content-store-shared",
    );
    env.set(
        "SINEX_API_CONTENT_STORE_PATH",
        "/tmp/sinex-content-store-gateway",
    );

    let config = GatewayConfig::load()?;

    assert_eq!(
        config.content_store_path,
        "/tmp/sinex-content-store-gateway"
    );
    Ok(())
}

#[cfg(unix)]
#[sinex_serial_test]
async fn gateway_config_rejects_non_unicode_database_url() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("DATABASE_URL", OsString::from_vec(vec![0x70, 0x80]));

    let error =
        GatewayConfig::load().expect_err("non-UTF8 DATABASE_URL should fail gateway config load");
    let message = error.to_string();

    assert!(message.contains("DATABASE_URL"));
    assert!(message.contains("not valid UTF-8"));
    Ok(())
}

#[cfg(unix)]
#[sinex_serial_test]
async fn gateway_config_rejects_non_unicode_shared_content_store_path() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("DATABASE_URL", "postgresql://gateway-config/sinex");
    env.set(
        "SINEX_CONTENT_STORE_PATH",
        OsString::from_vec(vec![0x2f, 0x74, 0x6d, 0x70, 0x80]),
    );

    let error = GatewayConfig::load()
        .expect_err("non-UTF8 SINEX_CONTENT_STORE_PATH should fail gateway config load");
    let message = error.to_string();

    assert!(message.contains("SINEX_CONTENT_STORE_PATH"));
    assert!(message.contains("not valid UTF-8"));
    Ok(())
}

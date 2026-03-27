use nkeys::KeyPair;
use sinex_primitives::nats::{NatsConnectionConfig, create_or_open_kv_store};
use std::io::Write;
use xtask::sandbox::EphemeralNats;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn nats_config_requires_tls_scheme_when_enabled() -> TestResult<()> {
    let mut config = NatsConnectionConfig::default();
    config.url = "nats://127.0.0.1:4222".to_string();
    config.require_tls = true;

    assert!(config.validate().is_err());
    Ok(())
}

#[sinex_test]
async fn nats_config_accepts_nkey_seed_file(_ctx: TestContext) -> TestResult<()> {
    let seed = KeyPair::new_user()
        .seed()
        .expect("nkey seed should be generated");
    let mut file = tempfile::NamedTempFile::new()?;
    write!(file, "{seed}")?;

    let mut config = NatsConnectionConfig::default();
    config.nkey_seed_file = Some(file.path().to_path_buf());
    config.to_options().await?;
    Ok(())
}

#[sinex_test]
async fn nats_config_rejects_multiple_auth_modes() -> TestResult<()> {
    let mut config = NatsConnectionConfig::default();
    config.token = Some("secret".to_string());
    config.token_file = Some(tempfile::NamedTempFile::new()?.path().to_path_buf());

    assert!(config.validate().is_err());
    Ok(())
}

#[sinex_test]
async fn create_or_open_kv_store_reuses_existing_bucket() -> TestResult<()> {
    let nats = EphemeralNats::start().await?;
    let js = nats.jetstream().await?;
    let bucket = format!("KV_PRIMITIVES_TEST_{}", uuid::Uuid::now_v7().simple());

    let first = create_or_open_kv_store(
        &js,
        async_nats::jetstream::kv::Config {
            bucket: bucket.clone(),
            history: 1,
            ..Default::default()
        },
    )
    .await?;
    let second = create_or_open_kv_store(
        &js,
        async_nats::jetstream::kv::Config {
            bucket,
            history: 1,
            ..Default::default()
        },
    )
    .await?;

    first.put("probe".to_string(), b"ok".to_vec().into()).await?;
    assert!(second.entry("probe").await?.is_some());
    nats.shutdown().await?;
    Ok(())
}

use nkeys::KeyPair;
use sinex_primitives::nats::NatsConnectionConfig;
use std::io::Write;
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
    config.nkey_file = Some(file.path().to_path_buf());
    config.to_options().await?;
    Ok(())
}

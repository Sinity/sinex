use async_nats::jetstream::kv;
use sinex_primitives::nats::NatsConnectionConfig;
use sinex_node_sdk::checkpoint::{CheckpointManager, CheckpointState};
use sinex_node_sdk::stream_processor::Checkpoint;
use std::path::PathBuf;
use xtask::sandbox::prelude::*;
use xtask::sandbox::EphemeralNats;

#[sinex_test]
async fn checkpoint_kv_over_mtls(ctx: TestContext) -> TestResult<()> {
    let _ctx = ctx;
    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../fixtures/tls");
    let fixtures = fixtures.canonicalize().unwrap_or(fixtures);

    let nats = EphemeralNats::builder()
        .with_tls_fixtures_path(&fixtures)
        .start()
        .await?;

    let nats_config = NatsConnectionConfig::builder()
        .url(nats.client_url().to_string())
        .require_tls(true)
        .ca_cert(fixtures.join("ca.pem"))
        .client_cert(fixtures.join("client.pem"))
        .client_key(fixtures.join("client-key.pem"))
        .build();

    let client = nats_config.connect().await?;
    let js = async_nats::jetstream::new(client);
    let kv_store = js
        .create_key_value(kv::Config {
            bucket: "KV_secure_checkpoints".to_string(),
            history: 5,
            ..Default::default()
        })
        .await?;

    let manager = CheckpointManager::new(
        kv_store,
        "secure-checkpoint-test".to_string(),
        "default".to_string(),
        "consumer-1".to_string(),
    );

    let mut state = CheckpointState::default();
    state.checkpoint = Checkpoint::stream("secure-offset-1", None);
    state.processed_count = 1;

    manager.save_checkpoint(&state).await?;
    let loaded = manager.load_checkpoint().await?;

    assert_eq!(loaded.processed_count, 1);
    assert_eq!(
        loaded.checkpoint.description(),
        state.checkpoint.description()
    );

    Ok(())
}

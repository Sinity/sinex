use async_nats::jetstream;
use color_eyre::eyre::{eyre, Result};

/// Ensure the material-related JetStream streams exist for tests.
pub async fn ensure_material_streams(nats: &async_nats::Client) -> Result<()> {
    let js = jetstream::new(nats.clone());
    let env = sinex_core::environment();

    js.get_or_create_stream(jetstream::stream::Config {
        name: env.nats_subject("source_material_begin"),
        subjects: vec![env.nats_subject("source_material.begin")],
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await
    .map_err(|err| eyre!("failed to create begin stream: {err}"))?;

    js.get_or_create_stream(jetstream::stream::Config {
        name: env.nats_subject("source_material_slices"),
        subjects: vec![env.nats_subject("source_material.slices.>")],
        storage: jetstream::stream::StorageType::File,
        max_message_size: 512 * 1024,
        ..Default::default()
    })
    .await
    .map_err(|err| eyre!("failed to create slices stream: {err}"))?;

    js.get_or_create_stream(jetstream::stream::Config {
        name: env.nats_subject("source_material_end"),
        subjects: vec![env.nats_subject("source_material.end")],
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await
    .map_err(|err| eyre!("failed to create end stream: {err}"))?;

    Ok(())
}

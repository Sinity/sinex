use async_nats::jetstream;
use sinex_test_utils::{sinex_test, EphemeralNats, PipelineNamespace};
use std::time::Duration;

#[sinex_test]
async fn subject_lookup_should_resolve_existing_stream() -> color_eyre::Result<()> {
    let nats = EphemeralNats::start().await?;
    let nats_client = nats.connect().await?;
    let js = nats.jetstream_with_client(nats_client);

    let namespace = PipelineNamespace::new("subject_lookup_should_resolve_existing_stream");
    let stream_name = namespace.stream("SOURCE_MATERIAL_BEGIN");
    let subject = namespace.subject("source_material.begin");

    js.get_or_create_stream(jetstream::stream::Config {
        name: stream_name.clone(),
        subjects: vec![subject.clone()],
        retention: jetstream::stream::RetentionPolicy::Limits,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    nats.wait_for_stream(&js, &stream_name, Duration::from_secs(5))
        .await?;

    Ok(())
}

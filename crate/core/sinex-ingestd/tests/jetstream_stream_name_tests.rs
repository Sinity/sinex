use async_nats::jetstream;
use sinex_test_utils::{sinex_test, TestContext};
use std::time::Duration;

#[sinex_test]
async fn subject_lookup_should_resolve_existing_stream() -> color_eyre::Result<()> {
    let ctx = TestContext::new().await?.with_nats().await?;
    let nats_client = ctx.nats_client();
    let js = jetstream::new(nats_client);
    let env = ctx.env();

    let stream_name = env.nats_stream_name("SOURCE_MATERIAL_BEGIN");
    let subject = env.nats_subject("source_material.begin");

    js.get_or_create_stream(jetstream::stream::Config {
        name: stream_name.clone(),
        subjects: vec![subject.clone()],
        retention: jetstream::stream::RetentionPolicy::Limits,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    // Give JetStream a moment to register the stream before lookup.
    tokio::time::sleep(Duration::from_millis(50)).await;

    js.get_stream(&env.nats_subject("source_material_begin"))
        .await
        .expect("stream lookup by subject should succeed");

    Ok(())
}

use async_nats::jetstream;
use std::time::Duration;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::Timeouts;

#[sinex_test]
async fn subject_lookup_should_resolve_existing_stream(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let js = nats.jetstream_with_client(nats_client);

    let namespace = ctx.pipeline_namespace();
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

    nats.wait_for_stream(&js, &stream_name, Duration::from_secs(Timeouts::QUICK))
        .await?;

    Ok(())
}

use async_nats::jetstream;
use color_eyre::eyre::eyre;
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

    wait_for_stream(&js, &stream_name, Duration::from_secs(5)).await?;

    Ok(())
}

async fn wait_for_stream(
    js: &jetstream::Context,
    name: &str,
    timeout: Duration,
) -> color_eyre::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match js.get_stream(name).await {
            Ok(_) => return Ok(()),
            Err(err) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(eyre!("stream {name} not ready: {err}"));
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

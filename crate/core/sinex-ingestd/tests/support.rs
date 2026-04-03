use async_nats::jetstream;
use color_eyre::eyre::eyre;
use sinex_db::DbPool;
use sinex_ingestd::{JetStreamConsumer, JetStreamTopology};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::Timeouts;

pub const FIXTURE_SOURCE_MATERIAL_ID: &str = "00000000-0000-7000-8000-000000000000";

pub async fn ensure_fixture_source_material(pool: &DbPool) -> color_eyre::Result<()> {
    sqlx::query!(
        r#"
        INSERT INTO raw.source_material_registry
            (id, material_kind, source_identifier, status, timing_info_type, staged_at)
        VALUES ($1::uuid, 'annex', 'test-fixture-material', 'completed', 'realtime', NOW())
        ON CONFLICT (id) DO UPDATE
        SET staged_at = EXCLUDED.staged_at,
            status = EXCLUDED.status,
            timing_info_type = EXCLUDED.timing_info_type
        "#,
        FIXTURE_SOURCE_MATERIAL_ID.parse::<uuid::Uuid>()?,
    )
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn spawn_consumer_and_wait_ready(
    ctx: &TestContext,
    js: &jetstream::Context,
    topology: &JetStreamTopology,
    consumer: JetStreamConsumer,
) -> TestResult<JoinHandle<sinex_ingestd::IngestdResult<()>>> {
    let nats = ctx.nats_handle()?;
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move { consumer.run_with_ready_signal(Some(ready_tx)).await });

    let stream_timeout = Duration::from_secs(Timeouts::SHORT);
    nats.wait_for_stream(js, &topology.events_stream, stream_timeout)
        .await?;
    nats.wait_for_stream(js, &topology.confirmations_stream, stream_timeout)
        .await?;
    nats.wait_for_stream(js, &topology.confirmation_retry_stream, stream_timeout)
        .await?;
    nats.wait_for_stream(js, &topology.dlq_stream, stream_timeout)
        .await?;
    timeout(stream_timeout, ready_rx)
        .await?
        .map_err(|_| eyre!("jetstream consumer exited before signalling readiness"))?;

    Ok(handle)
}

#[allow(dead_code)] // Shared across integration-test crates; each crate compiles its own copy.
pub async fn consume_one_stream_message(
    js: &jetstream::Context,
    stream_name: &str,
    consumer_name: &str,
) -> TestResult<jetstream::Message> {
    let stream = js.get_stream(stream_name).await?;
    let consumer = stream
        .get_or_create_consumer(
            consumer_name,
            jetstream::consumer::pull::Config {
                durable_name: Some(consumer_name.to_string()),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                deliver_policy: jetstream::consumer::DeliverPolicy::All,
                ..Default::default()
            },
        )
        .await?;
    let mut messages = consumer.messages().await?;
    let next_message = timeout(Duration::from_secs(Timeouts::SHORT), messages.next()).await?;
    let message =
        next_message.ok_or_else(|| eyre!("no message available in stream {stream_name}"))?;
    let message = message.map_err(|error| eyre!(error.to_string()))?;
    Ok(message)
}

#[allow(dead_code)] // Shared across integration-test crates; each crate compiles its own copy.
pub async fn wait_for_last_stream_message_by_subject(
    js: &jetstream::Context,
    stream_name: &str,
    subject: &str,
) -> TestResult<jetstream::message::StreamMessage> {
    let stream = js.get_stream(stream_name).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(Timeouts::SHORT);

    loop {
        match stream.get_last_raw_message_by_subject(subject).await {
            Ok(message) => return Ok(message),
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => {
                return Err(eyre!(
                    "no message available in stream {stream_name} for subject {subject}: {error}"
                ));
            }
        }
    }
}

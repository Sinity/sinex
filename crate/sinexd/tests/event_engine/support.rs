use async_nats::jetstream;
use color_eyre::eyre::eyre;
use sinex_db::DbPool;
use sinexd::event_engine::{JetStreamConsumer, JetStreamTopology};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::Timeouts;

pub const FIXTURE_SOURCE_MATERIAL_ID: &str = "00000000-0000-7000-8000-000000000000";

/// Wrap a single raw event in an `EventIntent` admission envelope.
///
/// Durable ingress on `events.raw.*` is envelope-only since #1149
/// (`Admission::admit_intent_bytes`): producers publish an `EventIntent`, not a
/// bare event. These integration tests predate that envelope, so they build the
/// inner event and wrap it here. The inner event shape is unchanged.
#[allow(dead_code)] // Shared across integration-test crates; each crate compiles its own copy.
pub fn admission_envelope(source_id: &str, event: serde_json::Value) -> serde_json::Value {
    admission_envelope_multi(source_id, vec![event])
}

/// Wrap several raw events sharing one physical `EventIntent` admission
/// envelope (sinex-r6d.12) — the shape a batched `event_transport`
/// `publish_intent_chunk` actually produces on the wire, letting tests drive
/// multi-child settlement scenarios (rejected/not-ready/poison siblings of a
/// valid event) through one raw JetStream message.
#[allow(dead_code)] // Shared across integration-test crates; each crate compiles its own copy.
pub fn admission_envelope_multi(
    source_id: &str,
    events: Vec<serde_json::Value>,
) -> serde_json::Value {
    serde_json::json!({
        "envelope_version": "1",
        "source_id": source_id,
        "parser_id": format!("{source_id}-parser"),
        "parser_version": "1.0.0",
        "events": events,
        "admitted_at": sinex_primitives::temporal::now().format_rfc3339(),
        "admitted_by": "test-host",
    })
}

/// Confirmed-event subject for material-provenance test events.
#[allow(dead_code)] // Shared across integration-test crates; each crate compiles its own copy.
pub fn confirmation_subject_for(prefix: &str, source: &str, event_type: &str) -> String {
    format!(
        "{prefix}material.{}.{}",
        sinex_primitives::environment::SinexEnvironment::nats_subject_token(source),
        sinex_primitives::environment::SinexEnvironment::nats_subject_token(event_type)
    )
}

#[allow(dead_code)] // Shared across integration-test crates; each crate compiles its own copy.
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

#[allow(dead_code)] // Shared across integration-test crates; each crate compiles its own copy.
pub async fn spawn_consumer_and_wait_ready(
    ctx: &TestContext,
    js: &jetstream::Context,
    topology: &JetStreamTopology,
    consumer: JetStreamConsumer,
) -> TestResult<JoinHandle<sinexd::event_engine::EventEngineResult<()>>> {
    let nats = ctx.nats_handle()?;
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move { consumer.run_with_ready_signal(Some(ready_tx)).await });

    let stream_timeout = Duration::from_secs(Timeouts::SHORT);
    nats.wait_for_stream(js, &topology.events_stream, stream_timeout)
        .await?;
    nats.wait_for_stream(js, &topology.confirmed_events_stream, stream_timeout)
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

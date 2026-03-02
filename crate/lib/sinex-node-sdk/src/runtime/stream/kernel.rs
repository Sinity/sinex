use crate::{NodeResult, SinexError};
use async_nats::jetstream;
use async_nats::jetstream::consumer::Consumer;
use async_nats::jetstream::consumer::pull::Config as PullConfig;
use futures::StreamExt;
use sinex_primitives::environment::SinexEnvironment;
use sinex_primitives::{Pagination, Timestamp, Ulid};
use std::future::Future;
use std::time::Duration;
use tracing::{debug, warn};

#[cfg(feature = "db")]
use sinex_db::{DbPool as PgPool, repositories::DbPoolExt};
#[cfg(feature = "db")]
use sinex_primitives::domain::EventSource;

#[derive(Debug, Clone)]
pub struct PullConsumerSpec {
    pub stream_name: String,
    pub durable_name: String,
    pub filter_subject: Option<String>,
    pub deliver_policy: jetstream::consumer::DeliverPolicy,
    pub ack_wait: Duration,
    pub max_ack_pending: i64,
    pub max_deliver: i64,
}

pub type PullConsumerHandle = Consumer<PullConfig>;

impl PullConsumerSpec {
    #[must_use]
    pub fn new(stream_name: impl Into<String>, durable_name: impl Into<String>) -> Self {
        Self {
            stream_name: stream_name.into(),
            durable_name: durable_name.into(),
            filter_subject: None,
            deliver_policy: jetstream::consumer::DeliverPolicy::All,
            ack_wait: Duration::from_secs(30),
            max_ack_pending: 1000,
            max_deliver: 10,
        }
    }
}

pub async fn ensure_pull_consumer(
    js: &jetstream::Context,
    spec: &PullConsumerSpec,
) -> NodeResult<PullConsumerHandle> {
    let stream = js.get_stream(&spec.stream_name).await.map_err(|e| {
        SinexError::processing(format!("Failed to get stream {}: {e}", spec.stream_name))
    })?;

    let mut cfg = PullConfig {
        durable_name: Some(spec.durable_name.clone()),
        ack_policy: jetstream::consumer::AckPolicy::Explicit,
        ack_wait: spec.ack_wait,
        deliver_policy: spec.deliver_policy,
        max_deliver: spec.max_deliver,
        max_ack_pending: spec.max_ack_pending,
        ..Default::default()
    };
    if let Some(filter_subject) = &spec.filter_subject {
        cfg.filter_subject = filter_subject.clone();
    }

    let mut consumer = stream
        .get_or_create_consumer(&spec.durable_name, cfg)
        .await
        .map_err(|e| SinexError::processing(format!("Failed to get or create consumer: {e}")))?;

    let info = consumer
        .info()
        .await
        .map_err(|e| SinexError::processing(format!("Failed to read consumer info: {e}")))?;

    validate_pull_consumer_config(spec, &info.config)?;
    Ok(consumer)
}

pub fn validate_pull_consumer_config(
    spec: &PullConsumerSpec,
    config: &jetstream::consumer::Config,
) -> NodeResult<()> {
    let mut mismatches = Vec::new();

    if config.durable_name.as_deref() != Some(spec.durable_name.as_str()) {
        mismatches.push(format!(
            "durable_name expected {}, got {:?}",
            spec.durable_name, config.durable_name
        ));
    }

    let expected_filter = spec.filter_subject.as_deref().unwrap_or("");
    if config.filter_subject != expected_filter {
        mismatches.push(format!(
            "filter_subject expected {}, got {}",
            expected_filter, config.filter_subject
        ));
    }

    if config.ack_policy != jetstream::consumer::AckPolicy::Explicit {
        mismatches.push(format!(
            "ack_policy expected Explicit, got {:?}",
            config.ack_policy
        ));
    }

    if config.ack_wait != spec.ack_wait {
        mismatches.push(format!(
            "ack_wait expected {:?}, got {:?}",
            spec.ack_wait, config.ack_wait
        ));
    }

    if config.max_ack_pending != spec.max_ack_pending {
        mismatches.push(format!(
            "max_ack_pending expected {}, got {}",
            spec.max_ack_pending, config.max_ack_pending
        ));
    }

    if config.deliver_policy != spec.deliver_policy {
        mismatches.push(format!(
            "deliver_policy expected {:?}, got {:?}",
            spec.deliver_policy, config.deliver_policy
        ));
    }

    if config.deliver_subject.is_some() {
        mismatches.push("deliver_subject expected None for pull consumer".to_string());
    }

    if mismatches.is_empty() {
        return Ok(());
    }

    Err(SinexError::processing(format!(
        "Consumer config mismatch for {} ({}): {}",
        spec.stream_name,
        spec.durable_name,
        mismatches.join(", ")
    )))
}

pub async fn pull_batch(
    consumer: &PullConsumerHandle,
    max_messages: usize,
    expires: Duration,
) -> NodeResult<Vec<jetstream::Message>> {
    let mut stream = consumer
        .batch()
        .max_messages(max_messages.max(1))
        .expires(expires)
        .messages()
        .await
        .map_err(|e| SinexError::processing(format!("Failed to fetch messages: {e}")))?;

    let mut batch = Vec::new();
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(message) => batch.push(message),
            Err(err) => {
                warn!(error = %err, "Error receiving JetStream message in pull_batch");
            }
        }
    }

    Ok(batch)
}

pub async fn consume_pull_loop<F, Fut>(
    consumer: &PullConsumerHandle,
    max_messages: usize,
    expires: Duration,
    mut on_batch: F,
) -> NodeResult<()>
where
    F: FnMut(Vec<jetstream::Message>) -> Fut,
    Fut: Future<Output = NodeResult<()>>,
{
    loop {
        let batch = pull_batch(consumer, max_messages, expires).await?;
        if batch.is_empty() {
            continue;
        }
        on_batch(batch).await?;
    }
}

#[derive(Debug, Clone)]
pub struct ShadowConsumerSpec {
    pub stream_name: String,
    pub consumer_name: String,
    pub subject_filter: String,
    pub from_sequence: Option<u64>,
    pub from_beginning: bool,
    pub create_timeout: Duration,
    pub ack_wait: Duration,
    pub max_deliver: i64,
}

impl ShadowConsumerSpec {
    #[must_use]
    pub fn new(
        stream_name: impl Into<String>,
        consumer_name: impl Into<String>,
        subject_filter: impl Into<String>,
    ) -> Self {
        Self {
            stream_name: stream_name.into(),
            consumer_name: consumer_name.into(),
            subject_filter: subject_filter.into(),
            from_sequence: None,
            from_beginning: false,
            create_timeout: Duration::from_secs(10),
            ack_wait: Duration::from_secs(30),
            max_deliver: 3,
        }
    }
}

pub async fn create_shadow_consumer(
    js: &jetstream::Context,
    spec: &ShadowConsumerSpec,
) -> NodeResult<jetstream::consumer::Info> {
    let stream = js.get_stream(&spec.stream_name).await.map_err(|e| {
        SinexError::processing(format!("Failed to get stream {}: {e}", spec.stream_name))
    })?;

    let deliver_policy = match spec.from_sequence {
        Some(seq) => jetstream::consumer::DeliverPolicy::ByStartSequence {
            start_sequence: seq,
        },
        None => {
            if spec.from_beginning {
                jetstream::consumer::DeliverPolicy::All
            } else {
                jetstream::consumer::DeliverPolicy::New
            }
        }
    };

    let create_future = stream.create_consumer(PullConfig {
        name: Some(spec.consumer_name.clone()),
        durable_name: Some(spec.consumer_name.clone()),
        filter_subject: spec.subject_filter.clone(),
        ack_policy: jetstream::consumer::AckPolicy::Explicit,
        deliver_policy,
        max_deliver: spec.max_deliver,
        ack_wait: spec.ack_wait,
        ..Default::default()
    });

    let mut consumer = tokio::time::timeout(spec.create_timeout, create_future)
        .await
        .map_err(|_| {
            SinexError::processing(format!(
                "Consumer creation timed out after {:?}",
                spec.create_timeout
            ))
        })?
        .map_err(|e| SinexError::processing(format!("Failed to create consumer: {e}")))?;

    consumer
        .info()
        .await
        .map(|info| info.clone())
        .map_err(|e| SinexError::processing(format!("Failed to fetch consumer info: {e}")))
}

pub async fn list_consumers(
    js: &jetstream::Context,
    stream_name: &str,
) -> NodeResult<Vec<jetstream::consumer::Info>> {
    let stream = js
        .get_stream(stream_name)
        .await
        .map_err(|e| SinexError::processing(format!("Failed to get stream {stream_name}: {e}")))?;

    let mut consumers = stream.consumers();
    let mut out = Vec::new();
    while let Some(result) = consumers.next().await {
        match result {
            Ok(info) => out.push(info),
            Err(err) => warn!(error = %err, "Failed to list JetStream consumer"),
        }
    }

    Ok(out)
}

pub async fn delete_consumer(
    js: &jetstream::Context,
    stream_name: &str,
    consumer_name: &str,
) -> NodeResult<()> {
    let stream = js
        .get_stream(stream_name)
        .await
        .map_err(|e| SinexError::processing(format!("Failed to get stream {stream_name}: {e}")))?;

    stream.delete_consumer(consumer_name).await.map_err(|e| {
        SinexError::processing(format!("Failed to delete consumer {consumer_name}: {e}"))
    })?;

    Ok(())
}

#[derive(Debug, Clone)]
pub struct ReplayPumpConfig {
    pub batch_size: i64,
    pub publish_ack_timeout: Duration,
}

impl Default for ReplayPumpConfig {
    fn default() -> Self {
        Self {
            batch_size: 500,
            publish_ack_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ReplayPumpProgress {
    pub processed_events: u64,
    pub last_event_id: Option<Ulid>,
    pub batch_number: u32,
}

#[derive(Debug, Clone)]
pub struct ReplayPublishEnvelope {
    pub subject: String,
    pub headers: async_nats::HeaderMap,
    pub payload_bytes: Vec<u8>,
    pub event_id: Ulid,
}

pub fn build_replay_publish_envelope(
    env: &SinexEnvironment,
    operation_id: Ulid,
    event: &sinex_primitives::events::Event,
    replay_timestamp: Timestamp,
) -> NodeResult<ReplayPublishEnvelope> {
    let event_id = event.id.map_or_else(Ulid::new, |id| *id.as_ulid());

    let subject = env.nats_subject(&format!(
        "events.raw.{}.{}",
        event.source.as_str().replace('.', "_"),
        event.event_type.as_str().replace('.', "_")
    ));

    let payload = serde_json::json!({
        "id": event_id.to_string(),
        "source": event.source.as_str(),
        "event_type": event.event_type.as_str(),
        "ts_orig": event.ts_orig.map(|t| t.format_rfc3339()),
        "host": event.host.as_str(),
        "payload": event.payload,
        "node_version": event.node_version,
        "payload_schema_id": event.payload_schema_id.map(|id| id.to_string()),
        "associated_blob_ids": event.associated_blob_ids.as_ref().map(|ids| ids.iter().map(std::string::ToString::to_string).collect::<Vec<_>>()),
        "replay_operation_id": operation_id.to_string(),
        "replay_timestamp": replay_timestamp.format_rfc3339(),
    });

    let payload_bytes = serde_json::to_vec(&payload).map_err(|e| {
        SinexError::serialization(format!("Failed to serialize replay payload: {e}"))
    })?;

    let mut headers = async_nats::HeaderMap::new();
    headers.insert(
        "Nats-Msg-Id",
        format!("replay-{operation_id}-{event_id}").as_str(),
    );
    headers.insert("X-Replay-Operation", operation_id.to_string().as_str());
    headers.insert("X-Original-Event-Id", event_id.to_string().as_str());

    Ok(ReplayPublishEnvelope {
        subject,
        headers,
        payload_bytes,
        event_id,
    })
}

pub async fn publish_replay_event(
    js: &jetstream::Context,
    env: &SinexEnvironment,
    operation_id: Ulid,
    event: &sinex_primitives::events::Event,
    ack_timeout: Duration,
) -> NodeResult<Ulid> {
    publish_replay_event_at(
        js,
        env,
        operation_id,
        event,
        sinex_primitives::temporal::now(),
        ack_timeout,
    )
    .await
}

pub async fn publish_replay_event_at(
    js: &jetstream::Context,
    env: &SinexEnvironment,
    operation_id: Ulid,
    event: &sinex_primitives::events::Event,
    replay_timestamp: Timestamp,
    ack_timeout: Duration,
) -> NodeResult<Ulid> {
    let envelope = build_replay_publish_envelope(env, operation_id, event, replay_timestamp)?;
    let ack_future = js
        .publish_with_headers(
            envelope.subject,
            envelope.headers,
            envelope.payload_bytes.into(),
        )
        .await
        .map_err(|e| SinexError::network(format!("Failed to publish replay event: {e}")))?;

    tokio::time::timeout(ack_timeout, ack_future)
        .await
        .map_err(|_| {
            SinexError::network(format!(
                "Timed out waiting for replay publish ack after {:?}",
                ack_timeout
            ))
        })?
        .map_err(|e| SinexError::network(format!("Replay publish ack failed: {e}")))?;

    Ok(envelope.event_id)
}

#[cfg(feature = "db")]
pub async fn replay_source_window<F, Fut>(
    pool: &PgPool,
    js: &jetstream::Context,
    env: &SinexEnvironment,
    operation_id: Ulid,
    node_id: &str,
    window: (Timestamp, Timestamp),
    config: &ReplayPumpConfig,
    mut on_progress: F,
) -> NodeResult<ReplayPumpProgress>
where
    F: FnMut(ReplayPumpProgress) -> Fut,
    Fut: Future<Output = NodeResult<()>>,
{
    let event_source = EventSource::new(node_id)?;
    let mut offset: i64 = 0;
    let mut progress = ReplayPumpProgress::default();

    loop {
        let events = pool
            .events()
            .get_by_source_and_time_range(
                &event_source,
                window.0,
                window.1,
                Pagination::new(Some(config.batch_size), Some(offset)),
            )
            .await
            .map_err(|e| SinexError::database(format!("Failed to query replay events: {e}")))?;

        if events.is_empty() {
            debug!(operation_id = %operation_id, offset, "Replay pump reached end of source window");
            break;
        }

        progress.batch_number = progress.batch_number.saturating_add(1);
        for event in events {
            let event_id =
                publish_replay_event(js, env, operation_id, &event, config.publish_ack_timeout)
                    .await?;
            progress.processed_events = progress.processed_events.saturating_add(1);
            progress.last_event_id = Some(event_id);
        }

        on_progress(progress.clone()).await?;
        offset += config.batch_size;
    }

    Ok(progress)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sinex_primitives::events::Provenance;
    use xtask::sandbox::sinex_test;

    fn sample_event() -> sinex_primitives::events::Event {
        sinex_primitives::events::Event::new_json(
            "terminal-history",
            "command.imported",
            json!({ "command": "echo hi" }),
            Provenance::from_material(Ulid::new(), 0, None, None),
        )
    }

    #[sinex_test]
    async fn replay_publish_envelope_is_deterministic_for_fixed_timestamp() -> TestResult<()> {
        let env = SinexEnvironment::new("dev")?;
        let operation_id = Ulid::new();
        let event = sample_event();
        let ts = Timestamp::parse_rfc3339("2026-01-01T00:00:00Z")?;
        let op_id = operation_id.to_string();

        let envelope = build_replay_publish_envelope(&env, operation_id, &event, ts)?;
        let payload: serde_json::Value = serde_json::from_slice(&envelope.payload_bytes)?;

        assert_eq!(
            payload
                .get("replay_timestamp")
                .and_then(serde_json::Value::as_str),
            Some("2026-01-01T00:00:00Z")
        );
        assert_eq!(
            payload
                .get("replay_operation_id")
                .and_then(serde_json::Value::as_str),
            Some(op_id.as_str())
        );
        Ok(())
    }

    #[sinex_test]
    async fn validate_pull_consumer_config_reports_mismatch() -> TestResult<()> {
        let spec = PullConsumerSpec::new("events", "durable-a");
        let config = jetstream::consumer::Config {
            durable_name: Some("durable-b".to_string()),
            filter_subject: "events.raw.foo".to_string(),
            ack_policy: jetstream::consumer::AckPolicy::None,
            ack_wait: Duration::from_secs(5),
            max_ack_pending: 10,
            deliver_policy: jetstream::consumer::DeliverPolicy::New,
            deliver_subject: Some("out.subject".to_string()),
            ..Default::default()
        };

        let err = validate_pull_consumer_config(&spec, &config).expect_err("expected mismatch");
        let text = err.to_string();
        assert!(text.contains("durable_name expected"));
        assert!(text.contains("ack_policy expected Explicit"));
        Ok(())
    }
}

use crate::runtime::{RuntimeResult, SinexError};
use async_nats::jetstream;
use async_nats::jetstream::consumer::Consumer;
use async_nats::jetstream::consumer::pull::Config as PullConfig;
use async_nats::jetstream::context::ConsumerInfoErrorKind;
use futures::StreamExt;
use std::time::Duration;
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct PullConsumerSpec {
    pub stream_name: String,
    pub durable_name: String,
    pub filter_subject: Option<String>,
    pub deliver_policy: jetstream::consumer::DeliverPolicy,
    pub ack_wait: Duration,
    pub max_ack_pending: i64,
    pub max_deliver: i64,
    pub reject_initial_replay: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullConsumerStartupSnapshot {
    pub stream_name: String,
    pub durable_name: String,
    pub consumer_existed: bool,
    pub deliver_policy: jetstream::consumer::DeliverPolicy,
    pub stream_messages: u64,
    pub stream_bytes: u64,
    pub stream_first_sequence: u64,
    pub stream_last_sequence: u64,
    pub consumer_pending: u64,
    pub consumer_ack_pending: usize,
    pub consumer_redelivered: usize,
    pub consumer_max_ack_pending: i64,
    pub consumer_max_deliver: i64,
}

impl PullConsumerStartupSnapshot {
    #[must_use]
    pub fn has_initial_replay_risk(&self) -> bool {
        !self.consumer_existed
            && matches!(self.deliver_policy, jetstream::consumer::DeliverPolicy::All)
            && self.stream_messages > 0
    }
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
            reject_initial_replay: false,
        }
    }
}

pub async fn ensure_pull_consumer(
    js: &jetstream::Context,
    spec: &PullConsumerSpec,
) -> RuntimeResult<PullConsumerHandle> {
    let mut stream = js.get_stream(&spec.stream_name).await.map_err(|e| {
        SinexError::processing(format!("Failed to get stream {}: {e}", spec.stream_name))
    })?;
    let stream_info = stream.info().await.cloned().map_err(|e| {
        SinexError::processing(format!(
            "Failed to read stream {} info: {e}",
            spec.stream_name
        ))
    })?;

    let consumer_existed = match stream.consumer_info(&spec.durable_name).await {
        Ok(_) => true,
        Err(err) if err.kind() == ConsumerInfoErrorKind::NotFound => false,
        Err(err) => {
            return Err(SinexError::processing(format!(
                "Failed to check consumer {} on stream {}: {err}",
                spec.durable_name, spec.stream_name
            )));
        }
    };

    if !consumer_existed
        && spec.reject_initial_replay
        && matches!(spec.deliver_policy, jetstream::consumer::DeliverPolicy::All)
        && stream_info.state.messages > 0
    {
        return Err(SinexError::processing(format!(
            "Refusing to create missing durable consumer {} on stream {} with DeliverPolicy::All while stream contains {} message(s), {} byte(s), seq {}..{}. Set an explicit replay policy before allowing this cold-start replay.",
            spec.durable_name,
            spec.stream_name,
            stream_info.state.messages,
            stream_info.state.bytes,
            stream_info.state.first_sequence,
            stream_info.state.last_sequence
        )));
    }

    let mut consumer = stream
        .get_or_create_consumer(&spec.durable_name, pull_consumer_config(spec))
        .await
        .map_err(|e| SinexError::processing(format!("Failed to get or create consumer: {e}")))?;

    let mut info = consumer
        .info()
        .await
        .cloned()
        .map_err(|e| SinexError::processing(format!("Failed to read consumer info: {e}")))?;

    let mismatches = pull_consumer_config_mismatches(spec, &info.config);
    if consumer_existed && can_reconcile_pull_consumer_config(&mismatches) {
        warn!(
            stream = %spec.stream_name,
            durable = %spec.durable_name,
            mismatches = %render_pull_consumer_config_mismatches(&mismatches),
            "Reconciling existing JetStream pull consumer config"
        );
        consumer = stream
            .update_consumer(pull_consumer_config(spec))
            .await
            .map_err(|e| {
                SinexError::processing(format!(
                    "Failed to update consumer {} on stream {}: {e}",
                    spec.durable_name, spec.stream_name
                ))
            })?;
        info =
            consumer.info().await.cloned().map_err(|e| {
                SinexError::processing(format!("Failed to read consumer info: {e}"))
            })?;
    }

    validate_pull_consumer_config(spec, &info.config)?;
    let snapshot = PullConsumerStartupSnapshot {
        stream_name: spec.stream_name.clone(),
        durable_name: spec.durable_name.clone(),
        consumer_existed,
        deliver_policy: spec.deliver_policy,
        stream_messages: stream_info.state.messages,
        stream_bytes: stream_info.state.bytes,
        stream_first_sequence: stream_info.state.first_sequence,
        stream_last_sequence: stream_info.state.last_sequence,
        consumer_pending: info.num_pending,
        consumer_ack_pending: info.num_ack_pending,
        consumer_redelivered: info.num_redelivered,
        consumer_max_ack_pending: info.config.max_ack_pending,
        consumer_max_deliver: info.config.max_deliver,
    };
    info!(
        stream = %snapshot.stream_name,
        durable = %snapshot.durable_name,
        consumer_existed = snapshot.consumer_existed,
        initial_replay_risk = snapshot.has_initial_replay_risk(),
        deliver_policy = ?snapshot.deliver_policy,
        filter_subject = spec.filter_subject.as_deref().unwrap_or(""),
        stream_messages = snapshot.stream_messages,
        stream_bytes = snapshot.stream_bytes,
        stream_first_sequence = snapshot.stream_first_sequence,
        stream_last_sequence = snapshot.stream_last_sequence,
        consumer_pending = snapshot.consumer_pending,
        consumer_ack_pending = snapshot.consumer_ack_pending,
        consumer_redelivered = snapshot.consumer_redelivered,
        consumer_max_ack_pending = snapshot.consumer_max_ack_pending,
        consumer_max_deliver = snapshot.consumer_max_deliver,
        "JetStream pull consumer bound"
    );
    Ok(consumer)
}

fn pull_consumer_config(spec: &PullConsumerSpec) -> PullConfig {
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
    cfg
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PullConsumerConfigMismatchKind {
    DurableName,
    FilterSubject,
    AckPolicy,
    AckWait,
    MaxAckPending,
    MaxDeliver,
    DeliverPolicy,
    DeliverSubject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PullConsumerConfigMismatch {
    kind: PullConsumerConfigMismatchKind,
    message: String,
}

impl PullConsumerConfigMismatch {
    fn new(kind: PullConsumerConfigMismatchKind, message: String) -> Self {
        Self { kind, message }
    }
}

fn can_reconcile_pull_consumer_config(mismatches: &[PullConsumerConfigMismatch]) -> bool {
    !mismatches.is_empty()
        && mismatches.iter().all(|mismatch| {
            matches!(
                mismatch.kind,
                PullConsumerConfigMismatchKind::MaxAckPending
                    | PullConsumerConfigMismatchKind::MaxDeliver
            )
        })
}

fn render_pull_consumer_config_mismatches(mismatches: &[PullConsumerConfigMismatch]) -> String {
    mismatches
        .iter()
        .map(|mismatch| mismatch.message.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn pull_consumer_config_mismatches(
    spec: &PullConsumerSpec,
    config: &jetstream::consumer::Config,
) -> Vec<PullConsumerConfigMismatch> {
    let mut mismatches = Vec::new();

    if config.durable_name.as_deref() != Some(spec.durable_name.as_str()) {
        mismatches.push(PullConsumerConfigMismatch::new(
            PullConsumerConfigMismatchKind::DurableName,
            format!(
                "durable_name expected {}, got {:?}",
                spec.durable_name, config.durable_name
            ),
        ));
    }

    let expected_filter = spec.filter_subject.as_deref().unwrap_or("");
    if config.filter_subject != expected_filter {
        mismatches.push(PullConsumerConfigMismatch::new(
            PullConsumerConfigMismatchKind::FilterSubject,
            format!(
                "filter_subject expected {}, got {}",
                expected_filter, config.filter_subject
            ),
        ));
    }

    if config.ack_policy != jetstream::consumer::AckPolicy::Explicit {
        mismatches.push(PullConsumerConfigMismatch::new(
            PullConsumerConfigMismatchKind::AckPolicy,
            format!("ack_policy expected Explicit, got {:?}", config.ack_policy),
        ));
    }

    if config.ack_wait != spec.ack_wait {
        mismatches.push(PullConsumerConfigMismatch::new(
            PullConsumerConfigMismatchKind::AckWait,
            format!(
                "ack_wait expected {:?}, got {:?}",
                spec.ack_wait, config.ack_wait
            ),
        ));
    }

    if config.max_ack_pending != spec.max_ack_pending {
        mismatches.push(PullConsumerConfigMismatch::new(
            PullConsumerConfigMismatchKind::MaxAckPending,
            format!(
                "max_ack_pending expected {}, got {}",
                spec.max_ack_pending, config.max_ack_pending
            ),
        ));
    }

    if config.max_deliver != spec.max_deliver {
        mismatches.push(PullConsumerConfigMismatch::new(
            PullConsumerConfigMismatchKind::MaxDeliver,
            format!(
                "max_deliver expected {}, got {}",
                spec.max_deliver, config.max_deliver
            ),
        ));
    }

    if config.deliver_policy != spec.deliver_policy {
        mismatches.push(PullConsumerConfigMismatch::new(
            PullConsumerConfigMismatchKind::DeliverPolicy,
            format!(
                "deliver_policy expected {:?}, got {:?}",
                spec.deliver_policy, config.deliver_policy
            ),
        ));
    }

    if config.deliver_subject.is_some() {
        mismatches.push(PullConsumerConfigMismatch::new(
            PullConsumerConfigMismatchKind::DeliverSubject,
            "deliver_subject expected None for pull consumer".to_string(),
        ));
    }

    mismatches
}

pub fn validate_pull_consumer_config(
    spec: &PullConsumerSpec,
    config: &jetstream::consumer::Config,
) -> RuntimeResult<()> {
    let mismatches = pull_consumer_config_mismatches(spec, config);

    if mismatches.is_empty() {
        return Ok(());
    }

    Err(SinexError::processing(format!(
        "Consumer config mismatch for {} ({}): {}",
        spec.stream_name,
        spec.durable_name,
        render_pull_consumer_config_mismatches(&mismatches)
    )))
}

pub async fn pull_batch(
    consumer: &PullConsumerHandle,
    max_messages: usize,
    expires: Duration,
) -> RuntimeResult<Vec<jetstream::Message>> {
    pull_batch_bounded(consumer, max_messages, 0, expires).await
}

/// Like [`pull_batch`], but additionally caps the cumulative payload bytes the
/// server may deliver in a single fetch. `max_bytes == 0` means unbounded (the
/// message count is the only limit, identical to [`pull_batch`]).
///
/// The byte cap bounds a consumer's in-flight decode high-watermark
/// *independent of per-message size*. This matters on the event-engine persist
/// path: each fetched message is itself an event batch whose payloads can reach
/// the 10 MiB NATS limit, and every message is materialized into a fully-owned
/// `serde_json::Value` DOM (~5-10x the wire bytes) before persistence. Without a
/// byte budget a single `max_messages`-sized fetch (default 100) can expand to
/// multiple GiB of transient heap during a backlog drain — heap-profiled as the
/// dominant source of sinexd's drain-time RSS. Capping by bytes keeps the
/// high-watermark flat regardless of backlog depth.
pub async fn pull_batch_bounded(
    consumer: &PullConsumerHandle,
    max_messages: usize,
    max_bytes: usize,
    expires: Duration,
) -> RuntimeResult<Vec<jetstream::Message>> {
    let mut builder = consumer.batch().max_messages(max_messages.max(1));
    if max_bytes > 0 {
        builder = builder.max_bytes(max_bytes);
    }
    let mut stream = builder
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
) -> RuntimeResult<()>
where
    F: FnMut(Vec<jetstream::Message>) -> Fut,
    Fut: Future<Output = RuntimeResult<()>>,
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
) -> RuntimeResult<jetstream::consumer::Info> {
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
        .cloned()
        .map_err(|e| SinexError::processing(format!("Failed to fetch consumer info: {e}")))
}

pub async fn list_consumers(
    js: &jetstream::Context,
    stream_name: &str,
) -> RuntimeResult<Vec<jetstream::consumer::Info>> {
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
) -> RuntimeResult<()> {
    let stream = js
        .get_stream(stream_name)
        .await
        .map_err(|e| SinexError::processing(format!("Failed to get stream {stream_name}: {e}")))?;

    stream.delete_consumer(consumer_name).await.map_err(|e| {
        SinexError::processing(format!("Failed to delete consumer {consumer_name}: {e}"))
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matching_consumer_config(spec: &PullConsumerSpec) -> jetstream::consumer::Config {
        jetstream::consumer::Config {
            durable_name: Some(spec.durable_name.clone()),
            filter_subject: spec.filter_subject.clone().unwrap_or_default(),
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            ack_wait: spec.ack_wait,
            deliver_policy: spec.deliver_policy,
            max_deliver: spec.max_deliver,
            max_ack_pending: spec.max_ack_pending,
            ..Default::default()
        }
    }

    #[test]
    fn ack_window_drift_is_reconcilable() {
        let mut spec = PullConsumerSpec::new("stream", "consumer");
        spec.max_ack_pending = 32;
        spec.max_deliver = 10;

        let mut config = matching_consumer_config(&spec);
        config.max_ack_pending = 1_000;
        config.max_deliver = 20;

        let mismatches = pull_consumer_config_mismatches(&spec, &config);

        assert!(can_reconcile_pull_consumer_config(&mismatches));
    }

    #[test]
    fn semantic_drift_is_not_reconcilable() {
        let mut spec = PullConsumerSpec::new("stream", "consumer");
        spec.filter_subject = Some("expected.>".to_string());

        let mut config = matching_consumer_config(&spec);
        config.filter_subject = "other.>".to_string();

        let mismatches = pull_consumer_config_mismatches(&spec, &config);

        assert!(!can_reconcile_pull_consumer_config(&mismatches));
    }
}

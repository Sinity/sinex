use crate::{NodeResult, SinexError};
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
) -> NodeResult<PullConsumerHandle> {
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
        .cloned()
        .map_err(|e| SinexError::processing(format!("Failed to read consumer info: {e}")))?;

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
        .cloned()
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

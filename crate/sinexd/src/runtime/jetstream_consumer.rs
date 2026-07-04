//! Confirmed-event `JetStream` consumer for automata.
//!
//! Each automaton opens ONE durable consumer on the confirmed-events stream
//! (`{base}_CONFIRMED`, subjects
//! `events.confirmed.<provenance>.<source>.<type>`). The event
//! engine publishes the full post-redaction `Event<JsonValue>` there, so this
//! consumer deserializes the authoritative event and dispatches it directly to
//! the automaton — no raw-events firehose, no provisional buffer, no Postgres
//! refetch, no commit/confirmation visibility race (the #2187 / #2202 redesign,
//! "Option C").
//!
//! Type/provenance-specific automata let the NATS server filter the stream
//! before delivery. Failure isolation is preserved: each automaton runs an
//! independent durable consumer, so a dead automaton never blocks others.

use crate::runtime::confirmation_handler::ConfirmedEventHandler;
use crate::runtime::automaton::traits::InputProvenanceFilter;
use crate::runtime::stream::{
    PullConsumerSpec, delete_consumer, ensure_pull_consumer, list_consumers, pull_batch_bounded,
};
use crate::runtime::{RuntimeResult, SinexError};
use async_nats::jetstream;
use sinex_primitives::JsonValue;
use sinex_primitives::environment::SinexEnvironment;
use sinex_primitives::error::SinexErrorKind;
use sinex_primitives::events::Event;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Cumulative payload-byte cap per confirmed-event fetch.
///
/// A count-only pull (`batch_size`) can otherwise stage up to `batch_size` × the
/// 10 MiB NATS payload limit of raw `Bytes` before decode. Full `Event` payloads
/// are larger than raw envelopes, so this keeps each fetch's footprint flat
/// regardless of message size. Ref #2187.
const CONFIRMED_EVENT_FETCH_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Configuration for the confirmed-event consumer.
#[derive(Debug, Clone)]
pub struct JetStreamEventConsumerConfig {
    /// Batch size for pulling confirmed events.
    pub batch_size: usize,
    /// Maximum number of unacknowledged messages allowed by the consumer.
    pub max_ack_pending: i64,
    /// Consumer name (durable).
    pub consumer_name: String,
    /// Where a newly-created durable consumer should begin delivering messages.
    pub deliver_policy: jetstream::consumer::DeliverPolicy,
    /// Provenance class to filter the confirmed stream by.
    pub provenance_filter: InputProvenanceFilter,
    /// Concrete event types to filter the confirmed stream by, if any.
    /// Combined with `provenance_filter` for server-side filtering. Empty
    /// means wildcard.
    pub event_type_filters: Vec<String>,
}

impl Default for JetStreamEventConsumerConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            max_ack_pending: 1000,
            consumer_name: "automaton-consumer".to_string(),
            deliver_policy: jetstream::consumer::DeliverPolicy::All,
            provenance_filter: InputProvenanceFilter::Any,
            event_type_filters: Vec::new(),
        }
    }
}

/// Confirmed-event consumer for automata.
pub struct JetStreamEventConsumer {
    nats_client: async_nats::Client,
    env: SinexEnvironment,
    config: JetStreamEventConsumerConfig,
    confirmed_handler: Arc<dyn ConfirmedEventHandler>,
    running: Arc<RwLock<bool>>,
    namespace: Option<String>,
}

impl JetStreamEventConsumer {
    #[allow(
        clippy::needless_pass_by_value,
        reason = "Internal helper: impl trait simplifies use"
    )]
    fn message_settlement_error(
        operation: &'static str,
        msg: &jetstream::Message,
        event_id: Option<impl std::fmt::Display>,
        error: impl std::fmt::Display,
    ) -> SinexError {
        crate::runtime::error_helpers::nats_settlement_error(
            operation,
            msg.subject.as_str(),
            event_id.as_ref().map(ToString::to_string).as_deref(),
            error,
        )
    }

    /// Create a new confirmed-event consumer.
    pub fn new(
        nats_client: async_nats::Client,
        env: SinexEnvironment,
        config: JetStreamEventConsumerConfig,
        confirmed_handler: Arc<dyn ConfirmedEventHandler>,
    ) -> Self {
        Self::new_with_namespace(nats_client, env, config, confirmed_handler, None)
    }

    /// Create a new confirmed-event consumer with an optional namespace.
    pub fn new_with_namespace(
        nats_client: async_nats::Client,
        env: SinexEnvironment,
        config: JetStreamEventConsumerConfig,
        confirmed_handler: Arc<dyn ConfirmedEventHandler>,
        namespace: Option<String>,
    ) -> Self {
        Self {
            nats_client,
            env,
            config,
            confirmed_handler,
            running: Arc::new(RwLock::new(false)),
            namespace,
        }
    }

    /// Start consuming confirmed events.
    pub async fn run(&self) -> RuntimeResult<()> {
        {
            let mut running = self.running.write().await;
            if *running {
                return Err(SinexError::lifecycle(
                    "Consumer already running".to_string(),
                ));
            }
            *running = true;
        }

        let result = self.run_inner().await;
        *self.running.write().await = false;
        result
    }

    async fn run_inner(&self) -> RuntimeResult<()> {
        info!(
            "Starting confirmed-event consumer: {}",
            self.config.consumer_name
        );

        let js = jetstream::new(self.nats_client.clone());

        // Confirmed-events stream is `{base}_CONFIRMED` (see event_engine
        // topology / `confirmed_events_stream`).
        let raw_stream = self
            .env
            .nats_stream_name_with_namespace(self.namespace.as_deref(), "SINEX_RAW_EVENTS");
        let confirmed_stream = format!("{raw_stream}_CONFIRMED");

        let confirmed_subject = self.confirmed_filter_subject();

        let consumer = self
            .create_or_get_consumer(&js, &confirmed_stream, &confirmed_subject)
            .await?;
        self.retire_legacy_filter_consumers(&js, &confirmed_stream)
            .await?;

        Self::consume_confirmed_events(
            consumer,
            self.config.batch_size,
            self.confirmed_handler.clone(),
            self.running.clone(),
        )
        .await
    }

    fn confirmed_filter_subject(&self) -> String {
        self.confirmed_filter_subjects()
            .into_iter()
            .next()
            .unwrap_or_else(|| self.env.nats_subject("events.confirmed.>"))
    }

    fn confirmed_filter_subjects(&self) -> Vec<String> {
        let subjects = confirmed_filter_subjects_for(
            &self.env,
            self.namespace.as_deref(),
            self.config.provenance_filter,
            &self.config.event_type_filters,
        );

        info!(
            consumer = %self.config.consumer_name,
            event_types = ?self.config.event_type_filters,
            provenance = ?self.config.provenance_filter,
            filter_subjects = ?subjects,
            "Confirmed-event consumer configured"
        );

        subjects
    }

    /// Stop the consumer.
    pub async fn stop(&self) {
        *self.running.write().await = false;
    }

    async fn create_or_get_consumer(
        &self,
        js: &jetstream::Context,
        stream_name: &str,
        filter: &str,
    ) -> RuntimeResult<jetstream::consumer::Consumer<jetstream::consumer::pull::Config>> {
        let mut spec =
            PullConsumerSpec::new(stream_name.to_string(), self.config.consumer_name.clone());
        let filters = self.confirmed_filter_subjects();
        if filters.len() <= 1 {
            spec.filter_subject = Some(filter.to_string());
        } else {
            spec.filter_subjects = filters;
        }
        spec.max_ack_pending = self.config.max_ack_pending;
        spec.max_deliver = 10;
        spec.ack_wait = Duration::from_secs(30);
        spec.deliver_policy = self.config.deliver_policy;
        ensure_pull_consumer(js, &spec).await
    }

    async fn retire_legacy_filter_consumers(
        &self,
        js: &jetstream::Context,
        stream_name: &str,
    ) -> RuntimeResult<()> {
        let existing = list_consumers(js, stream_name).await?;
        for info in existing {
            match confirmed_consumer_retirement_action(&self.config.consumer_name, &info.name) {
                ConfirmedConsumerRetirementAction::KeepCurrent
                | ConfirmedConsumerRetirementAction::IgnoreUnrelated => {}
                ConfirmedConsumerRetirementAction::DeleteStaleSameService => {
                    warn!(
                    stream = %stream_name,
                    consumer = %self.config.consumer_name,
                    stale_consumer = %info.name,
                    pending = info.num_pending,
                    ack_pending = info.num_ack_pending,
                    redelivered = info.num_redelivered,
                    "Deleting stale confirmed-event durable for this automaton; DB catch-up covers retained history"
                );
                    delete_consumer(js, stream_name, &info.name).await?;
                }
            }
        }

        Ok(())
    }

    async fn consume_confirmed_events(
        consumer: jetstream::consumer::Consumer<jetstream::consumer::pull::Config>,
        batch_size: usize,
        confirmed_handler: Arc<dyn ConfirmedEventHandler>,
        running: Arc<RwLock<bool>>,
    ) -> RuntimeResult<()> {
        while *running.read().await {
            let messages = pull_batch_bounded(
                &consumer,
                batch_size,
                CONFIRMED_EVENT_FETCH_MAX_BYTES,
                Duration::from_secs(1),
            )
            .await?;
            for msg in messages {
                // Break promptly on stop() instead of finishing the whole batch,
                // so graceful shutdown completes well under the stop timeout.
                if !*running.read().await {
                    break;
                }
                if !Self::handle_confirmed_message(msg, &*confirmed_handler).await? {
                    // Handler reported shutdown (channel closed). Leave the
                    // message unsettled so it is redelivered to the next run,
                    // and exit the loop cleanly.
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    /// Handle a single confirmed-event message: deserialize the full event,
    /// dispatch it, and ack/nak. Returns `false` if processing should stop
    /// (handler channel closed during shutdown), `true` to continue.
    async fn handle_confirmed_message(
        msg: jetstream::Message,
        confirmed_handler: &dyn ConfirmedEventHandler,
    ) -> RuntimeResult<bool> {
        let event: Event<JsonValue> = match serde_json::from_slice(&msg.payload) {
            Ok(event) => event,
            Err(e) => {
                // A confirmed-event message that does not deserialize as an
                // `Event` is a genuine poison payload. Ack/drop it (tracked by
                // the metric) rather than NAK-looping forever.
                error!(
                    target: "sinex_metrics",
                    metric = "runtime.confirmed_event_parse_failures_total",
                    error = %e,
                    "Failed to parse confirmed event message"
                );
                msg.ack().await.map_err(|ack_err| {
                    Self::message_settlement_error(
                        "failed to ack bad confirmed event",
                        &msg,
                        None::<String>,
                        ack_err,
                    )
                })?;
                return Ok(true);
            }
        };

        let event_id = event.id;
        match confirmed_handler.handle_confirmed(&event).await {
            Ok(()) => {
                msg.ack().await.map_err(|error| {
                    Self::message_settlement_error(
                        "failed to ack confirmed event",
                        &msg,
                        event_id,
                        error,
                    )
                })?;
                Ok(true)
            }
            Err(e) if e.kind() == SinexErrorKind::Lifecycle => {
                // Channel closed = shutdown in progress. Do NOT ack — leave the
                // message for redelivery to the next run.
                debug!(?event_id, "Confirmed handler channel closed (shutdown)");
                Ok(false)
            }
            Err(e) => {
                error!(
                    target: "sinex_metrics",
                    metric = "runtime.confirmation_handler_failures_total",
                    ?event_id,
                    error = %e,
                    "Confirmed handler failed"
                );
                msg.ack_with(async_nats::jetstream::AckKind::Nak(Some(
                    Duration::from_secs(5),
                )))
                .await
                .map_err(|error| {
                    Self::message_settlement_error(
                        "failed to NAK confirmed handler failure",
                        &msg,
                        event_id,
                        error,
                    )
                })?;
                Ok(true)
            }
        }
    }
}

fn confirmed_filter_subject_for(
    env: &SinexEnvironment,
    namespace: Option<&str>,
    provenance_filter: InputProvenanceFilter,
    event_type_filter: Option<&str>,
) -> String {
    let provenance_token = match provenance_filter {
        InputProvenanceFilter::Any => "*",
        InputProvenanceFilter::MaterialOnly => "material",
        InputProvenanceFilter::SynthesizedOnly => "synthesized",
    };

    match event_type_filter {
        Some(event_type) => env.nats_subject_with_namespace(
            namespace,
            &format!(
                "events.confirmed.{provenance_token}.*.{}",
                SinexEnvironment::nats_subject_token(event_type)
            ),
        ),
        None if provenance_filter == InputProvenanceFilter::Any => {
            env.nats_subject_with_namespace(namespace, "events.confirmed.>")
        }
        None => env.nats_subject_with_namespace(
            namespace,
            &format!("events.confirmed.{provenance_token}.>"),
        ),
    }
}

fn confirmed_filter_subjects_for(
    env: &SinexEnvironment,
    namespace: Option<&str>,
    provenance_filter: InputProvenanceFilter,
    event_type_filters: &[String],
) -> Vec<String> {
    if event_type_filters.is_empty() {
        return vec![confirmed_filter_subject_for(
            env,
            namespace,
            provenance_filter,
            None,
        )];
    }

    event_type_filters
        .iter()
        .map(|event_type| {
            confirmed_filter_subject_for(env, namespace, provenance_filter, Some(event_type))
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfirmedConsumerRetirementAction {
    KeepCurrent,
    DeleteStaleSameService,
    IgnoreUnrelated,
}

fn confirmed_consumer_retirement_action(
    current_name: &str,
    candidate_name: &str,
) -> ConfirmedConsumerRetirementAction {
    if candidate_name == current_name {
        return ConfirmedConsumerRetirementAction::KeepCurrent;
    }

    let Some(current_root) = confirmed_consumer_service_root(current_name) else {
        return ConfirmedConsumerRetirementAction::IgnoreUnrelated;
    };
    let Some(candidate_root) = confirmed_consumer_service_root(candidate_name) else {
        return ConfirmedConsumerRetirementAction::IgnoreUnrelated;
    };

    if candidate_root == current_root {
        ConfirmedConsumerRetirementAction::DeleteStaleSameService
    } else {
        ConfirmedConsumerRetirementAction::IgnoreUnrelated
    }
}

fn confirmed_consumer_service_root(name: &str) -> Option<&str> {
    let marker_start = name.find("-confirmed-events")?;
    let marker_end = marker_start + "-confirmed-events".len();
    let suffix = &name[marker_end..];

    if suffix.is_empty()
        || suffix.starts_with("-filter-")
        || suffix.starts_with("-material")
        || suffix.starts_with("-synthesized")
    {
        Some(&name[..marker_end])
    } else {
        None
    }
}

#[cfg(test)]
#[path = "jetstream_consumer_test.rs"]
mod tests;

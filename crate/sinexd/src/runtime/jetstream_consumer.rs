//! `JetStream` event consumer for automata
//!
//! This module provides a consumer that subscribes to `JetStream` events
//! and handles provisional/confirmed event processing with proper buffering.

use crate::runtime::confirmation_handler::{
    ConfirmationBuffer, ConfirmedEventHandler, EventConfirmation, ProcessingModel,
    ProvisionalEvent, ProvisionalEventHandler,
};
use crate::runtime::stream::{PullConsumerSpec, ensure_pull_consumer, pull_batch_bounded};
use crate::runtime::{RuntimeResult, SinexError};
use async_nats::jetstream;
use sinex_primitives::error::SinexErrorKind;
use sinex_primitives::{
    domain::{EventSource, EventType},
    environment::SinexEnvironment,
    source_contracts::ResourceProfile,
    temporal::Timestamp,
};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::{RwLock, Semaphore};
use tracing::{debug, error, info, warn};

const RAW_EVENT_BUFFER_BACKPRESSURE_SLEEP: Duration = Duration::from_secs(1);

/// Cumulative payload-byte budget per raw-event fetch for an automaton consumer.
///
/// Every automaton (14 of them) runs its own raw-events consumer. Without a byte
/// cap, a single count-only fetch (`batch_size`, default 128) can stage up to
/// `batch_size` × the 10 MiB NATS payload limit ≈ 1 GiB of raw `Bytes` per
/// consumer before decode — the same drain-time heap blowup fixed on the
/// event-engine path. The confirmation-buffer pull gate bounds the *retained
/// decoded* set, not this per-fetch raw staging. 64 MiB keeps each fetch's raw
/// footprint flat regardless of message size. Ref #2187.
const RAW_EVENT_FETCH_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Permit granularity for the process-global raw-event in-flight budget (1 MiB).
const RAW_EVENT_INFLIGHT_PERMIT_BYTES: usize = 1024 * 1024;

/// Default *aggregate* raw-event in-flight byte budget shared across every
/// automaton's raw consumer in this process.
///
/// `RAW_EVENT_FETCH_MAX_BYTES` bounds a *single* consumer's fetch, but sinexd
/// hosts ~14 automata that each run their own raw consumer. On a backlog drain
/// they all fetch at once, so the per-consumer cap multiplies: 14 × 64 MiB of
/// raw `Bytes`, then ~2.5× more once decoded into owned JSON DOMs — measured as
/// a ~2.2 GiB anon-heap burst the instant sinexd starts, climbing past the
/// cgroup cap into an OOM kill (#2187). This process-global semaphore caps the
/// *combined* raw fetch+decode footprint regardless of how many consumers run,
/// so adding automata cannot reintroduce the blowup. 512 MiB of raw staging
/// (~1.3 GiB decoded peak) leaves comfortable headroom under the prod cap while
/// keeping several consumers draining concurrently.
const DEFAULT_RAW_EVENT_INFLIGHT_BUDGET_BYTES: usize = 512 * 1024 * 1024;

/// Clamp the configured aggregate budget so it can never drop below a single
/// fetch — otherwise `acquire_many` for one fetch would block forever (deadlock).
const fn clamp_raw_event_inflight_budget_bytes(requested: usize) -> usize {
    if requested < RAW_EVENT_FETCH_MAX_BYTES {
        RAW_EVENT_FETCH_MAX_BYTES
    } else {
        requested
    }
}

/// Convert a byte count to whole permits (at least one).
const fn raw_event_inflight_permits(bytes: usize) -> u32 {
    let permits = bytes / RAW_EVENT_INFLIGHT_PERMIT_BYTES;
    if permits == 0 { 1 } else { permits as u32 }
}

/// Configured aggregate budget in bytes (env-overridable, then clamped).
fn raw_event_inflight_budget_bytes() -> usize {
    let requested = std::env::var("SINEX_RAW_EVENT_INFLIGHT_BUDGET_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_RAW_EVENT_INFLIGHT_BUDGET_BYTES);
    clamp_raw_event_inflight_budget_bytes(requested)
}

/// Process-global raw-event in-flight budget shared across all automaton raw
/// consumers in this process. Each consumer acquires `RAW_EVENT_FETCH_MAX_BYTES`
/// worth of permits before fetching+decoding a batch and releases them once the
/// batch has drained into the (separately bounded) confirmation buffer. Ref #2187.
static RAW_EVENT_INFLIGHT_BUDGET: LazyLock<Semaphore> =
    LazyLock::new(|| Semaphore::new(raw_event_inflight_permits(raw_event_inflight_budget_bytes()) as usize));

/// Configuration for `JetStream` event consumer
#[derive(Debug, Clone)]
pub struct JetStreamEventConsumerConfig {
    /// Processing model for this consumer
    pub processing_model: ProcessingModel,
    /// Batch size for pulling events
    pub batch_size: usize,
    /// Maximum number of unacknowledged messages allowed by the consumer
    pub max_ack_pending: i64,
    /// Maximum time to wait for confirmation before timeout
    pub confirmation_timeout: Duration,
    /// Consumer name (durable)
    pub consumer_name: String,
    /// Whether to process provisional events immediately
    pub enable_provisional_processing: bool,
    /// Whether to consume and buffer raw events while awaiting confirmations.
    pub buffer_raw_events: bool,
    /// Whether confirmations can be dispatched without a matching raw event.
    pub accept_unbuffered_confirmations: bool,
    /// Where a newly-created durable consumer should begin delivering messages.
    pub deliver_policy: jetstream::consumer::DeliverPolicy,
}

impl Default for JetStreamEventConsumerConfig {
    fn default() -> Self {
        Self {
            processing_model: ProcessingModel::StatelessWorker,
            batch_size: 100,
            max_ack_pending: 1000,
            confirmation_timeout: Duration::from_secs(30),
            consumer_name: "automaton-consumer".to_string(),
            enable_provisional_processing: false,
            buffer_raw_events: true,
            accept_unbuffered_confirmations: false,
            deliver_policy: jetstream::consumer::DeliverPolicy::All,
        }
    }
}

/// `JetStream` event consumer for automata
pub struct JetStreamEventConsumer {
    nats_client: async_nats::Client,
    env: SinexEnvironment,
    config: JetStreamEventConsumerConfig,
    confirmed_handler: Arc<dyn ConfirmedEventHandler>,
    provisional_handler: Option<Arc<dyn ProvisionalEventHandler>>,
    confirmation_buffer: Arc<ConfirmationBuffer>,
    running: Arc<RwLock<bool>>,
    namespace: Option<String>,
}

impl JetStreamEventConsumer {
    fn background_task_exit_result(
        task_name: &str,
        result: Result<RuntimeResult<()>, tokio::task::JoinError>,
        stop_requested: bool,
    ) -> RuntimeResult<()> {
        match result {
            Ok(Ok(())) if stop_requested => Ok(()),
            Ok(Ok(())) => Err(SinexError::service(format!(
                "{task_name} stopped unexpectedly"
            ))),
            Ok(Err(error)) => Err(error),
            Err(join_error) => Err(SinexError::service(format!(
                "{task_name} panicked: {join_error}"
            ))),
        }
    }

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

    /// Create a new `JetStream` event consumer
    pub fn new(
        nats_client: async_nats::Client,
        env: SinexEnvironment,
        config: JetStreamEventConsumerConfig,
        confirmed_handler: Arc<dyn ConfirmedEventHandler>,
        provisional_handler: Option<Arc<dyn ProvisionalEventHandler>>,
    ) -> Self {
        Self::new_with_namespace(
            nats_client,
            env,
            config,
            confirmed_handler,
            provisional_handler,
            None,
        )
    }

    /// Create a new `JetStream` event consumer with an optional namespace.
    pub fn new_with_namespace(
        nats_client: async_nats::Client,
        env: SinexEnvironment,
        config: JetStreamEventConsumerConfig,
        confirmed_handler: Arc<dyn ConfirmedEventHandler>,
        provisional_handler: Option<Arc<dyn ProvisionalEventHandler>>,
        namespace: Option<String>,
    ) -> Self {
        let confirmation_buffer = event_stream_confirmation_buffer(config.confirmation_timeout);

        Self {
            nats_client,
            env,
            config,
            confirmed_handler,
            provisional_handler,
            confirmation_buffer,
            running: Arc::new(RwLock::new(false)),
            namespace,
        }
    }

    /// Start consuming events
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
            "Starting JetStream event consumer: {}",
            self.config.consumer_name
        );

        let js = jetstream::new(self.nats_client.clone());

        // Align with event_engine topology: base stream is SINEX_RAW_EVENTS.
        let raw_stream = self
            .env
            .nats_stream_name_with_namespace(self.namespace.as_deref(), "SINEX_RAW_EVENTS");
        let confirmations_stream = format!("{raw_stream}_CONFIRMATIONS");
        let raw_subject = self
            .env
            .nats_subject_with_namespace(self.namespace.as_deref(), "events.raw.>");
        let confirmations_subject = self
            .env
            .nats_subject_with_namespace(self.namespace.as_deref(), "events.confirmations.>");

        let confirmations_consumer = self
            .create_or_get_consumer(&js, &confirmations_stream, &confirmations_subject)
            .await?;

        if !self.config.buffer_raw_events {
            return Self::consume_confirmations(
                confirmations_consumer,
                self.config.batch_size,
                self.confirmation_buffer.clone(),
                self.confirmed_handler.clone(),
                self.provisional_handler.clone(),
                self.running.clone(),
                self.config.accept_unbuffered_confirmations,
            )
            .await;
        }

        let raw_consumer = self
            .create_or_get_consumer(&js, &raw_stream, &raw_subject)
            .await?;

        let confirmation_buffer = self.confirmation_buffer.clone();
        let confirmed_handler = self.confirmed_handler.clone();
        let provisional_handler = self.provisional_handler.clone();
        let enable_provisional = self.config.enable_provisional_processing;
        let batch_size = self.config.batch_size;
        let running = self.running.clone();

        let provisional_task = tokio::spawn(async move {
            Self::consume_raw_events(
                raw_consumer,
                batch_size,
                confirmation_buffer.clone(),
                provisional_handler,
                enable_provisional,
                running.clone(),
            )
            .await
        });
        let provisional_abort = provisional_task.abort_handle();

        let confirmations_buffer = self.confirmation_buffer.clone();
        let running_confirmations = self.running.clone();
        let provisional_handler_for_confirmations = self.provisional_handler.clone();

        let accept_unbuffered = self.config.accept_unbuffered_confirmations;
        let confirmation_task = tokio::spawn(async move {
            Self::consume_confirmations(
                confirmations_consumer,
                batch_size,
                confirmations_buffer,
                confirmed_handler,
                provisional_handler_for_confirmations,
                running_confirmations,
                accept_unbuffered,
            )
            .await
        });
        let confirmation_abort = confirmation_task.abort_handle();

        let timeout_buffer = self.confirmation_buffer.clone();
        let provisional_handler_timeout = self.provisional_handler.clone();
        let running_timeout = self.running.clone();
        let check_interval = Duration::from_secs(10);

        let timeout_task = tokio::spawn(async move {
            Self::check_timeouts(
                timeout_buffer,
                provisional_handler_timeout,
                running_timeout,
                check_interval,
            )
            .await
        });
        let timeout_abort = timeout_task.abort_handle();

        tokio::select! {
            result = provisional_task => {
                let stop_requested = !*self.running.read().await;
                error!(
                    target: "sinex_metrics",
                    metric = "runtime.consumer_task_exits_total",
                    task = "provisional_events",
                    "Provisional events task stopped: {result:?}"
                );
                // Abort remaining tasks
                confirmation_abort.abort();
                timeout_abort.abort();
                Self::background_task_exit_result("provisional events task", result, stop_requested)?;
            }
            result = confirmation_task => {
                let stop_requested = !*self.running.read().await;
                error!(
                    target: "sinex_metrics",
                    metric = "runtime.consumer_task_exits_total",
                    task = "confirmation",
                    "Confirmation task stopped: {result:?}"
                );
                // Abort remaining tasks
                provisional_abort.abort();
                timeout_abort.abort();
                Self::background_task_exit_result("confirmation task", result, stop_requested)?;
            }
            result = timeout_task => {
                let stop_requested = !*self.running.read().await;
                error!(
                    target: "sinex_metrics",
                    metric = "runtime.consumer_task_exits_total",
                    task = "timeout_check",
                    "Timeout check task stopped: {result:?}"
                );
                // Abort remaining tasks
                provisional_abort.abort();
                confirmation_abort.abort();
                Self::background_task_exit_result("timeout check task", result, stop_requested)?;
            }
        }

        Ok(())
    }

    /// Stop the consumer
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
        spec.filter_subject = Some(filter.to_string());
        spec.max_ack_pending = self.config.max_ack_pending;
        spec.max_deliver = 10;
        spec.ack_wait = Duration::from_secs(30);
        spec.deliver_policy = self.config.deliver_policy;
        ensure_pull_consumer(js, &spec).await
    }

    async fn consume_raw_events(
        consumer: jetstream::consumer::Consumer<jetstream::consumer::pull::Config>,
        batch_size: usize,
        buffer: Arc<ConfirmationBuffer>,
        provisional_handler: Option<Arc<dyn ProvisionalEventHandler>>,
        enable_provisional: bool,
        running: Arc<RwLock<bool>>,
    ) -> RuntimeResult<()> {
        while *running.read().await {
            if !raw_event_buffer_accepts_pull(&buffer).await {
                let pending_count = buffer.len().await;
                debug!(
                    pending_count,
                    max_capacity = buffer.max_capacity(),
                    retained_payload_bytes = buffer.retained_payload_bytes(),
                    max_payload_bytes = buffer.max_payload_bytes(),
                    "Confirmation buffer saturated; delaying raw JetStream pull"
                );
                tokio::time::sleep(RAW_EVENT_BUFFER_BACKPRESSURE_SLEEP).await;
                continue;
            }

            // Acquire from the process-global in-flight budget BEFORE fetch+decode
            // so the ~14 automata consumers cannot collectively stage unbounded raw
            // batches (the #2187 startup-drain OOM). Acquired here — after the buffer
            // gate's sleep, so the permit is never held while idle — and released when
            // `_inflight_permit` drops at the end of this iteration, after the batch
            // has drained into the confirmation buffer.
            let _inflight_permit = RAW_EVENT_INFLIGHT_BUDGET
                .acquire_many(raw_event_inflight_permits(RAW_EVENT_FETCH_MAX_BYTES))
                .await
                .map_err(|e| {
                    SinexError::processing("raw-event in-flight budget semaphore closed")
                        .with_source(e.to_string())
                })?;

            let messages =
                pull_batch_bounded(&consumer, batch_size, RAW_EVENT_FETCH_MAX_BYTES, Duration::from_secs(1))
                    .await?;
            for msg in messages {
                // Break promptly on stop() instead of finishing the whole batch,
                // so graceful shutdown completes well under the stop timeout.
                if !*running.read().await {
                    break;
                }
                Self::handle_raw_message(msg, &buffer, &provisional_handler, enable_provisional)
                    .await?;
            }
        }

        Ok(())
    }

    /// Handle a single raw `JetStream` message: parse, buffer, ack/nak.
    async fn handle_raw_message(
        msg: jetstream::Message,
        buffer: &ConfirmationBuffer,
        provisional_handler: &Option<Arc<dyn ProvisionalEventHandler>>,
        enable_provisional: bool,
    ) -> RuntimeResult<()> {
        let events = match Self::parse_provisional_events(&msg) {
            Ok(events) => events,
            Err(e) => {
                // Genuinely unparseable payload — neither an EventIntent envelope
                // nor a flat event. Acked/dropped and tracked by the metric below;
                // logging each at error! floods the journal and stalls graceful
                // shutdown, so keep the metric and log at debug. (Normal envelope
                // traffic no longer reaches this arm.)
                debug!(
                    target: "sinex_metrics",
                    metric = "runtime.provisional_event_parse_failures_total",
                    error = %e,
                    "Failed to parse raw event message"
                );
                msg.ack().await.map_err(|ack_err| {
                    Self::message_settlement_error(
                        "failed to ack bad provisional message",
                        &msg,
                        None::<String>,
                        ack_err,
                    )
                })?;
                return Ok(());
            }
        };

        // Buffer every event the message carries (one for a flat event, N for an
        // EventIntent envelope). `add_provisional` is keyed by event_id, so a NAK
        // redelivery that re-adds an already-buffered sibling is idempotent.
        let mut handler_success = true;
        for event in &events {
            // Memory protection: if the buffer is full, NAK the whole message to
            // apply backpressure; redelivery re-buffers any siblings idempotently.
            let decision = buffer.add_provisional_with_pressure(event.clone()).await;
            if !decision.accepted {
                debug!(
                    target: "sinex_metrics",
                    metric = "runtime.confirmation_buffer_backpressure_total",
                    event_id = %event.event_id,
                    pressure_level = ?decision.pressure_level,
                    rejection_reason = ?decision.rejection_reason,
                    runtime_action = decision.runtime_action().as_str(),
                    pending_count = decision.pending_count,
                    max_capacity = decision.max_capacity,
                    retained_payload_bytes = decision.retained_payload_bytes,
                    max_payload_bytes = decision.max_payload_bytes,
                    attempted_payload_bytes = decision.attempted_payload_bytes,
                    projected_payload_bytes = decision.projected_payload_bytes,
                    redelivery_delay_ms = decision.rejected_redelivery_delay_ms(),
                    "Confirmation buffer rejected provisional event; NAKing with resource-pressure backoff"
                );
                let nak_delay = decision.rejected_redelivery_delay();
                msg.ack_with(async_nats::jetstream::AckKind::Nak(nak_delay))
                    .await
                    .map_err(|error| {
                        Self::message_settlement_error(
                            "failed to NAK provisional message during backpressure",
                            &msg,
                            Some(event.event_id),
                            error,
                        )
                    })?;
                return Ok(());
            }

            if enable_provisional
                && let Some(handler) = provisional_handler
                && let Err(e) = handler.handle_provisional(event).await
            {
                warn!("Provisional handler failed: {e}");
                handler_success = false;
            }
        }

        if handler_success {
            msg.ack().await.map_err(|error| {
                Self::message_settlement_error(
                    "failed to ack provisional message",
                    &msg,
                    None::<String>,
                    error,
                )
            })?;
        } else {
            msg.ack_with(async_nats::jetstream::AckKind::Nak(Some(
                Duration::from_secs(5),
            )))
            .await
            .map_err(|error| {
                Self::message_settlement_error(
                    "failed to NAK provisional handler failure",
                    &msg,
                    None::<String>,
                    error,
                )
            })?;
        }
        Ok(())
    }

    async fn consume_confirmations(
        consumer: jetstream::consumer::Consumer<jetstream::consumer::pull::Config>,
        batch_size: usize,
        buffer: Arc<ConfirmationBuffer>,
        confirmed_handler: Arc<dyn ConfirmedEventHandler>,
        provisional_handler: Option<Arc<dyn ProvisionalEventHandler>>,
        running: Arc<RwLock<bool>>,
        accept_unbuffered_confirmations: bool,
    ) -> RuntimeResult<()> {
        while *running.read().await {
            let messages =
                pull_batch_bounded(&consumer, batch_size, RAW_EVENT_FETCH_MAX_BYTES, Duration::from_secs(1))
                    .await?;
            for msg in messages {
                // Break promptly on stop() instead of finishing the whole batch,
                // so graceful shutdown completes well under the stop timeout.
                if !*running.read().await {
                    break;
                }
                Self::handle_confirmation_message(
                    msg,
                    &buffer,
                    &*confirmed_handler,
                    provisional_handler.as_ref(),
                    accept_unbuffered_confirmations,
                )
                .await?;
            }
        }

        Ok(())
    }

    /// Handle a single confirmation message: match to buffered event, dispatch, ack/nak.
    async fn handle_confirmation_message(
        msg: jetstream::Message,
        buffer: &ConfirmationBuffer,
        confirmed_handler: &dyn ConfirmedEventHandler,
        provisional_handler: Option<&Arc<dyn ProvisionalEventHandler>>,
        accept_unbuffered_confirmations: bool,
    ) -> RuntimeResult<()> {
        let confirmation = match Self::parse_confirmation(&msg) {
            Ok(c) => c,
            Err(e) => {
                error!(
                    target: "sinex_metrics",
                    metric = "runtime.confirmation_parse_failures_total",
                    error = %e,
                    "Failed to parse confirmation"
                );
                msg.ack().await.map_err(|ack_err| {
                    Self::message_settlement_error(
                        "failed to ack bad confirmation",
                        &msg,
                        None::<String>,
                        ack_err,
                    )
                })?;
                return Ok(());
            }
        };

        if confirmation.source.is_empty() || confirmation.event_type.is_empty() {
            // Legacy per-event-id confirmation (pre-#1306). Match by event_id
            // directly. Should not occur after upgrade but supported for mixed
            // deploys.
            return Self::handle_legacy_per_event_confirmation(
                msg,
                confirmation,
                buffer,
                confirmed_handler,
                provisional_handler,
                accept_unbuffered_confirmations,
            )
            .await;
        }

        // Per-kind watermark path (#1306).
        let confirmed_events = buffer
            .confirm_kind_up_to(
                &confirmation.source,
                &confirmation.event_type,
                confirmation.event_id,
            )
            .await;

        if !confirmed_events.is_empty() {
            if !confirmation.persisted {
                warn!(
                    source = %confirmation.source,
                    event_type = %confirmation.event_type,
                    watermark = %confirmation.event_id,
                    rollback_count = confirmed_events.len(),
                    "Confirmation watermark marked kind as not persisted; rolling back provisional effects for all events of kind <= watermark"
                );
                if let Some(handler) = provisional_handler {
                    for event in &confirmed_events {
                        if let Err(e) = handler.rollback_provisional(event.event_id).await {
                            error!(
                                event_id = %event.event_id,
                                error = %e,
                                "Failed to rollback provisional event after non-persisted confirmation"
                            );
                        }
                    }
                }
                msg.ack().await.map_err(|error| {
                    Self::message_settlement_error(
                        "failed to ack non-persisted kind confirmation",
                        &msg,
                        Some(confirmation.event_id),
                        error,
                    )
                })?;
                return Ok(());
            }

            let mut handler_success = true;
            for event in &confirmed_events {
                match confirmed_handler.handle_confirmed(event).await {
                    Ok(()) => {}
                    Err(e) if e.kind() == SinexErrorKind::Lifecycle => {
                        // Channel closed = shutdown in progress. Ack and exit cleanly.
                        debug!(event_id = %event.event_id, "Confirmed handler channel closed (shutdown)");
                        msg.ack().await.ok();
                        return Ok(());
                    }
                    Err(e) => {
                        error!(event_id = %event.event_id, error = %e, "Confirmed handler failed");
                        handler_success = false;
                    }
                }
            }

            if handler_success {
                msg.ack().await.map_err(|error| {
                    Self::message_settlement_error(
                        "failed to ack kind confirmation",
                        &msg,
                        Some(confirmation.event_id),
                        error,
                    )
                })?;
            } else {
                msg.ack_with(async_nats::jetstream::AckKind::Nak(Some(
                    Duration::from_secs(5),
                )))
                .await
                .map_err(|error| {
                    Self::message_settlement_error(
                        "failed to NAK confirmed handler failure",
                        &msg,
                        Some(confirmation.event_id),
                        error,
                    )
                })?;
            }
            return Ok(());
        }

        // Zero matching events in buffer. Either:
        // (a) consumer joined late and never buffered events of this kind, or
        // (b) all events of this kind already confirmed by an earlier watermark
        //     observation. The watermark itself was advanced inside
        //     `confirm_kind_up_to`, so future late-arriving provisional events
        //     <= watermark will short-circuit via `try_implicit_confirm_on_add`.
        // In either case it is safe to ack.
        if accept_unbuffered_confirmations {
            if !confirmation.persisted {
                msg.ack().await.map_err(|error| {
                    Self::message_settlement_error(
                        "failed to ack unbuffered non-persisted kind confirmation",
                        &msg,
                        Some(confirmation.event_id),
                        error,
                    )
                })?;
                return Ok(());
            }
            let synthetic = Self::event_from_unbuffered_confirmation(&confirmation);
            let handler_success = match confirmed_handler.handle_confirmed(&synthetic).await {
                Ok(()) => true,
                Err(e) if e.kind() == SinexErrorKind::Lifecycle => {
                    debug!(
                        "Confirmed handler channel closed on unbuffered kind watermark (shutdown)"
                    );
                    msg.ack().await.ok();
                    return Ok(());
                }
                Err(e) => {
                    error!(
                        target: "sinex_metrics",
                        metric = "runtime.confirmation_handler_failures_total",
                        error = %e,
                        "Confirmed handler failed on unbuffered kind watermark"
                    );
                    false
                }
            };
            if handler_success {
                msg.ack().await.map_err(|error| {
                    Self::message_settlement_error(
                        "failed to ack unbuffered kind confirmation",
                        &msg,
                        Some(confirmation.event_id),
                        error,
                    )
                })?;
            } else {
                msg.ack_with(async_nats::jetstream::AckKind::Nak(Some(
                    Duration::from_secs(5),
                )))
                .await
                .map_err(|error| {
                    Self::message_settlement_error(
                        "failed to NAK unbuffered kind confirmed handler failure",
                        &msg,
                        Some(confirmation.event_id),
                        error,
                    )
                })?;
            }
            return Ok(());
        }

        // No matching pending events, no unbuffered mode. Ack — the watermark
        // is recorded in the buffer so future provisionals of this kind <=
        // watermark short-circuit confirm at add time.
        debug!(
            source = %confirmation.source,
            event_type = %confirmation.event_type,
            watermark = %confirmation.event_id,
            "Watermark advanced with no pending events of kind; ACKing"
        );
        msg.ack().await.map_err(|error| {
            Self::message_settlement_error(
                "failed to ack empty kind watermark",
                &msg,
                Some(confirmation.event_id),
                error,
            )
        })?;
        Ok(())
    }

    /// Legacy per-event-id confirmation handling — preserved for mixed-deploy
    /// scenarios where event_engine pre-#1306 publishes per-event-id confirmations.
    /// New deploys publish per-kind watermarks; this branch is exercised only
    /// when `source`/`event_type` fields are absent on the confirmation payload.
    async fn handle_legacy_per_event_confirmation(
        msg: jetstream::Message,
        confirmation: EventConfirmation,
        buffer: &ConfirmationBuffer,
        confirmed_handler: &dyn ConfirmedEventHandler,
        provisional_handler: Option<&Arc<dyn ProvisionalEventHandler>>,
        accept_unbuffered_confirmations: bool,
    ) -> RuntimeResult<()> {
        if let Some(event) = buffer.confirm(confirmation.event_id).await {
            if !confirmation.persisted {
                warn!(
                    event_id = %confirmation.event_id,
                    "Confirmation marked event as not persisted; rolling back provisional effects"
                );

                if let Some(handler) = provisional_handler
                    && let Err(e) = handler.rollback_provisional(confirmation.event_id).await
                {
                    error!(
                        event_id = %confirmation.event_id,
                        error = %e,
                        "Failed to rollback provisional event after non-persisted confirmation"
                    );
                }

                msg.ack().await.map_err(|error| {
                    Self::message_settlement_error(
                        "failed to ack non-persisted confirmation",
                        &msg,
                        Some(confirmation.event_id),
                        error,
                    )
                })?;
                return Ok(());
            }

            let handler_success = match confirmed_handler.handle_confirmed(&event).await {
                Ok(()) => true,
                Err(e) if e.kind() == SinexErrorKind::Lifecycle => {
                    debug!("Confirmed handler channel closed (shutdown)");
                    msg.ack().await.ok();
                    return Ok(());
                }
                Err(e) => {
                    error!(
                        target: "sinex_metrics",
                        metric = "runtime.confirmation_handler_failures_total",
                        error = %e,
                        "Confirmed handler failed"
                    );
                    false
                }
            };
            if handler_success {
                msg.ack().await.map_err(|error| {
                    Self::message_settlement_error(
                        "failed to ack confirmation",
                        &msg,
                        Some(confirmation.event_id),
                        error,
                    )
                })?;
            } else {
                msg.ack_with(async_nats::jetstream::AckKind::Nak(Some(
                    Duration::from_secs(5),
                )))
                .await
                .map_err(|error| {
                    Self::message_settlement_error(
                        "failed to NAK confirmed handler failure",
                        &msg,
                        Some(confirmation.event_id),
                        error,
                    )
                })?;
            }
        } else {
            if accept_unbuffered_confirmations {
                if !confirmation.persisted {
                    msg.ack().await.map_err(|error| {
                        Self::message_settlement_error(
                            "failed to ack unbuffered non-persisted confirmation",
                            &msg,
                            Some(confirmation.event_id),
                            error,
                        )
                    })?;
                    return Ok(());
                }

                let event = Self::event_from_unbuffered_confirmation(&confirmation);
                let handler_success = match confirmed_handler.handle_confirmed(&event).await {
                    Ok(()) => true,
                    Err(e) if e.kind() == SinexErrorKind::Lifecycle => {
                        debug!(
                            "Confirmed handler channel closed on unbuffered confirmation (shutdown)"
                        );
                        msg.ack().await.ok();
                        return Ok(());
                    }
                    Err(e) => {
                        error!(
                            target: "sinex_metrics",
                            metric = "runtime.confirmation_handler_failures_total",
                            error = %e,
                            "Confirmed handler failed"
                        );
                        false
                    }
                };

                if handler_success {
                    msg.ack().await.map_err(|error| {
                        Self::message_settlement_error(
                            "failed to ack unbuffered confirmation",
                            &msg,
                            Some(confirmation.event_id),
                            error,
                        )
                    })?;
                } else {
                    msg.ack_with(async_nats::jetstream::AckKind::Nak(Some(
                        Duration::from_secs(5),
                    )))
                    .await
                    .map_err(|error| {
                        Self::message_settlement_error(
                            "failed to NAK unbuffered confirmed handler failure",
                            &msg,
                            Some(confirmation.event_id),
                            error,
                        )
                    })?;
                }
                return Ok(());
            }

            // Confirmation arrived before the provisional event was buffered
            // (race between consume_raw_events and consume_confirmations tasks).
            // NAK with a short redelivery delay so the confirmation is retried
            // after the provisional event has been added to the buffer.
            debug!(
                event_id = %confirmation.event_id,
                "Confirmation arrived before provisional event; NAKing for retry"
            );
            msg.ack_with(async_nats::jetstream::AckKind::Nak(Some(
                Duration::from_millis(200),
            )))
            .await
            .map_err(|error| {
                Self::message_settlement_error(
                    "failed to NAK early confirmation",
                    &msg,
                    Some(confirmation.event_id),
                    error,
                )
            })?;
        }
        Ok(())
    }

    async fn check_timeouts(
        buffer: Arc<ConfirmationBuffer>,
        provisional_handler: Option<Arc<dyn ProvisionalEventHandler>>,
        running: Arc<RwLock<bool>>,
        interval: Duration,
    ) -> RuntimeResult<()> {
        let mut ticker = tokio::time::interval(interval);

        while *running.read().await {
            ticker.tick().await;

            let timed_out_ids = buffer.check_timeouts().await;
            if !timed_out_ids.is_empty() {
                debug!(
                    timed_out = timed_out_ids.len(),
                    "Found timed-out provisional events; retaining them during the confirmation grace period"
                );

                for event_id in timed_out_ids {
                    if let Some(handler) = provisional_handler.as_ref()
                        && let Err(e) = handler.rollback_provisional(event_id).await
                    {
                        error!(
                            target: "sinex_metrics",
                            metric = "runtime.provisional_rollback_failures_total",
                            %event_id,
                            error = %e,
                            "Failed to rollback provisional event"
                        );
                    }
                }
            }

            let purged_events = buffer.purge_expired().await;
            if !purged_events.is_empty() {
                debug!(
                    purged = purged_events.len(),
                    "Purged timed-out provisional events after confirmation grace period"
                );
            }
        }

        Ok(())
    }

    fn event_from_unbuffered_confirmation(confirmation: &EventConfirmation) -> ProvisionalEvent {
        let source = EventSource::new(&confirmation.source)
            .unwrap_or_else(|_| EventSource::from_static("confirmed"));
        let event_type = EventType::new(&confirmation.event_type)
            .unwrap_or_else(|_| EventType::from_static("confirmed.event"));
        ProvisionalEvent {
            event_id: confirmation.event_id,
            source,
            event_type,
            payload: serde_json::Value::Null,
            ts_orig: confirmation.ts_ingest,
            received_at: Timestamp::now(),
        }
    }

    /// Parse a raw `JetStream` message into one-or-more provisional events.
    ///
    /// Raw traffic on `events.raw.>` arrives in two shapes and both must be
    /// buffered so confirmation watermarks resolve real buffered inputs instead
    /// of synthetic kind stand-ins:
    /// - **`EventIntent` admission envelope** (`{envelope_version, events: […]}`,
    ///   no top-level `id`) — the canonical batch format every source and
    ///   automaton publishes via `publish_intent`. Yields one provisional per
    ///   contained event. This is the dominant shape; flat-parsing it as a single
    ///   event was the source of the per-message "Missing event id" journal flood
    ///   and meant the buffer never populated.
    /// - **Flat single event** (`{id, source, event_type, …}`) — published by
    ///   `publish_telemetry` and the test/bootstrap raw-event escape hatch.
    ///
    /// A payload that is neither shape is a genuine parse failure.
    fn parse_provisional_events(msg: &jetstream::Message) -> RuntimeResult<Vec<ProvisionalEvent>> {
        Self::parse_provisional_events_from_bytes(&msg.payload)
    }

    fn parse_provisional_events_from_bytes(bytes: &[u8]) -> RuntimeResult<Vec<ProvisionalEvent>> {
        let payload: serde_json::Value = serde_json::from_slice(bytes)?;

        // A top-level string `id` unambiguously marks a flat single event.
        if payload
            .get("id")
            .and_then(serde_json::Value::as_str)
            .is_some()
        {
            return Ok(vec![Self::parse_single_provisional_event(payload)?]);
        }

        // Otherwise it is an EventIntent envelope: buffer each contained event.
        if let Some(events) = payload.get("events").and_then(serde_json::Value::as_array) {
            return events
                .iter()
                .map(|event| Self::parse_single_provisional_event(event.clone()))
                .collect();
        }

        Err(SinexError::processing(
            "raw event message is neither a flat event (missing id) nor an EventIntent envelope (missing events array)"
                .to_string(),
        ))
    }

    /// Parse one event JSON object — a flat event, or one element of an
    /// `EventIntent` envelope's `events` array — into a `ProvisionalEvent`.
    fn parse_single_provisional_event(
        payload: serde_json::Value,
    ) -> RuntimeResult<ProvisionalEvent> {
        let id_str = payload["id"]
            .as_str()
            .ok_or_else(|| SinexError::processing("Missing event id".to_string()))?;
        let event_id = id_str
            .parse()
            .map_err(|e| SinexError::processing(format!("Invalid event id '{id_str}': {e}")))?;

        let source = payload["source"]
            .as_str()
            .ok_or_else(|| SinexError::processing("Missing source".to_string()))?;
        let source = EventSource::new(source)?;

        let event_type = payload["event_type"]
            .as_str()
            .ok_or_else(|| SinexError::processing("Missing event_type".to_string()))?;
        let event_type = EventType::new(event_type)?;

        let ts_orig = if let Some(ts_orig_str) = payload["ts_orig"].as_str() {
            sinex_primitives::temporal::parse_rfc3339(ts_orig_str).map_err(|e| {
                SinexError::processing(format!("Invalid ts_orig '{ts_orig_str}': {e}"))
            })?
        } else {
            // ts_orig is optional in the Event schema; fall back to now (matching event_engine behavior)
            sinex_primitives::temporal::now()
        };

        Ok(ProvisionalEvent {
            event_id,
            source,
            event_type,
            payload,
            ts_orig,
            received_at: sinex_primitives::temporal::now(),
        })
    }

    fn parse_confirmation(msg: &jetstream::Message) -> RuntimeResult<EventConfirmation> {
        let payload: serde_json::Value = serde_json::from_slice(&msg.payload)?;

        let eid_str = payload["event_id"]
            .as_str()
            .ok_or_else(|| SinexError::processing("Missing event_id".to_string()))?;
        let event_id = eid_str
            .parse()
            .map_err(|e| SinexError::processing(format!("Invalid event_id '{eid_str}': {e}")))?;

        let persisted = payload["persisted"]
            .as_bool()
            .ok_or_else(|| SinexError::processing("Missing persisted".to_string()))?;

        let ts_ingest_str = payload["ts_ingest"]
            .as_str()
            .ok_or_else(|| SinexError::processing("Missing ts_ingest".to_string()))?;
        let ts_ingest = sinex_primitives::temporal::parse_rfc3339(ts_ingest_str).map_err(|e| {
            SinexError::processing(format!("Invalid ts_ingest '{ts_ingest_str}': {e}"))
        })?;

        let source = payload["source"]
            .as_str()
            .ok_or_else(|| SinexError::processing("Missing source".to_string()))?
            .to_string();
        let event_type = payload["event_type"]
            .as_str()
            .ok_or_else(|| SinexError::processing("Missing event_type".to_string()))?
            .to_string();

        Ok(EventConfirmation {
            event_id,
            source,
            event_type,
            persisted,
            ts_ingest,
        })
    }
}

fn event_stream_confirmation_buffer(timeout: Duration) -> Arc<ConfirmationBuffer> {
    Arc::new(ConfirmationBuffer::with_resource_budget(
        timeout,
        ResourceProfile::EventStreamConsumer.budget_spec(),
    ))
}

async fn raw_event_buffer_accepts_pull(buffer: &ConfirmationBuffer) -> bool {
    buffer.len().await < buffer.max_capacity()
        && buffer.retained_payload_bytes() < buffer.max_payload_bytes()
}

#[cfg(test)]
mod tests {
    // Small inline tests are justified here because they target private background-task
    // exit classification logic that is not exposed through the public consumer API.
    use super::{
        ConfirmationBuffer, EventConfirmation, JetStreamEventConsumer,
        JetStreamEventConsumerConfig, ProvisionalEvent, RAW_EVENT_FETCH_MAX_BYTES,
        RAW_EVENT_INFLIGHT_PERMIT_BYTES, clamp_raw_event_inflight_budget_bytes,
        event_stream_confirmation_buffer, raw_event_buffer_accepts_pull,
        raw_event_inflight_permits,
    };
    use async_nats::jetstream::consumer::DeliverPolicy;
    use sinex_primitives::{
        SinexError, Uuid,
        domain::{EventSource, EventType},
        events::builder::EventId,
        source_contracts::ResourceProfile,
        temporal::Timestamp,
    };
    use std::time::Duration;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn background_task_exit_is_error_while_running() -> xtask::sandbox::TestResult<()> {
        let handle = tokio::spawn(async { Ok::<(), SinexError>(()) });
        let result = handle.await;

        let error =
            JetStreamEventConsumer::background_task_exit_result("confirmation task", result, false)
                .expect_err("unexpected task exit while still running must fail");
        assert!(format!("{error:#}").contains("confirmation task stopped unexpectedly"));
        Ok(())
    }

    #[sinex_test]
    async fn background_task_exit_after_stop_is_clean() -> xtask::sandbox::TestResult<()> {
        let handle = tokio::spawn(async { Ok::<(), SinexError>(()) });
        let result = handle.await;

        JetStreamEventConsumer::background_task_exit_result("confirmation task", result, true)?;
        Ok(())
    }

    /// The aggregate budget must never clamp below a single fetch, or
    /// `acquire_many(fetch_permits)` would block forever (a hard deadlock that
    /// would silently wedge every automaton's raw consumer). Guards the #2187 fix.
    #[sinex_test]
    async fn inflight_budget_never_below_one_fetch() -> xtask::sandbox::TestResult<()> {
        // Even an absurdly small (or zero) requested budget is raised to one fetch.
        for requested in [0usize, 1, RAW_EVENT_FETCH_MAX_BYTES / 2, RAW_EVENT_FETCH_MAX_BYTES - 1] {
            let clamped = clamp_raw_event_inflight_budget_bytes(requested);
            assert!(
                raw_event_inflight_permits(clamped) >= raw_event_inflight_permits(RAW_EVENT_FETCH_MAX_BYTES),
                "budget {requested} clamped to {clamped} must hold at least one fetch worth of permits"
            );
        }
        // A larger requested budget is preserved verbatim.
        let big = RAW_EVENT_FETCH_MAX_BYTES * 8;
        assert_eq!(clamp_raw_event_inflight_budget_bytes(big), big);
        Ok(())
    }

    /// Permit accounting: whole 1 MiB permits, always at least one.
    #[sinex_test]
    async fn inflight_permits_round_down_with_floor() -> xtask::sandbox::TestResult<()> {
        assert_eq!(raw_event_inflight_permits(0), 1);
        assert_eq!(raw_event_inflight_permits(1), 1);
        assert_eq!(raw_event_inflight_permits(RAW_EVENT_INFLIGHT_PERMIT_BYTES), 1);
        assert_eq!(raw_event_inflight_permits(RAW_EVENT_INFLIGHT_PERMIT_BYTES * 64), 64);
        assert_eq!(raw_event_inflight_permits(RAW_EVENT_FETCH_MAX_BYTES), 64);
        Ok(())
    }

    #[sinex_test]
    async fn background_task_panic_is_preserved() -> xtask::sandbox::TestResult<()> {
        let handle = tokio::spawn(async { panic!("boom") });
        let result = handle.await.map(|_| Ok::<(), SinexError>(()));

        let error =
            JetStreamEventConsumer::background_task_exit_result("confirmation task", result, false)
                .expect_err("panic must surface as an error");
        assert!(format!("{error:#}").contains("confirmation task panicked"));
        Ok(())
    }

    #[sinex_test]
    async fn default_consumer_config_preserves_buffered_raw_mode() -> xtask::sandbox::TestResult<()>
    {
        let config = JetStreamEventConsumerConfig::default();

        assert!(config.buffer_raw_events);
        assert!(!config.accept_unbuffered_confirmations);
        assert_eq!(config.deliver_policy, DeliverPolicy::All);
        Ok(())
    }

    #[sinex_test]
    async fn event_stream_confirmation_buffer_uses_runtime_budget() -> xtask::sandbox::TestResult<()>
    {
        let budget = ResourceProfile::EventStreamConsumer.budget_spec();
        let buffer = event_stream_confirmation_buffer(std::time::Duration::from_secs(60));

        assert_eq!(
            buffer.max_capacity(),
            usize::try_from(budget.max_pending_candidates)?
        );
        assert_eq!(
            buffer.max_payload_bytes(),
            usize::try_from(budget.max_pending_material_bytes)?
        );
        Ok(())
    }

    #[sinex_test]
    async fn raw_event_pull_gate_closes_when_confirmation_buffer_is_full()
    -> xtask::sandbox::TestResult<()> {
        let buffer = ConfirmationBuffer::with_capacity(Duration::from_secs(60), 1);

        assert!(raw_event_buffer_accepts_pull(&buffer).await);
        buffer
            .add_provisional_with_pressure(ProvisionalEvent {
                event_id: EventId::from_uuid(Uuid::now_v7()),
                source: EventSource::from_static("test"),
                event_type: EventType::from_static("test.event"),
                payload: serde_json::json!({"ok": true}),
                ts_orig: Timestamp::now(),
                received_at: Timestamp::now(),
            })
            .await;

        assert!(!raw_event_buffer_accepts_pull(&buffer).await);
        Ok(())
    }

    #[sinex_test]
    async fn unbuffered_confirmation_event_carries_confirmation_kind()
    -> xtask::sandbox::TestResult<()> {
        let event_id = EventId::from_uuid(Uuid::now_v7());
        let ts_ingest = sinex_primitives::temporal::now();
        let confirmation = EventConfirmation {
            event_id,
            source: "shell.atuin".to_string(),
            event_type: "command.executed".to_string(),
            persisted: true,
            ts_ingest,
        };

        let event = JetStreamEventConsumer::event_from_unbuffered_confirmation(&confirmation);

        assert_eq!(event.event_id, event_id);
        assert_eq!(event.source.as_ref(), "shell.atuin");
        assert_eq!(event.event_type.as_ref(), "command.executed");
        assert!(event.payload.is_null());
        assert_eq!(event.ts_orig, ts_ingest);
        Ok(())
    }

    #[sinex_test]
    async fn parse_event_intent_envelope_buffers_each_event() -> xtask::sandbox::TestResult<()> {
        // The canonical raw-events wire shape (publish_intent): a top-level
        // `events` array, no top-level `id`. Each contained event must become a
        // provisional so confirmation watermarks resolve real buffered inputs.
        let id1 = Uuid::now_v7();
        let id2 = Uuid::now_v7();
        let envelope = serde_json::json!({
            "envelope_version": "1",
            "source_id": "shell.atuin",
            "parser_id": "atuin-history-parser",
            "parser_version": "1.0.0",
            "events": [
                {"id": id1.to_string(), "source": "shell.atuin",
                 "event_type": "command.executed", "payload": {}},
                {"id": id2.to_string(), "source": "shell.atuin",
                 "event_type": "command.executed", "payload": {}},
            ],
            "admitted_at": "2026-06-07T00:00:00Z",
            "admitted_by": "test-host",
        });
        let bytes = serde_json::to_vec(&envelope).expect("serialize envelope");

        let events = JetStreamEventConsumer::parse_provisional_events_from_bytes(&bytes)
            .expect("EventIntent envelope must parse");

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_id.as_uuid(), &id1);
        assert_eq!(events[1].event_id.as_uuid(), &id2);
        assert_eq!(events[0].source.as_str(), "shell.atuin");
        assert_eq!(events[0].event_type.as_str(), "command.executed");
        Ok(())
    }

    #[sinex_test]
    async fn parse_flat_single_event_yields_one() -> xtask::sandbox::TestResult<()> {
        // The flat single-event shape (publish_telemetry / raw-event escape hatch).
        let id = Uuid::now_v7();
        let event = serde_json::json!({
            "id": id.to_string(),
            "source": "sinexd.event_engine",
            "event_type": "sinexd.event_engine.batch",
            "payload": {},
        });
        let bytes = serde_json::to_vec(&event).expect("serialize flat event");

        let events = JetStreamEventConsumer::parse_provisional_events_from_bytes(&bytes)
            .expect("flat event must parse");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_id.as_uuid(), &id);
        Ok(())
    }

    #[sinex_test]
    async fn parse_rejects_non_event_payload() -> xtask::sandbox::TestResult<()> {
        // A payload that is neither a flat event nor an envelope is the only
        // shape that should count as a genuine parse failure.
        let bytes = serde_json::to_vec(&serde_json::json!({"foo": "bar"})).expect("serialize junk");

        let err = JetStreamEventConsumer::parse_provisional_events_from_bytes(&bytes)
            .expect_err("payload without id or events must be a parse failure");

        assert!(format!("{err:#}").contains("neither a flat event"));
        Ok(())
    }
}

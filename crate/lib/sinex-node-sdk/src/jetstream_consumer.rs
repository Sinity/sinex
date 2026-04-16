//! `JetStream` event consumer for automata
//!
//! This module provides a consumer that subscribes to `JetStream` events
//! and handles provisional/confirmed event processing with proper buffering.

use crate::confirmation_handler::{
    ConfirmationBuffer, ConfirmedEventHandler, EventConfirmation, ProcessingModel,
    ProvisionalEvent, ProvisionalEventHandler,
};
use crate::runtime::stream::{PullConsumerSpec, ensure_pull_consumer, pull_batch};
use crate::{NodeResult, SinexError};
use async_nats::jetstream;
use sinex_primitives::{
    domain::{EventSource, EventType},
    environment::SinexEnvironment,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

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
        result: Result<NodeResult<()>, tokio::task::JoinError>,
        stop_requested: bool,
    ) -> NodeResult<()> {
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

    fn message_settlement_error(
        operation: &'static str,
        msg: &jetstream::Message,
        event_id: Option<impl std::fmt::Display>,
        error: impl std::fmt::Display,
    ) -> SinexError {
        crate::error_helpers::nats_settlement_error(
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
        let confirmation_buffer = Arc::new(ConfirmationBuffer::new(config.confirmation_timeout));

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
    pub async fn run(&self) -> NodeResult<()> {
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

    async fn run_inner(&self) -> NodeResult<()> {
        info!(
            "Starting JetStream event consumer: {}",
            self.config.consumer_name
        );

        let js = jetstream::new(self.nats_client.clone());

        // Align with ingestd topology: base stream is SINEX_RAW_EVENTS.
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

        let raw_consumer = self
            .create_or_get_consumer(&js, &raw_stream, &raw_subject)
            .await?;
        let confirmations_consumer = self
            .create_or_get_consumer(&js, &confirmations_stream, &confirmations_subject)
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

        let confirmation_task = tokio::spawn(async move {
            Self::consume_confirmations(
                confirmations_consumer,
                batch_size,
                confirmations_buffer,
                confirmed_handler,
                provisional_handler_for_confirmations,
                running_confirmations,
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
                error!("Provisional events task stopped: {result:?}");
                // Abort remaining tasks
                confirmation_abort.abort();
                timeout_abort.abort();
                Self::background_task_exit_result("provisional events task", result, stop_requested)?;
            }
            result = confirmation_task => {
                let stop_requested = !*self.running.read().await;
                error!("Confirmation task stopped: {result:?}");
                // Abort remaining tasks
                provisional_abort.abort();
                timeout_abort.abort();
                Self::background_task_exit_result("confirmation task", result, stop_requested)?;
            }
            result = timeout_task => {
                let stop_requested = !*self.running.read().await;
                error!("Timeout check task stopped: {result:?}");
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
    ) -> NodeResult<jetstream::consumer::Consumer<jetstream::consumer::pull::Config>> {
        let mut spec =
            PullConsumerSpec::new(stream_name.to_string(), self.config.consumer_name.clone());
        spec.filter_subject = Some(filter.to_string());
        spec.max_ack_pending = self.config.max_ack_pending;
        spec.max_deliver = -1;
        spec.ack_wait = Duration::from_secs(30);
        spec.deliver_policy = jetstream::consumer::DeliverPolicy::All;
        ensure_pull_consumer(js, &spec).await
    }

    async fn consume_raw_events(
        consumer: jetstream::consumer::Consumer<jetstream::consumer::pull::Config>,
        batch_size: usize,
        buffer: Arc<ConfirmationBuffer>,
        provisional_handler: Option<Arc<dyn ProvisionalEventHandler>>,
        enable_provisional: bool,
        running: Arc<RwLock<bool>>,
    ) -> NodeResult<()> {
        while *running.read().await {
            let messages = pull_batch(&consumer, batch_size, Duration::from_secs(1)).await?;
            for msg in messages {
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
    ) -> NodeResult<()> {
        let event = match Self::parse_provisional_event(&msg) {
            Ok(event) => event,
            Err(e) => {
                error!("Failed to parse provisional event: {e}");
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

        // Memory protection: if buffer is full, NAK to apply backpressure
        if !buffer.add_provisional(event.clone()).await {
            warn!(
                event_id = %event.event_id,
                "Buffer at capacity, NAKing message to apply backpressure"
            );
            let nak_delay = Some(std::time::Duration::from_millis(500));
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

        let mut handler_success = true;
        if enable_provisional
            && let Some(handler) = provisional_handler
            && let Err(e) = handler.handle_provisional(&event).await
        {
            warn!("Provisional handler failed: {e}");
            handler_success = false;
        }

        if handler_success {
            msg.ack().await.map_err(|error| {
                Self::message_settlement_error(
                    "failed to ack provisional message",
                    &msg,
                    Some(event.event_id),
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
                    Some(event.event_id),
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
    ) -> NodeResult<()> {
        while *running.read().await {
            let messages = pull_batch(&consumer, batch_size, Duration::from_secs(1)).await?;
            for msg in messages {
                Self::handle_confirmation_message(
                    msg,
                    &buffer,
                    &*confirmed_handler,
                    provisional_handler.as_ref(),
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
    ) -> NodeResult<()> {
        let confirmation = match Self::parse_confirmation(&msg) {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to parse confirmation: {e}");
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
                Err(e) => {
                    error!("Confirmed handler failed: {e}");
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
    ) -> NodeResult<()> {
        let mut ticker = tokio::time::interval(interval);

        while *running.read().await {
            ticker.tick().await;

            let timed_out_ids = buffer.check_timeouts().await;
            if !timed_out_ids.is_empty() {
                warn!(
                    timed_out = timed_out_ids.len(),
                    "Found timed-out provisional events; retaining them during the confirmation grace period"
                );

                for event_id in timed_out_ids {
                    if let Some(handler) = provisional_handler.as_ref()
                        && let Err(e) = handler.rollback_provisional(event_id).await
                    {
                        error!("Failed to rollback provisional event {event_id}: {e}");
                    }
                }
            }

            let purged_events = buffer.purge_expired().await;
            if !purged_events.is_empty() {
                info!(
                    purged = purged_events.len(),
                    "Purged timed-out provisional events after confirmation grace period"
                );
            }
        }

        Ok(())
    }

    fn parse_provisional_event(msg: &jetstream::Message) -> NodeResult<ProvisionalEvent> {
        let payload: serde_json::Value = serde_json::from_slice(&msg.payload)?;

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
            // ts_orig is optional in the Event schema; fall back to now (matching ingestd behavior)
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

    fn parse_confirmation(msg: &jetstream::Message) -> NodeResult<EventConfirmation> {
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

        Ok(EventConfirmation {
            event_id,
            persisted,
            ts_ingest,
        })
    }
}

#[cfg(test)]
mod tests {
    // Small inline tests are justified here because they target private background-task
    // exit classification logic that is not exposed through the public consumer API.
    use super::JetStreamEventConsumer;
    use sinex_primitives::SinexError;
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
}

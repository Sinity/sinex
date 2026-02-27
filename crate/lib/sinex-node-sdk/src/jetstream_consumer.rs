//! JetStream event consumer for automata
//!
//! This module provides a consumer that subscribes to JetStream events
//! and handles provisional/confirmed event processing with proper buffering.

use crate::confirmation_handler::{
    ConfirmationBuffer, ConfirmedEventHandler, EventConfirmation, ProcessingModel,
    ProvisionalEvent, ProvisionalEventHandler,
};
use crate::{NodeResult, SinexError};
use async_nats::jetstream;
use async_nats::jetstream::consumer::PullConsumer;
use futures::StreamExt;
use sinex_primitives::{
    domain::{EventSource, EventType},
    environment::SinexEnvironment,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Configuration for JetStream event consumer
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

/// JetStream event consumer for automata
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
    /// Create a new JetStream event consumer
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

    /// Create a new JetStream event consumer with an optional namespace.
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
        let running = self.running.clone();

        let provisional_task = tokio::spawn(async move {
            Self::consume_raw_events(
                raw_consumer,
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

        let confirmation_task = tokio::spawn(async move {
            Self::consume_confirmations(
                confirmations_consumer,
                confirmations_buffer,
                confirmed_handler,
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
                error!("Provisional events task stopped: {:?}", result);
                // Abort remaining tasks
                confirmation_abort.abort();
                timeout_abort.abort();
            }
            result = confirmation_task => {
                error!("Confirmation task stopped: {:?}", result);
                // Abort remaining tasks
                provisional_abort.abort();
                timeout_abort.abort();
            }
            result = timeout_task => {
                error!("Timeout check task stopped: {:?}", result);
                // Abort remaining tasks
                provisional_abort.abort();
                confirmation_abort.abort();
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
    ) -> NodeResult<PullConsumer> {
        let stream = js.get_stream(stream_name).await.map_err(|e| {
            SinexError::processing(format!("Failed to get stream {stream_name}: {e}"))
        })?;

        // Use the filter subject as provided; it already contains environment and namespace prefixes.
        let filter_subject = filter.to_string();
        let ack_wait = Duration::from_secs(30);
        let max_ack_pending = self.config.max_ack_pending;

        let mut consumer = stream
            .get_or_create_consumer(
                &self.config.consumer_name,
                jetstream::consumer::pull::Config {
                    durable_name: Some(self.config.consumer_name.clone()),
                    filter_subject: filter_subject.clone(),
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    ack_wait,
                    max_ack_pending,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| {
                SinexError::processing(format!("Failed to get or create consumer: {e}"))
            })?;

        let info = consumer
            .info()
            .await
            .map_err(|e| SinexError::processing(format!("Failed to read consumer info: {e}")))?;
        self.validate_consumer_config(
            stream_name,
            &filter_subject,
            &info.config,
            ack_wait,
            max_ack_pending,
        )?;

        Ok(consumer)
    }

    fn validate_consumer_config(
        &self,
        stream_name: &str,
        filter_subject: &str,
        config: &jetstream::consumer::Config,
        ack_wait: Duration,
        max_ack_pending: i64,
    ) -> NodeResult<()> {
        let mut mismatches = Vec::new();
        let expected_name = self.config.consumer_name.as_str();

        if config.durable_name.as_deref() != Some(expected_name) {
            mismatches.push(format!(
                "durable_name expected {}, got {:?}",
                expected_name, config.durable_name
            ));
        }
        if config.filter_subject != filter_subject {
            mismatches.push(format!(
                "filter_subject expected {}, got {}",
                filter_subject, config.filter_subject
            ));
        }
        if config.ack_policy != jetstream::consumer::AckPolicy::Explicit {
            mismatches.push(format!(
                "ack_policy expected Explicit, got {:?}",
                config.ack_policy
            ));
        }
        if config.ack_wait != ack_wait {
            mismatches.push(format!(
                "ack_wait expected {:?}, got {:?}",
                ack_wait, config.ack_wait
            ));
        }
        if config.max_ack_pending != max_ack_pending {
            mismatches.push(format!(
                "max_ack_pending expected {}, got {}",
                max_ack_pending, config.max_ack_pending
            ));
        }
        if config.deliver_subject.is_some() {
            mismatches.push("deliver_subject expected None for pull consumer".to_string());
        }

        if mismatches.is_empty() {
            return Ok(());
        }

        Err(SinexError::processing(format!(
            "Consumer config mismatch for stream {} ({}): {}",
            stream_name,
            expected_name,
            mismatches.join(", ")
        )))
    }

    async fn consume_raw_events(
        consumer: PullConsumer,
        buffer: Arc<ConfirmationBuffer>,
        provisional_handler: Option<Arc<dyn ProvisionalEventHandler>>,
        enable_provisional: bool,
        running: Arc<RwLock<bool>>,
    ) -> NodeResult<()> {
        let mut messages = consumer
            .messages()
            .await
            .map_err(|e| SinexError::processing(format!("Failed to get messages: {e}")))?;

        while *running.read().await {
            match messages.next().await {
                Some(Ok(msg)) => {
                    Self::handle_raw_message(
                        msg,
                        &buffer,
                        &provisional_handler,
                        enable_provisional,
                    )
                    .await;
                }
                Some(Err(e)) => {
                    error!("Error receiving message: {}", e);
                }
                None => {
                    debug!("No more messages");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Handle a single raw JetStream message: parse, buffer, ack/nak.
    async fn handle_raw_message(
        msg: jetstream::Message,
        buffer: &ConfirmationBuffer,
        provisional_handler: &Option<Arc<dyn ProvisionalEventHandler>>,
        enable_provisional: bool,
    ) {
        let event = match Self::parse_provisional_event(&msg) {
            Ok(event) => event,
            Err(e) => {
                error!("Failed to parse provisional event: {}", e);
                if let Err(ack_err) = msg.ack().await {
                    error!("Failed to ack bad message: {}", ack_err);
                }
                return;
            }
        };

        // Memory protection: if buffer is full, NAK to apply backpressure
        if !buffer.add_provisional(event.clone()).await {
            warn!(
                event_id = %event.event_id,
                "Buffer at capacity, NAKing message to apply backpressure"
            );
            let nak_delay = Some(std::time::Duration::from_millis(500));
            if let Err(e) = msg
                .ack_with(async_nats::jetstream::AckKind::Nak(nak_delay))
                .await
            {
                error!("Failed to NAK message during backpressure: {}", e);
            }
            return;
        }

        let mut handler_success = true;
        if enable_provisional && let Some(handler) = provisional_handler && let Err(e) = handler.handle_provisional(&event).await {
            warn!("Provisional handler failed: {}", e);
            handler_success = false;
        }

        if handler_success {
            if let Err(e) = msg.ack().await {
                error!("Failed to ack message: {}", e);
            }
        } else {
            let _ = msg
                .ack_with(async_nats::jetstream::AckKind::Nak(Some(
                    Duration::from_secs(5),
                )))
                .await;
        }
    }

    async fn consume_confirmations(
        consumer: PullConsumer,
        buffer: Arc<ConfirmationBuffer>,
        confirmed_handler: Arc<dyn ConfirmedEventHandler>,
        running: Arc<RwLock<bool>>,
    ) -> NodeResult<()> {
        let mut messages = consumer
            .messages()
            .await
            .map_err(|e| SinexError::processing(format!("Failed to get messages: {e}")))?;

        while *running.read().await {
            match messages.next().await {
                Some(Ok(msg)) => {
                    Self::handle_confirmation_message(msg, &buffer, &*confirmed_handler).await;
                }
                Some(Err(e)) => {
                    error!("Error receiving confirmation: {}", e);
                }
                None => {
                    debug!("No more confirmations");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Handle a single confirmation message: match to buffered event, dispatch, ack/nak.
    async fn handle_confirmation_message(
        msg: jetstream::Message,
        buffer: &ConfirmationBuffer,
        confirmed_handler: &dyn ConfirmedEventHandler,
    ) {
        let confirmation = match Self::parse_confirmation(&msg) {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to parse confirmation: {}", e);
                if let Err(ack_err) = msg.ack().await {
                    error!("Failed to ack bad confirmation: {}", ack_err);
                }
                return;
            }
        };

        if let Some(event) = buffer.confirm(confirmation.event_id).await {
            let handler_success = match confirmed_handler.handle_confirmed(&event).await {
                Ok(()) => true,
                Err(e) => {
                    error!("Confirmed handler failed: {}", e);
                    false
                }
            };
            if handler_success {
                if let Err(e) = msg.ack().await {
                    error!("Failed to ack confirmation: {}", e);
                }
            } else {
                let _ = msg
                    .ack_with(async_nats::jetstream::AckKind::Nak(Some(
                        Duration::from_secs(5),
                    )))
                    .await;
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
            if let Err(e) = msg
                .ack_with(async_nats::jetstream::AckKind::Nak(Some(
                    Duration::from_millis(200),
                )))
                .await
            {
                error!("Failed to NAK early confirmation: {}", e);
            }
        }
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
                warn!("Found {} timed-out events", timed_out_ids.len());

                let timed_out_events = buffer.remove_timed_out(&timed_out_ids).await;

                for event_id in timed_out_ids {
                    if let Some(handler) = provisional_handler.as_ref() && let Err(e) = handler.rollback_provisional(event_id).await {
                        error!("Failed to rollback provisional event {}: {}", event_id, e);
                    }
                }

                info!("Removed {} timed-out events", timed_out_events.len());
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
        let source = EventSource::new(source);

        let event_type = payload["event_type"]
            .as_str()
            .ok_or_else(|| SinexError::processing("Missing event_type".to_string()))?;
        let event_type = EventType::new(event_type);

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
    use super::*;
    use async_trait::async_trait;
    use xtask::sandbox::{sinex_test, EphemeralNats};

    struct NoopHandler;

    #[async_trait]
    impl ProvisionalEventHandler for NoopHandler {
        async fn handle_provisional(&self, _event: &ProvisionalEvent) -> NodeResult<()> {
            Ok(())
        }

        async fn rollback_provisional(
            &self,
            _event_id: sinex_primitives::ids::Id<sinex_primitives::events::Event>,
        ) -> NodeResult<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl ConfirmedEventHandler for NoopHandler {
        async fn handle_confirmed(&self, _event: &ProvisionalEvent) -> NodeResult<()> {
            Ok(())
        }
    }

    #[sinex_test]
    async fn test_consumer_config_defaults() -> TestResult<()> {
        let config = JetStreamEventConsumerConfig::default();
        assert_eq!(config.processing_model, ProcessingModel::StatelessWorker);
        assert_eq!(config.batch_size, 100);
        assert!(!config.enable_provisional_processing);
        Ok(())
    }

    #[sinex_test]
    async fn running_flag_clears_after_startup_failure() -> TestResult<()> {
        let nats = EphemeralNats::start().await?;
        let client = nats.connect().await?;
        let env = sinex_primitives::environment::environment().clone();
        let handler = Arc::new(NoopHandler);
        let consumer = JetStreamEventConsumer::new(
            client,
            env,
            JetStreamEventConsumerConfig::default(),
            handler,
            None,
        );

        let first = tokio::time::timeout(Duration::from_secs(5), consumer.run()).await?;
        assert!(first.is_err());

        let second = tokio::time::timeout(Duration::from_secs(5), consumer.run()).await?;
        if let Err(SinexError::Lifecycle(details)) = second {
            assert_ne!(details.message(), "Consumer already running");
        }

        Ok(())
    }
}

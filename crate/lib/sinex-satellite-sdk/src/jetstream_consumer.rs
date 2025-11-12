//! JetStream event consumer for automata
//!
//! This module provides a consumer that subscribes to JetStream events
//! and handles provisional/confirmed event processing with proper buffering.

use crate::confirmation_handler::{
    ConfirmationBuffer, ConfirmedEventHandler, EventConfirmation, ProcessingModel,
    ProvisionalEvent, ProvisionalEventHandler,
};
use crate::{SatelliteError, SatelliteResult};
use async_nats::jetstream;
use async_nats::jetstream::consumer::PullConsumer;
use futures::StreamExt;
use sinex_core::environment::SinexEnvironment;
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
        let confirmation_buffer = Arc::new(ConfirmationBuffer::new(config.confirmation_timeout));

        Self {
            nats_client,
            env,
            config,
            confirmed_handler,
            provisional_handler,
            confirmation_buffer,
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Start consuming events
    pub async fn run(&self) -> SatelliteResult<()> {
        {
            let mut running = self.running.write().await;
            if *running {
                return Err(SatelliteError::Lifecycle(
                    "Consumer already running".to_string(),
                ));
            }
            *running = true;
        }

        info!(
            "Starting JetStream event consumer: {}",
            self.config.consumer_name
        );

        let js = jetstream::new(self.nats_client.clone());

        let raw_stream = self.env.nats_subject("events_raw");
        let confirmations_stream = self.env.nats_subject("events_confirmations");

        let raw_consumer = self
            .create_or_get_consumer(&js, &raw_stream, "events.raw.>")
            .await?;
        let confirmations_consumer = self
            .create_or_get_consumer(&js, &confirmations_stream, "events.confirmations.>")
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

        tokio::select! {
            result = provisional_task => {
                error!("Provisional events task stopped: {:?}", result);
            }
            result = confirmation_task => {
                error!("Confirmation task stopped: {:?}", result);
            }
            result = timeout_task => {
                error!("Timeout check task stopped: {:?}", result);
            }
        }

        *self.running.write().await = false;
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
    ) -> SatelliteResult<PullConsumer> {
        let stream = js.get_stream(stream_name).await.map_err(|e| {
            SatelliteError::Processing(format!("Failed to get stream {}: {}", stream_name, e))
        })?;

        let consumer = stream
            .create_consumer(jetstream::consumer::pull::Config {
                name: Some(self.config.consumer_name.clone()),
                durable_name: Some(self.config.consumer_name.clone()),
                filter_subject: filter.to_string(),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                ack_wait: Duration::from_secs(30),
                max_ack_pending: 1000,
                ..Default::default()
            })
            .await
            .map_err(|e| SatelliteError::Processing(format!("Failed to create consumer: {}", e)))?;

        Ok(consumer)
    }

    async fn consume_raw_events(
        consumer: PullConsumer,
        buffer: Arc<ConfirmationBuffer>,
        provisional_handler: Option<Arc<dyn ProvisionalEventHandler>>,
        enable_provisional: bool,
        running: Arc<RwLock<bool>>,
    ) -> SatelliteResult<()> {
        let mut messages = consumer
            .messages()
            .await
            .map_err(|e| SatelliteError::Processing(format!("Failed to get messages: {}", e)))?;

        while *running.read().await {
            match messages.next().await {
                Some(Ok(msg)) => match Self::parse_provisional_event(&msg) {
                    Ok(event) => {
                        buffer.add_provisional(event.clone()).await;

                        if enable_provisional {
                            if let Some(ref handler) = provisional_handler {
                                if let Err(e) = handler.handle_provisional(&event).await {
                                    warn!("Provisional handler failed: {}", e);
                                }
                            }
                        }

                        if let Err(e) = msg.ack().await {
                            error!("Failed to ack message: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("Failed to parse provisional event: {}", e);
                        if let Err(ack_err) = msg.ack().await {
                            error!("Failed to ack bad message: {}", ack_err);
                        }
                    }
                },
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

    async fn consume_confirmations(
        consumer: PullConsumer,
        buffer: Arc<ConfirmationBuffer>,
        confirmed_handler: Arc<dyn ConfirmedEventHandler>,
        running: Arc<RwLock<bool>>,
    ) -> SatelliteResult<()> {
        let mut messages = consumer
            .messages()
            .await
            .map_err(|e| SatelliteError::Processing(format!("Failed to get messages: {}", e)))?;

        while *running.read().await {
            match messages.next().await {
                Some(Ok(msg)) => match Self::parse_confirmation(&msg) {
                    Ok(confirmation) => {
                        if let Some(event) = buffer.confirm(confirmation.event_id).await {
                            if let Err(e) = confirmed_handler.handle_confirmed(&event).await {
                                error!("Confirmed handler failed: {}", e);
                            }
                        } else {
                            debug!("Confirmation for unknown event: {}", confirmation.event_id);
                        }

                        if let Err(e) = msg.ack().await {
                            error!("Failed to ack confirmation: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("Failed to parse confirmation: {}", e);
                        if let Err(ack_err) = msg.ack().await {
                            error!("Failed to ack bad confirmation: {}", ack_err);
                        }
                    }
                },
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

    async fn check_timeouts(
        buffer: Arc<ConfirmationBuffer>,
        provisional_handler: Option<Arc<dyn ProvisionalEventHandler>>,
        running: Arc<RwLock<bool>>,
        interval: Duration,
    ) -> SatelliteResult<()> {
        let mut ticker = tokio::time::interval(interval);

        while *running.read().await {
            ticker.tick().await;

            let timed_out_ids = buffer.check_timeouts().await;
            if !timed_out_ids.is_empty() {
                warn!("Found {} timed-out events", timed_out_ids.len());

                let timed_out_events = buffer.remove_timed_out(&timed_out_ids).await;

                for event_id in timed_out_ids {
                    if let Some(ref handler) = provisional_handler {
                        if let Err(e) = handler.rollback_provisional(event_id).await {
                            error!("Failed to rollback provisional event {}: {}", event_id, e);
                        }
                    }
                }

                info!("Removed {} timed-out events", timed_out_events.len());
            }
        }

        Ok(())
    }

    fn parse_provisional_event(msg: &jetstream::Message) -> SatelliteResult<ProvisionalEvent> {
        let payload: serde_json::Value = serde_json::from_slice(&msg.payload)?;

        let event_id = payload["id"]
            .as_str()
            .ok_or_else(|| SatelliteError::Processing("Missing event id".to_string()))?
            .parse()
            .map_err(|_| SatelliteError::Processing("Invalid event id".to_string()))?;

        let source = payload["source"]
            .as_str()
            .ok_or_else(|| SatelliteError::Processing("Missing source".to_string()))?
            .to_string();

        let event_type = payload["event_type"]
            .as_str()
            .ok_or_else(|| SatelliteError::Processing("Missing event_type".to_string()))?
            .to_string();

        let ts_orig = payload["ts_orig"]
            .as_str()
            .ok_or_else(|| SatelliteError::Processing("Missing ts_orig".to_string()))?
            .parse()
            .map_err(|_| SatelliteError::Processing("Invalid ts_orig".to_string()))?;

        Ok(ProvisionalEvent {
            event_id,
            source,
            event_type,
            payload,
            ts_orig,
            received_at: chrono::Utc::now(),
        })
    }

    fn parse_confirmation(msg: &jetstream::Message) -> SatelliteResult<EventConfirmation> {
        let payload: serde_json::Value = serde_json::from_slice(&msg.payload)?;

        let event_id = payload["event_id"]
            .as_str()
            .ok_or_else(|| SatelliteError::Processing("Missing event_id".to_string()))?
            .parse()
            .map_err(|_| SatelliteError::Processing("Invalid event_id".to_string()))?;

        let persisted = payload["persisted"]
            .as_bool()
            .ok_or_else(|| SatelliteError::Processing("Missing persisted".to_string()))?;

        let ts_ingest = payload["ts_ingest"]
            .as_str()
            .ok_or_else(|| SatelliteError::Processing("Missing ts_ingest".to_string()))?
            .parse()
            .map_err(|_| SatelliteError::Processing("Invalid ts_ingest".to_string()))?;

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
    use sinex_test_utils::sinex_test;
    use sinex_test_utils::TestResult;

    #[allow(dead_code)]
    #[sinex_test]
    async fn test_consumer_config_defaults() -> TestResult<()> {
        let config = JetStreamEventConsumerConfig::default();
        assert_eq!(config.processing_model, ProcessingModel::StatelessWorker);
        assert_eq!(config.batch_size, 100);
        assert!(!config.enable_provisional_processing);
        Ok(())
    }
}

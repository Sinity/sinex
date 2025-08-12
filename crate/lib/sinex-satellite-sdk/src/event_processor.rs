//! Event processor that handles batching and sending events to either NATS or gRPC
//!
//! This module bridges the gap between the event channel and the actual transport
//! mechanism (NATS JetStream or gRPC to ingestd).

#[cfg(feature = "nats-bypass")]
use crate::nats::publisher::NatsPublisher;
use crate::{grpc_client::IngestClient, SatelliteResult};
use sinex_core::RawEvent;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// Event transport mechanism
#[derive(Debug, Clone)]
pub enum EventTransport {
    /// Direct NATS JetStream publishing - DEPRECATED: Bypasses ingestd single-writer principle
    #[cfg(feature = "nats-bypass")]
    Nats(NatsPublisher),
    /// gRPC to ingestd (which then publishes to NATS)
    Grpc(IngestClient),
}

/// Configuration for event processing
#[derive(Debug, Clone)]
pub struct EventProcessorConfig {
    /// Maximum batch size before sending
    pub batch_size: usize,
    /// Maximum time to wait before sending a partial batch
    pub batch_timeout: Duration,
    /// Whether to retry failed sends
    pub retry_on_failure: bool,
    /// Maximum retries for failed batches
    pub max_retries: u32,
}

impl Default for EventProcessorConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            batch_timeout: Duration::from_secs(5),
            retry_on_failure: true,
            max_retries: 3,
        }
    }
}

/// Event processor that handles batching and sending
pub struct EventProcessor {
    transport: EventTransport,
    config: EventProcessorConfig,
    event_receiver: mpsc::UnboundedReceiver<RawEvent>,
    shutdown: tokio::sync::oneshot::Receiver<()>,
}

impl EventProcessor {
    /// Create a new event processor
    pub fn new(
        transport: EventTransport,
        config: EventProcessorConfig,
        event_receiver: mpsc::UnboundedReceiver<RawEvent>,
        shutdown: tokio::sync::oneshot::Receiver<()>,
    ) -> Self {
        Self {
            transport,
            config,
            event_receiver,
            shutdown,
        }
    }

    /// Run the event processing loop
    pub async fn run(mut self) -> SatelliteResult<()> {
        info!(
            transport = ?self.transport,
            batch_size = self.config.batch_size,
            batch_timeout_secs = self.config.batch_timeout.as_secs(),
            "Starting event processor"
        );

        let mut batch = Vec::with_capacity(self.config.batch_size);
        let mut ticker = interval(self.config.batch_timeout);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                // Receive events from channel
                Some(event) = self.event_receiver.recv() => {
                    batch.push(event);

                    // Send batch if it's full
                    if batch.len() >= self.config.batch_size {
                        self.send_batch(&mut batch).await;
                    }
                }

                // Timeout - send partial batch
                _ = ticker.tick() => {
                    if !batch.is_empty() {
                        self.send_batch(&mut batch).await;
                    }
                }

                // Shutdown signal
                _ = &mut self.shutdown => {
                    info!("Received shutdown signal");

                    // Send any remaining events
                    if !batch.is_empty() {
                        self.send_batch(&mut batch).await;
                    }

                    break;
                }

                // Channel closed
                else => {
                    warn!("Event channel closed");

                    // Send any remaining events
                    if !batch.is_empty() {
                        self.send_batch(&mut batch).await;
                    }

                    break;
                }
            }
        }

        info!("Event processor stopped");
        Ok(())
    }

    /// Send a batch of events
    async fn send_batch(&mut self, batch: &mut Vec<RawEvent>) {
        if batch.is_empty() {
            return;
        }

        let batch_size = batch.len();
        debug!("Sending batch of {} events", batch_size);

        let mut retry_count = 0;
        let mut success = false;

        while retry_count <= self.config.max_retries && !success {
            success = match &mut self.transport {
                #[cfg(feature = "nats-bypass")]
                EventTransport::Nats(publisher) => Self::send_batch_nats(publisher, &batch).await,
                EventTransport::Grpc(client) => Self::send_batch_grpc(client, &batch).await,
            };

            if !success && self.config.retry_on_failure && retry_count < self.config.max_retries {
                retry_count += 1;
                let delay = Duration::from_millis(100 * (1 << retry_count));
                warn!(
                    "Batch send failed, retrying in {:?} (attempt {}/{})",
                    delay, retry_count, self.config.max_retries
                );
                tokio::time::sleep(delay).await;
            } else {
                break;
            }
        }

        if success {
            debug!("Successfully sent batch of {} events", batch_size);
            batch.clear();
        } else {
            error!(
                "Failed to send batch of {} events after {} retries",
                batch_size, retry_count
            );
            // Store failed events in dead letter queue for later retry
            if let Err(e) = Self::store_dead_letter_events(&batch).await {
                error!("Failed to store events in dead letter queue: {}", e);
            }
            batch.clear();
        }
    }

    /// Store failed events in dead letter queue
    async fn store_dead_letter_events(
        events: &[RawEvent],
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Write to local file for now - could be enhanced with database storage
        let dead_letter_path = std::env::temp_dir().join("sinex_dead_letter_events.json");
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&dead_letter_path)
            .await?;

        for event in events {
            let json = serde_json::to_string(event)?;
            file.write_all(json.as_bytes()).await?;
            file.write_all(b"\n").await?;
        }

        info!(
            "Stored {} events in dead letter queue at {:?}",
            events.len(),
            dead_letter_path
        );
        Ok(())
    }

    /// Send batch via NATS - DEPRECATED: Bypasses ingestd single-writer principle
    #[cfg(feature = "nats-bypass")]
    async fn send_batch_nats(publisher: &NatsPublisher, events: &[RawEvent]) -> bool {
        let mut all_success = true;

        for event in events {
            match publisher.publish_event(event).await {
                Ok(ack) => {
                    debug!(
                        event_type = %event.event_type,
                        stream = %ack.stream,
                        sequence = ack.sequence,
                        "Event published to NATS"
                    );
                }
                Err(e) => {
                    error!(
                        event_type = %event.event_type,
                        error = %e,
                        "Failed to publish event to NATS"
                    );
                    all_success = false;
                }
            }
        }

        all_success
    }

    /// Send batch via gRPC
    async fn send_batch_grpc(client: &mut IngestClient, events: &[RawEvent]) -> bool {
        match client.ingest_batch(events).await {
            Ok(result) => {
                if result.success {
                    debug!(
                        processed = result.processed_count,
                        failed = result.failed_count,
                        "Batch sent via gRPC"
                    );
                    true
                } else {
                    error!(
                        processed = result.processed_count,
                        failed = result.failed_count,
                        error = ?result.error,
                        "Batch processing failed"
                    );
                    false
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to send batch via gRPC");
                false
            }
        }
    }
}

/// Spawn the event processor as a background task
pub fn spawn_event_processor(
    transport: EventTransport,
    config: EventProcessorConfig,
    event_receiver: mpsc::UnboundedReceiver<RawEvent>,
    shutdown: tokio::sync::oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<SatelliteResult<()>> {
    tokio::spawn(async move {
        let processor = EventProcessor::new(transport, config, event_receiver, shutdown);
        processor.run().await
    })
}

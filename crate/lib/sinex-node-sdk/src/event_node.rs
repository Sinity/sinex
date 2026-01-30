//! Event processor that handles batching and sending events.

use crate::{nats_publisher::NatsPublisher, NodeResult};
use serde::{Deserialize, Serialize};
use sinex_primitives::events::Event;
use sinex_primitives::{environment, JsonValue, Ulid};
use std::path::Path;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::io::{self, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// Event transport mechanism
#[derive(Clone)]
pub enum EventTransport {
    /// Direct NATS JetStream publishing
    Nats(Arc<NatsPublisher>),
}

impl std::fmt::Debug for EventTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventTransport::Nats(_) => write!(f, "EventTransport::Nats"),
        }
    }
}

impl EventTransport {
    /// Send an event to the Dead Letter Queue
    ///
    /// This method is used when processing fails and the event should be
    /// preserved for later retry or manual inspection.
    pub async fn send_to_dlq(
        &self,
        event: &Event<JsonValue>,
        error: &str,
        processor_name: &str,
    ) -> NodeResult<()> {
        match self {
            EventTransport::Nats(publisher) => publisher
                .publish_to_dlq(event, error, processor_name)
                .await
                .map_err(|e| {
                    crate::SinexError::processing(format!("Failed to send to DLQ: {}", e))
                }),
        }
    }
}

/// Configuration for the EventBatcher
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventBatcherConfig {
    /// Maximum size of a batch
    pub batch_size: usize,
    /// Maximum time to wait for a batch to fill
    pub batch_timeout_ms: u64,
}

impl Default for EventBatcherConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            batch_timeout_ms: 1000,
        }
    }
}

#[derive(Debug, Default)]
struct EventBatcherStats {
    batches_sent: AtomicU64,
    events_sent: AtomicU64,
    publish_failures: AtomicU64,
    dlq_write_failures: AtomicU64,
}

impl EventBatcherStats {
    fn log(&self) {
        info!(
            batches_sent = self.batches_sent.load(Ordering::Relaxed),
            events_sent = self.events_sent.load(Ordering::Relaxed),
            publish_failures = self.publish_failures.load(Ordering::Relaxed),
            dlq_write_failures = self.dlq_write_failures.load(Ordering::Relaxed),
            "Event processor stats"
        );
    }
}

struct BatchPublishResult {
    published: usize,
    failed: usize,
}

/// Event processor that handles batching and sending
pub struct EventBatcher {
    transport: EventTransport,
    config: EventBatcherConfig,
    event_receiver: mpsc::Receiver<Event<JsonValue>>,
    shutdown: tokio::sync::oneshot::Receiver<()>,
    stats: Arc<EventBatcherStats>,
}

impl EventBatcher {
    /// Create a new event processor
    pub fn new(
        transport: EventTransport,
        config: EventBatcherConfig,
        event_receiver: mpsc::Receiver<Event<JsonValue>>,
        shutdown: tokio::sync::oneshot::Receiver<()>,
    ) -> Self {
        Self {
            transport,
            config,
            event_receiver,
            shutdown,
            stats: Arc::new(EventBatcherStats::default()),
        }
    }

    /// Run the event processing loop
    pub async fn run(mut self) -> NodeResult<()> {
        info!(
            transport = ?self.transport,
            batch_size = self.config.batch_size,
            batch_timeout_ms = self.config.batch_timeout_ms,
            "Starting event processor"
        );

        let mut batch = Vec::with_capacity(self.config.batch_size);
        let mut ticker = interval(Duration::from_millis(self.config.batch_timeout_ms));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let stats = self.stats.clone();
        let mut stats_ticker = interval(Duration::from_secs(60));
        stats_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                // Receive events from channel
                Some(event) = self.event_receiver.recv() => {
                    batch.push(event);

                    // Send batch if it's full
                    if batch.len() >= self.config.batch_size {
                        self.send_batch(&mut batch).await?;
                    }
                }

                // Timeout - send partial batch
                _ = ticker.tick() => {
                    if !batch.is_empty() {
                        self.send_batch(&mut batch).await?;
                    }
                }

                // Shutdown signal
                _ = &mut self.shutdown => {
                    info!("Received shutdown signal");

                    // Send any remaining events
                    if !batch.is_empty() {
                        self.send_batch(&mut batch).await?;
                    }

                    break;
                }

                _ = stats_ticker.tick() => {
                    stats.log();
                }

                // Channel closed
                else => {
                    warn!("Event channel closed");

                    // Send any remaining events
                    if !batch.is_empty() {
                        self.send_batch(&mut batch).await?;
                    }

                    break;
                }
            }
        }

        info!("Event processor stopped");
        Ok(())
    }

    /// Send a batch of events
    async fn send_batch(&mut self, batch: &mut Vec<Event<JsonValue>>) -> NodeResult<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let batch_size = batch.len();
        debug!("Sending batch of {} events", batch_size);

        // The original code had retry logic here, but the new config doesn't include retry settings.
        // Assuming retries are now handled at a lower level or removed for this component.
        // For now, we'll attempt to send once. If retries are needed, they should be re-added
        // with appropriate configuration.

        let result = match &mut self.transport {
            EventTransport::Nats(publisher) => Self::send_batch_nats(publisher, batch).await,
        };

        if result.published > 0 {
            self.stats
                .events_sent
                .fetch_add(result.published as u64, Ordering::Relaxed);
        }

        if result.failed == 0 {
            self.stats.batches_sent.fetch_add(1, Ordering::Relaxed);
            debug!("Successfully sent batch of {} events", batch_size);
            batch.clear();
            return Ok(());
        }

        error!(
            batch_size,
            failed = batch.len(),
            "Failed to send batch; routing failures to DLQ"
        );
        self.stats
            .publish_failures
            .fetch_add(batch.len() as u64, Ordering::Relaxed);
        // Store failed events in dead letter queue for later retry.
        if let Err(e) = Self::store_dead_letter_events(batch).await {
            self.stats
                .dlq_write_failures
                .fetch_add(batch.len() as u64, Ordering::Relaxed);
            error!(
                dlq_events = batch.len(),
                error = %e,
                "Failed to store events in dead letter queue"
            );
            return Err(e);
        }

        batch.clear();
        Ok(())
    }

    /// Store failed events in dead letter queue
    async fn store_dead_letter_events(events: &[Event<JsonValue>]) -> NodeResult<()> {
        // Write to local file for now - could be enhanced with database storage
        let dead_letter_path = environment()
            .temp_dir()
            .join("sinex_dead_letter_events.json");
        Self::store_dead_letter_events_at_path(events, &dead_letter_path).await
    }

    async fn store_dead_letter_events_at_path(
        events: &[Event<JsonValue>],
        dead_letter_path: &Path,
    ) -> NodeResult<()> {
        let parent_dir = dead_letter_path.parent().unwrap_or_else(|| Path::new("."));
        tokio::fs::create_dir_all(parent_dir).await?;
        let temp_path = parent_dir.join(format!(".sinex_dead_letter_events.{}.tmp", Ulid::new()));

        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)
            .await?;

        if tokio::fs::metadata(dead_letter_path).await.is_ok() {
            let mut existing = tokio::fs::OpenOptions::new()
                .read(true)
                .open(dead_letter_path)
                .await?;
            io::copy(&mut existing, &mut file).await?;
        }

        for event in events {
            let json = serde_json::to_string(event)?;
            file.write_all(json.as_bytes()).await?;
            file.write_all(b"\n").await?;
        }

        file.flush().await?;
        file.sync_all().await?;
        tokio::fs::rename(&temp_path, dead_letter_path).await?;

        info!(
            dlq_events = events.len(),
            path = ?dead_letter_path,
            "Stored events in dead letter queue"
        );
        Ok(())
    }

    /// Send batch via NATS JetStream
    async fn send_batch_nats(
        publisher: &NatsPublisher,
        events: &mut Vec<Event<JsonValue>>,
    ) -> BatchPublishResult {
        let mut success_count = 0;
        let mut failure_count = 0;
        let mut idx = 0;

        while idx < events.len() {
            let publish_result = {
                let event = &events[idx];
                publisher.publish(event).await
            };

            match publish_result {
                Ok(_) => {
                    success_count += 1;
                    events.remove(idx);
                }
                Err(e) => {
                    error!(event_id = ?events[idx].id, error = %e, "Failed to publish event");
                    failure_count += 1;
                    idx += 1;
                }
            }
        }

        if failure_count == 0 {
            debug!(published = success_count, "Batch sent via NATS");
        } else {
            error!(
                published = success_count,
                failed = failure_count,
                "Batch processing failed"
            );
        }

        BatchPublishResult {
            published: success_count,
            failed: failure_count,
        }
    }
}

/// Spawn the event batcher loop
pub fn spawn_event_processor(
    transport: EventTransport,
    config: EventBatcherConfig,
    event_receiver: mpsc::Receiver<Event<JsonValue>>,
    shutdown: tokio::sync::oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<NodeResult<()>> {
    tokio::spawn(async move {
        let processor = EventBatcher::new(transport, config, event_receiver, shutdown);
        processor.run().await
    })
}

#[cfg(test)]
mod tests {
    use super::EventBatcher;
    use sinex_primitives::{DynamicPayload, EventId, Provenance, Ulid};
    use std::fs;
    use tempfile::tempdir;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn dead_letter_write_failure_is_propagated() -> TestResult<()> {
        let temp_dir = tempdir()?;
        let dead_letter_path = temp_dir.path().join("sinex_dead_letter_events.json");
        let original_permissions = fs::metadata(temp_dir.path())?.permissions();
        let mut read_only = original_permissions.clone();
        read_only.set_readonly(true);
        fs::set_permissions(temp_dir.path(), read_only)?;

        let event = DynamicPayload::new(
            "dlq.test",
            "dead_letter.failure",
            serde_json::json!({"ok": true}),
        )
        .with_provenance(Provenance::from_synthesis_safe(
            EventId::from_ulid(Ulid::new()),
            Vec::new(),
        ))
        .build()
        .expect("infallible: test provenance set");
        let result =
            EventBatcher::store_dead_letter_events_at_path(&[event], &dead_letter_path).await;

        fs::set_permissions(temp_dir.path(), original_permissions)?;
        assert!(result.is_err());
        Ok(())
    }
}

//! Event processor that handles batching and sending events.

use crate::{nats_publisher::NatsPublisher, NodeResult};
use sinex_core::db::models::Event;
use sinex_core::{environment, JsonValue, Ulid};
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

#[derive(Debug, Default)]
struct EventProcessorStats {
    batches_sent: AtomicU64,
    events_sent: AtomicU64,
    publish_failures: AtomicU64,
    dlq_write_failures: AtomicU64,
}

impl EventProcessorStats {
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
pub struct EventProcessor {
    transport: EventTransport,
    config: EventProcessorConfig,
    event_receiver: mpsc::Receiver<Event<JsonValue>>,
    shutdown: tokio::sync::oneshot::Receiver<()>,
    stats: Arc<EventProcessorStats>,
}

impl EventProcessor {
    /// Create a new event processor
    pub fn new(
        transport: EventTransport,
        config: EventProcessorConfig,
        event_receiver: mpsc::Receiver<Event<JsonValue>>,
        shutdown: tokio::sync::oneshot::Receiver<()>,
    ) -> Self {
        Self {
            transport,
            config,
            event_receiver,
            shutdown,
            stats: Arc::new(EventProcessorStats::default()),
        }
    }

    /// Run the event processing loop
    pub async fn run(mut self) -> NodeResult<()> {
        info!(
            transport = ?self.transport,
            batch_size = self.config.batch_size,
            batch_timeout_secs = self.config.batch_timeout.as_secs(),
            "Starting event processor"
        );

        let mut batch = Vec::with_capacity(self.config.batch_size);
        let mut ticker = interval(self.config.batch_timeout);
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

        let mut retry_count = 0;

        while retry_count <= self.config.max_retries {
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

            if !self.config.retry_on_failure || retry_count >= self.config.max_retries {
                break;
            }

            retry_count += 1;
            let delay = Duration::from_millis(100 * (1 << retry_count));
            warn!(
                failed = batch.len(),
                "Batch send failed, retrying in {:?} (attempt {}/{})",
                delay,
                retry_count,
                self.config.max_retries
            );
            tokio::time::sleep(delay).await;
        }

        error!(
            batch_size,
            failed = batch.len(),
            retry_count,
            "Failed to send batch after retries; routing failures to DLQ"
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

/// Spawn the event processor as a background task
pub fn spawn_event_processor(
    transport: EventTransport,
    config: EventProcessorConfig,
    event_receiver: mpsc::Receiver<Event<JsonValue>>,
    shutdown: tokio::sync::oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<NodeResult<()>> {
    tokio::spawn(async move {
        let processor = EventProcessor::new(transport, config, event_receiver, shutdown);
        processor.run().await
    })
}

#[cfg(test)]
mod tests {
    use super::EventProcessor;
    use sinex_core::{EventBuilder, EventId, Provenance, Ulid};
    use sinex_test_utils::sinex_test;
    use std::fs;
    use tempfile::tempdir;

    #[sinex_test]
    async fn dead_letter_write_failure_is_propagated() -> TestResult<()> {
        let temp_dir = tempdir()?;
        let dead_letter_path = temp_dir.path().join("sinex_dead_letter_events.json");
        let original_permissions = fs::metadata(temp_dir.path())?.permissions();
        let mut read_only = original_permissions.clone();
        read_only.set_readonly(true);
        fs::set_permissions(temp_dir.path(), read_only)?;

        let event = EventBuilder::new(
            "dlq.test".into(),
            "dead_letter.failure".into(),
            serde_json::json!({"ok": true}),
        )
        .with_provenance(Provenance::from_synthesis_safe(
            EventId::from_ulid(Ulid::new()),
            Vec::new(),
        ))
        .build()
        .expect("infallible: test provenance set");
        let result =
            EventProcessor::store_dead_letter_events_at_path(&[event], &dead_letter_path).await;

        fs::set_permissions(temp_dir.path(), original_permissions)?;
        assert!(result.is_err());
        Ok(())
    }
}

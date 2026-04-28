//! Event batcher that handles batching and sending events.

use crate::{NodeResult, nats_publisher::NatsPublisher};
use serde::{Deserialize, Serialize};
use sinex_primitives::events::Event;
use sinex_primitives::{JsonValue, Uuid};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// Event transport mechanism
#[derive(Clone)]
pub enum EventTransport {
    /// Direct NATS `JetStream` publishing
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
    /// Send a failed event to the processing-failure stream.
    ///
    /// This is for derived/runtime processing failures, not the raw-ingest DLQ.
    pub async fn send_to_processing_failure_queue(
        &self,
        event: &Event<JsonValue>,
        error: &str,
        node_name: &str,
    ) -> NodeResult<()> {
        match self {
            EventTransport::Nats(publisher) => publisher
                .publish_processing_failure(event, error, node_name)
                .await
                .map_err(|e| e.with_context("operation", "send_to_processing_failure_queue")),
        }
    }
}

/// Configuration for the `EventBatcher`
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
    recovery_spool_write_failures: AtomicU64,
}

impl EventBatcherStats {
    fn log(&self) {
        info!(
            batches_sent = self.batches_sent.load(Ordering::Relaxed),
            events_sent = self.events_sent.load(Ordering::Relaxed),
            publish_failures = self.publish_failures.load(Ordering::Relaxed),
            recovery_spool_write_failures =
                self.recovery_spool_write_failures.load(Ordering::Relaxed),
            "Event batcher stats"
        );
    }
}

struct BatchPublishResult {
    published: usize,
    failed: usize,
}

/// Event batcher that handles batching and sending
pub struct EventBatcher {
    transport: EventTransport,
    config: EventBatcherConfig,
    event_receiver: mpsc::Receiver<Event<JsonValue>>,
    shutdown: tokio::sync::oneshot::Receiver<()>,
    stats: Arc<EventBatcherStats>,
    /// Persistent work directory used for the local recovery-spool file.
    ///
    /// This must be a directory that survives service restarts (i.e. **not** under a
    /// `PrivateTmp` systemd namespace). It is populated from the node's `NodeConfig::work_dir`
    /// by the runtime, which in turn reads `SINEX_WORK_DIR` / defaults to the system cache dir.
    work_dir: PathBuf,
}

impl EventBatcher {
    /// Create a new event batcher.
    ///
    /// `work_dir` must be a persistent directory that survives service restarts (i.e. **not**
    /// under `PrivateTmp`). It is used as the fallback write location when raw-event publishing
    /// fails. On creation, any leftover recovery-spool files from a previous run are detected
    /// and logged as warnings so operators know there are events that require manual attention.
    #[must_use]
    pub fn new(
        transport: EventTransport,
        config: EventBatcherConfig,
        event_receiver: mpsc::Receiver<Event<JsonValue>>,
        shutdown: tokio::sync::oneshot::Receiver<()>,
        work_dir: PathBuf,
    ) -> Self {
        let batcher = Self {
            transport,
            config,
            event_receiver,
            shutdown,
            stats: Arc::new(EventBatcherStats::default()),
            work_dir,
        };
        batcher.warn_leftover_recovery_spool();
        batcher
    }

    /// Check for leftover local recovery-spool files from a previous run and emit a warn-level log.
    ///
    /// Automatic replay happens in `run()` before the main batching loop starts; this warning keeps
    /// leftover state visible even when replay cannot fully recover it.
    fn warn_leftover_recovery_spool(&self) {
        let recovery_spool_path = self.recovery_spool_path();
        match std::fs::metadata(&recovery_spool_path) {
            Ok(meta) => {
                warn!(
                    path = ?recovery_spool_path,
                    bytes = meta.len(),
                    "Found leftover local recovery spool from a previous run; \
                     events in this file were not delivered to NATS and require manual attention"
                );
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // No leftover file — normal startup path.
            }
            Err(e) => {
                warn!(
                    path = ?recovery_spool_path,
                    error = %e,
                    "Could not check for leftover local recovery spool on startup"
                );
            }
        }
    }

    /// Return the canonical path for the local recovery spool in the node's work directory.
    fn recovery_spool_path(&self) -> PathBuf {
        self.work_dir.join("sinex_event_recovery_spool.jsonl")
    }

    /// Run the event batching loop
    pub async fn run(mut self) -> NodeResult<()> {
        info!(
            transport = ?self.transport,
            batch_size = self.config.batch_size,
            batch_timeout_ms = self.config.batch_timeout_ms,
            "Starting event batcher"
        );
        // Non-fatal: recovery spool replay failure should not prevent the batcher from starting.
        // Events that could not be replayed remain in the spool file for manual inspection.
        if let Err(e) = self.recover_recovery_spool_events().await {
            warn!(error = %e, "Recovery spool replay failed on startup; batcher will continue without it");
        }

        let mut batch = Vec::with_capacity(self.config.batch_size);
        let mut ticker = interval(Duration::from_millis(self.config.batch_timeout_ms));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let stats = self.stats.clone();
        let mut stats_ticker = interval(Duration::from_mins(1));
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

        info!("Event batcher stopped");
        Ok(())
    }

    async fn recover_recovery_spool_events(&self) -> NodeResult<()> {
        let recovery_spool_path = self.recovery_spool_path();

        let file = match tokio::fs::File::open(&recovery_spool_path).await {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                warn!(
                    path = ?recovery_spool_path,
                    error = %error,
                    "Could not open leftover local recovery spool for replay"
                );
                return Ok(());
            }
        };

        // Maximum number of non-recoverable (malformed or publish-failed) lines to preserve
        // in the rewritten spool file. Prevents unbounded growth when the spool file
        // accumulates repeated failures.
        const MAX_REMAINING_LINES: usize = 1_000;

        let mut lines = BufReader::new(file).lines();
        let mut remaining_lines = Vec::new();
        let mut recovered = 0_u64;
        let mut malformed = 0_u64;
        let mut discarded = 0_u64;

        while let Some(line) = lines.next_line().await? {
            if line.trim().is_empty() {
                continue;
            }

            let event = match serde_json::from_str::<Event<JsonValue>>(&line) {
                Ok(event) => event,
                Err(error) => {
                    malformed += 1;
                    if remaining_lines.len() >= MAX_REMAINING_LINES {
                        discarded += 1;
                        warn!(
                            path = ?recovery_spool_path,
                            error = %error,
                            "Discarding malformed recovery-spool entry (remaining-lines cap reached)"
                        );
                    } else {
                        warn!(
                            path = ?recovery_spool_path,
                            error = %error,
                            "Preserving malformed local recovery-spool entry during replay"
                        );
                        remaining_lines.push(line);
                    }
                    continue;
                }
            };

            let publish_result = match &self.transport {
                EventTransport::Nats(publisher) => publisher.publish(&event).await,
            };

            if let Err(error) = publish_result {
                if remaining_lines.len() >= MAX_REMAINING_LINES {
                    discarded += 1;
                    warn!(
                        path = ?recovery_spool_path,
                        event_id = ?event.id,
                        error = %error,
                        "Discarding unpublished recovery-spool entry (remaining-lines cap reached)"
                    );
                } else {
                    warn!(
                        path = ?recovery_spool_path,
                        event_id = ?event.id,
                        error = %error,
                        "Preserving recovery-spool entry after replay publish failure"
                    );
                    remaining_lines.push(line);
                }
                continue;
            }

            recovered += 1;
        }

        if remaining_lines.is_empty() {
            tokio::fs::remove_file(&recovery_spool_path).await?;
            info!(
                path = ?recovery_spool_path,
                recovered,
                malformed,
                discarded,
                "Replayed and removed leftover local recovery spool"
            );
            return Ok(());
        }

        Self::rewrite_recovery_spool_file(&remaining_lines, &recovery_spool_path).await?;
        warn!(
            path = ?recovery_spool_path,
            recovered,
            malformed,
            discarded,
            remaining = remaining_lines.len(),
            "Replayed local recovery spool partially; unreadable or unpublished entries were preserved"
        );
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
            "Failed to send batch; routing failures to local recovery spool"
        );
        self.stats
            .publish_failures
            .fetch_add(batch.len() as u64, Ordering::Relaxed);
        // Store failed raw events in the local recovery spool for later replay.
        let recovery_spool_path = self.recovery_spool_path();
        if let Err(e) = Self::store_recovery_spool_events(batch, &recovery_spool_path).await {
            self.stats
                .recovery_spool_write_failures
                .fetch_add(batch.len() as u64, Ordering::Relaxed);
            error!(
                recovery_spool_events = batch.len(),
                error = %e,
                "Failed to store events in local recovery spool"
            );
            return Err(e);
        }

        batch.clear();
        Ok(())
    }

    /// Store failed raw events in the local recovery spool at `recovery_spool_path`.
    ///
    /// The caller is responsible for providing a persistent path (not under `PrivateTmp`).
    async fn store_recovery_spool_events(
        events: &[Event<JsonValue>],
        recovery_spool_path: &Path,
    ) -> NodeResult<()> {
        warn!(
            path = ?recovery_spool_path,
            events = events.len(),
            "Writing failed events to local recovery spool"
        );
        Self::store_recovery_spool_events_at_path(events, recovery_spool_path).await
    }

    async fn store_recovery_spool_events_at_path(
        events: &[Event<JsonValue>],
        recovery_spool_path: &Path,
    ) -> NodeResult<()> {
        let parent_dir = recovery_spool_path.parent().unwrap_or_else(|| Path::new("."));
        tokio::fs::create_dir_all(parent_dir).await?;
        let temp_path = parent_dir.join(format!(
            ".sinex_event_recovery_spool.{}.tmp",
            Uuid::now_v7()
        ));

        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)
            .await?;

        if tokio::fs::metadata(recovery_spool_path).await.is_ok() {
            let mut existing = tokio::fs::OpenOptions::new()
                .read(true)
                .open(recovery_spool_path)
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
        tokio::fs::rename(&temp_path, recovery_spool_path).await?;

        info!(
            recovery_spool_events = events.len(),
            path = ?recovery_spool_path,
            "Stored events in local recovery spool"
        );
        Ok(())
    }

    async fn rewrite_recovery_spool_file(
        lines: &[String],
        recovery_spool_path: &Path,
    ) -> NodeResult<()> {
        let parent_dir = recovery_spool_path.parent().unwrap_or_else(|| Path::new("."));
        tokio::fs::create_dir_all(parent_dir).await?;
        let temp_path = parent_dir.join(format!(
            ".sinex_event_recovery_spool.{}.tmp",
            Uuid::now_v7()
        ));
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)
            .await?;

        for line in lines {
            file.write_all(line.as_bytes()).await?;
            file.write_all(b"\n").await?;
        }

        file.flush().await?;
        file.sync_all().await?;
        tokio::fs::rename(&temp_path, recovery_spool_path).await?;
        Ok(())
    }

    /// Send batch via NATS `JetStream`
    async fn send_batch_nats(
        publisher: &NatsPublisher,
        events: &mut Vec<Event<JsonValue>>,
    ) -> BatchPublishResult {
        let mut success_count = 0;
        let mut failure_count = 0;
        let mut failed_events = Vec::new();

        for event in events.drain(..) {
            match publisher.publish(&event).await {
                Ok(()) => {
                    success_count += 1;
                }
                Err(e) => {
                    error!(event_id = ?event.id, error = %e, "Failed to publish event");
                    failure_count += 1;
                    failed_events.push(event);
                }
            }
        }

        *events = failed_events;

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

/// Spawn the event batcher loop.
///
/// `work_dir` must point to a directory that persists across service restarts so that any
/// local recovery-spool files survive a `PrivateTmp`-scoped restart and can be inspected.
#[must_use]
pub fn spawn_event_batcher(
    transport: EventTransport,
    config: EventBatcherConfig,
    event_receiver: mpsc::Receiver<Event<JsonValue>>,
    shutdown: tokio::sync::oneshot::Receiver<()>,
    work_dir: PathBuf,
) -> tokio::task::JoinHandle<NodeResult<()>> {
    tokio::spawn(async move {
        let batcher = EventBatcher::new(transport, config, event_receiver, shutdown, work_dir);
        batcher.run().await
    })
}

#[cfg(test)]
mod tests {
    use super::{EventBatcher, EventBatcherConfig, EventTransport};
    use crate::nats_publisher::NatsPublisher;
    use async_nats::jetstream;
    use futures::StreamExt;
    use sinex_primitives::{
        DynamicPayload, Id, JsonValue, Uuid,
        events::{Event, EventId},
    };
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::sync::{mpsc, oneshot};
    use xtask::sandbox::{TestResult, sinex_test};

    async fn remove_if_exists(path: &Path) -> TestResult<()> {
        match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn ensure_events_stream(
        client: &async_nats::Client,
        env: &sinex_primitives::environment::SinexEnvironment,
    ) -> TestResult<()> {
        jetstream::new(client.clone())
            .get_or_create_stream(jetstream::stream::Config {
                name: env.nats_stream_name("EVENTS"),
                subjects: vec![env.nats_subject("events.raw.>")],
                storage: jetstream::stream::StorageType::Memory,
                ..Default::default()
            })
            .await?;
        Ok(())
    }

    fn test_event(name: &str, ok: bool) -> sinex_primitives::Result<Event<JsonValue>> {
        let mut event = DynamicPayload::new("dlq.test", name, serde_json::json!({ "ok": ok }))
            .from_parents([EventId::from_uuid(Uuid::now_v7())])?
            .build()?;
        event.id = Some(Id::new());
        Ok(event)
    }

    #[sinex_test]
    async fn recovery_spool_write_failure_is_propagated() -> TestResult<()> {
        let temp_dir = tempdir()?;
        let recovery_spool_path = temp_dir.path().join("sinex_event_recovery_spool.jsonl");
        let original_permissions = fs::metadata(temp_dir.path())?.permissions();
        let mut read_only = original_permissions.clone();
        read_only.set_readonly(true);
        fs::set_permissions(temp_dir.path(), read_only)?;

        let event = DynamicPayload::new(
            "dlq.test",
            "recovery_spool.failure",
            serde_json::json!({"ok": true}),
        )
        .from_parents([EventId::from_uuid(Uuid::now_v7())])?
        .build()
        .expect("infallible: test provenance set");
        let result =
            EventBatcher::store_recovery_spool_events_at_path(&[event], &recovery_spool_path)
                .await;

        fs::set_permissions(temp_dir.path(), original_permissions)?;
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    async fn recovery_spool_write_uses_provided_work_directory() -> TestResult<()> {
        let temp_dir = tempdir()?;
        let work_dir = temp_dir.path().to_path_buf();
        let recovery_spool_path = work_dir.join("sinex_event_recovery_spool.jsonl");

        let event = DynamicPayload::new(
            "dlq.test",
            "recovery_spool.path",
            serde_json::json!({"ok": true}),
        )
        .from_parents([EventId::from_uuid(Uuid::now_v7())])?
        .build()
        .expect("infallible: test provenance set");

        remove_if_exists(&recovery_spool_path).await?;
        EventBatcher::store_recovery_spool_events(&[event], &recovery_spool_path).await?;
        assert!(
            recovery_spool_path.exists(),
            "expected recovery spool at {recovery_spool_path:?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn leftover_recovery_spool_events_are_republished_on_startup(
        ctx: xtask::sandbox::TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        ensure_events_stream(&ctx.nats_client(), ctx.env()).await?;

        let work_dir = tempdir()?;
        let recovery_spool_path = work_dir.path().join("sinex_event_recovery_spool.jsonl");
        let event = test_event("recovery_spool.recovered", true)?;
        let subject = ctx.env().nats_raw_event_subject_with_namespace(
            None,
            event.source.as_str(),
            event.event_type.as_str(),
        );
        let mut subscription = ctx.nats_client().subscribe(subject).await?;

        EventBatcher::store_recovery_spool_events_at_path(&[event], &recovery_spool_path)
            .await?;

        let (_sender, receiver) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let batcher = EventBatcher::new(
            EventTransport::Nats(Arc::new(NatsPublisher::new(ctx.nats_client()))),
            EventBatcherConfig::default(),
            receiver,
            shutdown_rx,
            work_dir.path().to_path_buf(),
        );
        batcher.recover_recovery_spool_events().await?;

        let message = tokio::time::timeout(Duration::from_secs(5), subscription.next())
            .await?
            .expect("replayed recovery-spool event should be published");
        let payload: JsonValue = serde_json::from_slice(&message.payload)?;
        assert_eq!(payload["event_type"], "recovery_spool.recovered");
        assert!(
            tokio::fs::metadata(&recovery_spool_path).await.is_err(),
            "fully replayed recovery spool should be removed"
        );
        Ok(())
    }

    #[sinex_test]
    async fn malformed_recovery_spool_entries_are_preserved_during_replay(
        ctx: xtask::sandbox::TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        ensure_events_stream(&ctx.nats_client(), ctx.env()).await?;

        let work_dir = tempdir()?;
        let recovery_spool_path = work_dir.path().join("sinex_event_recovery_spool.jsonl");
        let event = test_event("recovery_spool.partial_recovery", true)?;
        let valid_line = serde_json::to_string(&event)?;
        EventBatcher::rewrite_recovery_spool_file(
            &[valid_line, "{not-json".to_string()],
            &recovery_spool_path,
        )
        .await?;

        let (_sender, receiver) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let batcher = EventBatcher::new(
            EventTransport::Nats(Arc::new(NatsPublisher::new(ctx.nats_client()))),
            EventBatcherConfig::default(),
            receiver,
            shutdown_rx,
            work_dir.path().to_path_buf(),
        );
        batcher.recover_recovery_spool_events().await?;

        let contents = tokio::fs::read_to_string(&recovery_spool_path).await?;
        assert!(
            contents.contains("{not-json"),
            "malformed recovery-spool entry should remain for manual inspection"
        );
        assert!(
            !contents.contains("recovery_spool.partial_recovery"),
            "successfully replayed entries should be removed from the preserved recovery spool"
        );
        Ok(())
    }
}

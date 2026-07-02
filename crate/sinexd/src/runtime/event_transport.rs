//! Event batcher that handles batching and sending events.

use crate::runtime::{RuntimeResult, nats_publisher::NatsPublisher};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use sinex_primitives::domain::HostName;
use sinex_primitives::events::{Event, admission::EventIntent};
use sinex_primitives::{JsonValue, SinexError, Uuid};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// Callback type for the direct in-process admission path.
///
/// Receives a batch of events and routes them straight to `AdmissionService`
/// without a JetStream publish → ack → re-consume round-trip. Defined as a
/// type alias here (in `runtime`) so that `EventTransport` does not depend on
/// `crate::event_engine::admission`, which already imports from `runtime`
/// and would create a circular dependency if the relationship were reversed.
///
/// In production, callers in `event_engine` construct this by capturing
/// `Arc<AdmissionService>` in a closure. In tests, use
/// `EventTransport::new_noop_direct()` for a no-op variant that discards events.
pub type DirectAdmissionFn =
    Arc<dyn Fn(Vec<Event<JsonValue>>) -> BoxFuture<'static, RuntimeResult<()>> + Send + Sync>;

/// Event transport mechanism.
#[derive(Clone)]
pub enum EventTransport {
    /// Durable JetStream publishing: events are published to the raw-events
    /// stream, acked, then re-consumed by `event_engine`.  Use this for
    /// cross-process producers and any path that must survive NATS-only outages
    /// with local recovery-spool fallback.
    Nats(Arc<NatsPublisher>),
    /// Direct in-process path: events bypass JetStream and go straight to the
    /// `AdmissionService` running in the same process.
    ///
    /// Use for co-located staged parsers that produce local material events.
    /// The `Nats` variant remains correct for durable cross-process and
    /// external-producer paths where JetStream replay semantics matter.
    Direct(DirectAdmissionFn),
}

impl std::fmt::Debug for EventTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventTransport::Nats(_) => write!(f, "EventTransport::Nats"),
            EventTransport::Direct(_) => write!(f, "EventTransport::Direct"),
        }
    }
}

impl EventTransport {
    /// Construct a `Direct` transport with the given admission callback.
    ///
    /// The closure is typically created in `event_engine` by capturing
    /// `Arc<AdmissionService>`.
    #[must_use]
    pub fn new_direct(
        f: impl Fn(Vec<Event<JsonValue>>) -> BoxFuture<'static, RuntimeResult<()>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        EventTransport::Direct(Arc::new(f))
    }

    /// Construct a no-op `Direct` transport that silently discards all events.
    ///
    /// Intended for unit tests that exercise adapter/parser state machines
    /// without requiring event persistence. Do **not** use in production paths.
    #[cfg(test)]
    #[must_use]
    pub fn new_noop_direct() -> Self {
        use futures::FutureExt as _;
        EventTransport::Direct(Arc::new(|_events| async { Ok(()) }.boxed()))
    }

    /// Extract the inner `NatsPublisher`, or return an error for `Direct` transports.
    ///
    /// Call sites that require NATS — JetStream consumers, checkpoint KV,
    /// Core-NATS command listeners, `AcquisitionManager` — should use this
    /// helper so that match exhaustion is centralised here rather than
    /// scattered across the codebase.
    pub fn nats_publisher(&self) -> RuntimeResult<&NatsPublisher> {
        match self {
            EventTransport::Nats(publisher) => Ok(publisher),
            EventTransport::Direct(_) => Err(SinexError::configuration(
                "Direct transport does not provide a NATS publisher; \
                 this operation requires a Nats-backed EventTransport",
            )),
        }
    }

    /// Send a failed event to the processing-failure stream.
    ///
    /// This is for derived/runtime processing failures, not the raw-ingest DLQ.
    /// Direct transport does not have a processing-failure stream; a warning is
    /// logged and the method returns `Ok(())` so callers do not abort on a
    /// secondary concern.
    pub async fn send_to_processing_failure_queue(
        &self,
        event: &Event<JsonValue>,
        error: &str,
        module_name: &str,
    ) -> RuntimeResult<()> {
        match self {
            EventTransport::Nats(publisher) => publisher
                .publish_processing_failure(
                    event,
                    error,
                    module_name,
                    sinex_primitives::transport::Class::Derived,
                )
                .await
                .map_err(|e| e.with_context("operation", "send_to_processing_failure_queue")),
            EventTransport::Direct(_) => {
                warn!(
                    module = module_name,
                    error,
                    "Direct transport: processing failure dropped (no JetStream failure stream)"
                );
                Ok(())
            }
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
    /// Source identifier for the admission envelope (e.g., "fs-watcher").
    /// Required for production use — the batcher constructs an `EventIntent`.
    #[serde(default)]
    pub source_id: String,
    /// Parser identifier for the admission envelope (e.g., "inotify-watcher").
    #[serde(default)]
    pub parser_id: String,
    /// Parser version for the admission envelope (e.g., "1.0.0").
    #[serde(default)]
    pub parser_version: String,
}

impl Default for EventBatcherConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            batch_timeout_ms: 1000,
            source_id: String::new(),
            parser_id: String::new(),
            parser_version: String::new(),
        }
    }
}

#[derive(Debug, Default)]
struct EventBatcherStats {
    batches_sent: AtomicU64,
    events_sent: AtomicU64,
    publish_failures: AtomicU64,
    recovery_spool_write_failures: AtomicU64,
    /// Events silently discarded because the recovery spool replay cap
    /// (`MAX_REMAINING_LINES`) was hit. #751 F36: these events are lost
    /// after sustained NATS outages and require manual intervention.
    recovery_spool_discards: AtomicU64,
}

impl EventBatcherStats {
    fn log(&self) {
        debug!(
            batches_sent = self.batches_sent.load(Ordering::Relaxed),
            events_sent = self.events_sent.load(Ordering::Relaxed),
            publish_failures = self.publish_failures.load(Ordering::Relaxed),
            recovery_spool_write_failures =
                self.recovery_spool_write_failures.load(Ordering::Relaxed),
            recovery_spool_discards = self.recovery_spool_discards.load(Ordering::Relaxed),
            "Event batcher stats"
        );
    }
}

struct BatchPublishResult {
    published: usize,
    failed: usize,
}

const RECOVERY_SPOOL_REPLAY_AFTER_SUCCESS_INTERVAL: Duration = Duration::from_secs(10);

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
    /// `PrivateTmp` systemd namespace). It is populated from the module's `RuntimeConfig::work_dir`
    /// by the runtime, which in turn reads `SINEX_WORK_DIR` / defaults to the system cache dir.
    work_dir: PathBuf,
    last_recovery_spool_replay_attempt: Option<Instant>,
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
            last_recovery_spool_replay_attempt: None,
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

    /// Return the canonical path for the local recovery spool in the module's work directory.
    fn recovery_spool_path(&self) -> PathBuf {
        self.work_dir.join("sinex_event_recovery_spool.jsonl")
    }

    /// Run the event batching loop
    pub async fn run(mut self) -> RuntimeResult<()> {
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

    async fn recover_recovery_spool_events(&self) -> RuntimeResult<()> {
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
                        self.stats
                            .recovery_spool_discards
                            .fetch_add(1, Ordering::Relaxed);
                        error!(
                            target: "sinex_metrics",
                            metric = "runtime.recovery_spool_discards_total",
                            path = ?recovery_spool_path,
                            error = %error,
                            "Discarding malformed recovery-spool entry (remaining-lines cap of {MAX_REMAINING_LINES} reached); event is permanently lost"
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

            let event_id = event.id;
            let publish_result = match &self.transport {
                EventTransport::Nats(publisher) => {
                    let intent = EventIntent::new(
                        if self.config.source_id.is_empty() {
                            "recovery-spool"
                        } else {
                            &self.config.source_id
                        },
                        if self.config.parser_id.is_empty() {
                            "recovery-spool"
                        } else {
                            &self.config.parser_id
                        },
                        if self.config.parser_version.is_empty() {
                            "0.0.0"
                        } else {
                            &self.config.parser_version
                        },
                        vec![event],
                        HostName::from_static("sinex-batcher"),
                    );
                    publisher
                        .publish_intent(&intent, sinex_primitives::transport::Class::Critical)
                        .await
                }
                EventTransport::Direct(direct_fn) => direct_fn(vec![event])
                    .await
                    .map_err(|e| e.with_context("operation", "recovery_spool_replay_direct")),
            };

            if let Err(error) = publish_result {
                if remaining_lines.len() >= MAX_REMAINING_LINES {
                    discarded += 1;
                    self.stats
                        .recovery_spool_discards
                        .fetch_add(1, Ordering::Relaxed);
                    error!(
                        target: "sinex_metrics",
                        metric = "runtime.recovery_spool_discards_total",
                        path = ?recovery_spool_path,
                        event_id = ?event_id,
                        error = %error,
                        "Discarding recovery-spool event after replay publish failure (remaining-lines cap of {MAX_REMAINING_LINES} reached); event is permanently lost"
                    );
                } else {
                    warn!(
                        path = ?recovery_spool_path,
                        event_id = ?event_id,
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
            if discarded > 0 {
                // #751 F36: events were lost due to MAX_REMAINING_LINES cap during
                // sustained NATS outage. The spool is now clean but the loss is durable.
                error!(
                    target: "sinex_metrics",
                    metric = "runtime.recovery_spool_discards_total",
                    path = ?recovery_spool_path,
                    recovered,
                    malformed,
                    discarded,
                    max_remaining = MAX_REMAINING_LINES,
                    "Replayed and removed leftover local recovery spool; {discarded} events permanently lost due to remaining-lines cap"
                );
            } else {
                info!(
                    path = ?recovery_spool_path,
                    recovered,
                    malformed,
                    discarded,
                    "Replayed and removed leftover local recovery spool"
                );
            }
            return Ok(());
        }

        Self::rewrite_recovery_spool_file(&remaining_lines, &recovery_spool_path).await?;
        if discarded > 0 {
            error!(
                target: "sinex_metrics",
                metric = "runtime.recovery_spool_discards_total",
                path = ?recovery_spool_path,
                recovered,
                malformed,
                discarded,
                remaining = remaining_lines.len(),
                max_remaining = MAX_REMAINING_LINES,
                "Replayed local recovery spool partially; {discarded} events permanently lost due to remaining-lines cap — unreadable or unpublished entries were preserved"
            );
        } else {
            warn!(
                path = ?recovery_spool_path,
                recovered,
                malformed,
                discarded,
                remaining = remaining_lines.len(),
                "Replayed local recovery spool partially; unreadable or unpublished entries were preserved"
            );
        }
        Ok(())
    }

    /// Send a batch of events
    async fn send_batch(&mut self, batch: &mut Vec<Event<JsonValue>>) -> RuntimeResult<()> {
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
            EventTransport::Nats(publisher) => {
                Self::send_batch_nats(
                    publisher,
                    batch,
                    &self.config.source_id,
                    &self.config.parser_id,
                    &self.config.parser_version,
                )
                .await
            }
            EventTransport::Direct(direct_fn) => Self::send_batch_direct(direct_fn, batch).await,
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
            self.recover_spool_after_success_if_due().await;
            return Ok(());
        }

        error!(
            target: "sinex_metrics",
            metric = "runtime.batch_send_failures_total",
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
                target: "sinex_metrics",
                metric = "runtime.recovery_spool_write_failures_total",
                recovery_spool_events = batch.len(),
                error = %e,
                "Failed to store events in local recovery spool"
            );
            return Err(e);
        }

        batch.clear();
        Ok(())
    }

    async fn recover_spool_after_success_if_due(&mut self) {
        if self
            .last_recovery_spool_replay_attempt
            .is_some_and(|last_attempt| {
                last_attempt.elapsed() < RECOVERY_SPOOL_REPLAY_AFTER_SUCCESS_INTERVAL
            })
        {
            return;
        }
        self.last_recovery_spool_replay_attempt = Some(Instant::now());

        if let Err(error) = self.recover_recovery_spool_events().await {
            warn!(
                error = %error,
                "Recovery spool replay failed after successful send; preserved entries remain on disk"
            );
        }
    }

    /// Store failed raw events in the local recovery spool at `recovery_spool_path`.
    ///
    /// The caller is responsible for providing a persistent path (not under `PrivateTmp`).
    async fn store_recovery_spool_events(
        events: &[Event<JsonValue>],
        recovery_spool_path: &Path,
    ) -> RuntimeResult<()> {
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
    ) -> RuntimeResult<()> {
        let parent_dir = recovery_spool_path
            .parent()
            .unwrap_or_else(|| Path::new("."));
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
    ) -> RuntimeResult<()> {
        let parent_dir = recovery_spool_path
            .parent()
            .unwrap_or_else(|| Path::new("."));
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

    /// Send a batch of events through the direct in-process admission path.
    ///
    /// Events are drained from `batch` and passed to the `DirectAdmissionFn`.
    /// On success the batch is cleared; on failure the events are returned so
    /// the caller can route them to the local recovery spool.
    async fn send_batch_direct(
        direct_fn: &DirectAdmissionFn,
        events: &mut Vec<Event<JsonValue>>,
    ) -> BatchPublishResult {
        if events.is_empty() {
            return BatchPublishResult {
                published: 0,
                failed: 0,
            };
        }

        let event_count = events.len();
        // Deliver a clone so the originals stay in `events` if admission fails.
        // This mirrors the NATS arm, which restores `intent.events` on failure so
        // `send_batch` can route the undelivered events to the recovery spool.
        // Without this, a failing `DirectAdmissionFn` would consume the batch and
        // the recovery-spool safety net would silently spool nothing.
        let to_deliver = events.clone();

        match direct_fn(to_deliver).await {
            Ok(()) => {
                events.clear();
                debug!(
                    published = event_count,
                    "Intent batch sent via Direct admission"
                );
                BatchPublishResult {
                    published: event_count,
                    failed: 0,
                }
            }
            Err(e) => {
                error!(
                    target: "sinex_metrics",
                    metric = "runtime.event_publish_failures_total",
                    event_count,
                    error = %e,
                    "Failed to admit event batch via Direct path"
                );
                // `events` is left intact so `send_batch` can spool the batch.
                BatchPublishResult {
                    published: 0,
                    failed: event_count,
                }
            }
        }
    }

    async fn send_batch_nats(
        publisher: &NatsPublisher,
        events: &mut Vec<Event<JsonValue>>,
        source_id: &str,
        parser_id: &str,
        parser_version: &str,
    ) -> BatchPublishResult {
        if events.is_empty() {
            return BatchPublishResult {
                published: 0,
                failed: 0,
            };
        }

        let event_count = events.len();

        let intent = EventIntent::new(
            if source_id.is_empty() {
                "unknown"
            } else {
                source_id
            },
            parser_id,
            parser_version,
            std::mem::take(events),
            HostName::from_static("sinex-batcher"),
        );

        match publisher
            .publish_intent(&intent, sinex_primitives::transport::Class::Critical)
            .await
        {
            Ok(()) => {
                debug!(published = event_count, "Intent batch sent via NATS");
                *events = Vec::new();
                BatchPublishResult {
                    published: event_count,
                    failed: 0,
                }
            }
            Err(e) => {
                error!(
                    target: "sinex_metrics",
                    metric = "runtime.event_publish_failures_total",
                    event_count = event_count,
                    error = %e,
                    "Failed to publish event intent envelope"
                );
                *events = intent.events;
                BatchPublishResult {
                    published: 0,
                    failed: event_count,
                }
            }
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
) -> tokio::task::JoinHandle<RuntimeResult<()>> {
    tokio::spawn(async move {
        let batcher = EventBatcher::new(transport, config, event_receiver, shutdown, work_dir);
        batcher.run().await
    })
}

#[cfg(test)]
mod tests {
    use super::{EventBatcher, EventBatcherConfig, EventTransport};
    use crate::runtime::{jetstream_streams, nats_publisher::NatsPublisher};
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
        _env: &sinex_primitives::environment::SinexEnvironment,
    ) -> TestResult<()> {
        jetstream_streams::bootstrap_raw_events_stream(client, None).await?;
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
            EventBatcher::store_recovery_spool_events_at_path(&[event], &recovery_spool_path).await;

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

        EventBatcher::store_recovery_spool_events_at_path(&[event], &recovery_spool_path).await?;

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
        // The recovery path publishes via `publish_intent`, so the message
        // payload is an `EventIntent` envelope — the event_type lives under
        // `events[0]`, not at the top level. (Inherited assertion bug: the
        // raw-event-to-intent switch in #1653 left this asserting `event_type`
        // at the envelope root, where it is always null.)
        let payload: JsonValue = serde_json::from_slice(&message.payload)?;
        assert_eq!(
            payload["events"][0]["event_type"],
            "recovery_spool.recovered"
        );
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

    /// Proves the `Direct` transport routes a batch synchronously to its
    /// admission closure without any NATS infrastructure: the closure captures
    /// the delivered events, and after `send_batch` the captured set matches the
    /// sent set by both count and event identity, and the input batch is drained.
    #[sinex_test]
    async fn direct_transport_send_batch_delivers_to_closure() -> TestResult<()> {
        use std::sync::Mutex;

        let delivered: Arc<Mutex<Vec<Event<JsonValue>>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&delivered);
        let transport = EventTransport::new_direct(move |events| {
            let sink = Arc::clone(&sink);
            Box::pin(async move {
                sink.lock()
                    .expect("delivered-events mutex should not be poisoned")
                    .extend(events);
                Ok(())
            })
        });

        let work_dir = tempdir()?;
        let (_sender, receiver) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let mut batcher = EventBatcher::new(
            transport,
            EventBatcherConfig::default(),
            receiver,
            shutdown_rx,
            work_dir.path().to_path_buf(),
        );

        let first = test_event("direct.first", true)?;
        let second = test_event("direct.second", true)?;
        let expected_ids = vec![first.id, second.id];
        let mut batch = vec![first, second];

        batcher.send_batch(&mut batch).await?;

        assert!(
            batch.is_empty(),
            "Direct send_batch must drain the input batch on success"
        );
        let captured = delivered
            .lock()
            .expect("delivered-events mutex should not be poisoned");
        assert_eq!(
            captured.len(),
            2,
            "Direct path must deliver every event in the batch"
        );
        let captured_ids: Vec<_> = captured.iter().map(|event| event.id).collect();
        assert_eq!(
            captured_ids, expected_ids,
            "Direct path must deliver the same events (by identity) that were sent"
        );
        Ok(())
    }

    /// Proves a `Direct` admission closure that returns an error does not drop
    /// silently: the events are routed to the local recovery spool so they can be
    /// replayed, exactly as the NATS publish-failure path does.
    #[sinex_test]
    async fn direct_transport_failure_routes_to_recovery_spool() -> TestResult<()> {
        let transport = EventTransport::new_direct(|_events| {
            Box::pin(async {
                Err(sinex_primitives::SinexError::processing(
                    "admission rejected",
                ))
            })
        });

        let work_dir = tempdir()?;
        let recovery_spool_path = work_dir.path().join("sinex_event_recovery_spool.jsonl");
        let (_sender, receiver) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let mut batcher = EventBatcher::new(
            transport,
            EventBatcherConfig::default(),
            receiver,
            shutdown_rx,
            work_dir.path().to_path_buf(),
        );

        let mut batch = vec![test_event("direct.failed", true)?];
        // send_batch swallows the failure and spools; it returns Ok once spooled.
        batcher.send_batch(&mut batch).await?;

        assert!(
            tokio::fs::metadata(&recovery_spool_path).await.is_ok(),
            "Direct admission failure must persist events to the recovery spool"
        );
        let contents = tokio::fs::read_to_string(&recovery_spool_path).await?;
        assert!(
            contents.contains("direct.failed"),
            "recovery spool must contain the undelivered Direct event"
        );
        Ok(())
    }

    #[sinex_test]
    async fn direct_transport_reports_nats_required_operations() -> TestResult<()> {
        let transport = EventTransport::new_noop_direct();

        let error = transport
            .nats_publisher()
            .expect_err("Direct transport should not expose a NATS publisher");
        let message = error.to_string();

        assert!(
            message.contains("Direct transport does not provide a NATS publisher"),
            "NATS-required call sites should receive an explicit Direct-transport error: {message}"
        );
        Ok(())
    }
}

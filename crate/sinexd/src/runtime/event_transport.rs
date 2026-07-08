//! Event batcher that handles batching and sending events.

use crate::runtime::{
    RuntimeResult,
    nats_payload::{NATS_PUBLISH_PAYLOAD_HARD_LIMIT_BYTES, ensure_nats_payload_fits},
    nats_publisher::NatsPublisher,
};
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
    /// Malformed recovery-spool entries moved to the durable quarantine file
    /// during replay (sinex-r6d.5). Quarantined, never discarded: an operator
    /// can inspect/retry/delete via the quarantine file. This counter used to
    /// track events permanently lost past a line-count cap (#751 F36); that
    /// cap no longer exists.
    recovery_spool_quarantined: AtomicU64,
}

impl EventBatcherStats {
    fn log(&self) {
        debug!(
            batches_sent = self.batches_sent.load(Ordering::Relaxed),
            events_sent = self.events_sent.load(Ordering::Relaxed),
            publish_failures = self.publish_failures.load(Ordering::Relaxed),
            recovery_spool_write_failures =
                self.recovery_spool_write_failures.load(Ordering::Relaxed),
            recovery_spool_quarantined = self.recovery_spool_quarantined.load(Ordering::Relaxed),
            "Event batcher stats"
        );
    }
}

struct BatchPublishResult {
    published: usize,
    failed: usize,
}

const NATS_INTENT_PAYLOAD_SOFT_LIMIT_BYTES: usize = NATS_PUBLISH_PAYLOAD_HARD_LIMIT_BYTES;
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

    /// Path for the durable quarantine file: malformed spool lines that cannot
    /// be parsed as an `Event<JsonValue>` land here instead of being discarded
    /// or endlessly re-attempted inline with retryable entries (sinex-r6d.5).
    fn recovery_spool_quarantine_path(&self) -> PathBuf {
        self.work_dir
            .join("sinex_event_recovery_spool.quarantine.jsonl")
    }

    /// Best-effort `fsync` of a directory so a preceding `rename()` into it is
    /// durable across a crash, not just the renamed file's own content
    /// (sinex-r6d.5 red-team finding: temp-file writes fsync the file but the
    /// rename into place was never followed by a parent-dir fsync, so the
    /// rename itself could be lost on crash even though the data was synced).
    /// Opening a directory read-only and calling `sync_all` is the portable
    /// Unix idiom; sinex targets Linux only (see repo CLAUDE.md), so this is
    /// not expected to fail in practice, but a failure here is logged rather
    /// than propagated — directory durability is a hardening improvement over
    /// the previous complete absence of it, not a new hard dependency.
    async fn fsync_dir(dir: &Path) {
        match tokio::fs::File::open(dir).await {
            Ok(dir_file) => {
                if let Err(error) = dir_file.sync_all().await {
                    warn!(path = ?dir, %error, "Failed to fsync recovery-spool parent directory after rename");
                }
            }
            Err(error) => {
                warn!(path = ?dir, %error, "Failed to open recovery-spool parent directory for fsync");
            }
        }
    }

    /// Append one quarantine record (original line + line number + error +
    /// timestamp) to the quarantine file, fsyncing after every write since
    /// quarantine entries are rare (malformed data, not routine traffic) and
    /// must never be lost between appends.
    async fn append_quarantine_record(
        quarantine_path: &Path,
        line_no: u64,
        original_line: &str,
        error: &str,
    ) -> RuntimeResult<()> {
        let record = serde_json::json!({
            "quarantined_at": sinex_primitives::temporal::now().format_rfc3339(),
            "line_no": line_no,
            "error": error,
            "original_line": original_line,
        });
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(quarantine_path)
            .await?;
        file.write_all(serde_json::to_string(&record)?.as_bytes())
            .await?;
        file.write_all(b"\n").await?;
        file.flush().await?;
        file.sync_all().await?;
        Ok(())
    }

    /// Replay the leftover local recovery spool, publishing every entry that
    /// still parses and streaming everything that doesn't recover (publish
    /// failure) back into a rewritten spool file.
    ///
    /// sinex-r6d.5: no line-count cap, no discard. Malformed lines move to a
    /// durable quarantine file with enough metadata (original bytes, line
    /// number, error, timestamp) for an operator to inspect/retry/delete
    /// explicitly; publish-failed-but-valid lines are preserved verbatim and
    /// retried on the next replay pass. Both the rewritten spool and the
    /// quarantine file are written incrementally (bounded memory — no
    /// unbounded in-memory `Vec` of pending lines) and their directory is
    /// fsynced after each rename so a crash cannot lose the durability the
    /// per-file fsync already provides.
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

        let parent_dir = recovery_spool_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        tokio::fs::create_dir_all(&parent_dir).await?;
        let quarantine_path = self.recovery_spool_quarantine_path();
        let remaining_temp_path = parent_dir.join(format!(
            ".sinex_event_recovery_spool.{}.tmp",
            Uuid::now_v7()
        ));

        let mut lines = BufReader::new(file).lines();
        let mut remaining_file: Option<tokio::fs::File> = None;
        let mut recovered = 0_u64;
        let mut malformed = 0_u64;
        let mut preserved = 0_u64;
        let mut line_no = 0_u64;

        while let Some(line) = lines.next_line().await? {
            line_no += 1;
            if line.trim().is_empty() {
                continue;
            }

            let event = match serde_json::from_str::<Event<JsonValue>>(&line) {
                Ok(event) => event,
                Err(error) => {
                    malformed += 1;
                    self.stats
                        .recovery_spool_quarantined
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(
                        target: "sinex_metrics",
                        metric = "runtime.recovery_spool_quarantined_total",
                        path = ?recovery_spool_path,
                        line_no,
                        error = %error,
                        "Quarantining malformed recovery-spool entry (durable, never discarded — see quarantine file for operator inspection/retry/delete)"
                    );
                    Self::append_quarantine_record(
                        &quarantine_path,
                        line_no,
                        &line,
                        &error.to_string(),
                    )
                    .await?;
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
                preserved += 1;
                warn!(
                    path = ?recovery_spool_path,
                    event_id = ?event_id,
                    error = %error,
                    "Preserving recovery-spool entry after replay publish failure (streamed to rewritten spool, no cap)"
                );
                let out = match remaining_file.as_mut() {
                    Some(out) => out,
                    None => {
                        let out = tokio::fs::OpenOptions::new()
                            .create_new(true)
                            .write(true)
                            .open(&remaining_temp_path)
                            .await?;
                        remaining_file = Some(out);
                        remaining_file
                            .as_mut()
                            .expect("just inserted Some above")
                    }
                };
                out.write_all(line.as_bytes()).await?;
                out.write_all(b"\n").await?;
                continue;
            }

            recovered += 1;
        }

        match remaining_file {
            Some(mut out) => {
                out.flush().await?;
                out.sync_all().await?;
                drop(out);
                tokio::fs::rename(&remaining_temp_path, &recovery_spool_path).await?;
                Self::fsync_dir(&parent_dir).await;
                warn!(
                    path = ?recovery_spool_path,
                    recovered,
                    malformed,
                    preserved,
                    quarantine_path = ?quarantine_path,
                    "Replayed local recovery spool partially; unpublished entries were preserved (streamed rewrite, zero discard); malformed entries quarantined"
                );
            }
            None => {
                tokio::fs::remove_file(&recovery_spool_path).await?;
                Self::fsync_dir(&parent_dir).await;
                info!(
                    path = ?recovery_spool_path,
                    recovered,
                    malformed,
                    quarantine_path = ?quarantine_path,
                    "Replayed and removed leftover local recovery spool (zero discard; malformed entries, if any, are in the quarantine file)"
                );
            }
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

        // Single attempt here by design, not a durability gap: a failed batch
        // routes to the local recovery spool below (store_recovery_spool_events),
        // which sinex-r6d.5 hardened to never discard entries and to fsync both
        // the temp file and its parent directory before/after rename. Retry
        // happens on the next scheduled recovery-spool replay pass, not inline.

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
        Self::fsync_dir(parent_dir).await;

        info!(
            recovery_spool_events = events.len(),
            path = ?recovery_spool_path,
            "Stored events in local recovery spool"
        );
        Ok(())
    }

    /// Test-only helper for seeding a spool file with arbitrary line content
    /// (including deliberately malformed lines) before exercising replay.
    /// `recover_recovery_spool_events` no longer calls this itself (sinex-r6d.5
    /// rewrote it to stream incrementally instead of rewriting the whole file
    /// from an in-memory Vec), so this would otherwise be dead code outside tests.
    #[cfg(test)]
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
        Self::fsync_dir(parent_dir).await;
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
        let source_id = if source_id.is_empty() {
            "unknown"
        } else {
            source_id
        };
        let pending = std::mem::take(events);
        let mut pending_iter = pending.into_iter();
        let mut published = 0usize;
        let mut chunk = Vec::new();
        let mut estimated_chunk_payload_bytes =
            Self::intent_payload_base_estimate(source_id, parser_id, parser_version);

        while let Some(event) = pending_iter.next() {
            let event_payload_estimate = Self::event_payload_estimate(&event);
            chunk.push(event);
            estimated_chunk_payload_bytes =
                estimated_chunk_payload_bytes.saturating_add(event_payload_estimate);

            if estimated_chunk_payload_bytes <= NATS_INTENT_PAYLOAD_SOFT_LIMIT_BYTES {
                continue;
            }

            match Self::intent_payload_len(source_id, parser_id, parser_version, &chunk) {
                Ok(payload_len)
                    if payload_len > NATS_INTENT_PAYLOAD_SOFT_LIMIT_BYTES && chunk.len() > 1 =>
                {
                    let overflow = chunk
                        .pop()
                        .expect("chunk len checked above; overflow event must exist");
                    estimated_chunk_payload_bytes =
                        Self::intent_payload_base_estimate(source_id, parser_id, parser_version)
                            .saturating_add(Self::event_payload_estimate(&overflow));
                    match Self::publish_intent_chunk(
                        publisher,
                        source_id,
                        parser_id,
                        parser_version,
                        chunk,
                    )
                    .await
                    {
                        Ok(count) => {
                            published += count;
                            chunk = vec![overflow];
                        }
                        Err((failed_chunk, error)) => {
                            *events = failed_chunk
                                .into_iter()
                                .chain(std::iter::once(overflow))
                                .chain(pending_iter)
                                .collect();
                            Self::log_nats_publish_failure(event_count, events.len(), &error);
                            return BatchPublishResult {
                                published,
                                failed: events.len(),
                            };
                        }
                    }
                }
                Ok(payload_len) => {
                    estimated_chunk_payload_bytes = payload_len;
                    if payload_len > NATS_INTENT_PAYLOAD_SOFT_LIMIT_BYTES {
                        warn!(
                            payload_len,
                            soft_limit = NATS_INTENT_PAYLOAD_SOFT_LIMIT_BYTES,
                            "Single event intent envelope exceeds soft NATS payload split limit; publishing alone"
                        );
                    }
                }
                Err(error) => {
                    *events = chunk.into_iter().chain(pending_iter).collect();
                    Self::log_nats_publish_failure(event_count, events.len(), &error);
                    return BatchPublishResult {
                        published,
                        failed: events.len(),
                    };
                }
            }
        }

        if !chunk.is_empty() {
            match Self::publish_intent_chunk(publisher, source_id, parser_id, parser_version, chunk)
                .await
            {
                Ok(count) => {
                    published += count;
                }
                Err((failed_chunk, error)) => {
                    *events = failed_chunk;
                    Self::log_nats_publish_failure(event_count, events.len(), &error);
                    return BatchPublishResult {
                        published,
                        failed: events.len(),
                    };
                }
            }
        }

        debug!(published, original_batch_size = event_count, "Intent batch sent via NATS");
        BatchPublishResult {
            published,
            failed: 0,
        }
    }

    fn intent_payload_len(
        source_id: &str,
        parser_id: &str,
        parser_version: &str,
        events: &[Event<JsonValue>],
    ) -> RuntimeResult<usize> {
        let intent = EventIntent::new(
            source_id,
            parser_id,
            parser_version,
            events.to_vec(),
            HostName::from_static("sinex-batcher"),
        );
        serde_json::to_vec(&intent)
            .map(|payload| payload.len())
            .map_err(SinexError::from)
    }

    fn intent_payload_base_estimate(
        source_id: &str,
        parser_id: &str,
        parser_version: &str,
    ) -> usize {
        1024usize
            .saturating_add(source_id.len())
            .saturating_add(parser_id.len())
            .saturating_add(parser_version.len())
    }

    fn event_payload_estimate(event: &Event<JsonValue>) -> usize {
        serde_json::to_vec(event)
            .map(|payload| payload.len().saturating_add(64))
            .unwrap_or(NATS_INTENT_PAYLOAD_SOFT_LIMIT_BYTES)
    }

    async fn publish_intent_chunk(
        publisher: &NatsPublisher,
        source_id: &str,
        parser_id: &str,
        parser_version: &str,
        events: Vec<Event<JsonValue>>,
    ) -> Result<usize, (Vec<Event<JsonValue>>, SinexError)> {
        let event_count = events.len();
        let payload_len = match Self::intent_payload_len(
            source_id,
            parser_id,
            parser_version,
            &events,
        ) {
            Ok(payload_len) => payload_len,
            Err(error) => return Err((events, error)),
        };
        if let Err(error) =
            ensure_nats_payload_fits("event intent envelope", source_id, payload_len)
        {
            return Err((events, error));
        }

        let intent = EventIntent::new(
            source_id,
            parser_id,
            parser_version,
            events,
            HostName::from_static("sinex-batcher"),
        );

        match publisher
            .publish_intent(&intent, sinex_primitives::transport::Class::Critical)
            .await
        {
            Ok(()) => Ok(event_count),
            Err(error) => Err((intent.events, error)),
        }
    }

    fn log_nats_publish_failure(original_batch_size: usize, failed: usize, error: &SinexError) {
        error!(
            target: "sinex_metrics",
            metric = "runtime.event_publish_failures_total",
            event_count = original_batch_size,
            failed,
            error = %error,
            "Failed to publish event intent envelope"
        );
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
#[path = "event_transport_test.rs"]
mod tests;

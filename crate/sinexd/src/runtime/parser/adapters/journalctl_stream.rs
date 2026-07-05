//! Adapter for streaming journald entries via `journalctl -f -o json`.
//!
//! Linux/systemd only — gated behind `#[cfg(target_os = "linux")]`.
//! Each line is a JSON object representing one journal entry. The adapter
//! extracts `__CURSOR` from each record to form the checkpoint cursor.
//!
//! Cursor is the journal cursor string (`String`). No replay — journald
//! manages retention. This adapter resumes from where it left off by
//! passing `--cursor=<cursor>` to the child process.
//!
//! # Fan-out: [`SharedJournalctlStream`]
//!
//! When multiple source contracts share a single `journalctl` subprocess (e.g.
//! `system.systemd` + `system.journald`), use [`SharedJournalctlStream`] to
//! avoid spawning one subprocess per unit. One background task drives the
//! subprocess and broadcasts each [`SourceRecord`] to all registered
//! [`JournalctlSubscriber`]s via a `tokio::sync::broadcast` channel.
//!
//! ## Broadcast lag policy
//!
//! `tokio::sync::broadcast` is bounded. If a subscriber falls more than
//! [`BROADCAST_CAPACITY`] records behind, subsequent `recv()` calls return
//! [`tokio::sync::broadcast::error::RecvError::Lagged`] and report the number
//! of records dropped. Subscribers MUST consume promptly. The recommended
//! pattern is to process records inline in the stream and offload heavy work
//! to a separate task. A lagged subscriber does NOT affect other subscribers
//! or the subprocess driver — it simply misses the overflowed records and
//! resumes at the oldest available message.

use async_trait::async_trait;
use futures::stream::BoxStream;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::broadcast;

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeKind, MaterialAnchor, SourceRecord};

use crate::runtime::parser::{InputShapeAdapter, ParserError, ParserResult};

// =============================================================================
// JournalctlStreamAdapter
// =============================================================================

/// Adapter that streams journald entries via `journalctl -f -o json`.
///
/// Emits one [`SourceRecord`] per journal line. The record bytes are the
/// raw UTF-8 JSON line; parsers typically `serde_json::from_slice` them.
///
/// Cursor is the journal cursor string extracted from `__CURSOR`.
#[derive(Debug, Clone, Default)]
pub struct JournalctlStreamAdapter;

/// Configuration for [`JournalctlStreamAdapter`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct JournalctlStreamConfig {
    /// Systemd units to filter (maps to `--unit=<unit>` args).
    /// Empty = no unit filter (all units).
    #[serde(default)]
    pub units: Vec<String>,

    /// Maximum priority to include (0=emerg … 7=debug).
    /// Maps to `--priority=<p>`. `None` = no filter.
    #[serde(default)]
    pub priority: Option<u8>,

    /// If provided, pass `--cursor=<cursor>` to resume from a checkpoint.
    /// Typically passed via the `cursor` argument to `open()` rather than
    /// directly in config; this field is for completeness.
    #[serde(default)]
    pub from_cursor: Option<String>,

    /// Start at the current end of the journal when no cursor is available.
    ///
    /// This is set by the generic adapter source runtime when a continuous
    /// binding declares `continuous_start_position = "latest"`. Historical
    /// imports leave it false so a no-cursor run can still read retained
    /// journal data deliberately.
    #[serde(default)]
    pub start_at_now_without_cursor: bool,
}

/// Cursor for [`JournalctlStreamAdapter`] — the journal cursor string.
///
/// Extracted from `__CURSOR` in each journal JSON record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalctlCursor {
    pub cursor: String,
}

impl JournalctlCursor {
    #[must_use]
    pub fn new(cursor: impl Into<String>) -> Self {
        Self {
            cursor: cursor.into(),
        }
    }
}

#[async_trait]
impl InputShapeAdapter for JournalctlStreamAdapter {
    type Config = JournalctlStreamConfig;
    type Cursor = JournalctlCursor;
    const KIND: InputShapeKind = InputShapeKind::Subprocess;

    async fn open(
        &self,
        material_id: Id<SourceMaterial>,
        config: &Self::Config,
        cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        let mut cmd = Command::new("journalctl");
        cmd.args(journalctl_args(config, cursor.as_ref()))
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);

        let mut child = cmd
            .spawn()
            .map_err(|e| ParserError::Adapter(format!("failed to spawn journalctl: {e}")))?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ParserError::Adapter("journalctl stdout not captured".into()))?;

        let reader = BufReader::new(stdout);
        let mut lines = reader.lines();

        let stream = async_stream::stream! {
            let mut frame_index: u64 = 0;

            // Keep child alive until this async block is dropped.
            let _child = child;

            loop {
                match lines.next_line().await {
                    Err(e) => {
                        yield Err(ParserError::Io(e));
                        break;
                    }
                    Ok(None) => break,
                    Ok(Some(line)) => {
                        if line.is_empty() {
                            continue;
                        }

                        let bytes = line.as_bytes().to_vec();
                        let anchor = MaterialAnchor::StreamFrame {
                            material_offset: 0,
                            frame_index,
                        };

                        let record = SourceRecord {
                            material_id,
                            anchor,
                            bytes,
                            logical_path: None,
                            source_ts_hint: None,
                            metadata: serde_json::Value::Null,
                        };

                        frame_index += 1;
                        yield Ok(record);
                    }
                }
            }
        };

        Ok(Box::pin(stream))
    }

    fn cursor_after(&self, record: &SourceRecord) -> ParserResult<Self::Cursor> {
        // Extract __CURSOR from the record bytes (expected to be a JSON object).
        let json: serde_json::Value = serde_json::from_slice(&record.bytes).map_err(|e| {
            ParserError::Cursor(format!("failed to parse journal record as JSON: {e}"))
        })?;

        if let Some(cursor) = json.get("__CURSOR").and_then(|v| v.as_str()) {
            Ok(JournalctlCursor::new(cursor))
        } else {
            Err(ParserError::Cursor(
                "journalctl record has no __CURSOR; refusing to synthesize a cursor".into(),
            ))
        }
    }
}

fn journalctl_args(
    config: &JournalctlStreamConfig,
    cursor: Option<&JournalctlCursor>,
) -> Vec<String> {
    let mut args = vec![
        "-f".to_string(),
        "-o".to_string(),
        "json".to_string(),
        "--no-pager".to_string(),
    ];

    for unit in &config.units {
        args.push(format!("--unit={unit}"));
    }

    if let Some(p) = config.priority {
        args.push(format!("--priority={p}"));
    }

    let resume_cursor = cursor
        .map(|c| c.cursor.clone())
        .or_else(|| config.from_cursor.clone());
    if let Some(c) = resume_cursor {
        args.push(format!("--after-cursor={c}"));
    } else if config.start_at_now_without_cursor {
        args.push("--since=now".to_string());
    }

    args
}

// =============================================================================
// SharedJournalctlStream + JournalctlSubscriber
// =============================================================================

/// Default broadcast channel capacity for [`SharedJournalctlStream`].
///
/// If a subscriber falls more than this many records behind, it receives
/// [`tokio::sync::broadcast::error::RecvError::Lagged`] on the next `recv()`.
/// Adjust per deployment based on burst volume and subscriber processing speed.
pub const BROADCAST_CAPACITY: usize = 512;

/// A single underlying `journalctl` subprocess shared across multiple
/// [`JournalctlSubscriber`]s.
///
/// One background [`tokio::task`] drives the subprocess and broadcasts
/// each [`SourceRecord`] to all active subscribers. Subscribers register
/// filter predicates and receive only matching records.
///
/// # Construction
///
/// ```rust,ignore
/// let shared = SharedJournalctlStream::new(material_id, config).await?;
/// let systemd_sub = shared.subscribe(|r| {
///     // Only systemd units
///     serde_json::from_slice::<serde_json::Value>(&r.bytes)
///         .map(|v| v.get("SYSLOG_IDENTIFIER").is_some())
///         .unwrap_or(false)
/// });
/// let journald_sub = shared.subscribe(|_| true); // All records
/// ```
pub struct SharedJournalctlStream {
    sender: broadcast::Sender<SourceRecord>,
}

impl SharedJournalctlStream {
    /// Spawn a `journalctl` subprocess and start broadcasting records.
    ///
    /// The driver task runs for the lifetime of the last `Sender` handle —
    /// i.e., until all cloned `Sender`s from `subscribe()` are dropped.
    pub async fn new(
        material_id: sinex_primitives::ids::Id<sinex_primitives::events::SourceMaterial>,
        config: &JournalctlStreamConfig,
    ) -> crate::runtime::parser::ParserResult<Self> {
        Self::with_capacity(material_id, config, BROADCAST_CAPACITY).await
    }

    /// Like [`new`](Self::new) but with a configurable broadcast channel capacity.
    pub async fn with_capacity(
        material_id: sinex_primitives::ids::Id<sinex_primitives::events::SourceMaterial>,
        config: &JournalctlStreamConfig,
        capacity: usize,
    ) -> crate::runtime::parser::ParserResult<Self> {
        let (tx, _rx) = broadcast::channel(capacity);

        // Open the underlying adapter to get the record stream.
        let adapter = JournalctlStreamAdapter;
        let stream = adapter.open(material_id, config, None).await?;

        let driver_tx = tx.clone();
        tokio::spawn(async move {
            use futures::StreamExt;
            let mut pinned = std::pin::pin!(stream);
            while let Some(item) = pinned.next().await {
                match item {
                    Ok(record) => {
                        // send() only fails when there are no active receivers.
                        // That is not an error from the driver's perspective —
                        // the subprocess can continue running in case a new
                        // subscriber is added later.
                        let _ = driver_tx.send(record);
                    }
                    Err(e) => {
                        // Log and continue — stream errors are per-record
                        // (e.g. a malformed line). A fatal subprocess death
                        // results in the stream ending (next iteration None).
                        tracing::warn!(error = %e, "SharedJournalctlStream: record error from subprocess");
                    }
                }
            }
            tracing::debug!(
                "SharedJournalctlStream: subprocess stream ended — driver task exiting"
            );
        });

        Ok(Self { sender: tx })
    }

    /// Register a new subscriber with an optional filter predicate.
    ///
    /// The subscriber receives every record for which `filter` returns `true`.
    /// Pass `|_| true` to receive all records.
    ///
    /// # Lag policy
    ///
    /// If the subscriber falls more than the broadcast capacity behind,
    /// [`JournalctlSubscriber`]'s stream emits a warning and skips the
    /// lost records. See module-level docs for details.
    pub fn subscribe<F>(&self, filter: F) -> JournalctlSubscriber
    where
        F: Fn(&SourceRecord) -> bool + Send + Sync + 'static,
    {
        JournalctlSubscriber {
            receiver: self.sender.subscribe(),
            filter: Box::new(filter),
        }
    }
}

/// A filtered view of a [`SharedJournalctlStream`].
///
/// Implements [`InputShapeAdapter`] so it can plug into
/// `register_source!` in source contracts that share a subprocess.
///
/// Each subscriber maintains an independent cursor (the last journal cursor
/// string seen through this filtered view). The underlying broadcast channel
/// advances independently — subscribers don't drive the subprocess.
pub struct JournalctlSubscriber {
    receiver: broadcast::Receiver<SourceRecord>,
    filter: Box<dyn Fn(&SourceRecord) -> bool + Send + Sync + 'static>,
}

#[async_trait]
impl InputShapeAdapter for JournalctlSubscriber {
    type Config = (); // Config is owned by SharedJournalctlStream.
    type Cursor = JournalctlCursor;
    const KIND: InputShapeKind = InputShapeKind::Subprocess;

    async fn open(
        &self,
        _material_id: sinex_primitives::ids::Id<sinex_primitives::events::SourceMaterial>,
        _config: &Self::Config,
        _cursor: Option<Self::Cursor>,
    ) -> crate::runtime::parser::ParserResult<
        BoxStream<'static, crate::runtime::parser::ParserResult<SourceRecord>>,
    > {
        // JournalctlSubscriber cannot be opened multiple times — it wraps a
        // single-use `broadcast::Receiver`. The caller must construct a new
        // `JournalctlSubscriber` via `SharedJournalctlStream::subscribe()` for
        // each open call.
        Err(crate::runtime::parser::ParserError::Adapter(
            "JournalctlSubscriber::open() is not supported — use \
             into_stream() to consume the subscriber as a stream instead. \
             For integration with register_source!, call \
             SharedJournalctlStream::subscribe() fresh for each open."
                .into(),
        ))
    }

    fn cursor_after(
        &self,
        record: &SourceRecord,
    ) -> crate::runtime::parser::ParserResult<Self::Cursor> {
        // Delegate to JournalctlStreamAdapter's cursor extraction.
        JournalctlStreamAdapter.cursor_after(record)
    }
}

impl JournalctlSubscriber {
    /// Consume the subscriber into an async stream of [`SourceRecord`]s.
    ///
    /// This is the primary consumption path. Each received record is passed
    /// through the filter predicate; non-matching records are silently dropped.
    ///
    /// On lag, a warning is emitted and the subscriber skips the lost records,
    /// resuming at the oldest available message.
    pub fn into_stream(
        mut self,
    ) -> impl futures::Stream<Item = crate::runtime::parser::ParserResult<SourceRecord>> + Send + 'static
    {
        async_stream::stream! {
            loop {
                match self.receiver.recv().await {
                    Ok(record) => {
                        if (self.filter)(&record) {
                            yield Ok(record);
                        }
                        // Non-matching records are silently skipped.
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(
                            skipped = n,
                            "JournalctlSubscriber: broadcast channel lagged — \
                             {n} records dropped; subscriber was too slow to consume"
                        );
                        // Continue — resume at the oldest available message.
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // Sender dropped (driver task exited). Stream ends.
                        break;
                    }
                }
            }
        }
    }
}

// =============================================================================
// Test helpers
// =============================================================================

/// Feed a slice of pre-formed journal JSON lines through the journalctl
/// line parser without spawning a real process.
///
/// This function mirrors what `open()` does to a stream of lines, so tests
/// can exercise the record-building and cursor logic without a live systemd.
#[must_use]
pub fn records_from_journal_lines(
    material_id: Id<SourceMaterial>,
    lines: &[&str],
) -> Vec<ParserResult<SourceRecord>> {
    lines
        .iter()
        .enumerate()
        .filter(|(_, l)| !l.is_empty())
        .map(|(i, line)| {
            Ok(SourceRecord {
                material_id,
                anchor: MaterialAnchor::StreamFrame {
                    material_offset: 0,
                    frame_index: i as u64,
                },
                bytes: line.as_bytes().to_vec(),
                logical_path: None,
                source_ts_hint: None,
                metadata: serde_json::Value::Null,
            })
        })
        .collect()
}

#[cfg(test)]
#[path = "journalctl_stream_test.rs"]
mod tests;

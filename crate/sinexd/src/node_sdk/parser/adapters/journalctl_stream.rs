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

use crate::node_sdk::parser::{InputShapeAdapter, ParserError, ParserResult};

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
        cmd.arg("-f")
            .arg("-o")
            .arg("json")
            .arg("--no-pager")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);

        // Unit filters.
        for unit in &config.units {
            cmd.arg(format!("--unit={unit}"));
        }

        // Priority filter.
        if let Some(p) = config.priority {
            cmd.arg(format!("--priority={p}"));
        }

        // Cursor resumption — prefer runtime cursor over config.
        let resume_cursor = cursor
            .map(|c| c.cursor)
            .or_else(|| config.from_cursor.clone());
        if let Some(ref c) = resume_cursor {
            cmd.arg(format!("--cursor={c}"));
        }

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
            // No cursor field: use the frame_index as a fallback string.
            match &record.anchor {
                MaterialAnchor::StreamFrame { frame_index, .. } => {
                    Ok(JournalctlCursor::new(format!("frame:{frame_index}")))
                }
                other => Err(ParserError::Cursor(format!(
                    "journalctl record has no __CURSOR and unexpected anchor: {other:?}"
                ))),
            }
        }
    }
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
    ) -> crate::node_sdk::parser::ParserResult<Self> {
        Self::with_capacity(material_id, config, BROADCAST_CAPACITY).await
    }

    /// Like [`new`](Self::new) but with a configurable broadcast channel capacity.
    pub async fn with_capacity(
        material_id: sinex_primitives::ids::Id<sinex_primitives::events::SourceMaterial>,
        config: &JournalctlStreamConfig,
        capacity: usize,
    ) -> crate::node_sdk::parser::ParserResult<Self> {
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
/// `register_adapter_ingestor!` in source contracts that share a subprocess.
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
    ) -> crate::node_sdk::parser::ParserResult<
        BoxStream<'static, crate::node_sdk::parser::ParserResult<SourceRecord>>,
    > {
        // JournalctlSubscriber cannot be opened multiple times — it wraps a
        // single-use `broadcast::Receiver`. The caller must construct a new
        // `JournalctlSubscriber` via `SharedJournalctlStream::subscribe()` for
        // each open call.
        Err(crate::node_sdk::parser::ParserError::Adapter(
            "JournalctlSubscriber::open() is not supported — use \
             into_stream() to consume the subscriber as a stream instead. \
             For integration with register_adapter_ingestor!, call \
             SharedJournalctlStream::subscribe() fresh for each open."
                .into(),
        ))
    }

    fn cursor_after(
        &self,
        record: &SourceRecord,
    ) -> crate::node_sdk::parser::ParserResult<Self::Cursor> {
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
    ) -> impl futures::Stream<Item = crate::node_sdk::parser::ParserResult<SourceRecord>> + Send + 'static
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
mod tests {
    use super::*;
    use xtask::sandbox::prelude::sinex_test;

    fn dummy_material_id() -> Id<SourceMaterial> {
        Id::from_uuid(uuid::Uuid::new_v4())
    }

    const JOURNAL_LINE_WITH_CURSOR: &str =
        r#"{"__CURSOR":"s=abc;i=1;b=x","MESSAGE":"hello","PRIORITY":"6"}"#;
    const JOURNAL_LINE_NO_CURSOR: &str = r#"{"MESSAGE":"no cursor here","PRIORITY":"6"}"#;

    #[sinex_test]
    async fn test_records_from_lines_happy_path() -> xtask::sandbox::TestResult<()> {
        let mid = dummy_material_id();
        let records = records_from_journal_lines(mid, &[JOURNAL_LINE_WITH_CURSOR]);
        assert_eq!(records.len(), 1);
        assert!(records[0].is_ok());
        Ok(())
    }

    #[sinex_test]
    async fn test_cursor_after_extracts_cursor_field() -> xtask::sandbox::TestResult<()> {
        let mid = dummy_material_id();
        let records = records_from_journal_lines(mid, &[JOURNAL_LINE_WITH_CURSOR]);
        let record = records[0].as_ref().unwrap();

        let adapter = JournalctlStreamAdapter;
        let cursor = adapter.cursor_after(record).unwrap();
        assert_eq!(cursor.cursor, "s=abc;i=1;b=x");
        Ok(())
    }

    #[sinex_test]
    async fn test_cursor_after_fallback_to_frame_index() -> xtask::sandbox::TestResult<()> {
        let mid = dummy_material_id();
        let records = records_from_journal_lines(mid, &[JOURNAL_LINE_NO_CURSOR]);
        let record = records[0].as_ref().unwrap();

        let adapter = JournalctlStreamAdapter;
        let cursor = adapter.cursor_after(record).unwrap();
        assert!(cursor.cursor.starts_with("frame:"));
        Ok(())
    }

    #[sinex_test]
    async fn test_cursor_after_non_json_errors() -> xtask::sandbox::TestResult<()> {
        let mid = dummy_material_id();
        let record = SourceRecord {
            material_id: mid,
            anchor: MaterialAnchor::StreamFrame {
                material_offset: 0,
                frame_index: 0,
            },
            bytes: b"not json at all".to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        };

        let adapter = JournalctlStreamAdapter;
        assert!(adapter.cursor_after(&record).is_err());
        Ok(())
    }

    #[sinex_test]
    async fn test_records_skips_empty_lines() -> xtask::sandbox::TestResult<()> {
        let mid = dummy_material_id();
        let records = records_from_journal_lines(mid, &["", JOURNAL_LINE_WITH_CURSOR, ""]);
        assert_eq!(records.len(), 1);
        Ok(())
    }

    #[sinex_test]
    async fn test_kind_is_subprocess() -> xtask::sandbox::TestResult<()> {
        assert_eq!(JournalctlStreamAdapter::KIND, InputShapeKind::Subprocess);
        Ok(())
    }

    #[sinex_test]
    async fn test_multiple_lines_have_monotonic_frame_indices() -> xtask::sandbox::TestResult<()> {
        let mid = dummy_material_id();
        let lines = [JOURNAL_LINE_WITH_CURSOR, JOURNAL_LINE_NO_CURSOR];
        let records = records_from_journal_lines(mid, &lines);
        let indices: Vec<u64> = records
            .iter()
            .map(|r| match &r.as_ref().unwrap().anchor {
                MaterialAnchor::StreamFrame { frame_index, .. } => *frame_index,
                _ => panic!("unexpected anchor"),
            })
            .collect();
        for w in indices.windows(2) {
            assert!(w[0] < w[1]);
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_cursor_serde_roundtrip() -> xtask::sandbox::TestResult<()> {
        let cursor = JournalctlCursor::new("s=abc;i=42;b=deadbeef");
        let json = serde_json::to_string(&cursor).unwrap();
        let back: JournalctlCursor = serde_json::from_str(&json).unwrap();
        assert_eq!(cursor, back);
        Ok(())
    }

    #[sinex_test]
    async fn test_cursor_after_non_stream_frame_anchor_errors() -> xtask::sandbox::TestResult<()> {
        // Cover the fallback Err arm: record has no __CURSOR field AND its
        // anchor is not StreamFrame. This pins the contract that the only
        // anchors journalctl can survive without a __CURSOR are stream frames.
        let adapter = JournalctlStreamAdapter;
        let record = SourceRecord {
            material_id: dummy_material_id(),
            anchor: MaterialAnchor::SqliteRow {
                table: "fake".into(),
                rowid: 1,
            },
            bytes: b"{\"MESSAGE\":\"hi\"}".to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        };
        let err = adapter.cursor_after(&record);
        assert!(matches!(err, Err(ParserError::Cursor(_))));
        Ok(())
    }

    #[sinex_test]
    async fn test_cursor_after_invalid_json_errors() -> xtask::sandbox::TestResult<()> {
        let adapter = JournalctlStreamAdapter;
        let record = SourceRecord {
            material_id: dummy_material_id(),
            anchor: MaterialAnchor::StreamFrame {
                material_offset: 0,
                frame_index: 1,
            },
            bytes: b"not-json".to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        };
        let err = adapter.cursor_after(&record);
        assert!(matches!(err, Err(ParserError::Cursor(_))));
        Ok(())
    }

    // =========================================================================
    // SharedJournalctlStream structural tests
    //
    // These tests do NOT spawn a real journalctl process.  Instead, they drive
    // the broadcast channel directly to verify subscriber routing semantics.
    // =========================================================================

    /// Build a `SourceRecord` from raw bytes — minimal helper for shared tests.
    fn make_record_bytes(bytes: &[u8]) -> SourceRecord {
        SourceRecord {
            material_id: dummy_material_id(),
            anchor: MaterialAnchor::StreamFrame {
                material_offset: 0,
                frame_index: 0,
            },
            bytes: bytes.to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        }
    }

    /// Drive records into a broadcast sender and collect what a subscriber
    /// with a given filter receives.
    async fn drive_and_collect(
        tx: broadcast::Sender<SourceRecord>,
        subscriber: super::JournalctlSubscriber,
        records: Vec<SourceRecord>,
    ) -> Vec<Vec<u8>> {
        use futures::StreamExt;
        // Spawn a task that sends records then drops the tx.
        let sender_task = tokio::spawn(async move {
            for rec in records {
                let _ = tx.send(rec);
            }
            // tx dropped here — closes the channel.
        });

        // Collect from subscriber stream until it ends.
        let mut stream = std::pin::pin!(subscriber.into_stream());
        let mut received = Vec::new();
        while let Some(item) = stream.next().await {
            if let Ok(rec) = item {
                received.push(rec.bytes.clone());
            }
        }
        sender_task.await.unwrap();
        received
    }

    #[sinex_test]
    async fn test_subscriber_filter_passes_matching_records() -> xtask::sandbox::TestResult<()> {
        let (tx, rx_primary) = broadcast::channel::<SourceRecord>(64);

        // Filter: only records whose bytes start with b"MATCH"
        let subscriber = super::JournalctlSubscriber {
            receiver: rx_primary,
            filter: Box::new(|rec: &SourceRecord| rec.bytes.starts_with(b"MATCH")),
        };

        let records = vec![
            make_record_bytes(b"MATCH_1"),
            make_record_bytes(b"SKIP_1"),
            make_record_bytes(b"MATCH_2"),
            make_record_bytes(b"SKIP_2"),
        ];

        let received = drive_and_collect(tx, subscriber, records).await;
        assert_eq!(
            received.len(),
            2,
            "expected 2 matching records, got {}",
            received.len()
        );
        assert!(received[0].starts_with(b"MATCH"));
        assert!(received[1].starts_with(b"MATCH"));
        Ok(())
    }

    #[sinex_test]
    async fn test_two_subscribers_receive_independently() -> xtask::sandbox::TestResult<()> {
        use futures::StreamExt;

        let (tx, _) = broadcast::channel::<SourceRecord>(64);

        // subscriber A: only "A" records
        let sub_a = super::JournalctlSubscriber {
            receiver: tx.subscribe(),
            filter: Box::new(|r: &SourceRecord| r.bytes.starts_with(b"A")),
        };
        // subscriber B: only "B" records
        let sub_b = super::JournalctlSubscriber {
            receiver: tx.subscribe(),
            filter: Box::new(|r: &SourceRecord| r.bytes.starts_with(b"B")),
        };

        let records = vec![
            make_record_bytes(b"A1"),
            make_record_bytes(b"B1"),
            make_record_bytes(b"A2"),
            make_record_bytes(b"B2"),
            make_record_bytes(b"C1"), // neither
        ];

        // Collect both subscribers concurrently.
        let tx_clone = tx.clone();
        drop(tx); // release the original; subscribers hold their own receivers

        let sender_task = tokio::spawn(async move {
            for rec in records {
                let _ = tx_clone.send(rec);
            }
            // tx_clone dropped → channel closes
        });

        let stream_a = std::pin::pin!(sub_a.into_stream());
        let stream_b = std::pin::pin!(sub_b.into_stream());

        let (results_a, results_b, _) = tokio::join!(
            stream_a.collect::<Vec<_>>(),
            stream_b.collect::<Vec<_>>(),
            sender_task,
        );

        let bytes_a: Vec<_> = results_a
            .into_iter()
            .filter_map(std::result::Result::ok)
            .map(|r| r.bytes)
            .collect();
        let bytes_b: Vec<_> = results_b
            .into_iter()
            .filter_map(std::result::Result::ok)
            .map(|r| r.bytes)
            .collect();

        assert_eq!(bytes_a.len(), 2, "sub_a should get 2 records");
        assert_eq!(bytes_b.len(), 2, "sub_b should get 2 records");
        assert!(bytes_a.iter().all(|b| b.starts_with(b"A")));
        assert!(bytes_b.iter().all(|b| b.starts_with(b"B")));
        Ok(())
    }

    #[sinex_test]
    async fn test_subscriber_kind_is_subprocess() -> xtask::sandbox::TestResult<()> {
        assert_eq!(
            super::JournalctlSubscriber::KIND,
            InputShapeKind::Subprocess
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_subscriber_cursor_after_extracts_journal_cursor() -> xtask::sandbox::TestResult<()>
    {
        let (tx, rx) = broadcast::channel::<SourceRecord>(4);
        drop(tx); // no sending needed for this test
        let subscriber = super::JournalctlSubscriber {
            receiver: rx,
            filter: Box::new(|_| true),
        };

        let record = SourceRecord {
            material_id: dummy_material_id(),
            anchor: MaterialAnchor::StreamFrame {
                material_offset: 0,
                frame_index: 0,
            },
            bytes: JOURNAL_LINE_WITH_CURSOR.as_bytes().to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        };

        let cursor = subscriber.cursor_after(&record).unwrap();
        assert_eq!(cursor.cursor, "s=abc;i=1;b=x");
        Ok(())
    }

    #[sinex_test]
    async fn test_subscriber_open_returns_error() -> xtask::sandbox::TestResult<()> {
        let (tx, rx) = broadcast::channel::<SourceRecord>(4);
        drop(tx);
        let subscriber = super::JournalctlSubscriber {
            receiver: rx,
            filter: Box::new(|_| true),
        };
        let mid = dummy_material_id();
        let result = subscriber.open(mid, &(), None).await;
        assert!(
            result.is_err(),
            "open() must return an error — use into_stream() instead"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_broadcast_capacity_constant_is_reasonable() -> xtask::sandbox::TestResult<()> {
        // Pin the value so changes are visible in review.
        assert_eq!(super::BROADCAST_CAPACITY, 512);
        Ok(())
    }
}

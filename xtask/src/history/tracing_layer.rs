//! Persistent tracing layer that writes selected trace events to the history SQLite database.
//!
//! Architecture:
//! - A bounded channel (`mpsc::sync_channel(512)`) receives `TraceRecord`s from `on_event()`
//! - A background thread drains the channel and batch-inserts into `trace_events` (SQLite)
//! - Batch flush threshold: 64 events or 200ms timeout
//! - `try_send` is used — tracing never blocks command execution; enqueue/write failures warn once
//!
//! The invocation ID is shared via a module-level `CURRENT_INVOCATION_ID` atomic.
//! `lib.rs` calls `CURRENT_INVOCATION_ID.store(id, Ordering::SeqCst)` after `start_invocation()`.
//!
//! Persistence filter: ERROR and WARN always; INFO only from coordinator, preflight, cargo.
//! DEBUG and TRACE are never persisted.

use rusqlite::{Connection, params};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};
use std::thread;
use std::time::Duration;
use time::OffsetDateTime;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

/// Module-level static for the current invocation ID.
///
/// Updated by `lib.rs` after `HistoryDb::start_invocation()` returns.
/// `-1` is the sentinel for "no active invocation" (SQLite AUTOINCREMENT starts at 1).
pub static CURRENT_INVOCATION_ID: LazyLock<Arc<AtomicI64>> =
    LazyLock::new(|| Arc::new(AtomicI64::new(-1)));
static TRACE_HISTORY_WARNING_EMITTED: AtomicBool = AtomicBool::new(false);

fn warn_trace_history_once(message: &str) {
    if !TRACE_HISTORY_WARNING_EMITTED.swap(true, Ordering::Relaxed) {
        eprintln!("xtask: {message}");
    }
}

fn format_trace_timestamp(timestamp: OffsetDateTime) -> Result<String, time::error::Format> {
    timestamp.format(&time::format_description::well_known::Rfc3339)
}

/// A single trace event to persist to the history database.
struct TraceRecord {
    invocation_id: Option<i64>,
    ts: String,
    level: &'static str,
    target: String,
    event_kind: Option<&'static str>,
    message: String,
    fields: Option<String>,
}

/// Tracing layer that persists selected events to the history SQLite database.
pub struct HistoryTracingLayer {
    db_path: PathBuf,
    tx: OnceLock<std::sync::mpsc::SyncSender<TraceRecord>>,
    invocation_id: Arc<AtomicI64>,
}

impl HistoryTracingLayer {
    /// Create a new layer.
    ///
    /// The history writer thread is spawned lazily on the first persisted event
    /// so read-only commands do not pay a second `HistoryDb::open()` just for
    /// unused trace plumbing.
    #[must_use]
    pub fn new(db_path: PathBuf) -> Self {
        Self {
            db_path,
            tx: OnceLock::new(),
            invocation_id: Arc::clone(&*CURRENT_INVOCATION_ID),
        }
    }

    fn current_invocation_id(&self) -> Option<i64> {
        let id = self.invocation_id.load(Ordering::SeqCst);
        if id == -1 { None } else { Some(id) }
    }

    fn trace_tx(&self) -> &std::sync::mpsc::SyncSender<TraceRecord> {
        self.tx.get_or_init(|| {
            let (tx, rx) = std::sync::mpsc::sync_channel(512);
            let db_path = self.db_path.clone();
            thread::spawn(move || writer_loop(db_path, rx));
            tx
        })
    }
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for HistoryTracingLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();
        if !should_persist(meta.level(), meta.target()) {
            return;
        }
        let mut visitor = FieldExtractor::default();
        event.record(&mut visitor);
        let event_kind = classify_event_kind(meta.target(), meta.level(), &visitor.fields);
        let ts = match format_trace_timestamp(OffsetDateTime::now_utc()) {
            Ok(timestamp) => timestamp,
            Err(error) => {
                warn_trace_history_once(&format!(
                    "failed to format trace event timestamp for history persistence: {error}"
                ));
                return;
            }
        };
        let fields = visitor.extra_fields_as_json();
        let record = TraceRecord {
            invocation_id: self.current_invocation_id(),
            ts,
            level: level_str(meta.level()),
            target: meta.target().to_string(),
            event_kind,
            message: visitor.message,
            fields,
        };
        if let Err(err) = self.trace_tx().try_send(record) {
            warn_trace_history_once(&format!(
                "failed to enqueue trace event for history persistence: {err}"
            ));
        }
    }
}

fn level_str(level: &Level) -> &'static str {
    match *level {
        Level::ERROR => "ERROR",
        Level::WARN => "WARN",
        Level::INFO => "INFO",
        Level::DEBUG => "DEBUG",
        Level::TRACE => "TRACE",
    }
}

fn should_persist(level: &Level, target: &str) -> bool {
    match *level {
        Level::ERROR | Level::WARN => true,
        Level::INFO => {
            target.starts_with("xtask::coordinator")
                || target.starts_with("xtask::preflight")
                || target.starts_with("xtask::cargo")
        }
        Level::DEBUG | Level::TRACE => false,
    }
}

fn classify_event_kind(
    target: &str,
    level: &Level,
    fields: &HashMap<String, Value>,
) -> Option<&'static str> {
    if *level == Level::ERROR {
        return Some("error");
    }
    if *level == Level::WARN {
        return Some("warn");
    }
    if target.starts_with("xtask::coordinator") {
        return Some("coordinator.decision");
    }
    if target.starts_with("xtask::preflight") {
        return Some("preflight.action");
    }
    if target.starts_with("xtask::cargo") {
        return Some(if fields.contains_key("pid") {
            "cargo.spawn"
        } else {
            "cargo.complete"
        });
    }
    None
}

#[derive(Default)]
struct FieldExtractor {
    message: String,
    fields: HashMap<String, Value>,
}

impl Visit for FieldExtractor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else {
            self.fields
                .insert(field.name().to_string(), Value::String(value.to_string()));
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // For tracing's implicit message field, format_args!() Debug == Display
        // (no extra quotes). For other fields, preserve Debug representation.
        if field.name() == "message" {
            if self.message.is_empty() {
                self.message = format!("{value:?}");
            }
        } else {
            self.fields.insert(
                field.name().to_string(),
                Value::String(format!("{value:?}")),
            );
        }
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), Value::Bool(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .insert(field.name().to_string(), Value::Number(value.into()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_string(), Value::Number(value.into()));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        if let Some(n) = serde_json::Number::from_f64(value) {
            self.fields
                .insert(field.name().to_string(), Value::Number(n));
        }
    }
}

impl FieldExtractor {
    fn extra_fields_as_json(&self) -> Option<String> {
        if self.fields.is_empty() {
            return None;
        }
        serde_json::to_string(&self.fields).ok()
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "thread::spawn requires ownership"
)]
fn writer_loop(db_path: PathBuf, rx: std::sync::mpsc::Receiver<TraceRecord>) {
    // X11: Use HistoryDb::open() instead of Connection::open() so the full schema
    // (including the `invocations` table that trace_events FK-references) is
    // guaranteed to exist before we create the trace_events table. HistoryDb::open()
    // also handles WAL mode, busy_timeout, integrity checks, and schema migrations.
    let mut conn = match super::HistoryDb::open(&db_path) {
        Ok(db) => db.conn,
        Err(err) => {
            warn_trace_history_once(&format!(
                "failed to open trace history database at {}: {err}",
                db_path.display()
            ));
            return;
        }
    };
    if let Err(err) = ensure_trace_events_table(&conn) {
        warn_trace_history_once(&format!(
            "failed to ensure trace_events history table exists: {err}"
        ));
        return;
    }

    let mut batch: Vec<TraceRecord> = Vec::with_capacity(64);
    loop {
        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(record) => {
                batch.push(record);
                // Drain remaining without blocking
                while batch.len() < 64 {
                    match rx.try_recv() {
                        Ok(r) => batch.push(r),
                        Err(_) => break,
                    }
                }
                flush_batch(&mut conn, &mut batch);
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                if !batch.is_empty() {
                    flush_batch(&mut conn, &mut batch);
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                flush_batch(&mut conn, &mut batch);
                break;
            }
        }
    }
}

fn ensure_trace_events_table(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        r"
        CREATE TABLE IF NOT EXISTS trace_events (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            invocation_id INTEGER REFERENCES invocations(id) ON DELETE CASCADE,
            ts            TEXT    NOT NULL,
            level         TEXT    NOT NULL,
            target        TEXT    NOT NULL,
            event_kind    TEXT,
            message       TEXT    NOT NULL,
            fields        TEXT
        );
        CREATE INDEX IF NOT EXISTS trace_events_invocation_idx  ON trace_events(invocation_id);
        CREATE INDEX IF NOT EXISTS trace_events_level_idx       ON trace_events(level);
        CREATE INDEX IF NOT EXISTS trace_events_event_kind_idx  ON trace_events(event_kind);
        CREATE INDEX IF NOT EXISTS trace_events_ts_idx          ON trace_events(ts);
        ",
    )
}

fn flush_batch(conn: &mut Connection, batch: &mut Vec<TraceRecord>) {
    if batch.is_empty() {
        return;
    }
    let pending = std::mem::take(batch);
    let tx = match conn.transaction() {
        Ok(tx) => tx,
        Err(err) => {
            warn_trace_history_once(&format!(
                "failed to start trace history transaction; dropped {} trace event(s): {err}",
                pending.len()
            ));
            return;
        }
    };
    for record in pending {
        if let Err(err) = tx.execute(
            r"INSERT INTO trace_events
              (invocation_id, ts, level, target, event_kind, message, fields)
              VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                record.invocation_id,
                record.ts,
                record.level,
                record.target,
                record.event_kind,
                record.message,
                record.fields
            ],
        ) {
            warn_trace_history_once(&format!(
                "failed to persist trace event; remaining trace batch entries will be dropped: {err}"
            ));
            return;
        }
    }
    if let Err(err) = tx.commit() {
        warn_trace_history_once(&format!("failed to commit trace history batch: {err}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;
    use crate::sandbox::timing::WaitHelpers;
    use color_eyre::eyre::Context;
    use tempfile::tempdir;
    use tracing_subscriber::prelude::*;

    #[sinex_test]
    async fn test_history_tracing_layer_is_lazy_until_first_persisted_event() -> TestResult<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("history.db");

        let _layer = HistoryTracingLayer::new(db_path.clone());

        assert!(
            !db_path.exists(),
            "history trace DB should not exist before the first persisted event"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_history_tracing_layer_persists_first_warn_event() -> TestResult<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("history.db");
        let subscriber =
            tracing_subscriber::registry().with(HistoryTracingLayer::new(db_path.clone()));

        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(target: "xtask::history.tests", code = 17_i64, "persist trace event");
        });

        WaitHelpers::wait_for_condition(
            || {
                let db_path = db_path.clone();
                async move {
                    if !db_path.exists() {
                        return Ok::<bool, color_eyre::Report>(false);
                    }

                    let conn = Connection::open(&db_path)
                        .with_context(|| format!("failed to open {}", db_path.display()))?;
                    let table_exists = conn
                        .query_row(
                            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'trace_events'",
                            [],
                            |_| Ok(()),
                        )
                        .map(|()| true)
                        .or_else(|error| {
                            if matches!(error, rusqlite::Error::QueryReturnedNoRows) {
                                Ok(false)
                            } else {
                                Err(error)
                            }
                        })?;
                    if !table_exists {
                        return Ok::<bool, color_eyre::Report>(false);
                    }

                    let count: i64 =
                        conn.query_row("SELECT COUNT(*) FROM trace_events", [], |row| row.get(0))?;
                    Ok::<bool, color_eyre::Report>(count == 1)
                }
            },
            5,
        )
        .await?;

        Ok(())
    }
}

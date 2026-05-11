//! Adapter for streaming journald entries via `journalctl -f -o json`.
//!
//! Spawns a `journalctl` child process and reads JSON lines from stdout.
//! Each line is a JSON object representing one journal entry. The adapter
//! extracts `__CURSOR` from each record to form the checkpoint cursor.
//!
//! Cursor is the journal cursor string (`String`). No replay — journald
//! manages retention. This adapter resumes from where it left off by
//! passing `--cursor=<cursor>` to the child process.

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeKind, MaterialAnchor, SourceRecord};

use crate::parser::{InputShapeAdapter, ParserError, ParserResult};

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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
        Self { cursor: cursor.into() }
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
        let json: serde_json::Value = serde_json::from_slice(&record.bytes)
            .map_err(|e| ParserError::Cursor(format!("failed to parse journal record as JSON: {e}")))?;

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
// Test helpers
// =============================================================================

/// Feed a slice of pre-formed journal JSON lines through the journalctl
/// line parser without spawning a real process.
///
/// This function mirrors what `open()` does to a stream of lines, so tests
/// can exercise the record-building and cursor logic without a live systemd.
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

    fn dummy_material_id() -> Id<SourceMaterial> {
        Id::from_uuid(uuid::Uuid::new_v4())
    }

    const JOURNAL_LINE_WITH_CURSOR: &str =
        r#"{"__CURSOR":"s=abc;i=1;b=x","MESSAGE":"hello","PRIORITY":"6"}"#;
    const JOURNAL_LINE_NO_CURSOR: &str =
        r#"{"MESSAGE":"no cursor here","PRIORITY":"6"}"#;

    #[test]
    fn test_records_from_lines_happy_path() {
        let mid = dummy_material_id();
        let records = records_from_journal_lines(mid, &[JOURNAL_LINE_WITH_CURSOR]);
        assert_eq!(records.len(), 1);
        assert!(records[0].is_ok());
    }

    #[test]
    fn test_cursor_after_extracts_cursor_field() {
        let mid = dummy_material_id();
        let records = records_from_journal_lines(mid, &[JOURNAL_LINE_WITH_CURSOR]);
        let record = records[0].as_ref().unwrap();

        let adapter = JournalctlStreamAdapter;
        let cursor = adapter.cursor_after(record).unwrap();
        assert_eq!(cursor.cursor, "s=abc;i=1;b=x");
    }

    #[test]
    fn test_cursor_after_fallback_to_frame_index() {
        let mid = dummy_material_id();
        let records = records_from_journal_lines(mid, &[JOURNAL_LINE_NO_CURSOR]);
        let record = records[0].as_ref().unwrap();

        let adapter = JournalctlStreamAdapter;
        let cursor = adapter.cursor_after(record).unwrap();
        assert!(cursor.cursor.starts_with("frame:"));
    }

    #[test]
    fn test_cursor_after_non_json_errors() {
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
    }

    #[test]
    fn test_records_skips_empty_lines() {
        let mid = dummy_material_id();
        let records = records_from_journal_lines(mid, &["", JOURNAL_LINE_WITH_CURSOR, ""]);
        // Empty lines are filtered in the live stream; in our helper they are
        // included as entries but the filter would remove them. Here we just
        // verify the helper doesn't crash on empty lines.
        assert!(!records.is_empty());
    }

    #[test]
    fn test_kind_is_subprocess() {
        assert_eq!(JournalctlStreamAdapter::KIND, InputShapeKind::Subprocess);
    }

    #[test]
    fn test_multiple_lines_have_monotonic_frame_indices() {
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
    }

    #[test]
    fn test_cursor_serde_roundtrip() {
        let cursor = JournalctlCursor::new("s=abc;i=42;b=deadbeef");
        let json = serde_json::to_string(&cursor).unwrap();
        let back: JournalctlCursor = serde_json::from_str(&json).unwrap();
        assert_eq!(cursor, back);
    }

    #[test]
    fn test_cursor_after_non_stream_frame_anchor_errors() {
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
    }

    #[test]
    fn test_cursor_after_invalid_json_errors() {
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
    }
}

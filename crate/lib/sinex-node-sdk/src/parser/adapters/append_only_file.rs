//! Adapter for append-only (log-style) files.
//!
//! Detects inode rotation: when the file at `path` has a different inode
//! from the one observed in the cursor, the adapter resets the byte/line
//! offsets to 0 and marks the first post-rotation record's `metadata` with
//! `{"rotation_detected": true, "previous_inode": ..., "current_inode": ...}`.
//! Parsers that need to dedupe across a rotation can layer the
//! [`crate::parser::dedup::ContentHashWindow`] helper on top.

use async_trait::async_trait;
use camino::Utf8Path;
use futures::stream::{self, BoxStream, StreamExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeKind, MaterialAnchor, SourceRecord};

use crate::parser::{InputShapeAdapter, ParserError, ParserResult};

// =============================================================================
// AppendOnlyFileAdapter
// =============================================================================

/// Adapter for a file that grows by appending lines.
///
/// Yields one [`SourceRecord`] per line.
/// Supports resumption via line-number cursor.
#[derive(Debug, Clone, Default)]
pub struct AppendOnlyFileAdapter;

/// Configuration for [`AppendOnlyFileAdapter`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AppendOnlyFileConfig {
    /// Path to the file on disk.
    pub path: String,

    /// If true, skip empty lines.
    #[serde(default)]
    pub skip_empty: bool,
}

/// Cursor for [`AppendOnlyFileAdapter`] — tracks the last-read line number,
/// byte offset, and (when available) the file's inode at the time of capture.
///
/// `inode` is an `Option<u64>` so cursors persisted before inode tracking
/// existed continue to deserialize without breakage. On the first scan after
/// upgrading, the adapter populates `inode` and rotation detection becomes
/// active on subsequent scans.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendOnlyCursor {
    pub last_line: u64,
    pub last_byte_offset: u64,
    /// Inode of the file when the cursor was last advanced. `None` for cursors
    /// persisted before inode tracking was introduced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inode: Option<u64>,
}

impl AppendOnlyCursor {
    #[must_use]
    pub const fn start() -> Self {
        Self {
            last_line: 0,
            last_byte_offset: 0,
            inode: None,
        }
    }
}

#[async_trait]
impl InputShapeAdapter for AppendOnlyFileAdapter {
    type Config = AppendOnlyFileConfig;
    type Cursor = AppendOnlyCursor;
    const KIND: InputShapeKind = InputShapeKind::AppendOnlyFile;

    async fn open(
        &self,
        material_id: Id<SourceMaterial>,
        config: &Self::Config,
        cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        let path = config.path.clone();
        let skip_empty = config.skip_empty;

        // Graceful empty / missing path: when the binding wires this leg
        // optionally (e.g. ChainedAdapter's secondary slot for browser.history
        // dump exports that the deployer hasn't configured), an unset path
        // arrives as the empty string. Treat that as "no records yet" rather
        // than a hard adapter error. Same for paths that don't exist on disk
        // — the file may appear later (e.g. browser export drop directory).
        if path.is_empty() || std::fs::metadata(&path).is_err() {
            let empty: BoxStream<'static, ParserResult<SourceRecord>> = Box::pin(stream::empty());
            return Ok(empty);
        }

        // Detect rotation by comparing the cursor's stored inode with the
        // file's current inode. If they differ, the file was rotated: the old
        // log's bytes are gone (or have been moved aside), so we must start
        // scanning at offset 0 instead of inheriting stale offsets.
        let current_ino = current_inode(&path);
        let (start_offset, start_line, rotation_marker) =
            match (cursor.as_ref().and_then(|c| c.inode), current_ino) {
                (Some(prev), Some(curr)) if prev != curr => {
                    // Rotation observed: reset to start-of-file, surface the
                    // transition on the first emitted record so parsers can react
                    // (emit a `parser.stream_rotation_detected` synthesis event,
                    // flush dedup window, etc.).
                    let marker = serde_json::json!({
                        "rotation_detected": true,
                        "previous_inode": prev,
                        "current_inode": curr,
                    });
                    (0_u64, 1_u64, Some(marker))
                }
                _ => (
                    cursor.as_ref().map_or(0, |c| c.last_byte_offset),
                    cursor.as_ref().map_or(1, |c| c.last_line + 1),
                    None,
                ),
            };

        let content = std::fs::read_to_string(&path)?;

        let mut records = Vec::new();
        let mut line_num: u64 = 0;
        let mut byte_offset: u64 = 0;
        let mut rotation_marker = rotation_marker;

        for line in content.lines() {
            line_num += 1;
            let line_bytes = line.as_bytes().to_vec();
            let line_len = line_bytes.len() as u64;

            if line_num < start_line {
                byte_offset += line_len + 1; // +1 for newline
                continue;
            }

            if byte_offset < start_offset {
                byte_offset += line_len + 1;
                continue;
            }

            if skip_empty && line.is_empty() {
                byte_offset += line_len + 1;
                continue;
            }

            // Embed the file's inode in every record so cursor_after() can
            // round-trip it back into AppendOnlyCursor.inode without keeping
            // adapter-side mutable state. The first post-rotation record also
            // carries the rotation marker.
            let metadata = build_record_metadata(current_ino, rotation_marker.take());

            records.push(SourceRecord {
                material_id,
                anchor: MaterialAnchor::Line {
                    byte_start: byte_offset,
                    line: line_num,
                },
                bytes: line_bytes,
                logical_path: Some(Utf8Path::new(&path).to_owned()),
                source_ts_hint: None,
                metadata,
            });

            byte_offset += line_len + 1;
        }

        Ok(stream::iter(records.into_iter().map(Ok)).boxed())
    }

    fn cursor_after(&self, record: &SourceRecord) -> ParserResult<Self::Cursor> {
        match &record.anchor {
            MaterialAnchor::Line { byte_start, line } => Ok(AppendOnlyCursor {
                last_line: *line,
                last_byte_offset: *byte_start,
                inode: record
                    .metadata
                    .get(ADAPTER_INODE_KEY)
                    .and_then(serde_json::Value::as_u64),
            }),
            other => Err(ParserError::Cursor(format!(
                "expected Line anchor, got {other:?}"
            ))),
        }
    }
}

/// Metadata key under which the adapter embeds the file's inode in every
/// emitted record. Exposed for parsers that need to inspect it.
pub const ADAPTER_INODE_KEY: &str = "_append_only_inode";

/// Read the inode of `path` on Unix; returns `None` on other platforms or if
/// the path is unreadable. The `None` case disables rotation detection but
/// does not error out — the adapter still functions, just without rotation
/// awareness (the prior behavior).
#[cfg(unix)]
fn current_inode(path: &str) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).ok().map(|m| m.ino())
}

#[cfg(not(unix))]
fn current_inode(_path: &str) -> Option<u64> {
    None
}

/// Build the per-record metadata value, merging the inode (when known) with
/// an optional rotation marker. Always emits an object when either is set so
/// `cursor_after` can read the inode back via `get()`.
fn build_record_metadata(
    inode: Option<u64>,
    rotation: Option<serde_json::Value>,
) -> serde_json::Value {
    match (inode, rotation) {
        (None, None) => serde_json::Value::Null,
        (Some(ino), None) => serde_json::json!({ ADAPTER_INODE_KEY: ino }),
        (None, Some(rot)) => rot,
        (Some(ino), Some(serde_json::Value::Object(mut map))) => {
            map.insert(ADAPTER_INODE_KEY.to_string(), ino.into());
            serde_json::Value::Object(map)
        }
        (Some(ino), Some(other)) => serde_json::json!({
            ADAPTER_INODE_KEY: ino,
            "rotation": other,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;
    use xtask::sandbox::prelude::sinex_test;

    fn dummy_material_id() -> Id<SourceMaterial> {
        Id::from_uuid(uuid::Uuid::new_v4())
    }

    #[sinex_test]
    async fn test_append_only_yields_one_record_per_line() -> xtask::sandbox::TestResult<()> {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f, "line1").unwrap();
        writeln!(f, "line2").unwrap();
        writeln!(f, "line3").unwrap();
        let path = f.path().to_str().unwrap().to_string();

        let adapter = AppendOnlyFileAdapter;
        let config = AppendOnlyFileConfig {
            path,
            skip_empty: false,
        };
        let stream = adapter
            .open(dummy_material_id(), &config, None)
            .await
            .unwrap();
        let records: Vec<_> = stream.collect().await;

        assert_eq!(records.len(), 3);
        assert_eq!(records[0].as_ref().unwrap().bytes, b"line1");
        assert_eq!(records[1].as_ref().unwrap().bytes, b"line2");
        assert_eq!(records[2].as_ref().unwrap().bytes, b"line3");
        Ok(())
    }

    #[sinex_test]
    async fn test_append_only_skip_empty_lines() -> xtask::sandbox::TestResult<()> {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f, "first").unwrap();
        writeln!(f).unwrap();
        writeln!(f, "second").unwrap();
        let path = f.path().to_str().unwrap().to_string();

        let adapter = AppendOnlyFileAdapter;
        let config = AppendOnlyFileConfig {
            path,
            skip_empty: true,
        };
        let stream = adapter
            .open(dummy_material_id(), &config, None)
            .await
            .unwrap();
        let records: Vec<_> = stream.collect().await;

        assert_eq!(records.len(), 2);
        Ok(())
    }

    #[sinex_test]
    async fn test_append_only_resume_from_cursor() -> xtask::sandbox::TestResult<()> {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f, "line1").unwrap();
        writeln!(f, "line2").unwrap();
        writeln!(f, "line3").unwrap();
        let path = f.path().to_str().unwrap().to_string();

        let adapter = AppendOnlyFileAdapter;
        let config = AppendOnlyFileConfig {
            path: path.clone(),
            skip_empty: false,
        };

        // First pass to get cursor for line 2
        let stream = adapter
            .open(dummy_material_id(), &config, None)
            .await
            .unwrap();
        let records: Vec<_> = stream.collect().await;
        let cursor_after_line2 = adapter.cursor_after(records[1].as_ref().unwrap()).unwrap();

        // Resume: should only yield line3
        let stream2 = adapter
            .open(dummy_material_id(), &config, Some(cursor_after_line2))
            .await
            .unwrap();
        let records2: Vec<_> = stream2.collect().await;

        assert_eq!(records2.len(), 1);
        assert_eq!(records2[0].as_ref().unwrap().bytes, b"line3");
        Ok(())
    }

    #[sinex_test]
    async fn test_append_only_line_anchor() -> xtask::sandbox::TestResult<()> {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f, "hello").unwrap();
        let path = f.path().to_str().unwrap().to_string();

        let adapter = AppendOnlyFileAdapter;
        let config = AppendOnlyFileConfig {
            path,
            skip_empty: false,
        };
        let mut stream = adapter
            .open(dummy_material_id(), &config, None)
            .await
            .unwrap();
        let record = stream.next().await.unwrap().unwrap();

        assert!(matches!(
            record.anchor,
            MaterialAnchor::Line { line: 1, .. }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn test_append_only_cursor_after_wrong_anchor_errors() -> xtask::sandbox::TestResult<()> {
        let adapter = AppendOnlyFileAdapter;
        let record = SourceRecord {
            material_id: dummy_material_id(),
            anchor: MaterialAnchor::ByteRange { start: 0, len: 10 },
            bytes: b"x".to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        };
        assert!(adapter.cursor_after(&record).is_err());
        Ok(())
    }

    #[sinex_test]
    async fn test_append_only_missing_file_returns_error() -> xtask::sandbox::TestResult<()> {
        let adapter = AppendOnlyFileAdapter;
        let config = AppendOnlyFileConfig {
            path: "/nonexistent/file.log".into(),
            skip_empty: false,
        };
        assert!(
            adapter
                .open(dummy_material_id(), &config, None)
                .await
                .is_err()
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_append_only_records_carry_inode_when_unix() -> xtask::sandbox::TestResult<()> {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f, "x").unwrap();
        let path = f.path().to_str().unwrap().to_string();

        let adapter = AppendOnlyFileAdapter;
        let config = AppendOnlyFileConfig {
            path,
            skip_empty: false,
        };
        let stream = adapter
            .open(dummy_material_id(), &config, None)
            .await
            .unwrap();
        let records: Vec<_> = stream.collect().await;

        let rec = records[0].as_ref().unwrap();
        if cfg!(unix) {
            // Inode should be embedded in metadata on every record so
            // cursor_after can round-trip it.
            let ino = rec.metadata.get(ADAPTER_INODE_KEY).and_then(|v| v.as_u64());
            assert!(ino.is_some(), "expected inode in metadata on unix");
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_append_only_rotation_resets_offsets() -> xtask::sandbox::TestResult<()> {
        // Two distinct temp files act as "before rotation" and "after rotation".
        // The cursor returned from scanning the first file carries the first
        // file's inode; supplying it to a scan of the second file (different
        // inode) MUST cause the adapter to reset to offset 0 and tag the first
        // emitted record with rotation metadata.
        let mut f1 = NamedTempFile::new().unwrap();
        writeln!(f1, "old-line-1").unwrap();
        writeln!(f1, "old-line-2").unwrap();
        let path1 = f1.path().to_str().unwrap().to_string();

        let mut f2 = NamedTempFile::new().unwrap();
        writeln!(f2, "new-line-1").unwrap();
        writeln!(f2, "new-line-2").unwrap();
        let path2 = f2.path().to_str().unwrap().to_string();

        let adapter = AppendOnlyFileAdapter;

        // Scan f1 fully, capture cursor.
        let cfg1 = AppendOnlyFileConfig {
            path: path1,
            skip_empty: false,
        };
        let stream1 = adapter
            .open(dummy_material_id(), &cfg1, None)
            .await
            .unwrap();
        let records1: Vec<_> = stream1.collect().await;
        let cursor1 = adapter
            .cursor_after(records1.last().unwrap().as_ref().unwrap())
            .unwrap();
        if cfg!(unix) {
            assert!(cursor1.inode.is_some(), "f1 cursor must capture inode");
        }

        // Resume against f2 using f1's cursor (offsets non-zero, inode different).
        let cfg2 = AppendOnlyFileConfig {
            path: path2,
            skip_empty: false,
        };
        let stream2 = adapter
            .open(dummy_material_id(), &cfg2, Some(cursor1.clone()))
            .await
            .unwrap();
        let records2: Vec<_> = stream2.collect().await;

        // On unix the rotation is detected: both new lines are emitted from
        // offset 0 with rotation metadata on the first. Without unix support
        // (no inode), the adapter falls back to inheriting offsets — which
        // would emit zero records because f2 is shorter than cursor1.offset.
        if cfg!(unix) {
            assert_eq!(
                records2.len(),
                2,
                "rotation should reset and emit all of f2"
            );
            let first_meta = &records2[0].as_ref().unwrap().metadata;
            assert_eq!(
                first_meta
                    .get("rotation_detected")
                    .and_then(|v| v.as_bool()),
                Some(true),
                "first post-rotation record must carry rotation_detected: true"
            );
            assert!(
                first_meta
                    .get("previous_inode")
                    .and_then(|v| v.as_u64())
                    .is_some(),
                "rotation marker must include previous_inode"
            );
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_append_only_empty_file_yields_no_records() -> xtask::sandbox::TestResult<()> {
        let f = NamedTempFile::new().unwrap();
        let path = f.path().to_str().unwrap().to_string();

        let adapter = AppendOnlyFileAdapter;
        let config = AppendOnlyFileConfig {
            path,
            skip_empty: false,
        };
        let stream = adapter
            .open(dummy_material_id(), &config, None)
            .await
            .unwrap();
        let records: Vec<_> = stream.collect().await;

        assert!(records.is_empty());
        Ok(())
    }
}

//! Adapter for append-only (log-style) files.
//!
//! Detects inode rotation: when the file at `path` has a different inode
//! from the one observed in the cursor, the adapter resets the byte/line
//! offsets to 0 and marks the first post-rotation record's `metadata` with
//! `{"rotation_detected": true, "previous_inode": ..., "current_inode": ...}`.
//! Parsers that need to dedupe across a rotation can layer the
//! [`crate::runtime::parser::dedup::ContentHashWindow`] helper on top.

use async_trait::async_trait;
use camino::Utf8Path;
use futures::stream::{self, BoxStream, StreamExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeKind, MaterialAnchor, SourceRecord};

use crate::runtime::parser::{InputShapeAdapter, ParserError, ParserResult};

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
                    // (emit a `parser.stream_rotation_detected` derived event,
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
#[path = "append_only_file_test.rs"]
mod tests;

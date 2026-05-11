//! Adapter for append-only (log-style) files.

use async_trait::async_trait;
use camino::Utf8Path;
use futures::stream::{self, BoxStream, StreamExt};
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendOnlyFileConfig {
    /// Path to the file on disk.
    pub path: String,

    /// If true, skip empty lines.
    #[serde(default)]
    pub skip_empty: bool,
}

/// Cursor for [`AppendOnlyFileAdapter`] — tracks the last-read line number
/// and byte offset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendOnlyCursor {
    pub last_line: u64,
    pub last_byte_offset: u64,
}

impl AppendOnlyCursor {
    #[must_use]
    pub const fn start() -> Self {
        Self {
            last_line: 0,
            last_byte_offset: 0,
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
        let start_offset = cursor.as_ref().map_or(0, |c| c.last_byte_offset);
        let start_line = cursor.as_ref().map_or(1, |c| c.last_line + 1);

        let content = std::fs::read_to_string(&path)?;

        let mut records = Vec::new();
        let mut line_num: u64 = 0;
        let mut byte_offset: u64 = 0;

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

            records.push(SourceRecord {
                material_id,
                anchor: MaterialAnchor::Line {
                    byte_start: byte_offset,
                    line: line_num,
                },
                bytes: line_bytes,
                logical_path: Some(Utf8Path::new(&path).to_owned().into()),
                source_ts_hint: None,
                metadata: serde_json::Value::Null,
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
            }),
            other => Err(ParserError::Cursor(format!(
                "expected Line anchor, got {other:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::sinex_test;
    use std::io::Write;
    use tempfile::NamedTempFile;

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
        let config = AppendOnlyFileConfig { path, skip_empty: false };
        let stream = adapter.open(dummy_material_id(), &config, None).await.unwrap();
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
        let config = AppendOnlyFileConfig { path, skip_empty: true };
        let stream = adapter.open(dummy_material_id(), &config, None).await.unwrap();
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
        let config = AppendOnlyFileConfig { path: path.clone(), skip_empty: false };

        // First pass to get cursor for line 2
        let stream = adapter.open(dummy_material_id(), &config, None).await.unwrap();
        let records: Vec<_> = stream.collect().await;
        let cursor_after_line2 = adapter.cursor_after(records[1].as_ref().unwrap()).unwrap();

        // Resume: should only yield line3
        let stream2 = adapter.open(dummy_material_id(), &config, Some(cursor_after_line2)).await.unwrap();
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
        let config = AppendOnlyFileConfig { path, skip_empty: false };
        let mut stream = adapter.open(dummy_material_id(), &config, None).await.unwrap();
        let record = stream.next().await.unwrap().unwrap();

        assert!(matches!(record.anchor, MaterialAnchor::Line { line: 1, .. }));
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
        assert!(adapter.open(dummy_material_id(), &config, None).await.is_err());
        Ok(())
    }

    #[sinex_test]
    async fn test_append_only_empty_file_yields_no_records() -> xtask::sandbox::TestResult<()> {
        let f = NamedTempFile::new().unwrap();
        let path = f.path().to_str().unwrap().to_string();

        let adapter = AppendOnlyFileAdapter;
        let config = AppendOnlyFileConfig { path, skip_empty: false };
        let stream = adapter.open(dummy_material_id(), &config, None).await.unwrap();
        let records: Vec<_> = stream.collect().await;

        assert!(records.is_empty());
        Ok(())
    }
}

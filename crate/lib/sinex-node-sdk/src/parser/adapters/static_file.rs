//! Adapter for static (one-shot) file reads.

use async_trait::async_trait;
use camino::Utf8Path;
use futures::stream::{self, BoxStream, StreamExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeKind, MaterialAnchor, SourceRecord};

use crate::parser::{InputShapeAdapter, ParserResult};

// =============================================================================
// StaticFileAdapter
// =============================================================================

/// Adapter for a single static file read once.
///
/// Yields one [`SourceRecord`] containing the entire file contents.
/// Suitable for JSON/CSV/XML exports and other one-shot file formats.
#[derive(Debug, Clone, Default)]
pub struct StaticFileAdapter;

/// Configuration for [`StaticFileAdapter`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StaticFileConfig {
    /// Path to the file on disk.
    pub path: String,
}

/// Cursor for [`StaticFileAdapter`] — a single boolean indicating
/// whether the file has been processed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaticFileCursor {
    pub processed: bool,
}

#[async_trait]
impl InputShapeAdapter for StaticFileAdapter {
    type Config = StaticFileConfig;
    type Cursor = StaticFileCursor;
    const KIND: InputShapeKind = InputShapeKind::StaticFile;

    async fn open(
        &self,
        material_id: Id<SourceMaterial>,
        config: &Self::Config,
        cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        if cursor.is_some_and(|c| c.processed) {
            return Ok(stream::empty().boxed());
        }

        let path = config.path.clone();

        let bytes = std::fs::read(&path)?;

        let len = bytes.len() as u64;
        let record = SourceRecord {
            material_id,
            anchor: MaterialAnchor::ByteRange { start: 0, len },
            bytes,
            logical_path: Some(Utf8Path::new(&path).to_owned()),
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        };

        Ok(stream::once(async move { Ok(record) }).boxed())
    }

    fn cursor_after(&self, _record: &SourceRecord) -> ParserResult<Self::Cursor> {
        Ok(StaticFileCursor { processed: true })
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
    async fn test_static_file_reads_entire_contents() -> xtask::sandbox::TestResult<()> {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"hello world").unwrap();
        let path = f.path().to_str().unwrap().to_string();

        let adapter = StaticFileAdapter;
        let config = StaticFileConfig { path };
        let mut stream = adapter
            .open(dummy_material_id(), &config, None)
            .await
            .unwrap();

        let record = stream.next().await.unwrap().unwrap();
        assert_eq!(record.bytes, b"hello world");
        assert!(stream.next().await.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_static_file_already_processed_returns_empty() -> xtask::sandbox::TestResult<()> {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"data").unwrap();
        let path = f.path().to_str().unwrap().to_string();

        let adapter = StaticFileAdapter;
        let config = StaticFileConfig { path };
        let cursor = Some(StaticFileCursor { processed: true });
        let mut stream = adapter
            .open(dummy_material_id(), &config, cursor)
            .await
            .unwrap();

        assert!(stream.next().await.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_static_file_not_processed_cursor_yields_record() -> xtask::sandbox::TestResult<()>
    {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"content").unwrap();
        let path = f.path().to_str().unwrap().to_string();

        let adapter = StaticFileAdapter;
        let config = StaticFileConfig { path };
        let cursor = Some(StaticFileCursor { processed: false });
        let mut stream = adapter
            .open(dummy_material_id(), &config, cursor)
            .await
            .unwrap();

        assert!(stream.next().await.unwrap().is_ok());
        Ok(())
    }

    #[sinex_test]
    async fn test_static_file_anchor_is_byte_range() -> xtask::sandbox::TestResult<()> {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"abc").unwrap();
        let path = f.path().to_str().unwrap().to_string();

        let adapter = StaticFileAdapter;
        let config = StaticFileConfig { path };
        let mut stream = adapter
            .open(dummy_material_id(), &config, None)
            .await
            .unwrap();
        let record = stream.next().await.unwrap().unwrap();

        assert!(matches!(
            record.anchor,
            MaterialAnchor::ByteRange { start: 0, len: 3 }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn test_static_file_cursor_after() -> xtask::sandbox::TestResult<()> {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"x").unwrap();
        let path = f.path().to_str().unwrap().to_string();

        let adapter = StaticFileAdapter;
        let config = StaticFileConfig { path };
        let mut stream = adapter
            .open(dummy_material_id(), &config, None)
            .await
            .unwrap();
        let record = stream.next().await.unwrap().unwrap();

        let cursor = adapter.cursor_after(&record).unwrap();
        assert!(cursor.processed);
        Ok(())
    }

    #[sinex_test]
    async fn test_static_file_missing_path_returns_error() -> xtask::sandbox::TestResult<()> {
        let adapter = StaticFileAdapter;
        let config = StaticFileConfig {
            path: "/nonexistent/path/file.txt".into(),
        };
        let result = adapter.open(dummy_material_id(), &config, None).await;
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    async fn test_static_file_empty_file_yields_one_empty_record() -> xtask::sandbox::TestResult<()>
    {
        let f = NamedTempFile::new().unwrap();
        let path = f.path().to_str().unwrap().to_string();

        let adapter = StaticFileAdapter;
        let config = StaticFileConfig { path };
        let mut stream = adapter
            .open(dummy_material_id(), &config, None)
            .await
            .unwrap();

        let record = stream.next().await.unwrap().unwrap();
        assert!(record.bytes.is_empty());
        assert!(stream.next().await.is_none());
        Ok(())
    }
}

//! Adapter for static (one-shot) file reads.

use async_trait::async_trait;
use camino::Utf8Path;
use futures::stream::{self, BoxStream, StreamExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeKind, MaterialAnchor, SourceRecord};

use crate::node_sdk::parser::{
    InputShapeAdapter, ParserError, ParserResult, SourceRecordFingerprint,
};

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

    fn input_fingerprint(
        &self,
        config: &Self::Config,
    ) -> ParserResult<Option<SourceRecordFingerprint>> {
        let bytes = std::fs::read(&config.path)?;
        match Utf8Path::new(&config.path)
            .extension()
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("csv") => SourceRecordFingerprint::from_csv_bytes(&bytes)
                .map(Some)
                .map_err(|e| ParserError::Adapter(format!("failed to fingerprint CSV file: {e}"))),
            Some("tsv") => SourceRecordFingerprint::from_tsv_bytes(&bytes)
                .map(Some)
                .map_err(|e| ParserError::Adapter(format!("failed to fingerprint TSV file: {e}"))),
            Some("jsonl") => SourceRecordFingerprint::from_jsonl_bytes(&bytes)
                .map(Some)
                .map_err(|e| {
                    ParserError::Adapter(format!("failed to fingerprint JSONL file: {e}"))
                }),
            Some("json") => {
                let value = serde_json::from_slice(&bytes).map_err(|e| {
                    ParserError::Adapter(format!("failed to fingerprint JSON file: {e}"))
                })?;
                Ok(Some(SourceRecordFingerprint::from_json(&value)))
            }
            _ => Ok(None),
        }
    }

    fn cursor_after(&self, _record: &SourceRecord) -> ParserResult<Self::Cursor> {
        Ok(StaticFileCursor { processed: true })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::Builder;
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

    #[sinex_test]
    async fn static_file_csv_input_fingerprint_reports_header_shape()
    -> xtask::sandbox::TestResult<()> {
        let mut f = Builder::new().suffix(".csv").tempfile().unwrap();
        f.write_all(b"id,name\n1,Alice\n").unwrap();
        let path = f.path().to_str().unwrap().to_string();

        let adapter = StaticFileAdapter;
        let config = StaticFileConfig { path };
        let fingerprint = adapter
            .input_fingerprint(&config)
            .unwrap()
            .expect("CSV static files should expose a structural fingerprint");

        assert_eq!(fingerprint.format, "csv");
        assert_eq!(fingerprint.keys, vec!["id", "name"]);
        assert_eq!(fingerprint.type_map["id"], "integer");
        assert_eq!(fingerprint.type_map["name"], "string");
        Ok(())
    }

    #[sinex_test]
    async fn static_file_json_input_fingerprint_reports_nested_shape()
    -> xtask::sandbox::TestResult<()> {
        let mut f = Builder::new().suffix(".json").tempfile().unwrap();
        f.write_all(br#"{"id":1,"profile":{"name":"Alice"}}"#)
            .unwrap();
        let path = f.path().to_str().unwrap().to_string();

        let adapter = StaticFileAdapter;
        let config = StaticFileConfig { path };
        let fingerprint = adapter
            .input_fingerprint(&config)
            .unwrap()
            .expect("JSON static files should expose a structural fingerprint");

        assert_eq!(fingerprint.format, "json");
        assert!(fingerprint.keys.contains(&"/id".to_string()));
        assert!(fingerprint.keys.contains(&"/profile/name".to_string()));
        assert_eq!(fingerprint.type_map["/id"], "integer");
        assert_eq!(fingerprint.type_map["/profile/name"], "string");
        Ok(())
    }

    #[sinex_test]
    async fn static_file_jsonl_input_fingerprint_reports_row_shape()
    -> xtask::sandbox::TestResult<()> {
        let mut f = Builder::new().suffix(".jsonl").tempfile().unwrap();
        f.write_all(
            br#"{"id":1,"created_at":"2026-01-01"}
{"id":2,"created_at":"2026-01-02","score":7}
"#,
        )
        .unwrap();
        let path = f.path().to_str().unwrap().to_string();

        let adapter = StaticFileAdapter;
        let config = StaticFileConfig { path };
        let fingerprint = adapter
            .input_fingerprint(&config)
            .unwrap()
            .expect("JSONL static files should expose a structural fingerprint");

        assert_eq!(fingerprint.format, "jsonl");
        assert!(fingerprint.keys.contains(&"/[]/id".to_string()));
        assert!(fingerprint.keys.contains(&"/[]/created_at".to_string()));
        assert!(fingerprint.keys.contains(&"/[]/score".to_string()));
        Ok(())
    }

    #[sinex_test]
    async fn static_file_unknown_extension_has_no_input_fingerprint()
    -> xtask::sandbox::TestResult<()> {
        let mut f = Builder::new().suffix(".txt").tempfile().unwrap();
        f.write_all(b"not a structured export").unwrap();
        let path = f.path().to_str().unwrap().to_string();

        let adapter = StaticFileAdapter;
        let config = StaticFileConfig { path };

        assert!(adapter.input_fingerprint(&config).unwrap().is_none());
        Ok(())
    }
}

//! Adapter for static (one-shot) file reads.

use async_trait::async_trait;
use camino::Utf8Path;
use futures::stream::{self, BoxStream, StreamExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeKind, MaterialAnchor, SourceRecord};

use crate::runtime::parser::{
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

        let metadata = std::fs::metadata(&path)?;
        let bytes = if metadata.is_dir() {
            Vec::new()
        } else {
            std::fs::read(&path)?
        };

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
        if std::fs::metadata(&config.path)?.is_dir() {
            return Ok(None);
        }

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
#[path = "static_file_test.rs"]
mod tests;

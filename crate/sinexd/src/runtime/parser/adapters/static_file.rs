//! Adapter for static (one-shot) file reads.

use async_trait::async_trait;
use camino::{Utf8Path, Utf8PathBuf};
use futures::stream::{self, BoxStream, StreamExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::Path;

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

/// Cursor for [`StaticFileAdapter`].
///
/// Ordinary static files remain one-shot via `processed`. Directory inputs
/// that are git worktrees also carry a resolved HEAD token, so continuous
/// polling can re-read them when the repository advances.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaticFileCursor {
    pub processed: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_token: Option<String>,
}

fn static_state_token(path: &str) -> Option<String> {
    let path = Path::new(path);
    if !path.is_dir() {
        return None;
    }

    let git_dir = path.join(".git");
    let head_path = git_dir.join("HEAD");
    let head = std::fs::read_to_string(&head_path).ok()?;
    let head = head.trim();
    if let Some(ref_name) = head.strip_prefix("ref: ") {
        let ref_path = git_dir.join(ref_name);
        let oid = std::fs::read_to_string(ref_path).ok()?;
        return Some(format!("git-head:{ref_name}:{}", oid.trim()));
    }

    if head.is_empty() {
        None
    } else {
        Some(format!("git-head:detached:{head}"))
    }
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
        let current_state_token = static_state_token(&config.path);
        if cursor.as_ref().is_some_and(|c| {
            c.processed && c.state_token.as_deref() == current_state_token.as_deref()
        }) {
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
            logical_path: Some(Utf8PathBuf::from(path.clone())),
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
        let state_token = _record
            .logical_path
            .as_ref()
            .and_then(|path| static_state_token(path.as_str()));
        Ok(StaticFileCursor {
            processed: true,
            state_token,
        })
    }
}

#[cfg(test)]
#[path = "static_file_test.rs"]
mod tests;

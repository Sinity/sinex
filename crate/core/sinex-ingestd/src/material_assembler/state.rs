//! State management types and utilities for material assembly.
//!
//! This module contains the core state structures, message types, and helper
//! functions used by the material assembler to track in-flight assembly operations.

use async_nats::jetstream;
use blake3::Hasher;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sinex_core::types::Ulid;
use std::{collections::BTreeMap, path::PathBuf, str::FromStr};
use tokio::fs::File;
use tracing::{debug, info, warn};

use super::MaterialAssembler;
use crate::{IngestdResult, SinexError};

pub(super) const BUFFER_DIR_NAME: &str = "buffers";
pub(super) const STATE_FILE_NAME: &str = "state.json";

pub(super) const WAL_FILE_NAME: &str = "state.wal";
pub(super) const TEMP_FILE_NAME: &str = "material.bin";
pub(super) const DLQ_CONSUMER: &str = "ingestd";

/// Message from `source_material.begin`
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct MaterialBeginMessage {
    pub material_id: String,
    pub material_kind: String,
    pub source_identifier: String,
    pub metadata: JsonValue,
    pub started_at: String,
}

/// Message from `source_material.end`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct MaterialEndMessage {
    pub material_id: String,
    pub ended_at: String,
    pub content_hash: String,
    pub total_slices: usize,
    pub total_size_bytes: i64,
    #[serde(default)]
    pub metadata: JsonValue,
}

/// Entry in the Write-Ahead Log
#[derive(Debug, Serialize, Deserialize)]
pub(super) enum WalEntry {
    /// Initial or updated metadata (from Begin message)
    Begin(MaterialBeginMessage),
    /// A localized update about a slice being received
    Slice { offset: i64, len: usize },
    /// Buffered slice (out of order)
    BufferedSlice { offset: i64, path: PathBuf },
    /// Buffered slice taken (processed)
    BufferedSliceTaken { offset: i64 },
    /// End message received
    End(MaterialEndMessage),
    /// Checkpoint (snapshot of full state, usually followed by log truncation)
    Checkpoint(PersistedState),
}

/// Persisted assembler state (stored on disk for restart recovery)
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct PersistedState {
    pub material_id: String,
    pub expected_offset: i64,
    pub slice_count: usize,
    pub started_at: String,
    pub material_kind: String,
    pub source_identifier: String,
    pub metadata: JsonValue,
    #[serde(default)]
    pub has_begin: bool,
    #[serde(default)]
    pub pending_write: Option<PendingWrite>,
    #[serde(default)]
    pub pending_end: Option<MaterialEndMessage>,
    #[serde(default)]
    pub finalizing: bool,
}

/// Assembler state held in memory
#[derive(Debug)]
pub(super) struct AssemblerState {
    pub material_id: Ulid,
    pub temp_path: PathBuf,
    pub temp_file: Option<tokio::fs::File>,
    /// Append-only log file
    pub wal_file: Option<tokio::fs::File>,
    pub expected_offset: i64,
    pub slice_count: usize,
    pub buffered_slices: BTreeMap<i64, PathBuf>,
    pub state_dir: PathBuf,
    pub started_at: DateTime<Utc>,
    pub material_kind: String,
    pub source_identifier: String,
    pub metadata: JsonValue,
    pub has_begin: bool,
    pub hasher: Hasher,
    pub pending_write: Option<PendingWrite>,
    pub pending_end: Option<MaterialEndMessage>,
    pub finalizing: bool,
    pub last_slice_received: DateTime<Utc>,
    /// Semaphore permit held for the duration of the assembly
    pub _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct PendingWrite {
    pub offset: i64,
    pub len: usize,
    pub slice_count_delta: usize,
}

#[derive(Clone)]
pub(super) struct FinalizationState {
    pub material_id: Ulid,
    pub temp_path: PathBuf,
    pub expected_offset: i64,
    pub slice_count: usize,
    pub buffered_count: usize,
    pub metadata: JsonValue,
    pub material_kind: String,
    pub source_identifier: String,
    pub started_at: DateTime<Utc>,
}

impl AssemblerState {
    pub fn buffers_dir(&self) -> PathBuf {
        self.state_dir.join(BUFFER_DIR_NAME)
    }

    pub fn _state_file(&self) -> PathBuf {
        self.state_dir.join(STATE_FILE_NAME)
    }

    pub fn finalization_view(&self) -> FinalizationState {
        FinalizationState {
            material_id: self.material_id,
            temp_path: self.temp_path.clone(),
            expected_offset: self.expected_offset,
            slice_count: self.slice_count,
            buffered_count: self.buffered_slices.len(),
            metadata: self.metadata.clone(),
            material_kind: self.material_kind.clone(),
            source_identifier: self.source_identifier.clone(),
            started_at: self.started_at,
        }
    }
}

#[cfg(test)]
pub(super) fn take_buffered_slice(
    state: &mut AssemblerState,
    material_id: Ulid,
    offset: i64,
) -> IngestdResult<PathBuf> {
    state.buffered_slices.remove(&offset).ok_or_else(|| {
        SinexError::service(format!(
            "Missing buffered slice for {material_id} at offset {offset}"
        ))
    })
}

pub(super) fn normalize_metadata(value: JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(_) => value,
        JsonValue::Null => serde_json::json!({}),
        other => {
            let mut map = JsonMap::new();
            map.insert("value".to_string(), other);
            JsonValue::Object(map)
        }
    }
}

pub(super) fn merge_metadata(base: &JsonValue, updates: &JsonValue) -> JsonValue {
    let mut merged = normalize_metadata(base.clone());
    if let Some(target) = merged.as_object_mut() {
        match updates {
            JsonValue::Object(map) => {
                for (key, value) in map {
                    target.insert(key.clone(), value.clone());
                }
            }
            JsonValue::Null => {}
            other => {
                target.insert("value".to_string(), other.clone());
            }
        }
    }
    merged
}

pub(super) fn is_terminal_status(status: &str) -> bool {
    use sinex_core::db::repositories::material_status;
    matches!(
        status,
        material_status::COMPLETED | material_status::FAILED | material_status::RECOVERED_PARTIAL
    )
}

pub(super) fn build_finalize_metadata(
    state: &FinalizationState,
    end_metadata: &JsonValue,
    ended_at: DateTime<Utc>,
    total_bytes: i64,
    content_hash: &str,
) -> JsonValue {
    let mut merged = merge_metadata(&state.metadata, end_metadata);
    let map = merged
        .as_object_mut()
        .expect("metadata normalized to object during merge");
    map.insert(
        "finalize_reason".to_string(),
        JsonValue::String("jetstream-material".to_string()),
    );
    map.insert(
        "finalized_at".to_string(),
        JsonValue::String(ended_at.to_rfc3339()),
    );
    map.insert(
        "content_hash".to_string(),
        JsonValue::String(content_hash.to_string()),
    );
    map.insert(
        "total_slices".to_string(),
        JsonValue::Number(state.slice_count.into()),
    );
    map.insert(
        "total_bytes".to_string(),
        JsonValue::Number(total_bytes.into()),
    );
    map.entry("material_kind".to_string())
        .or_insert_with(|| JsonValue::String(state.material_kind.clone()));
    map.entry("source_identifier".to_string())
        .or_insert_with(|| JsonValue::String(state.source_identifier.clone()));
    merged
}

/// Handle a begin message by initializing or updating assembler state.
pub(super) async fn handle_begin(
    assembler: &MaterialAssembler,
    msg: jetstream::Message,
) -> IngestdResult<()> {
    let begin: MaterialBeginMessage = match serde_json::from_slice(&msg.payload) {
        Ok(begin) => begin,
        Err(e) => {
            warn!("Failed to decode begin message payload: {}", e);
            return Ok(());
        }
    };

    let material_id = match Ulid::from_str(&begin.material_id) {
        Ok(id) => id,
        Err(e) => {
            warn!(
                material_id = %begin.material_id,
                "Invalid material_id in begin message: {}",
                e
            );
            return Ok(());
        }
    };

    let started_at = DateTime::parse_from_rfc3339(&begin.started_at)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| {
            warn!(
                material_id = %material_id,
                started_at = %begin.started_at,
                "Invalid started_at on begin message, defaulting to now"
            );
            Utc::now()
        });

    if assembler.pool.is_closed() {
        return Err(SinexError::database(
            "database pool closed before begin processing".to_string(),
        ));
    }

    let metadata = normalize_metadata(begin.metadata);
    let material_kind = begin.material_kind;
    let source_identifier = begin.source_identifier;

    let state_handle = if let Some(existing) = assembler.get_state_handle(&material_id).await {
        existing
    } else {
        if assembler.material_is_terminal(material_id).await? {
            info!(
                material_id = %material_id,
                "Begin message received after completion; skipping"
            );
            return Ok(());
        }

        let mut state = assembler.create_placeholder_state(material_id).await?;
        state.material_kind = material_kind.clone();
        state.source_identifier = source_identifier.clone();
        state.metadata = metadata.clone();
        state.started_at = started_at;
        state.has_begin = true;
        assembler.insert_state_handle(material_id, state).await
    };

    let merged_metadata = {
        let mut state = state_handle.lock().await;
        if state.finalizing {
            debug!(
                material_id = %material_id,
                "Ignoring begin message while material is finalizing"
            );
            return Ok(());
        }

        state.material_kind = material_kind.clone();
        state.source_identifier = source_identifier.clone();
        state.metadata = merge_metadata(&state.metadata, &metadata);
        state.started_at = started_at;
        state.has_begin = true;

        if state.temp_file.is_none() {
            let temp_file = File::options()
                .create(true)
                .append(true)
                .open(&state.temp_path)
                .await
                .map_err(|e| SinexError::io(format!("Failed to open temp file: {}", e)))?;
            state.temp_file = Some(temp_file);
        }

        let metadata_clone = state.metadata.clone();
        super::io::append_wal_entry(
            assembler,
            &mut state,
            WalEntry::Begin(MaterialBeginMessage {
                material_id: material_id.to_string(),
                material_kind: material_kind.clone(),
                source_identifier: source_identifier.clone(),
                metadata: metadata_clone,
                started_at: started_at.to_rfc3339(),
            }),
        )
        .await?;
        state.metadata.clone()
    };

    assembler
        .register_material_record(
            material_id,
            &material_kind,
            &source_identifier,
            merged_metadata,
            started_at,
        )
        .await?;

    let has_pending_end = {
        let state = state_handle.lock().await;
        state.pending_end.is_some()
    };

    if has_pending_end {
        assembler
            .try_finalize_pending_end(
                material_id,
                state_handle,
                super::finalize::PendingEndBehavior::Ignore,
            )
            .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use blake3::Hasher;
    use chrono::Utc;
    use std::{collections::BTreeMap, str::FromStr};
    use tempfile::tempdir;

    fn test_state(material_id: Ulid) -> AssemblerState {
        let temp_dir = tempdir().expect("temp dir should be creatable");
        AssemblerState {
            material_id,
            temp_path: temp_dir.path().join(TEMP_FILE_NAME),
            temp_file: None,
            wal_file: None,
            expected_offset: 0,
            slice_count: 0,
            buffered_slices: BTreeMap::new(),
            state_dir: temp_dir.path().to_path_buf(),
            started_at: Utc::now(),
            material_kind: "test".to_string(),
            source_identifier: "test".to_string(),
            metadata: JsonValue::Null,
            has_begin: true,
            hasher: Hasher::new(),
            pending_write: None,
            pending_end: None,
            finalizing: false,
            last_slice_received: Utc::now(),
            _permit: None,
        }
    }

    #[test]
    fn missing_buffered_slice_returns_error_instead_of_panic() {
        let material_id = Ulid::from_str("01J00000000000000000000000").unwrap();
        let mut state = test_state(material_id);

        let result = take_buffered_slice(&mut state, material_id, 42);

        assert!(result.is_err());
    }

    #[test]
    fn buffered_slice_is_removed_and_returned() {
        let material_id = Ulid::from_str("01J00000000000000000000000").unwrap();
        let mut state = test_state(material_id);
        let buffer_path = state.state_dir.join("buffers/42.bin");
        state.buffered_slices.insert(42, buffer_path.clone());

        let result = take_buffered_slice(&mut state, material_id, 42).unwrap();

        assert_eq!(result, buffer_path);
        assert!(state.buffered_slices.is_empty());
    }
}

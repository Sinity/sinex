//! Unified checkpoint management for both ingestors and automata.
//!
//! This module implements the unified checkpoint system that supports both
//! external positions (for ingestors) and internal event IDs (for automata).
//!
//! # Architecture
//!
//! The checkpoint system provides:
//! - **Unified Storage**: All checkpoints stored in NATS KV (`KV_sinex_checkpoints`)
//! - **Type Safety**: Strongly typed checkpoint variants for different use cases
//! - **Persistence**: Atomic checkpoint updates with optimistic concurrency
//!
//! # Checkpoint Types
//!
//! - `External`: For ingestors tracking external system state (file positions, timestamps)
//! - `Internal`: For automata tracking processed event ULIDs
//! - `Stream`: For message stream IDs (NATS JetStream)
//! - `Timestamp`: For time-based processing resumption
//!
//! # Storage Layout
//!
//! The NATS KV entries store:
//! - `processor_name`: Processor identifier
//! - `consumer_group`: Consumer group (for stream processing)
//! - `consumer_name`: Instance identifier (hostname + PID)
//! - `checkpoint_data`: JSON-serialized unified checkpoint (v2+)
//!
//! # Error Handling
//!
//! Common error scenarios:
//! - **Serialization failures**: Corrupt checkpoint data falls back to `Checkpoint::None`
//! - **KV errors**: NATS KV failures are propagated as `NodeError::Checkpoint`
//!
//! # Performance Considerations
//!
//! - Checkpoints are saved atomically using `ON CONFLICT` upserts
//! - Frequent checkpoint updates are batched for better performance
//! - Historical checkpoint queries are limited to prevent memory issues

use crate::{stream_processor::Checkpoint, NodeError, NodeResult};
use async_nats::jetstream::kv::Operation;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use sinex_core::types::ulid::Ulid;
use std::convert::TryInto;
use tracing::{debug, info, warn};

/// Unified checkpoint state for both ingestors and automata.
///
/// This structure wraps the unified `Checkpoint` enum with additional metadata
/// for persistence and monitoring.
///
/// # Version
/// - **Version 2**: Unified format with strongly-typed `Checkpoint` enum
///
/// # Fields
/// - `checkpoint`: The actual checkpoint data (position, event ID, etc.)
/// - `processed_count`: Total messages/events processed (for monitoring)
/// - `last_activity`: When this checkpoint was last updated
/// - `data`: Processor-specific state (arbitrary JSON)
/// - `version`: Checkpoint format version for migration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointState {
    /// Unified checkpoint data
    pub checkpoint: Checkpoint,

    /// Total number of messages/events processed
    pub processed_count: u64,

    /// Last activity timestamp
    pub last_activity: chrono::DateTime<chrono::Utc>,

    /// Processor-specific state data
    pub data: Option<serde_json::Value>,

    /// Checkpoint version (for schema evolution)
    pub version: u32,
}

impl CheckpointState {
    pub fn last_processed_id(&self) -> Option<String> {
        match &self.checkpoint {
            Checkpoint::None => None,
            Checkpoint::Internal { event_id, .. } => Some(event_id.to_string()),
            Checkpoint::External { .. } => None, // External checkpoints don't have event IDs
            Checkpoint::Stream { message_id, .. } => Some(message_id.clone()),
            Checkpoint::Timestamp { .. } => None, // Timestamp checkpoints don't have event IDs
        }
    }

    /// Update the checkpoint with a new processed ID.
    ///
    /// # Complex Invariants
    ///
    /// This function implements complex logic to determine checkpoint type based on ID format:
    /// - **ULID strings**: Parsed and stored as `Checkpoint::Internal` for automata
    /// - **Other strings**: Stored as `Checkpoint::Stream` for message stream IDs
    /// - **None**: Resets to `Checkpoint::None` (initial state)
    ///
    /// The function maintains important invariants:
    /// - `processed_count` is preserved when converting checkpoint types
    /// - Stream checkpoints set `event_id: None` (they don't map to events)
    /// - Invalid ULIDs gracefully fall back to stream ID interpretation
    ///
    /// This design allows the same checkpoint API to work for both ingestors
    /// (external positions) and automata (event IDs).
    pub fn set_last_processed_id(&mut self, id: Option<String>) {
        self.checkpoint = match id {
            Some(id_str) => {
                // Try to parse as ULID first, then fall back to stream ID
                if let Ok(ulid) = id_str.parse::<Ulid>() {
                    Checkpoint::Internal {
                        event_id: ulid,
                        message_count: self.processed_count,
                    }
                } else {
                    Checkpoint::Stream {
                        message_id: id_str,
                        event_id: None,
                    }
                }
            }
            None => Checkpoint::None,
        };
    }
}

impl Default for CheckpointState {
    fn default() -> Self {
        Self {
            checkpoint: Checkpoint::None,
            processed_count: 0,
            last_activity: chrono::Utc::now(),
            data: None,
            version: 2, // Version 2 for unified checkpoint format
        }
    }
}

impl CheckpointState {
    /// Save checkpoint state to a local file.
    ///
    /// Used for hot reload state continuity. When a SIGTERM is received,
    /// the state is quickly saved to a local file so it can be restored
    /// when the process restarts.
    ///
    /// The file format is JSON with a magic header for validation.
    pub fn save_to_file(&self, path: &std::path::Path) -> std::io::Result<()> {
        use std::io::Write;

        let wrapper = FileCheckpointWrapper {
            magic: FILE_CHECKPOINT_MAGIC.to_string(),
            version: FILE_CHECKPOINT_VERSION,
            state: self.clone(),
        };

        let json = serde_json::to_string_pretty(&wrapper)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        // Atomic write: write to temp file, then rename
        let temp_path = path.with_extension("tmp");
        let mut file = std::fs::File::create(&temp_path)?;
        file.write_all(json.as_bytes())?;
        file.sync_all()?;
        std::fs::rename(&temp_path, path)?;

        info!(
            path = %path.display(),
            processed_count = self.processed_count,
            "Saved checkpoint to file"
        );

        Ok(())
    }

    /// Load checkpoint state from a local file.
    ///
    /// Used to restore state after a hot reload. If the file doesn't exist
    /// or is invalid, returns None (allowing fresh start).
    pub fn load_from_file(path: &std::path::Path) -> Option<Self> {
        let contents = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                debug!(path = %path.display(), "No checkpoint file found");
                return None;
            }
            Err(e) => {
                warn!(
                    path = %path.display(),
                    error = %e,
                    "Failed to read checkpoint file"
                );
                return None;
            }
        };

        let wrapper: FileCheckpointWrapper = match serde_json::from_str(&contents) {
            Ok(w) => w,
            Err(e) => {
                warn!(
                    path = %path.display(),
                    error = %e,
                    "Failed to parse checkpoint file"
                );
                return None;
            }
        };

        // Validate magic and version
        if wrapper.magic != FILE_CHECKPOINT_MAGIC {
            warn!(
                path = %path.display(),
                expected = FILE_CHECKPOINT_MAGIC,
                found = wrapper.magic,
                "Invalid checkpoint file magic"
            );
            return None;
        }

        if wrapper.version > FILE_CHECKPOINT_VERSION {
            warn!(
                path = %path.display(),
                file_version = wrapper.version,
                supported_version = FILE_CHECKPOINT_VERSION,
                "Checkpoint file version too new"
            );
            return None;
        }

        info!(
            path = %path.display(),
            processed_count = wrapper.state.processed_count,
            "Loaded checkpoint from file"
        );

        Some(wrapper.state)
    }

    /// Delete the checkpoint file if it exists.
    ///
    /// Called after successfully syncing state to the primary checkpoint store
    /// (NATS KV) to avoid stale file state.
    pub fn delete_file(path: &std::path::Path) -> std::io::Result<()> {
        match std::fs::remove_file(path) {
            Ok(()) => {
                debug!(path = %path.display(), "Deleted checkpoint file");
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

/// Magic string for file-based checkpoint validation
const FILE_CHECKPOINT_MAGIC: &str = "SINEX_CHECKPOINT_V1";
/// Current file checkpoint format version
const FILE_CHECKPOINT_VERSION: u32 = 1;

/// Wrapper for file-based checkpoint storage with validation
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileCheckpointWrapper {
    magic: String,
    version: u32,
    state: CheckpointState,
}

fn sanitize_kv_key_component(raw: &str) -> String {
    if raw.is_empty() {
        return "_".to_string();
    }

    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '/' | '=') {
            out.push(ch);
        } else {
            out.push('_');
        }
    }

    if out.is_empty() {
        "_".to_string()
    } else {
        out
    }
}

/// Resolve the NATS KV bucket name for checkpoints.
pub fn checkpoint_bucket_name(prefix: Option<&str>) -> String {
    let base = "KV_sinex_checkpoints";
    match prefix {
        Some(prefix) if !prefix.trim().is_empty() => format!("{prefix}_{base}"),
        _ => base.to_string(),
    }
}

/// Parse a checkpoint KV key into (processor, group, consumer) components.
pub fn parse_checkpoint_key(key: &str) -> Option<(String, String, String)> {
    let mut parts = key.splitn(3, '.');
    let processor = parts.next()?.trim();
    let group = parts.next()?.trim();
    let consumer = parts.next()?.trim();

    if processor.is_empty() || group.is_empty() || consumer.is_empty() {
        return None;
    }

    Some((
        processor.to_string(),
        group.to_string(),
        consumer.to_string(),
    ))
}

/// Manager for unified checkpoint persistence (both ingestors and automata).
///
/// This manager handles checkpoint storage, retrieval, and migration in the
/// NATS KV bucket. It supports both ingestors and automata
///
/// # Usage Pattern
/// ```rust
/// use sinex_node_sdk::CheckpointManager;
///
/// let manager = CheckpointManager::new(
///     pool,
///     "my-processor".to_string(),
///     "default".to_string(),
///     "hostname-1234".to_string(),
/// );
///
/// // Load existing checkpoint (or get default)
/// let checkpoint = manager.load_checkpoint().await?;
///
/// // Process events...
///
/// // Save updated checkpoint
/// manager.save_checkpoint(&updated_checkpoint).await?;
/// ```
///
/// # Thread Safety
/// `CheckpointManager` is `Clone` and can be safely shared across threads.
/// KV updates are atomic per key; concurrent writers follow last-write-wins semantics.
#[derive(Debug, Clone)]
pub struct CheckpointManager {
    kv: async_nats::jetstream::kv::Store,
    processor_name: String,
    consumer_group: String,
    consumer_name: String,
}

impl CheckpointManager {
    /// Create a new checkpoint manager with NATS KV.
    pub fn new(
        kv: async_nats::jetstream::kv::Store,
        processor_name: String,
        consumer_group: String,
        consumer_name: String,
    ) -> Self {
        Self {
            kv,
            processor_name,
            consumer_group,
            consumer_name,
        }
    }

    ///
    /// - **Version 2+**: Deserializes `checkpoint_data` JSON field
    /// - **Version 1**: Migrates from `last_processed_id` string field
    /// - **No checkpoint**: Returns default `CheckpointState` with `Checkpoint::None`
    ///
    /// # Returns
    /// - `Ok(CheckpointState)`: Successfully loaded or migrated checkpoint
    /// - `Err(NodeError::Checkpoint)`: NATS KV read error
    /// - `Err(NodeError::Serialization)`: Corrupt checkpoint data (falls back to None)
    ///
    /// # Behavior
    /// - Corrupt checkpoint data logs warnings and falls back to `Checkpoint::None`
    /// - If no checkpoint exists for this consumer, the latest checkpoint in the same
    ///   processor/group is used as a fallback (supports failover/restarts)
    /// - First-time processors get a default checkpoint with `processed_count: 0`
    pub async fn load_checkpoint(&self) -> NodeResult<CheckpointState> {
        let key = self.kv_key();
        if let Some(state) = self.load_checkpoint_for_key(&key).await? {
            debug!(
                processor = %self.processor_name,
                consumer_group = %self.consumer_group,
                consumer_name = %self.consumer_name,
                "Loaded checkpoint from KV"
            );
            return Ok(state);
        }

        if let Some(state) = self.load_latest_checkpoint_for_group().await? {
            info!(
                processor = %self.processor_name,
                consumer_group = %self.consumer_group,
                consumer_name = %self.consumer_name,
                "Loaded checkpoint from KV fallback"
            );
            return Ok(state);
        }

        info!(
            processor = %self.processor_name,
            consumer_group = %self.consumer_group,
            consumer_name = %self.consumer_name,
            "No existing checkpoint found, starting fresh"
        );

        Ok(CheckpointState::default())
    }

    async fn load_checkpoint_for_key(&self, key: &str) -> NodeResult<Option<CheckpointState>> {
        let data = self
            .kv
            .get(key)
            .await
            .map_err(|e| NodeError::Checkpoint(format!("Failed to read checkpoint KV: {e}")))?;

        let Some(data) = data else {
            return Ok(None);
        };

        if data.is_empty() {
            return Ok(None);
        }

        match serde_json::from_slice::<CheckpointState>(&data) {
            Ok(state) => Ok(Some(state)),
            Err(err) => {
                warn!(
                    processor = %self.processor_name,
                    consumer_group = %self.consumer_group,
                    consumer_name = %self.consumer_name,
                    error = %err,
                    "Failed to decode checkpoint from KV; falling back"
                );
                Ok(None)
            }
        }
    }

    async fn load_latest_checkpoint_for_group(&self) -> NodeResult<Option<CheckpointState>> {
        let prefix = self.kv_group_prefix();
        let mut keys = self.kv.keys().await.map_err(|e| {
            NodeError::Checkpoint(format!("Failed to list checkpoint KV keys: {e}"))
        })?;

        let mut latest: Option<(i128, CheckpointState)> = None;

        while let Some(key) = keys
            .try_next()
            .await
            .map_err(|e| NodeError::Checkpoint(format!("Failed to scan checkpoint KV keys: {e}")))?
        {
            if !key.starts_with(&prefix) {
                continue;
            }

            let entry = match self.kv.entry(&key).await.map_err(|e| {
                NodeError::Checkpoint(format!("Failed to read checkpoint KV entry: {e}"))
            })? {
                Some(entry) => entry,
                None => continue,
            };

            if !matches!(entry.operation, Operation::Put) || entry.value.is_empty() {
                continue;
            }

            let state = match serde_json::from_slice::<CheckpointState>(&entry.value) {
                Ok(state) => state,
                Err(err) => {
                    warn!(
                        processor = %self.processor_name,
                        consumer_group = %self.consumer_group,
                        consumer_name = %self.consumer_name,
                        key = %entry.key,
                        error = %err,
                        "Failed to decode checkpoint entry; skipping"
                    );
                    continue;
                }
            };

            let created_nanos = entry.created.unix_timestamp_nanos();
            match &latest {
                Some((created, _)) if *created >= created_nanos => {}
                _ => latest = Some((created_nanos, state)),
            }
        }

        Ok(latest.map(|(_, state)| state))
    }

    /// Save checkpoint to NATS KV only.
    ///
    /// DB writes are no longer performed - NATS KV is the sole source of truth.
    ///
    /// # Parameters
    /// - `state`: The checkpoint state to save
    ///
    /// # Returns
    /// - `Ok(())`: Checkpoint successfully saved
    /// - `Err(NodeError::Checkpoint)`: KV write error
    /// - `Err(NodeError::Serialization)`: Checkpoint serialization error
    pub async fn save_checkpoint(&self, state: &CheckpointState) -> NodeResult<()> {
        let processed_count: i64 = state.processed_count.try_into().map_err(|_| {
            NodeError::Checkpoint("processed_count exceeds supported range for storage".to_string())
        })?;

        // Save to NATS KV only
        let encoded = serde_json::to_vec(state).map_err(NodeError::Serialization)?;
        self.kv
            .put(&self.kv_key(), encoded.into())
            .await
            .map_err(|e| {
                NodeError::Checkpoint(format!("Failed to persist checkpoint to KV: {e}"))
            })?;

        debug!(
            processor = %self.processor_name,
            consumer_group = %self.consumer_group,
            consumer_name = %self.consumer_name,
            processed_count = processed_count,
            checkpoint = %state.checkpoint.description(),
            "Saved checkpoint to KV"
        );

        Ok(())
    }

    fn kv_group_prefix(&self) -> String {
        let processor = sanitize_kv_key_component(&self.processor_name);
        let consumer_group = sanitize_kv_key_component(&self.consumer_group);

        format!("{processor}.{consumer_group}.")
    }

    fn kv_key(&self) -> String {
        let processor = sanitize_kv_key_component(&self.processor_name);
        let consumer_group = sanitize_kv_key_component(&self.consumer_group);
        let consumer = sanitize_kv_key_component(&self.consumer_name);

        format!("{processor}.{consumer_group}.{consumer}")
    }

    /// Get checkpoint history for debugging.
    ///
    /// NATS KV only stores the latest value, so we return the current checkpoint as a
    /// single-entry history when available.
    pub async fn get_checkpoint_history(
        &self,
        limit: i64,
    ) -> NodeResult<Vec<CheckpointHistoryEntry>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let entry = self
            .kv
            .get(&self.kv_key())
            .await
            .map_err(|e| NodeError::Checkpoint(format!("Failed to read checkpoint KV: {e}")))?;

        let Some(entry) = entry else {
            return Ok(Vec::new());
        };

        let state: CheckpointState =
            serde_json::from_slice(&entry).map_err(NodeError::Serialization)?;
        let timestamp = state.last_activity;
        let history_entry = CheckpointHistoryEntry {
            id: self.kv_key(),
            last_processed_id: state.last_processed_id(),
            processed_count: state.processed_count,
            last_activity: state.last_activity,
            checkpoint_version: state.version,
            created_at: timestamp,
            updated_at: timestamp,
        };

        Ok(vec![history_entry])
    }

    /// Reset checkpoint (for testing or manual intervention)
    pub async fn reset_checkpoint(&self) -> NodeResult<()> {
        // Reset KV (primary)
        self.kv
            .purge(&self.kv_key())
            .await
            .map_err(|e| NodeError::Checkpoint(format!("Failed to purge checkpoint: {e}")))?;

        info!(
            processor = %self.processor_name,
            consumer_group = %self.consumer_group,
            consumer_name = %self.consumer_name,
            "Checkpoint reset"
        );

        Ok(())
    }

    /// Get checkpoint statistics
    pub async fn get_checkpoint_stats(&self) -> NodeResult<CheckpointStats> {
        let entry = self
            .kv
            .get(&self.kv_key())
            .await
            .map_err(|e| NodeError::Checkpoint(format!("Failed to read checkpoint KV: {e}")))?;

        let (processed_count, last_update) = if let Some(entry) = entry {
            if let Ok(state) = serde_json::from_slice::<CheckpointState>(&entry) {
                (state.processed_count, Some(state.last_activity))
            } else {
                (0, None)
            }
        } else if let Some(state) = self.load_latest_checkpoint_for_group().await? {
            (state.processed_count, Some(state.last_activity))
        } else {
            (0, None)
        };

        Ok(CheckpointStats {
            total_checkpoints: 1, // KV stores one version
            max_processed: processed_count,
            last_update,
            first_checkpoint: None,
        })
    }
}

/// Historical checkpoint entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointHistoryEntry {
    pub id: String,
    pub last_processed_id: Option<String>,
    pub processed_count: u64,
    pub last_activity: chrono::DateTime<chrono::Utc>,
    pub checkpoint_version: u32,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// Checkpoint statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointStats {
    pub total_checkpoints: u64,
    pub max_processed: u64,
    pub last_update: Option<chrono::DateTime<chrono::Utc>>,
    pub first_checkpoint: Option<chrono::DateTime<chrono::Utc>>,
}
#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::{sinex_test, TestContext};

    #[sinex_test]
    async fn save_checkpoint_rejects_processed_count_overflow(
        ctx: TestContext,
    ) -> sinex_test_utils::TestResult<()> {
        let ctx = ctx.with_nats().await?;
        let kv = ctx.checkpoint_kv().await?;
        let manager = CheckpointManager::new(
            kv,
            "processor".to_string(),
            "group".to_string(),
            "consumer".to_string(),
        );
        let mut state = CheckpointState::default();
        state.processed_count = u64::MAX;

        let err = manager.save_checkpoint(&state).await.unwrap_err();
        assert!(matches!(err, NodeError::Checkpoint(_)));
        Ok(())
    }

    #[sinex_test]
    async fn checkpoint_keys_accept_invalid_chars(
        ctx: TestContext,
    ) -> sinex_test_utils::TestResult<()> {
        let ctx = ctx.with_nats().await?;
        let kv = ctx.checkpoint_kv().await?;
        let manager = CheckpointManager::new(
            kv,
            "processor:with:colons".to_string(),
            "group.with.dots".to_string(),
            "consumer name with spaces".to_string(),
        );
        let mut state = CheckpointState::default();
        state.processed_count = 1;

        manager.save_checkpoint(&state).await?;
        let loaded = manager.load_checkpoint().await?;
        assert_eq!(loaded.processed_count, 1);
        Ok(())
    }
}

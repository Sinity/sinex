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
//! - `Internal`: For automata tracking processed event `UUIDv7` IDs
//! - `Stream`: For message stream IDs (NATS `JetStream`)
//! - `Timestamp`: For time-based processing resumption
//!
//! # Storage Layout
//!
//! The NATS KV entries store:
//! - `node_name`: Node identifier
//! - `consumer_group`: Consumer group (for stream processing)
//! - `consumer_name`: Instance identifier (hostname + PID)
//! - `checkpoint_data`: JSON-serialized unified checkpoint
//!
//! # Error Handling
//!
//! Common error scenarios:
//! - **Serialization failures**: Corrupt checkpoint data is surfaced as an error with context
//! - **KV errors**: NATS KV failures are propagated as `SinexError::checkpoint`
//!
//! # Performance Considerations
//!
//! - Checkpoints are saved atomically using KV compare-and-set revisions
//! - Frequent checkpoint updates are batched for better performance
//! - Historical checkpoint queries are limited to prevent memory issues

use crate::{
    NodeResult, SinexError,
    error_helpers::{env_bool_with_default, env_parse_with_default},
    runtime::stream::Checkpoint,
};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sinex_primitives::temporal::Timestamp;
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
/// - `data`: Node-specific state (arbitrary JSON)
/// - `version`: Checkpoint format version for schema evolution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointState {
    /// Unified checkpoint data
    pub checkpoint: Checkpoint,

    /// Total number of messages/events processed
    pub processed_count: u64,

    /// Last activity timestamp
    pub last_activity: Timestamp,

    /// Node-specific state data
    pub data: Option<serde_json::Value>,

    /// Checkpoint version (for schema evolution)
    pub version: u32,

    /// NATS KV Revision (optimistic concurrency control)
    #[serde(skip)]
    pub revision: u64,
}

fn checkpoint_states_match(lhs: &CheckpointState, rhs: &CheckpointState) -> bool {
    lhs.checkpoint == rhs.checkpoint
        && lhs.processed_count == rhs.processed_count
        && lhs.last_activity == rhs.last_activity
        && lhs.data == rhs.data
        && lhs.version == rhs.version
}

impl CheckpointState {
    #[must_use]
    pub fn last_processed_id(&self) -> Option<String> {
        match &self.checkpoint {
            Checkpoint::None => None,
            Checkpoint::Internal { event_id, .. } => Some(event_id.to_string()),
            Checkpoint::External { .. } => None, // External checkpoints don't have event IDs
            Checkpoint::Stream { message_id, .. } => Some(message_id.clone()),
            Checkpoint::Timestamp { .. } => None, // Timestamp checkpoints don't have event IDs
        }
    }
}

impl Default for CheckpointState {
    fn default() -> Self {
        Self {
            checkpoint: Checkpoint::None,
            processed_count: 0,
            last_activity: Timestamp::now(),
            data: None,
            version: 2, // Version 2 for unified checkpoint format
            revision: 0,
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
    pub async fn save_to_file(&self, path: &std::path::Path) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;

        let record = FileCheckpointRecord {
            magic: FILE_CHECKPOINT_MAGIC.to_string(),
            version: FILE_CHECKPOINT_VERSION,
            revision: self.revision,
            state: self.clone(),
        };

        let json = serde_json::to_string_pretty(&record)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        // Atomic write: write to temp file, then rename
        let temp_path = path.with_extension("tmp");
        let mut file = tokio::fs::File::create(&temp_path).await?;
        file.write_all(json.as_bytes()).await?;
        file.sync_all().await?;
        tokio::fs::rename(&temp_path, path).await?;

        if let Some(parent) = path.parent() {
            let dir = tokio::fs::File::open(parent).await?;
            dir.sync_all().await?;
        }

        info!(
            path = %path.display(),
            processed_count = self.processed_count,
            "Saved checkpoint to file"
        );

        Ok(())
    }

    /// Load checkpoint state from a local file.
    ///
    /// Used to restore state after a hot reload. Missing files are treated as
    /// "no checkpoint"; unreadable or invalid files are surfaced as errors.
    pub async fn load_from_file(path: &std::path::Path) -> NodeResult<Option<Self>> {
        let contents = match tokio::fs::read_to_string(path).await {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                debug!(path = %path.display(), "No checkpoint file found");
                return Ok(None);
            }
            Err(error) => {
                return Err(SinexError::io("Failed to read checkpoint file")
                    .with_context("path", path.display().to_string())
                    .with_std_error(&error));
            }
        };

        let record = serde_json::from_str::<FileCheckpointRecord>(&contents).map_err(|error| {
            SinexError::serialization("Failed to parse checkpoint file")
                .with_context("path", path.display().to_string())
                .with_std_error(&error)
        })?;

        // Validate magic and version
        if record.magic != FILE_CHECKPOINT_MAGIC {
            return Err(SinexError::checkpoint("Invalid checkpoint file magic")
                .with_context("path", path.display().to_string())
                .with_context("expected", FILE_CHECKPOINT_MAGIC)
                .with_context("found", record.magic));
        }

        if record.version > FILE_CHECKPOINT_VERSION {
            return Err(SinexError::checkpoint("Checkpoint file version too new")
                .with_context("path", path.display().to_string())
                .with_context("file_version", record.version.to_string())
                .with_context("supported_version", FILE_CHECKPOINT_VERSION.to_string()));
        }

        let mut state = record.state;
        state.revision = record.revision;

        info!(
            path = %path.display(),
            processed_count = state.processed_count,
            "Loaded checkpoint from file"
        );

        Ok(Some(state))
    }

    /// Delete the checkpoint file if it exists.
    ///
    /// Called after successfully syncing state to the primary checkpoint store
    /// (NATS KV) to avoid stale file state.
    pub async fn delete_file(path: &std::path::Path) -> std::io::Result<()> {
        match tokio::fs::remove_file(path).await {
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

/// Record envelope for file-based checkpoint storage with validation
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileCheckpointRecord {
    magic: String,
    version: u32,
    #[serde(default)]
    revision: u64,
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

    if out.is_empty() { "_".to_string() } else { out }
}

/// Resolve the NATS KV bucket name for checkpoints.
#[must_use]
pub fn checkpoint_bucket_name(prefix: Option<&str>) -> String {
    let env = sinex_primitives::environment::environment();
    let base_bucket = "sinex_checkpoints";

    let namespaced_base = if let Some(prefix) = prefix.filter(|p| !p.trim().is_empty()) {
        env.nats_kv_bucket_with_namespace(Some(prefix), base_bucket)
    } else {
        env.nats_kv_bucket_name(base_bucket)
    };

    format!("KV_{namespaced_base}")
}

/// Parse a checkpoint KV key into (node, group, consumer) components.
#[must_use]
pub fn parse_checkpoint_key(key: &str) -> Option<(String, String, String)> {
    let mut parts = key.splitn(3, '.');
    let node = parts.next()?.trim();
    let group = parts.next()?.trim();
    let consumer = parts.next()?.trim();

    if node.is_empty() || group.is_empty() || consumer.is_empty() {
        return None;
    }

    Some((node.to_string(), group.to_string(), consumer.to_string()))
}

/// Manager for unified checkpoint persistence (both ingestors and automata).
///
/// This manager handles checkpoint storage and retrieval in the
/// NATS KV bucket. It supports both ingestors and automata
///
/// # Usage Pattern
/// ```rust
/// use sinex_node_sdk::CheckpointManager;
///
/// let manager = CheckpointManager::new(
///     pool,
///     "my-node".to_string(),
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
/// KV updates are atomic per key; stale writers fail fast on revision mismatch instead of
/// silently overwriting an already-created checkpoint.
///
/// # Checkpoint Cleanup
///
/// Stale checkpoint cleanup is implemented via [`spawn_checkpoint_cleanup_task`] and
/// [`cleanup_stale_checkpoints`]. The cleanup is opt-in via environment variables:
///
/// - `SINEX_CHECKPOINT_CLEANUP_ENABLED=true` - Enable automatic cleanup
/// - `SINEX_CHECKPOINT_CLEANUP_MAX_AGE_DAYS=30` - Max age before deletion (default: 30)
/// - `SINEX_CHECKPOINT_CLEANUP_INTERVAL_HOURS=24` - Run interval (default: 24)
///
/// To enable in your node, call [`spawn_checkpoint_cleanup_task`] during startup:
///
/// ```rust,ignore
/// let config = CheckpointCleanupConfig::from_env();
/// if config.enabled {
///     let kv = /* your checkpoint KV store */;
///     let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
///     let _cleanup_handle = spawn_checkpoint_cleanup_task(kv, config, shutdown_rx);
/// }
/// ```
#[derive(Debug, Clone)]
pub struct CheckpointManager {
    kv: async_nats::jetstream::kv::Store,
    node_name: String,
    consumer_group: String,
    consumer_name: String,
    warn_on_missing_checkpoint: bool,
}

impl CheckpointManager {
    /// Create a new checkpoint manager with NATS KV.
    #[must_use]
    pub fn new(
        kv: async_nats::jetstream::kv::Store,
        node_name: String,
        consumer_group: String,
        consumer_name: String,
    ) -> Self {
        Self::with_missing_checkpoint_warning(kv, node_name, consumer_group, consumer_name, false)
    }

    /// Create a checkpoint manager with an explicit missing-checkpoint log policy.
    #[must_use]
    pub fn with_missing_checkpoint_warning(
        kv: async_nats::jetstream::kv::Store,
        node_name: String,
        consumer_group: String,
        consumer_name: String,
        warn_on_missing_checkpoint: bool,
    ) -> Self {
        Self {
            kv,
            node_name,
            consumer_group,
            consumer_name,
            warn_on_missing_checkpoint,
        }
    }

    #[cfg(test)]
    #[must_use]
    fn missing_checkpoint_logs_as_warning(&self) -> bool {
        self.warn_on_missing_checkpoint
    }

    ///
    /// - Deserializes `checkpoint_data` JSON field into `CheckpointState`
    /// - **No checkpoint**: Returns default `CheckpointState` with `Checkpoint::None`
    ///
    /// # Returns
    /// - `Ok(CheckpointState)`: Successfully loaded checkpoint
    /// - `Err(SinexError::checkpoint)`: NATS KV read error
    /// - `Err(SinexError::Serialization)`: Corrupt checkpoint data
    ///
    /// # Behavior
    /// - If no checkpoint exists for this consumer, a default checkpoint is returned
    /// - First-time nodes get a default checkpoint with `processed_count: 0`
    pub async fn load_checkpoint(&self) -> NodeResult<CheckpointState> {
        let key = self.kv_key();
        if let Some(state) = self.load_checkpoint_for_key(&key).await? {
            debug!(
                node = %self.node_name,
                consumer_group = %self.consumer_group,
                consumer_name = %self.consumer_name,
                "Loaded checkpoint from KV"
            );

            // Warn if the restored checkpoint is stale — the node may replay already-processed events.
            // Only warn for non-empty checkpoints (Checkpoint::None means a fresh/reset state).
            if !matches!(state.checkpoint, Checkpoint::None) {
                let max_age_hours: u64 = env_parse_with_default(
                    "SINEX_CHECKPOINT_MAX_AGE_HOURS",
                    24_u64,
                    "checkpoint staleness",
                );

                let age: time::Duration = Timestamp::now() - state.last_activity;
                let age_hours = age.whole_hours();

                if age_hours > max_age_hours as i64 {
                    warn!(
                        node = %self.node_name,
                        checkpoint_age_hours = age_hours,
                        max_age_hours = max_age_hours,
                        "checkpoint is stale — node may replay already-processed events"
                    );
                }
            }

            return Ok(state);
        }

        if self.warn_on_missing_checkpoint {
            warn!(
                node = %self.node_name,
                consumer_group = %self.consumer_group,
                consumer_name = %self.consumer_name,
                "No existing checkpoint found; derived node will replay all historical events"
            );
        } else {
            info!(
                node = %self.node_name,
                consumer_group = %self.consumer_group,
                consumer_name = %self.consumer_name,
                "No existing checkpoint found, starting fresh"
            );
        }

        Ok(CheckpointState::default())
    }

    async fn load_checkpoint_for_key(&self, key: &str) -> NodeResult<Option<CheckpointState>> {
        let entry =
            self.kv.entry(key).await.map_err(|e| {
                SinexError::checkpoint("Failed to read checkpoint KV").with_source(e)
            })?;

        let Some(entry) = entry else {
            return Ok(None);
        };

        if entry.value.is_empty() {
            return Err(SinexError::checkpoint("Checkpoint KV entry is empty")
                .with_context("key", key.to_string())
                .with_context("node", self.node_name.clone())
                .with_context("consumer_group", self.consumer_group.clone())
                .with_context("consumer_name", self.consumer_name.clone()));
        }

        let mut state = self.decode_checkpoint_state(key, &entry.value)?;
        state.revision = entry.revision;
        Ok(Some(state))
    }

    fn decode_checkpoint_state(&self, key: &str, value: &[u8]) -> NodeResult<CheckpointState> {
        serde_json::from_slice::<CheckpointState>(value).map_err(|error| {
            SinexError::serialization("Failed to decode checkpoint from KV")
                .with_context("node", self.node_name.clone())
                .with_context("consumer_group", self.consumer_group.clone())
                .with_context("consumer_name", self.consumer_name.clone())
                .with_context("key", key.to_string())
                .with_std_error(&error)
        })
    }

    /// Save checkpoint to NATS KV only.
    ///
    /// Checkpoints are persisted to NATS KV; this path does not write to SQL.
    ///
    /// # Parameters
    /// - `state`: The checkpoint state to save
    ///
    /// # Returns
    /// - `Ok(u64)`: The new revision number of the saved checkpoint
    /// - `Err(SinexError::checkpoint)`: KV write error (including CAS failure)
    /// - `Err(SinexError::Serialization)`: Checkpoint serialization error
    pub async fn save_checkpoint(&self, state: &CheckpointState) -> NodeResult<u64> {
        let processed_count: i64 = state.processed_count.try_into().map_err(|_| {
            SinexError::checkpoint(
                "processed_count exceeds supported range for storage".to_string(),
            )
        })?;

        // Save to NATS KV only
        let encoded = serde_json::to_vec(state).map_err(SinexError::serialization)?;
        let key = self.kv_key();

        let revision = if state.revision > 0 {
            match self
                .kv
                .update(&key, encoded.clone().into(), state.revision)
                .await
            {
                Ok(revision) => revision,
                Err(update_error) => {
                    let existing_entry = self.kv.entry(&key).await.map_err(|error| {
                        SinexError::checkpoint("Failed to check checkpoint KV after update failure")
                            .with_source(error)
                    })?;

                    if existing_entry.is_none() {
                        warn!(
                            node = %self.node_name,
                            consumer_group = %self.consumer_group,
                            consumer_name = %self.consumer_name,
                            stale_revision = state.revision,
                            "Checkpoint KV entry is missing after restoring a local checkpoint revision; recreating it"
                        );
                        self.kv
                            .create(&key, encoded.into())
                            .await
                            .map_err(|error| {
                                SinexError::checkpoint(
                                    "Failed to recreate missing checkpoint in KV after stale local revision",
                                )
                                .with_source(error)
                            })?
                    } else {
                        return Err(SinexError::checkpoint(
                            "Failed to update checkpoint in KV (CAS failure?)",
                        )
                        .with_source(update_error));
                    }
                }
            }
        } else {
            match self.kv.create(&key, encoded.clone().into()).await {
                Ok(revision) => revision,
                Err(create_error) => {
                    let existing_entry = self.kv.entry(&key).await.map_err(|error| {
                        SinexError::checkpoint("Failed to check checkpoint KV after create failure")
                            .with_source(error)
                    })?;

                    if let Some(existing_entry) = existing_entry {
                        let mut existing_state =
                            self.decode_checkpoint_state(&key, &existing_entry.value)?;
                        existing_state.revision = existing_entry.revision;

                        if checkpoint_states_match(&existing_state, state) {
                            warn!(
                                node = %self.node_name,
                                consumer_group = %self.consumer_group,
                                consumer_name = %self.consumer_name,
                                revision = existing_entry.revision,
                                "Checkpoint create reported an error but the matching entry already exists; treating as an idempotent save"
                            );
                            existing_entry.revision
                        } else {
                            return Err(SinexError::checkpoint(
                                "Failed to create checkpoint in KV (already exists or create failed)",
                            )
                            .with_source(create_error));
                        }
                    } else {
                        return Err(SinexError::checkpoint(
                            "Failed to create checkpoint in KV (already exists or create failed)",
                        )
                        .with_source(create_error));
                    }
                }
            }
        };

        debug!(
            node = %self.node_name,
            consumer_group = %self.consumer_group,
            consumer_name = %self.consumer_name,
            processed_count = processed_count,
            checkpoint = %state.checkpoint.description(),
            revision = revision,
            "Saved checkpoint to KV"
        );

        Ok(revision)
    }

    fn kv_key(&self) -> String {
        let node = sanitize_kv_key_component(&self.node_name);
        let consumer_group = sanitize_kv_key_component(&self.consumer_group);
        let consumer = sanitize_kv_key_component(&self.consumer_name);

        format!("{node}.{consumer_group}.{consumer}")
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

        let entry =
            self.kv.get(&self.kv_key()).await.map_err(|e| {
                SinexError::checkpoint("Failed to read checkpoint KV").with_source(e)
            })?;

        let Some(entry) = entry else {
            return Ok(Vec::new());
        };

        let state = self.decode_checkpoint_state(&self.kv_key(), &entry)?;
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
            .map_err(|e| SinexError::checkpoint("Failed to purge checkpoint").with_source(e))?;

        info!(
            node = %self.node_name,
            consumer_group = %self.consumer_group,
            consumer_name = %self.consumer_name,
            "Checkpoint reset"
        );

        Ok(())
    }

    /// Get checkpoint statistics
    pub async fn get_checkpoint_stats(&self) -> NodeResult<CheckpointStats> {
        let entry =
            self.kv.get(&self.kv_key()).await.map_err(|e| {
                SinexError::checkpoint("Failed to read checkpoint KV").with_source(e)
            })?;

        let (processed_count, last_update) = match entry {
            Some(entry) => {
                let state = self.decode_checkpoint_state(&self.kv_key(), &entry)?;
                (state.processed_count, Some(state.last_activity))
            }
            None => (0, None),
        };

        Ok(CheckpointStats {
            total_checkpoints: 1, // KV stores one version
            max_processed: processed_count,
            last_update,
            first_checkpoint: None,
        })
    }
}

pub(crate) fn decode_checkpoint_data<T: DeserializeOwned>(
    data: serde_json::Value,
    state_label: &str,
    node_name: &str,
) -> NodeResult<T> {
    serde_json::from_value::<T>(data).map_err(|error| {
        SinexError::serialization(format!("Failed to decode {state_label}"))
            .with_context("node", node_name.to_string())
            .with_std_error(&error)
    })
}

/// Historical checkpoint entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointHistoryEntry {
    pub id: String,
    pub last_processed_id: Option<String>,
    pub processed_count: u64,
    pub last_activity: sinex_primitives::temporal::Timestamp,
    pub checkpoint_version: u32,
    pub created_at: sinex_primitives::temporal::Timestamp,
    pub updated_at: sinex_primitives::temporal::Timestamp,
}

/// Checkpoint statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointStats {
    pub total_checkpoints: u64,
    pub max_processed: u64,
    pub last_update: Option<sinex_primitives::temporal::Timestamp>,
    pub first_checkpoint: Option<sinex_primitives::temporal::Timestamp>,
}

/// Configuration for checkpoint cleanup.
#[derive(Debug, Clone)]
pub struct CheckpointCleanupConfig {
    /// Maximum age for checkpoints before cleanup (default: 30 days)
    pub max_age: std::time::Duration,
    /// How often to run cleanup (default: 24 hours)
    pub interval: std::time::Duration,
    /// Whether cleanup is enabled (default: true)
    pub enabled: bool,
}

impl Default for CheckpointCleanupConfig {
    fn default() -> Self {
        Self {
            max_age: std::time::Duration::from_hours(720), // 30 days
            interval: std::time::Duration::from_hours(24),
            enabled: false,
        }
    }
}

impl CheckpointCleanupConfig {
    /// Load cleanup configuration from environment variables.
    ///
    /// - `SINEX_CHECKPOINT_CLEANUP_ENABLED`: Enable cleanup (default: false)
    /// - `SINEX_CHECKPOINT_CLEANUP_MAX_AGE_DAYS`: Max age in days (default: 30)
    /// - `SINEX_CHECKPOINT_CLEANUP_INTERVAL_HOURS`: Run interval in hours (default: 24)
    #[must_use]
    pub fn from_env() -> Self {
        let enabled = env_bool_with_default(
            "SINEX_CHECKPOINT_CLEANUP_ENABLED",
            false,
            "checkpoint cleanup",
        );
        let max_age_days: u64 = env_parse_with_default(
            "SINEX_CHECKPOINT_CLEANUP_MAX_AGE_DAYS",
            30_u64,
            "checkpoint cleanup",
        );
        let interval_hours: u64 = env_parse_with_default(
            "SINEX_CHECKPOINT_CLEANUP_INTERVAL_HOURS",
            24_u64,
            "checkpoint cleanup",
        );

        Self {
            max_age: std::time::Duration::from_secs(max_age_days * 24 * 60 * 60),
            interval: std::time::Duration::from_secs(interval_hours * 60 * 60),
            enabled,
        }
    }
}

/// Result of a checkpoint cleanup run
#[derive(Debug, Clone)]
pub struct CheckpointCleanupResult {
    /// Number of checkpoints scanned
    pub scanned: usize,
    /// Number of stale checkpoints deleted
    pub deleted: usize,
    /// Number of errors encountered
    pub errors: usize,
}

/// Cleanup stale checkpoints from the KV bucket.
///
/// Scans all checkpoints in the bucket and deletes those with `last_activity`
/// older than the configured `max_age`.
///
/// # Arguments
/// - `kv`: The NATS KV store containing checkpoints
/// - `max_age`: Maximum age for checkpoints before deletion
///
/// # Returns
/// - `Ok(CheckpointCleanupResult)`: Cleanup completed with stats
/// - `Err(SinexError)`: Failed to scan or delete checkpoints
pub async fn cleanup_stale_checkpoints(
    kv: &async_nats::jetstream::kv::Store,
    max_age: std::time::Duration,
) -> NodeResult<CheckpointCleanupResult> {
    let now = Timestamp::now();
    let cutoff = checkpoint_cleanup_cutoff(now, max_age)?;

    let mut result = CheckpointCleanupResult {
        scanned: 0,
        deleted: 0,
        errors: 0,
    };

    // List all keys in the bucket
    let mut keys = kv.keys().await.map_err(|e| {
        SinexError::checkpoint("Failed to list checkpoint keys for cleanup").with_source(e)
    })?;

    while let Some(key) = keys
        .try_next()
        .await
        .map_err(|e| SinexError::checkpoint("Failed to scan checkpoint keys").with_source(e))?
    {
        result.scanned += 1;

        // Get the checkpoint entry
        let entry = match kv.get(&key).await {
            Ok(Some(entry)) => entry,
            Ok(None) => continue, // Key deleted between list and get
            Err(e) => {
                warn!(key = %key, error = %e, "Failed to read checkpoint during cleanup");
                result.errors += 1;
                continue;
            }
        };

        // Parse the checkpoint state
        let Ok(state) = serde_json::from_slice::<CheckpointState>(&entry) else {
            warn!(key = %key, "Failed to parse checkpoint during cleanup");
            result.errors += 1;
            continue;
        };

        // Check if checkpoint is stale
        if state.last_activity < cutoff {
            match kv.purge(&key).await {
                Ok(()) => {
                    debug!(
                        key = %key,
                        last_activity = %state.last_activity,
                        "Deleted stale checkpoint"
                    );
                    result.deleted += 1;
                }
                Err(e) => {
                    warn!(key = %key, error = %e, "Failed to delete stale checkpoint");
                    result.errors += 1;
                }
            }
        }
    }

    info!(
        scanned = result.scanned,
        deleted = result.deleted,
        errors = result.errors,
        max_age_days = max_age.as_secs() / 86400,
        "Checkpoint cleanup completed"
    );

    Ok(result)
}

fn checkpoint_cleanup_cutoff(
    now: Timestamp,
    max_age: std::time::Duration,
) -> NodeResult<Timestamp> {
    let max_age = time::Duration::try_from(max_age).map_err(|error| {
        SinexError::checkpoint("Checkpoint cleanup max age is out of range")
            .with_context("max_age_seconds", max_age.as_secs_f64().to_string())
            .with_std_error(&error)
    })?;
    Ok(now - max_age)
}

/// Spawn a background task for periodic checkpoint cleanup.
///
/// This function starts a background task that runs checkpoint cleanup
/// at the configured interval. The task runs until cancelled.
///
/// # Arguments
/// - `kv`: The NATS KV store containing checkpoints
/// - `config`: Cleanup configuration
///
/// # Returns
/// A `JoinHandle` for the background task. The task can be cancelled
/// by aborting the handle.
#[must_use]
pub fn spawn_checkpoint_cleanup_task(
    kv: async_nats::jetstream::kv::Store,
    config: CheckpointCleanupConfig,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if !config.enabled {
            debug!("Checkpoint cleanup disabled");
            return;
        }

        info!(
            interval_hours = config.interval.as_secs() / 3600,
            max_age_days = config.max_age.as_secs() / 86400,
            "Starting checkpoint cleanup background task"
        );

        let mut interval = tokio::time::interval(config.interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match cleanup_stale_checkpoints(&kv, config.max_age).await {
                        Ok(result) => {
                            if result.deleted > 0 {
                                info!(
                                    deleted = result.deleted,
                                    scanned = result.scanned,
                                    "Checkpoint cleanup run completed"
                                );
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "Checkpoint cleanup failed");
                        }
                    }
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        debug!("Checkpoint cleanup task received shutdown");
                        break;
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    // Inline because this covers local checkpoint env/default semantics.
    use super::{CheckpointCleanupConfig, CheckpointManager, checkpoint_cleanup_cutoff};
    use sinex_primitives::prelude::Timestamp;
    use xtask::sandbox::{EnvGuard, sinex_serial_test, sinex_test};

    #[sinex_serial_test]
    async fn checkpoint_cleanup_default_is_disabled() -> xtask::sandbox::TestResult<()> {
        assert!(!CheckpointCleanupConfig::default().enabled);
        Ok(())
    }

    #[sinex_serial_test]
    async fn checkpoint_cleanup_from_env_defaults_invalid_overrides()
    -> xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("SINEX_CHECKPOINT_CLEANUP_ENABLED", "maybe");
        env.set("SINEX_CHECKPOINT_CLEANUP_MAX_AGE_DAYS", "bogus");
        env.set("SINEX_CHECKPOINT_CLEANUP_INTERVAL_HOURS", "bogus");

        let config = CheckpointCleanupConfig::from_env();
        assert!(!config.enabled);
        assert_eq!(config.max_age.as_secs(), 30 * 24 * 60 * 60);
        assert_eq!(config.interval.as_secs(), 24 * 60 * 60);
        Ok(())
    }

    #[sinex_serial_test]
    async fn checkpoint_cleanup_cutoff_rejects_out_of_range_max_age()
    -> xtask::sandbox::TestResult<()> {
        let error = checkpoint_cleanup_cutoff(Timestamp::now(), std::time::Duration::MAX)
            .expect_err("out-of-range cleanup ages must fail honestly");
        assert!(
            error
                .to_string()
                .contains("Checkpoint cleanup max age is out of range")
        );
        Ok(())
    }

    #[sinex_test]
    async fn checkpoint_manager_can_enable_warning_for_missing_checkpoint(
        ctx: xtask::sandbox::TestContext,
    ) -> xtask::sandbox::TestResult<()> {
        let kv = ctx.with_nats().shared().await?.checkpoint_kv().await?;
        let manager = CheckpointManager::with_missing_checkpoint_warning(
            kv,
            "checkpoint-test-node".to_string(),
            "test-group".to_string(),
            "test-consumer".to_string(),
            true,
        );

        assert!(manager.missing_checkpoint_logs_as_warning());
        Ok(())
    }
}

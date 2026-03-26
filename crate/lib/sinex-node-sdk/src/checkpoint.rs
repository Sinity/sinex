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
//! - **Serialization failures**: Corrupt checkpoint data falls back to `Checkpoint::None`
//! - **KV errors**: NATS KV failures are propagated as `SinexError::checkpoint`
//!
//! # Performance Considerations
//!
//! - Checkpoints are saved atomically using KV compare-and-set revisions
//! - Frequent checkpoint updates are batched for better performance
//! - Historical checkpoint queries are limited to prevent memory issues

use crate::{NodeResult, SinexError, runtime::stream::Checkpoint};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
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
    pub async fn load_from_file(path: &std::path::Path) -> Option<Self> {
        let Ok(contents) = tokio::fs::read_to_string(path).await else {
            debug!(path = %path.display(), "No checkpoint file found or failed to read");
            return None;
        };

        let Ok(record) = serde_json::from_str::<FileCheckpointRecord>(&contents) else {
            warn!(
                path = %path.display(),
                "Failed to parse checkpoint file"
            );
            return None;
        };

        // Validate magic and version
        if record.magic != FILE_CHECKPOINT_MAGIC {
            warn!(
                path = %path.display(),
                expected = FILE_CHECKPOINT_MAGIC,
                found = record.magic,
                "Invalid checkpoint file magic"
            );
            return None;
        }

        if record.version > FILE_CHECKPOINT_VERSION {
            warn!(
                path = %path.display(),
                file_version = record.version,
                supported_version = FILE_CHECKPOINT_VERSION,
                "Checkpoint file version too new"
            );
            return None;
        }

        info!(
            path = %path.display(),
            processed_count = record.state.processed_count,
            "Loaded checkpoint from file"
        );

        Some(record.state)
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
///     let _cleanup_handle = spawn_checkpoint_cleanup_task(kv, config);
/// }
/// ```
#[derive(Debug, Clone)]
pub struct CheckpointManager {
    kv: async_nats::jetstream::kv::Store,
    node_name: String,
    consumer_group: String,
    consumer_name: String,
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
        Self {
            kv,
            node_name,
            consumer_group,
            consumer_name,
        }
    }

    ///
    /// - Deserializes `checkpoint_data` JSON field into `CheckpointState`
    /// - **No checkpoint**: Returns default `CheckpointState` with `Checkpoint::None`
    ///
    /// # Returns
    /// - `Ok(CheckpointState)`: Successfully loaded checkpoint
    /// - `Err(SinexError::checkpoint)`: NATS KV read error
    /// - `Err(SinexError::Serialization)`: Corrupt checkpoint data (falls back to None)
    ///
    /// # Behavior
    /// - Corrupt checkpoint data logs warnings and falls back to `Checkpoint::None`
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
                let max_age_hours: u64 = std::env::var("SINEX_CHECKPOINT_MAX_AGE_HOURS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(24);

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

        info!(
            node = %self.node_name,
            consumer_group = %self.consumer_group,
            consumer_name = %self.consumer_name,
            "No existing checkpoint found, starting fresh"
        );

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
            return Ok(None);
        }

        match serde_json::from_slice::<CheckpointState>(&entry.value) {
            Ok(mut state) => {
                state.revision = entry.revision;
                Ok(Some(state))
            }
            Err(err) => {
                warn!(
                    node = %self.node_name,
                    consumer_group = %self.consumer_group,
                    consumer_name = %self.consumer_name,
                    error = %err,
                    "Failed to decode checkpoint from KV; falling back"
                );
                Ok(None)
            }
        }
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

        let revision = if state.revision > 0 {
            self.kv
                .update(&self.kv_key(), encoded.into(), state.revision)
                .await
                .map_err(|e| {
                    SinexError::checkpoint("Failed to update checkpoint in KV (CAS failure?)")
                        .with_source(e)
                })?
        } else {
            self.kv
                .create(&self.kv_key(), encoded.into())
                .await
                .map_err(|e| {
                    SinexError::checkpoint(
                        "Failed to create checkpoint in KV (already exists or create failed)",
                    )
                        .with_source(e)
                })?
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

        let state: CheckpointState =
            serde_json::from_slice(&entry).map_err(SinexError::serialization)?;
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
            Some(e) => {
                if let Ok(state) = serde_json::from_slice::<CheckpointState>(&e) {
                    (state.processed_count, Some(state.last_activity))
                } else {
                    (0, None)
                }
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
            enabled: true,
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
        let enabled = std::env::var("SINEX_CHECKPOINT_CLEANUP_ENABLED")
            .is_ok_and(|v| v.to_lowercase() == "true" || v == "1");

        let max_age_days: u64 = std::env::var("SINEX_CHECKPOINT_CLEANUP_MAX_AGE_DAYS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30);

        let interval_hours: u64 = std::env::var("SINEX_CHECKPOINT_CLEANUP_INTERVAL_HOURS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(24);

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
    let cutoff = now - time::Duration::try_from(max_age).unwrap_or(time::Duration::days(30));

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
            interval.tick().await;

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
    })
}

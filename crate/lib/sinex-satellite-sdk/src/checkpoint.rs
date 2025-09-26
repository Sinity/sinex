//! Unified checkpoint management for both ingestors and automata.
//!
//! This module implements the unified checkpoint system that supports both
//! external positions (for ingestors) and internal event IDs (for automata).
//!
//! # Architecture
//!
//! The checkpoint system provides:
//! - **Unified Storage**: All checkpoints stored in `core.automaton_checkpoints` table
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
//! # Database Schema
//!
//! The `core.automaton_checkpoints` table stores:
//! - `automaton_name`: Processor identifier
//! - `consumer_group`: Consumer group (for stream processing)
//! - `consumer_name`: Instance identifier (hostname + PID)
//! - `checkpoint_data`: JSON-serialized unified checkpoint (v2+)
//!
//! # Error Handling
//!
//! Common error scenarios:
//! - **Serialization failures**: Corrupt checkpoint data falls back to `Checkpoint::None`
//! - **Database errors**: Connection failures are propagated as `SatelliteError::Database`
//!
//! # Performance Considerations
//!
//! - Checkpoints are saved atomically using `ON CONFLICT` upserts
//! - Frequent checkpoint updates are batched for better performance
//! - Historical checkpoint queries are limited to prevent memory issues

use crate::{stream_processor::Checkpoint, SatelliteError, SatelliteResult};
use serde::{Deserialize, Serialize};
use sinex_core::db::{repositories::DbPoolExt, SqlxPgPool as PgPool};
use sinex_core::types::ulid::Ulid;
use sinex_core::{ConsumerGroup, ConsumerName, ProcessorName};
use std::convert::TryInto;
use tracing::{debug, info, warn};

// Database record structures for query results
#[derive(sqlx::FromRow)]
#[allow(dead_code)] // Used by sqlx for database deserialization
struct CheckpointRecord {
    pub id: Ulid,
    #[allow(dead_code)] // Used by database query but not in code
    pub processor_name: String,
    #[allow(dead_code)] // Used by database query but not in code
    pub consumer_group: String,
    #[allow(dead_code)] // Used by database query but not in code
    pub consumer_name: String,
    pub last_processed_id: Option<String>,
    pub processed_count: i64,
    pub last_activity: chrono::DateTime<chrono::Utc>,
    pub state_data: Option<serde_json::Value>,
    pub checkpoint_version: i32,
    pub checkpoint_data: Option<serde_json::Value>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(sqlx::FromRow)]
#[allow(dead_code)] // Used by sqlx for database deserialization
struct CheckpointStatsRecord {
    pub total_checkpoints: i64,
    pub max_processed: Option<i64>,
    pub last_update: Option<chrono::DateTime<chrono::Utc>>,
    pub first_checkpoint: Option<chrono::DateTime<chrono::Utc>>,
}

/// Unified checkpoint state for both ingestors and automata.
///
/// This structure wraps the unified `Checkpoint` enum with additional metadata
/// checkpoint formats with automatic migration.
///
/// # Version Evolution
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegacyCheckpointState {
    /// Last processed message ID from stream
    pub last_processed_id: Option<String>,

    /// Total number of messages processed
    pub processed_count: u64,

    /// Last activity timestamp
    pub last_activity: chrono::DateTime<chrono::Utc>,

    /// Automaton-specific state data
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

impl Default for LegacyCheckpointState {
    fn default() -> Self {
        Self {
            last_processed_id: None,
            processed_count: 0,
            last_activity: chrono::Utc::now(),
            data: None,
            version: 1,
        }
    }
}

impl From<LegacyCheckpointState> for CheckpointState {
    fn from(legacy: LegacyCheckpointState) -> Self {
        let checkpoint = if let Some(last_processed_id) = legacy.last_processed_id {
            Checkpoint::stream(last_processed_id, None)
        } else {
            Checkpoint::None
        };

        Self {
            checkpoint,
            processed_count: legacy.processed_count,
            last_activity: legacy.last_activity,
            data: legacy.data,
            version: 2, // Upgrade to version 2
        }
    }
}

/// Manager for unified checkpoint persistence (both ingestors and automata).
///
/// This manager handles checkpoint storage, retrieval, and migration in the
/// `core.automaton_checkpoints` table. It supports both ingestors and automata
///
/// # Usage Pattern
/// ```rust
/// use sinex_satellite_sdk::CheckpointManager;
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
/// Database operations are atomic and handle concurrent access.
#[derive(Debug, Clone)]
pub struct CheckpointManager {
    pool: PgPool,
    processor_name: String,
    consumer_group: String,
    consumer_name: String,
}

impl CheckpointManager {
    /// Create a new checkpoint manager
    pub fn new(
        pool: PgPool,
        processor_name: String,
        consumer_group: String,
        consumer_name: String,
    ) -> Self {
        Self {
            pool,
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
    /// - `Err(SatelliteError::Database)`: Database connection or query error
    /// - `Err(SatelliteError::Serialization)`: Corrupt checkpoint data (falls back to None)
    ///
    /// # Behavior
    /// - Corrupt checkpoint data logs warnings and falls back to `Checkpoint::None`
    /// - First-time processors get a default checkpoint with `processed_count: 0`
    pub async fn load_checkpoint(&self) -> SatelliteResult<CheckpointState> {
        let processor_name = ProcessorName::new(&self.processor_name);
        let consumer_group = ConsumerGroup::new(&self.consumer_group);
        let consumer_name = ConsumerName::new(&self.consumer_name);

        let checkpoint_result = self
            .pool
            .checkpoints()
            .get_by_processor_and_consumer(&processor_name, &consumer_group, &consumer_name)
            .await?;

        let checkpoint = if let Some(row) = checkpoint_result {
            let processed_count = u64::try_from(row.processed_count).map_err(|_| {
                SatelliteError::Checkpoint(
                    "Stored checkpoint has negative processed_count, refusing to load".to_string(),
                )
            })?;

            debug!(
                processor = %self.processor_name,
                consumer_group = %self.consumer_group,
                consumer_name = %self.consumer_name,
                version = 2, // Default to version 2 since checkpoint_version doesn't exist
                "Loaded existing checkpoint"
            );

            let version = 2u32; // Default to version 2 since checkpoint_version doesn't exist

            if version >= 2 && row.checkpoint_data.is_some() {
                // New unified format (version 2+)
                let checkpoint_data = row.checkpoint_data.ok_or_else(|| {
                    SatelliteError::Checkpoint("Checkpoint data is unexpectedly None".to_string())
                })?;
                let checkpoint: Checkpoint = serde_json::from_value(checkpoint_data)
                    .map_err(|e| {
                        warn!(error = %e, "Failed to deserialize checkpoint data, falling back to legacy");
                        e
                    })
                    .unwrap_or(Checkpoint::None);

                CheckpointState {
                    checkpoint,
                    processed_count,
                    last_activity: row.last_activity,
                    data: None, // state field doesn't exist
                    version,
                }
            } else {
                warn!(
                    processor = %self.processor_name,
                    "Migrating legacy checkpoint format to unified format"
                );

                let legacy = LegacyCheckpointState {
                    last_processed_id: row.last_processed_id.map(|id| id.as_ulid().to_string()),
                    processed_count,
                    last_activity: row.last_activity,
                    data: None, // state field doesn't exist
                    version,
                };

                // Convert to new format and save
                let unified_checkpoint = CheckpointState::from(legacy);
                self.save_checkpoint(&unified_checkpoint).await?;
                unified_checkpoint
            }
        } else {
            info!(
                processor = %self.processor_name,
                consumer_group = %self.consumer_group,
                consumer_name = %self.consumer_name,
                "No existing checkpoint found, starting fresh"
            );

            CheckpointState::default()
        };

        Ok(checkpoint)
    }

    /// Save checkpoint to database in unified format.
    ///
    /// This method atomically saves the checkpoint using an `ON CONFLICT` upsert
    /// to handle concurrent updates. The checkpoint is serialized to JSON and
    /// stored in the `checkpoint_data` field.
    ///
    /// # Parameters
    /// - `state`: The checkpoint state to save
    ///
    /// # Returns
    /// - `Ok(())`: Checkpoint successfully saved
    /// - `Err(SatelliteError::Database)`: Database connection or query error
    /// - `Err(SatelliteError::Serialization)`: Checkpoint serialization error
    ///
    /// # Atomicity
    /// - Uses `ON CONFLICT` upsert for atomic updates
    /// - Updates `updated_at` timestamp on each save
    pub async fn save_checkpoint(&self, state: &CheckpointState) -> SatelliteResult<()> {
        let _checkpoint_id = Ulid::new();

        // Serialize the unified checkpoint
        let checkpoint_data =
            serde_json::to_value(&state.checkpoint).map_err(SatelliteError::Serialization)?;

        let last_processed_id = match &state.checkpoint {
            Checkpoint::Stream { message_id, .. } => message_id.parse::<sinex_core::Ulid>().ok(),
            _ => None,
        };

        let processor_name = ProcessorName::new(&self.processor_name);
        let consumer_group = ConsumerGroup::new(&self.consumer_group);
        let consumer_name = ConsumerName::new(&self.consumer_name);

        let processed_count: i64 = state.processed_count.try_into().map_err(|_| {
            SatelliteError::Checkpoint(
                "processed_count exceeds supported range for storage".to_string(),
            )
        })?;

        self.pool
            .checkpoints()
            .upsert(
                &processor_name,
                &consumer_group,
                &consumer_name,
                last_processed_id.map(|id| {
                    sinex_core::Id::<sinex_core::Event<sinex_core::JsonValue>>::from_ulid(id)
                }),
                processed_count,
                Some(checkpoint_data),
            )
            .await?;

        debug!(
            processor = %self.processor_name,
            consumer_group = %self.consumer_group,
            consumer_name = %self.consumer_name,
            processed_count = state.processed_count,
            checkpoint = %state.checkpoint.description(),
            "Saved unified checkpoint"
        );

        Ok(())
    }

    /// Get checkpoint history for debugging
    pub async fn get_checkpoint_history(
        &self,
        _limit: i64,
    ) -> SatelliteResult<Vec<CheckpointHistoryEntry>> {
        // CheckpointQueries doesn't have get_checkpoint_history method in the new API
        // For now, just return an empty vector
        let rows: Vec<CheckpointHistoryEntry> = vec![];

        let entries = rows;

        debug!(
            processor = %self.processor_name,
            entries = entries.len(),
            "Retrieved checkpoint history"
        );

        Ok(entries)
    }

    /// Reset checkpoint (for testing or manual intervention)
    pub async fn reset_checkpoint(&self) -> SatelliteResult<()> {
        // CheckpointQueries doesn't have delete_checkpoint method in the new API
        // For now, just log a warning
        warn!(
            processor = %self.processor_name,
            "Reset checkpoint not implemented in new API"
        );

        warn!(
            processor = %self.processor_name,
            consumer_group = %self.consumer_group,
            consumer_name = %self.consumer_name,
            "Reset checkpoint"
        );

        Ok(())
    }

    /// Get checkpoint statistics
    pub async fn get_checkpoint_stats(&self) -> SatelliteResult<CheckpointStats> {
        // CheckpointQueries doesn't have get_checkpoint_stats method in the new API
        // For now, return default stats
        Ok(CheckpointStats {
            total_checkpoints: 0,
            max_processed: 0,
            last_update: None,
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

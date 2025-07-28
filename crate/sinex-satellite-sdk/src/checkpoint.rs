//! Unified checkpoint management for both ingestors and automata.
//!
//! This module implements the unified checkpoint system that supports both
//! external positions (for ingestors) and internal event IDs (for automata).
//!
//! # Architecture
//!
//! The checkpoint system provides:
//! - **Unified Storage**: All checkpoints stored in `core.automaton_checkpoints` table
//! - **Format Migration**: Automatic migration from legacy (v1) to unified (v2) format
//! - **Type Safety**: Strongly typed checkpoint variants for different use cases
//! - **Persistence**: Atomic checkpoint updates with optimistic concurrency
//!
//! # Checkpoint Types
//!
//! - `External`: For ingestors tracking external system state (file positions, timestamps)
//! - `Internal`: For automata tracking processed event ULIDs
//! - `Stream`: For Redis Stream message IDs
//! - `Timestamp`: For time-based processing resumption
//!
//! # Database Schema
//!
//! The `core.automaton_checkpoints` table stores:
//! - `automaton_name`: Processor identifier
//! - `consumer_group`: Redis consumer group (for automata)
//! - `consumer_name`: Instance identifier (hostname + PID)
//! - `checkpoint_data`: JSON-serialized unified checkpoint (v2+)
//! - `last_processed_id`: Legacy field for Redis Stream ID (v1 compatibility)
//!
//! # Error Handling
//!
//! Common error scenarios:
//! - **Serialization failures**: Corrupt checkpoint data falls back to `Checkpoint::None`
//! - **Database errors**: Connection failures are propagated as `SatelliteError::Database`
//! - **Migration failures**: Legacy format migration logged as warnings
//!
//! # Performance Considerations
//!
//! - Checkpoints are saved atomically using `ON CONFLICT` upserts
//! - Frequent checkpoint updates are batched for better performance
//! - Historical checkpoint queries are limited to prevent memory issues

use crate::{stream_processor::Checkpoint, SatelliteError, SatelliteResult};
use serde::{Deserialize, Serialize};
use sinex_db::{queries::CheckpointQueries, SqlxPgPool as PgPool};
use sinex_ulid::Ulid;
use tracing::{debug, info, warn};

// Database record structures for query results
#[derive(sqlx::FromRow)]
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
struct CheckpointStatsRecord {
    pub total_checkpoints: i64,
    pub max_processed: Option<i64>,
    pub last_update: Option<chrono::DateTime<chrono::Utc>>,
    pub first_checkpoint: Option<chrono::DateTime<chrono::Utc>>,
}

/// Unified checkpoint state for both ingestors and automata.
///
/// This structure wraps the unified `Checkpoint` enum with additional metadata
/// for persistence and monitoring. It supports both current (v2) and legacy (v1)
/// checkpoint formats with automatic migration.
///
/// # Version Evolution
/// - **Version 1**: Legacy format with `last_processed_id` string field
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

/// Legacy checkpoint state for backward compatibility
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegacyCheckpointState {
    /// Last processed message ID from Redis Stream
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
    /// Extract the last processed ID in string format for backward compatibility
    pub fn last_processed_id(&self) -> Option<String> {
        match &self.checkpoint {
            Checkpoint::None => None,
            Checkpoint::Internal { event_id, .. } => Some(event_id.to_string()),
            Checkpoint::External { .. } => None, // External checkpoints don't have event IDs
            Checkpoint::Stream { message_id, .. } => Some(message_id.clone()),
            Checkpoint::Timestamp { .. } => None, // Timestamp checkpoints don't have event IDs
        }
    }

    /// Set the last processed ID (for backward compatibility)
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
/// with automatic format migration from legacy checkpoints.
///
/// # Usage Pattern
/// ```rust
/// use sinex_satellite_sdk::checkpoint::CheckpointManager;
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

    /// Load checkpoint from database with automatic migration from legacy format.
    ///
    /// This method handles both current (v2) and legacy (v1) checkpoint formats:
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
    /// - Legacy checkpoints are automatically migrated and saved in v2 format
    /// - Corrupt checkpoint data logs warnings and falls back to `Checkpoint::None`
    /// - First-time processors get a default checkpoint with `processed_count: 0`
    pub async fn load_checkpoint(&self) -> SatelliteResult<CheckpointState> {
        let row: Option<CheckpointRecord> = CheckpointQueries::get_checkpoint(
            self.processor_name.clone(),
            self.consumer_group.clone(),
            self.consumer_name.clone(),
        )
        .fetch_optional(&self.pool)
        .await?;

        let checkpoint = if let Some(row) = row {
            debug!(
                processor = %self.processor_name,
                consumer_group = %self.consumer_group,
                consumer_name = %self.consumer_name,
                version = row.checkpoint_version,
                "Loaded existing checkpoint"
            );

            let version = row.checkpoint_version as u32;

            if version >= 2 && row.checkpoint_data.is_some() {
                // New unified format (version 2+)
                let checkpoint_data = row.checkpoint_data.unwrap();
                let checkpoint: Checkpoint = serde_json::from_value(checkpoint_data)
                    .map_err(|e| {
                        warn!(error = %e, "Failed to deserialize checkpoint data, falling back to legacy");
                        e
                    })
                    .unwrap_or(Checkpoint::None);

                CheckpointState {
                    checkpoint,
                    processed_count: row.processed_count as u64,
                    last_activity: row.last_activity,
                    data: row.state_data,
                    version,
                }
            } else {
                // Legacy format (version 1) - migrate to new format
                warn!(
                    processor = %self.processor_name,
                    "Migrating legacy checkpoint format to unified format"
                );

                let legacy = LegacyCheckpointState {
                    last_processed_id: row.last_processed_id,
                    processed_count: row.processed_count as u64,
                    last_activity: row.last_activity,
                    data: row.state_data,
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
    /// - Maintains backward compatibility with legacy `last_processed_id` field
    pub async fn save_checkpoint(&self, state: &CheckpointState) -> SatelliteResult<()> {
        let checkpoint_id = Ulid::new();

        // Serialize the unified checkpoint
        let checkpoint_data =
            serde_json::to_value(&state.checkpoint).map_err(SatelliteError::Serialization)?;

        // Extract legacy fields for backward compatibility
        let last_processed_id = match &state.checkpoint {
            Checkpoint::Stream { message_id, .. } => Some(message_id.clone()),
            _ => None,
        };

        CheckpointQueries::upsert_checkpoint_with_conflict(
            &self.pool,
            checkpoint_id,
            self.processor_name.clone(),
            self.consumer_group.clone(),
            self.consumer_name.clone(),
            last_processed_id,
            state.processed_count as i64,
            state.last_activity,
            state.data.clone(),
            state.version as i32,
            Some(checkpoint_data),
            chrono::Utc::now(),
            chrono::Utc::now(),
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
        limit: i64,
    ) -> SatelliteResult<Vec<CheckpointHistoryEntry>> {
        let rows: Vec<CheckpointRecord> = CheckpointQueries::get_checkpoint_history(
            self.processor_name.clone(),
            self.consumer_group.clone(),
            self.consumer_name.clone(),
            limit,
        )
        .fetch_all(&self.pool)
        .await?;

        let entries: Vec<CheckpointHistoryEntry> = rows
            .into_iter()
            .map(|row| CheckpointHistoryEntry {
                id: row.id.to_string(),
                last_processed_id: row.last_processed_id,
                processed_count: row.processed_count as u64,
                last_activity: row.last_activity,
                checkpoint_version: row.checkpoint_version as u32,
                created_at: row.created_at,
                updated_at: row.updated_at,
            })
            .collect();

        debug!(
            processor = %self.processor_name,
            entries = entries.len(),
            "Retrieved checkpoint history"
        );

        Ok(entries)
    }

    /// Reset checkpoint (for testing or manual intervention)
    pub async fn reset_checkpoint(&self) -> SatelliteResult<()> {
        CheckpointQueries::delete_checkpoint(
            self.processor_name.clone(),
            self.consumer_group.clone(),
            self.consumer_name.clone(),
        )
        .execute(&self.pool)
        .await?;

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
        let row: CheckpointStatsRecord = CheckpointQueries::get_checkpoint_stats(
            self.processor_name.clone(),
            self.consumer_group.clone(),
            self.consumer_name.clone(),
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(CheckpointStats {
            total_checkpoints: row.total_checkpoints as u64,
            max_processed: row.max_processed.unwrap_or(0) as u64,
            last_update: row.last_update,
            first_checkpoint: row.first_checkpoint,
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

//! Unified checkpoint management for both ingestors and automata
//!
//! This module implements the unified checkpoint system that supports both
//! external positions (for ingestors) and internal event IDs (for automata).

use crate::{stream_processor::Checkpoint, SatelliteResult, SatelliteError};
use serde::{Deserialize, Serialize};
use sinex_db::SqlxPgPool as PgPool;
use sinex_ulid::Ulid;
use tracing::{debug, info, warn};

/// Unified checkpoint state for both ingestors and automata
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

/// Manager for unified checkpoint persistence (both ingestors and automata)
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

    /// Load checkpoint from database with automatic migration from legacy format
    pub async fn load_checkpoint(&self) -> SatelliteResult<CheckpointState> {
        let row = sqlx::query!(
            r#"
            SELECT 
                last_processed_id,
                processed_count,
                last_activity,
                state_data,
                checkpoint_version,
                checkpoint_data
            FROM core.automaton_checkpoints 
            WHERE automaton_name = $1 
                AND consumer_group = $2 
                AND consumer_name = $3
            "#,
            self.processor_name,
            self.consumer_group,
            self.consumer_name
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

    /// Save checkpoint to database in unified format
    pub async fn save_checkpoint(&self, state: &CheckpointState) -> SatelliteResult<()> {
        let checkpoint_id = Ulid::new();
        
        // Serialize the unified checkpoint
        let checkpoint_data = serde_json::to_value(&state.checkpoint)
            .map_err(SatelliteError::Serialization)?;

        // Extract legacy fields for backward compatibility
        let last_processed_id = match &state.checkpoint {
            Checkpoint::Stream { message_id, .. } => Some(message_id.clone()),
            _ => None,
        };

        sqlx::query!(
            r#"
            INSERT INTO core.automaton_checkpoints (
                id,
                automaton_name,
                consumer_group,
                consumer_name,
                last_processed_id,
                processed_count,
                last_activity,
                state_data,
                checkpoint_version,
                checkpoint_data,
                created_at,
                updated_at
            ) VALUES (
                $1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11
            )
            ON CONFLICT (automaton_name, consumer_group, consumer_name) 
            DO UPDATE SET
                last_processed_id = EXCLUDED.last_processed_id,
                processed_count = EXCLUDED.processed_count,
                last_activity = EXCLUDED.last_activity,
                state_data = EXCLUDED.state_data,
                checkpoint_version = EXCLUDED.checkpoint_version,
                checkpoint_data = EXCLUDED.checkpoint_data,
                updated_at = EXCLUDED.updated_at
            "#,
            checkpoint_id.to_uuid(),
            self.processor_name,
            self.consumer_group,
            self.consumer_name,
            last_processed_id,
            state.processed_count as i64,
            state.last_activity,
            state.data,
            state.version as i32,
            checkpoint_data,
            chrono::Utc::now()
        )
        .execute(&self.pool)
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
    pub async fn get_checkpoint_history(&self, limit: i64) -> SatelliteResult<Vec<CheckpointHistoryEntry>> {
        let rows = sqlx::query!(
            r#"
            SELECT 
                id::text,
                last_processed_id,
                processed_count,
                last_activity,
                checkpoint_version,
                created_at,
                updated_at
            FROM core.automaton_checkpoints 
            WHERE automaton_name = $1 
                AND consumer_group = $2 
                AND consumer_name = $3
            ORDER BY updated_at DESC
            LIMIT $4
            "#,
            self.processor_name,
            self.consumer_group,
            self.consumer_name,
            limit
        )
        .fetch_all(&self.pool)
        .await?;

        let entries: Vec<CheckpointHistoryEntry> = rows
            .into_iter()
            .map(|row| CheckpointHistoryEntry {
                id: row.id.unwrap_or_default(),
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
        sqlx::query!(
            r#"
            DELETE FROM core.automaton_checkpoints 
            WHERE automaton_name = $1 
                AND consumer_group = $2 
                AND consumer_name = $3
            "#,
            self.processor_name,
            self.consumer_group,
            self.consumer_name
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
        let row = sqlx::query!(
            r#"
            SELECT 
                COUNT(*) as total_checkpoints,
                MAX(processed_count) as max_processed,
                MAX(updated_at) as last_update,
                MIN(created_at) as first_checkpoint
            FROM core.automaton_checkpoints 
            WHERE automaton_name = $1 
                AND consumer_group = $2 
                AND consumer_name = $3
            "#,
            self.processor_name,
            self.consumer_group,
            self.consumer_name
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(CheckpointStats {
            total_checkpoints: row.total_checkpoints.unwrap_or(0) as u64,
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
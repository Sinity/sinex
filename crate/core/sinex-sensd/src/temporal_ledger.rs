#![doc = include_str!("../doc/temporal_ledger.md")]

//! Temporal ledger management.
//!
//! The temporal ledger records precise capture-time information for all
//! source materials, ensuring temporal integrity and provenance tracking.

use crate::config::TemporalLedgerConfig;
use chrono::{DateTime, Utc};
use color_eyre::eyre::{eyre, Context, Result};
use serde_json::json;
use sinex_core::db::repositories::legacy_material_types;
use sinex_core::types::Ulid;
use sinex_core::DbPoolExt;
use sinex_core::Id;
use sinex_core::SourceMaterialRecord;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, info};

/// Temporal ledger entry matching the database schema
#[derive(Debug, Clone)]
pub struct LedgerEntry {
    pub source_material_id: Ulid,
    pub offset_start: i64,
    pub offset_end: i64,
    pub offset_kind: String,
    pub ts_capture: DateTime<Utc>,
    pub precision: String,
    pub clock: String,
    pub source_type: String,
}

/// Temporal ledger manager
pub struct TemporalLedger {
    db_pool: PgPool,
    config: TemporalLedgerConfig,
    entry_buffer: Arc<Mutex<Vec<LedgerEntry>>>,
    entry_sender: mpsc::Sender<LedgerEntry>,
    entry_receiver: Arc<Mutex<mpsc::Receiver<LedgerEntry>>>,
}

impl TemporalLedger {
    /// Create new temporal ledger
    pub async fn new(db_pool: PgPool, config: TemporalLedgerConfig) -> Result<Self> {
        // Create channel for ledger entries
        let (entry_sender, entry_receiver) = mpsc::channel(1000);

        Ok(Self {
            db_pool,
            config,
            entry_buffer: Arc::new(Mutex::new(Vec::new())),
            entry_sender,
            entry_receiver: Arc::new(Mutex::new(entry_receiver)),
        })
    }

    /// Create in-memory temporal ledger for testing
    pub async fn new_in_memory() -> Result<Self> {
        let database_url = std::env::var("DATABASE_URL")
            .wrap_err("TemporalLedger::new_in_memory requires DATABASE_URL to be set")?;

        let config = TemporalLedgerConfig {
            batch_size: 100,
            flush_interval_ms: 1000,
            max_slice_size: 10 * 1024 * 1024, // 10MB
        };

        let pool = PgPool::connect(&database_url)
            .await
            .wrap_err("Failed to connect to database for TemporalLedger::new_in_memory")?;

        Self::new(pool, config).await
    }

    /// Register a new in-flight source material for sensing pipelines
    pub async fn create_material(
        &self,
        source_identifier: &str,
        source_type: &str,
        source_uri: Option<&str>,
        material_hint: Option<&str>,
    ) -> Result<Ulid> {
        let legacy_hint = material_hint.unwrap_or(legacy_material_types::STREAM);

        let mut metadata = json!({
            "source_type": source_type,
            "source_identifier": source_identifier,
        });

        if let Some(hint) = material_hint {
            let key = if hint.contains('/') {
                "content_type"
            } else {
                "material_hint"
            };

            metadata
                .as_object_mut()
                .expect("metadata is an object")
                .insert(key.to_string(), json!(hint));
        }

        let record = self
            .db_pool
            .source_materials()
            .register_in_flight(legacy_hint, source_uri, metadata)
            .await
            .wrap_err("Failed to register in-flight source material")?;

        Ok(record.id)
    }

    /// Finalize a source material once capture is complete
    pub async fn finalize_material(
        &self,
        material_id: Ulid,
        reason: &str,
        total_bytes: Option<i64>,
    ) -> Result<()> {
        let repo = self.db_pool.source_materials();
        let id: Id<SourceMaterialRecord> = Id::from_ulid(material_id);

        // Attach finalize metadata for observability
        let metadata = json!({
            "finalize_reason": reason,
            "finalized_at": Utc::now().to_rfc3339(),
        });
        let _ = repo
            .update_metadata(id, metadata)
            .await
            .wrap_err("Failed to update material metadata before finalize")?;

        let id: Id<SourceMaterialRecord> = Id::from_ulid(material_id);
        repo.finalize_in_flight(id, None, None, None, total_bytes)
            .await
            .wrap_err("Failed to finalize source material")?;

        info!(%material_id, reason, total_bytes, "Finalized source material");
        Ok(())
    }

    /// Retrieve total captured bytes for a material
    pub async fn get_material_bytes(&self, material_id: Ulid) -> Result<i64> {
        let total_bytes: i64 = sqlx::query_scalar!(
            r#"
            SELECT COALESCE(MAX(offset_end), 0)::BIGINT AS "size!"
            FROM raw.temporal_ledger
            WHERE source_material_id = $1::ulid
            "#,
            material_id as Ulid
        )
        .fetch_one(&self.db_pool)
        .await
        .wrap_err("Failed to compute material size from temporal ledger")?;

        Ok(total_bytes)
    }

    /// Record a new ledger entry
    pub async fn record_entry(&self, entry: LedgerEntry) -> Result<()> {
        self.entry_sender
            .send(entry)
            .await
            .map_err(|e| eyre!("Failed to send ledger entry: {}", e))?;
        Ok(())
    }

    /// Run background worker for batch writing
    pub async fn run_background_worker(&self) -> Result<()> {
        info!("Starting temporal ledger background worker");

        let mut receiver = self.entry_receiver.lock().await;
        let mut buffer = Vec::new();
        let mut last_flush = tokio::time::Instant::now();

        loop {
            // Wait for entries or timeout
            let timeout = tokio::time::Duration::from_millis(self.config.flush_interval_ms);

            tokio::select! {
                Some(entry) = receiver.recv() => {
                    buffer.push(entry);

                    // Flush if buffer is full
                    if buffer.len() >= self.config.batch_size {
                        self.flush_entries(&mut buffer).await?;
                        last_flush = tokio::time::Instant::now();
                    }
                }
                _ = tokio::time::sleep_until(last_flush + timeout) => {
                    // Flush on timeout if buffer has entries
                    if !buffer.is_empty() {
                        self.flush_entries(&mut buffer).await?;
                        last_flush = tokio::time::Instant::now();
                    }
                }
            }
        }
    }

    /// Flush entries to database
    async fn flush_entries(&self, entries: &mut Vec<LedgerEntry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        debug!("Flushing {} ledger entries to database", entries.len());

        // Batch insert into temporal_ledger table
        let mut tx = self.db_pool.begin().await?;

        for entry in entries.iter() {
            sqlx::query!(
                r#"
                INSERT INTO raw.temporal_ledger (
                    source_material_id, offset_start, offset_end,
                    offset_kind, ts_capture, precision, clock, source_type
                )
                VALUES ($1::ulid, $2, $3, $4, $5, $6, $7, $8)
                "#,
                entry.source_material_id as Ulid,
                entry.offset_start,
                entry.offset_end,
                entry.offset_kind,
                entry.ts_capture,
                entry.precision,
                entry.clock,
                entry.source_type,
            )
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        debug!("Successfully flushed {} ledger entries", entries.len());
        entries.clear();

        Ok(())
    }
}

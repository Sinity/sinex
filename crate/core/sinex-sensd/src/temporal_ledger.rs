//! Temporal ledger management
//!
//! The temporal ledger records precise capture-time information for all
//! source materials, ensuring temporal integrity and provenance tracking.

use crate::config::TemporalLedgerConfig;
use chrono::{DateTime, Utc};
use color_eyre::eyre::{eyre, Result};
use sinex_core::types::Ulid;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, info};

/// Temporal ledger entry
#[derive(Debug, Clone)]
pub struct LedgerEntry {
    pub material_id: Ulid,
    pub offset_start: i64,
    pub offset_end: i64,
    pub ts_capture: DateTime<Utc>,
    pub offset_kind: String,
    pub precision: String,
    pub clock: String,
    pub source_type: String,
    pub note: Option<String>,
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
                    material_id, offset_start, offset_end,
                    offset_kind, ts_capture, precision, clock, source_type, note
                )
                VALUES ($1::ulid, $2, $3, $4, $5, $6, $7, $8, $9)
                "#,
                entry.material_id as Ulid,
                entry.offset_start,
                entry.offset_end,
                entry.offset_kind,
                entry.ts_capture,
                entry.precision,
                entry.clock,
                entry.source_type,
                entry.note,
            )
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        debug!("Successfully flushed {} ledger entries", entries.len());
        entries.clear();

        Ok(())
    }

    /// Create a new material record
    pub async fn create_material(
        &self,
        source_identifier: &str,
        source_type: &str,
        source_path: Option<&str>,
        content_type: Option<&str>,
    ) -> Result<Ulid> {
        let material_id = Ulid::new();

        sqlx::query!(
            r#"
            INSERT INTO raw.source_material_registry (
                blob_id, source_identifier, source_type, source_path,
                content_type, status, created_at
            )
            VALUES ($1::ulid, $2, $3, $4, $5, 'sensing', NOW())
            "#,
            material_id as Ulid,
            source_identifier,
            source_type,
            source_path,
            content_type,
        )
        .execute(&self.db_pool)
        .await?;

        info!(
            "Created new material: {} for {}",
            material_id, source_identifier
        );

        Ok(material_id)
    }

    /// Finalize a material record
    pub async fn finalize_material(
        &self,
        material_id: Ulid,
        status: &str,
        total_bytes: Option<i64>,
    ) -> Result<()> {
        sqlx::query!(
            r#"
            UPDATE raw.source_material_registry
            SET status = $2, 
                finalized_at = NOW(),
                total_bytes = $3
            WHERE source_material_id = $1::ulid
            "#,
            material_id as Ulid,
            status,
            total_bytes,
        )
        .execute(&self.db_pool)
        .await?;

        info!(
            "Finalized material: {} with status: {}",
            material_id, status
        );

        Ok(())
    }
}

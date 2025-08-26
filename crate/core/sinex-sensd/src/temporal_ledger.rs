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
        // For testing, we create a mock temporal ledger that doesn't require a real database
        let (entry_sender, entry_receiver) = mpsc::channel(1000);

        // Create a mock database connection pool - this will only work for tests
        // that don't actually call database methods
        let mock_pool = PgPool::connect("postgres://test:test@localhost/test")
            .await
            .unwrap_or_else(|_| {
                // If we can't connect to a real database, create a minimal mock
                // This is a temporary solution for tests
                panic!("new_in_memory() requires a test database or mock implementation")
            });

        let config = TemporalLedgerConfig {
            batch_size: 100,
            flush_interval_ms: 1000,
            max_slice_size: 10 * 1024 * 1024, // 10MB
        };

        Ok(Self {
            db_pool: mock_pool,
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
                id, source_identifier, material_kind, status, 
                timing_info_type, metadata, staged_at, staged_by
            )
            VALUES ($1::ulid, $2, $3, 'active', 'realtime', $4, NOW(), 'temporal_ledger')
            "#,
            material_id as Ulid,
            source_identifier,
            source_type,
            serde_json::json!({
                "source_path": source_path,
                "content_type": content_type
            }),
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
                metadata = jsonb_set(
                    metadata,
                    '{total_bytes}',
                    to_jsonb($3::bigint)
                )
            WHERE id = $1::ulid
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

    /// Get total bytes for a material from temporal ledger
    pub async fn get_material_bytes(&self, material_id: Ulid) -> Result<i64> {
        // Query temporal ledger for total bytes written for this material
        let result = sqlx::query!(
            r#"
            SELECT SUM(offset_end - offset_start) as total_bytes
            FROM raw.temporal_ledger
            WHERE source_material_id = $1::ulid
            "#,
            material_id as Ulid
        )
        .fetch_optional(&self.db_pool)
        .await?;

        use sqlx::types::BigDecimal;
        use std::str::FromStr;
        
        Ok(result
            .and_then(|row| row.total_bytes)
            .and_then(|bd| bd.to_string().parse::<i64>().ok())
            .unwrap_or(0))
    }
}

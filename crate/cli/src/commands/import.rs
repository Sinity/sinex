//! Historical data import commands.
//!
//! Connects directly to the database (bypassing the gateway) for bulk import
//! performance. Uses `HistoricalImporter` for provenance, bisect-retry, and
//! progress tracking.

use std::env;
use std::path::PathBuf;
use std::time::Instant;

use clap::{Parser, Subcommand};
use color_eyre::Result;
use rusqlite::{Connection, OpenFlags};
use serde_json::json;
use sinex_db::repositories::StreamBatchRow;
use sinex_db::{DbPoolExt, create_pool};
use sinex_node_sdk::HistoricalImporter;
use sinex_primitives::domain::{EventSource, EventType, RecordedPath};
use sinex_primitives::events::payloads::shell::AtuinCommandExecutedPayload;
use sinex_primitives::{Id, Uuid};

/// Historical data import subcommands
#[derive(Debug, Subcommand)]
#[command(after_help = "\
EXAMPLES:
    # Import Atuin history from default location
    sinexctl import atuin

    # Import from custom path
    sinexctl import atuin --db-path /path/to/history.db

    # Resume an interrupted import
    sinexctl import atuin --resume

    # Import with larger batches
    sinexctl import atuin --batch-size 2000
")]
pub enum ImportCommands {
    /// Import Atuin shell history into sinex
    Atuin(AtuinImportCommand),
}

impl ImportCommands {
    pub async fn execute(&self) -> Result<()> {
        match self {
            Self::Atuin(cmd) => cmd.execute().await,
        }
    }
}

/// Import Atuin shell history from its SQLite database.
///
/// Reads the Atuin history database and bulk-inserts events into sinex
/// as `shell.atuin / command.executed` events with full provenance tracking.
#[derive(Debug, Parser)]
pub struct AtuinImportCommand {
    /// Path to the Atuin SQLite database
    #[arg(long, default_value_os_t = default_atuin_db_path())]
    pub db_path: PathBuf,

    /// Number of rows to process per batch
    #[arg(long, default_value = "1000")]
    pub batch_size: usize,

    /// Resume from last imported row (skip already-imported events)
    #[arg(long)]
    pub resume: bool,
}

fn default_atuin_db_path() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("~/.local/share"))
        .join("atuin/history.db")
}

/// A single row from the Atuin SQLite history table.
struct AtuinRow {
    id: String,
    timestamp: i64,
    duration: i64,
    exit: i64,
    command: String,
    cwd: String,
    session: String,
    hostname: String,
}

impl AtuinImportCommand {
    pub async fn execute(&self) -> Result<()> {
        // Validate Atuin DB exists
        if !self.db_path.exists() {
            return Err(color_eyre::eyre::eyre!(
                "Atuin database not found at: {}\n\
                 Use --db-path to specify an alternative location.",
                self.db_path.display()
            ));
        }

        // Connect to sinex Postgres
        let database_url = env::var("DATABASE_URL").map_err(|_| {
            color_eyre::eyre::eyre!(
                "DATABASE_URL not set. Set it in your environment."
            )
        })?;

        let pool = create_pool(&database_url)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to connect to database: {e}"))?;

        // Open Atuin SQLite read-only
        let sqlite = Connection::open_with_flags(
            &self.db_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|e| color_eyre::eyre::eyre!("Failed to open Atuin database: {e}"))?;

        // Count total rows for progress reporting
        let total_rows: i64 = sqlite
            .query_row(
                "SELECT COUNT(*) FROM history WHERE deleted_at IS NULL",
                [],
                |row| row.get(0),
            )
            .map_err(|e| color_eyre::eyre::eyre!("Failed to count Atuin rows: {e}"))?;

        println!("Atuin database: {}", self.db_path.display());
        println!("Total rows: {total_rows}");
        println!("Batch size: {}", self.batch_size);

        if total_rows == 0 {
            println!("Nothing to import.");
            return Ok(());
        }

        // Register source material via HistoricalImporter
        let source_path = self.db_path.to_string_lossy();
        let deterministic_material_id = HistoricalImporter::material_uuid_for_path(&source_path);
        let existing_event_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM core.events WHERE source_material_id = $1",
        )
        .bind(deterministic_material_id)
        .fetch_one(&pool)
        .await
        .unwrap_or(0);
        if !self.resume && existing_event_count > 0 {
            return Err(color_eyre::eyre::eyre!(
                "Material {deterministic_material_id} already has {existing_event_count} imported events. \
                 Rerun with --resume to continue from the recorded row offset instead of duplicating history."
            ));
        }

        let mut importer = if self.resume {
            // Check if material already registered
            let existing = pool
                .source_materials()
                .get_by_id(Id::from_uuid(deterministic_material_id))
                .await
                .ok()
                .flatten();

            if existing.is_some() {
                println!("Resuming import (material_id={deterministic_material_id})");
                HistoricalImporter::resume(&pool, deterministic_material_id)
            } else {
                HistoricalImporter::register(
                    &pool,
                    &source_path,
                    "sqlite-database",
                    json!({
                        "application": "atuin",
                        "total_rows": total_rows,
                    }),
                )
                .await?
            }
        } else {
            HistoricalImporter::register(
                &pool,
                &source_path,
                "sqlite-database",
                json!({
                    "application": "atuin",
                    "total_rows": total_rows,
                }),
            )
            .await?
        };

        let material_id = importer.material_id;
        let material_id_typed: Id<sinex_primitives::events::SourceMaterial> =
            Id::from_uuid(material_id);

        // Determine resume offset: count events already imported from this material
        let resume_offset: i64 = if self.resume {
            sqlx::query_scalar(
                "SELECT COALESCE(MAX(offset_end), 0) \
                 FROM core.events \
                 WHERE source_material_id = $1 AND offset_kind = 'row'",
            )
            .bind(material_id)
            .fetch_one(&pool)
            .await
            .unwrap_or(0)
        } else {
            0
        };

        if resume_offset > 0 {
            println!("Skipping {resume_offset} already-imported rows");
        }

        let started = Instant::now();
        let source = EventSource::from_static("shell.atuin");
        let event_type = EventType::from_static("command.executed");

        // Read and import in batches
        let mut stmt = sqlite
            .prepare(
                "SELECT id, timestamp, duration, exit, command, cwd, session, hostname \
                 FROM history \
                 WHERE deleted_at IS NULL \
                 ORDER BY timestamp ASC, ROWID ASC \
                 LIMIT -1 OFFSET ?1",
            )
            .map_err(|e| color_eyre::eyre::eyre!("Failed to prepare SQLite query: {e}"))?;

        let rows = stmt
            .query_map([resume_offset], |row| {
                Ok(AtuinRow {
                    id: row.get(0)?,
                    timestamp: row.get(1)?,
                    duration: row.get(2)?,
                    exit: row.get(3)?,
                    command: row.get(4)?,
                    cwd: row.get(5)?,
                    session: row.get(6)?,
                    hostname: row.get(7)?,
                })
            })
            .map_err(|e| color_eyre::eyre::eyre!("Failed to query Atuin history: {e}"))?;

        let import_result: Result<(u64, u64, std::time::Duration)> = async {
            let mut batch: Vec<StreamBatchRow> = Vec::with_capacity(self.batch_size);
            let mut row_index: i64 = resume_offset;
            let mut total_submitted: u64 = 0;

            for row_result in rows {
                let row = row_result
                    .map_err(|e| color_eyre::eyre::eyre!("Failed to read Atuin row: {e}"))?;

                let payload = match AtuinCommandExecutedPayload::from_raw_history(
                    row.command,
                    RecordedPath::from(row.cwd),
                    row.exit,
                    row.duration,
                    row.id,
                    row.session,
                    row.timestamp,
                    row.hostname,
                ) {
                    Ok(payload) => payload,
                    Err(error) => {
                        importer.quarantine_row(
                            Some(row_index),
                            &format!("invalid Atuin row: {error}"),
                        );
                        row_index += 1;
                        continue;
                    }
                };

                let host = payload.hostname.clone();
                let ts_orig = payload.ts_start_orig;
                let payload = serde_json::to_value(payload)
                    .map_err(|e| color_eyre::eyre::eyre!("Failed to serialize Atuin payload: {e}"))?;

                let batch_row = StreamBatchRow {
                    id: Uuid::now_v7(),
                    source: source.clone(),
                    event_type: event_type.clone(),
                    ts_orig,
                    host,
                    payload,
                    source_material_id: Some(material_id_typed.clone()),
                    anchor_byte: Some(row_index),
                    offset_start: Some(row_index),
                    offset_end: Some(row_index + 1),
                    offset_kind: Some("row".to_string()),
                    source_event_ids: None,
                    payload_schema_id: None,
                    node_run_id: None,
                    associated_blob_ids: None,
                    temporal_policy: None,
                    semantics_version: None,
                    scope_key: None,
                    equivalence_key: None,
                    created_by_operation_id: None,
                    node_model: None,
                };

                batch.push(batch_row);
                row_index += 1;

                if batch.len() >= self.batch_size {
                    let submitted = importer.submit_batch(std::mem::take(&mut batch)).await
                        .map_err(|e| color_eyre::eyre::eyre!("Batch submit failed: {e}"))?;
                    total_submitted += submitted;
                    batch = Vec::with_capacity(self.batch_size);

                    if total_submitted % 5000 < self.batch_size as u64 {
                        let elapsed = started.elapsed().as_secs_f64();
                        let rate = total_submitted as f64 / elapsed;
                        let remaining = (total_rows - resume_offset) as u64 - total_submitted;
                        let eta = remaining as f64 / rate;
                        println!(
                            "  [{total_submitted}/{} events] {:.0} events/sec, ETA {:.0}s",
                            total_rows - resume_offset,
                            rate,
                            eta
                        );
                    }
                }
            }

            if !batch.is_empty() {
                importer.submit_batch(batch).await
                    .map_err(|e| color_eyre::eyre::eyre!("Final batch submit failed: {e}"))?;
            }

            importer.finalize(None).await
                .map_err(|e| color_eyre::eyre::eyre!("Failed to finalize import: {e}"))?;

            Ok((
                importer.events_processed(),
                importer.rows_quarantined(),
                started.elapsed(),
            ))
        }
        .await;

        let (events_processed, rows_quarantined, elapsed) = match import_result {
            Ok(stats) => stats,
            Err(error) => {
                if let Err(mark_error) = importer.fail(&error.to_string()).await {
                    return Err(color_eyre::eyre::eyre!(
                        "{error}\n(additionally failed to mark material {material_id} as failed: {mark_error})"
                    ));
                }
                return Err(error);
            }
        };

        let rate = events_processed as f64 / elapsed.as_secs_f64();

        println!();
        println!("Import complete:");
        println!("  Imported:    {events_processed} events");
        println!("  Quarantined: {rows_quarantined} events");
        println!("  Duration:    {:.2}s", elapsed.as_secs_f64());
        println!("  Rate:        {:.0} events/sec", rate);
        println!("  Material ID: {material_id}");

        Ok(())
    }
}

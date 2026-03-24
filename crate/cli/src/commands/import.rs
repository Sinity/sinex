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

const ATUIN_RESUME_CURSOR_SQL: &str =
    "SELECT COALESCE(MAX(anchor_byte), 0) FROM core.events \
     WHERE source_material_id = $1 AND offset_kind = 'rowid'";
const ATUIN_LEGACY_RESUME_CURSOR_SQL: &str =
    "SELECT COALESCE(MAX(offset_end), 0) FROM core.events \
     WHERE source_material_id = $1 AND offset_kind = 'row'";
const ATUIN_HISTORY_SELECT_SQL: &str =
    "SELECT ROWID, id, timestamp, duration, exit, command, cwd, session, hostname \
     FROM history \
     WHERE deleted_at IS NULL AND ROWID > ?1 \
     ORDER BY ROWID ASC";

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
    #[arg(long, default_value = "1000", value_parser = parse_batch_size)]
    pub batch_size: usize,

    /// Resume from last imported row (skip already-imported events)
    #[arg(long)]
    pub resume: bool,
}

fn default_atuin_db_path() -> PathBuf {
    default_atuin_data_dir(dirs::data_local_dir(), dirs::home_dir())
        .join("atuin/history.db")
}

fn parse_batch_size(value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|error| format!("invalid batch size '{value}': {error}"))?;
    if parsed == 0 {
        return Err("batch size must be at least 1".to_string());
    }
    Ok(parsed)
}

fn default_atuin_data_dir(data_local_dir: Option<PathBuf>, home_dir: Option<PathBuf>) -> PathBuf {
    data_local_dir
        .or_else(|| home_dir.map(|home| home.join(".local/share")))
        .unwrap_or_else(|| PathBuf::from(".local/share"))
}

fn resolve_atuin_resume_row_id(rowid_cursor: i64, legacy_row_cursor: i64) -> Result<i64> {
    if rowid_cursor > 0 {
        return Ok(rowid_cursor);
    }
    if legacy_row_cursor > 0 {
        return Err(color_eyre::eyre::eyre!(
            "Cannot safely resume Atuin material imported with legacy row-index offsets. \
             Reset the existing material and re-import so progress is tracked by SQLite ROWID."
        ));
    }
    Ok(0)
}

/// A single row from the Atuin SQLite history table.
struct AtuinRow {
    row_id: i64,
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

        let db_path = std::fs::canonicalize(&self.db_path).map_err(|e| {
            color_eyre::eyre::eyre!(
                "Failed to canonicalize Atuin database path {}: {e}",
                self.db_path.display()
            )
        })?;

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
            &db_path,
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

        println!("Atuin database: {}", db_path.display());
        println!("Total rows: {total_rows}");
        println!("Batch size: {}", self.batch_size);

        if total_rows == 0 {
            println!("Nothing to import.");
            return Ok(());
        }

        // Register source material via HistoricalImporter
        let source_path = db_path.to_string_lossy();
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

        let imported_event_count = if self.resume { existing_event_count as u64 } else { 0 };

        // Determine resume cursor from the last persisted SQLite ROWID.
        let resume_row_id: i64 = if self.resume {
            let rowid_cursor = sqlx::query_scalar(ATUIN_RESUME_CURSOR_SQL)
                .bind(material_id)
                .fetch_one(&pool)
                .await
                .unwrap_or(0);
            let legacy_row_cursor = sqlx::query_scalar(ATUIN_LEGACY_RESUME_CURSOR_SQL)
                .bind(material_id)
                .fetch_one(&pool)
                .await
                .unwrap_or(0);
            resolve_atuin_resume_row_id(rowid_cursor, legacy_row_cursor)?
        } else {
            0
        };

        if resume_row_id > 0 {
            println!(
                "Resuming after Atuin ROWID {resume_row_id} ({imported_event_count} events already imported)"
            );
        }

        let started = Instant::now();
        let source = EventSource::from_static("shell.atuin");
        let event_type = EventType::from_static("command.executed");

        // Read and import in batches
        let mut stmt = sqlite
            .prepare(ATUIN_HISTORY_SELECT_SQL)
            .map_err(|e| color_eyre::eyre::eyre!("Failed to prepare SQLite query: {e}"))?;

        let rows = stmt
            .query_map([resume_row_id], |row| {
                Ok(AtuinRow {
                    row_id: row.get(0)?,
                    id: row.get(1)?,
                    timestamp: row.get(2)?,
                    duration: row.get(3)?,
                    exit: row.get(4)?,
                    command: row.get(5)?,
                    cwd: row.get(6)?,
                    session: row.get(7)?,
                    hostname: row.get(8)?,
                })
            })
            .map_err(|e| color_eyre::eyre::eyre!("Failed to query Atuin history: {e}"))?;

        let import_result: Result<(u64, u64, std::time::Duration)> = async {
            let mut batch: Vec<StreamBatchRow> = Vec::with_capacity(self.batch_size);
            let mut total_submitted: u64 = 0;
            let rows_remaining_for_progress =
                (total_rows as u64).saturating_sub(imported_event_count);

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
                            Some(row.row_id),
                            &format!("invalid Atuin row: {error}"),
                        );
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
                    anchor_byte: Some(row.row_id),
                    offset_start: Some(row.row_id),
                    offset_end: Some(row.row_id + 1),
                    offset_kind: Some("rowid".to_string()),
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

                if batch.len() >= self.batch_size {
                    let submitted = importer.submit_batch(std::mem::take(&mut batch)).await
                        .map_err(|e| color_eyre::eyre::eyre!("Batch submit failed: {e}"))?;
                    total_submitted += submitted;
                    batch = Vec::with_capacity(self.batch_size);

                    if total_submitted % 5000 < self.batch_size as u64 {
                        let elapsed = started.elapsed().as_secs_f64();
                        let rate = total_submitted as f64 / elapsed;
                        let remaining = rows_remaining_for_progress.saturating_sub(total_submitted);
                        let eta = remaining as f64 / rate;
                        println!(
                            "  [{total_submitted}/{} events] {:.0} events/sec, ETA {:.0}s",
                            rows_remaining_for_progress,
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
        let partial = rows_quarantined > 0;

        println!();
        if partial {
            println!("Import completed with quarantined rows:");
            println!("  Status:      recovered_partial");
        } else {
            println!("Import complete:");
        }
        println!("  Imported:    {events_processed} events");
        println!("  Quarantined: {rows_quarantined} events");
        println!("  Duration:    {:.2}s", elapsed.as_secs_f64());
        println!("  Rate:        {:.0} events/sec", rate);
        println!("  Material ID: {material_id}");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn test_default_atuin_data_dir_falls_back_under_home() -> xtask::sandbox::TestResult<()> {
        let path = default_atuin_data_dir(None, Some(PathBuf::from("/home/test")));
        assert_eq!(path, PathBuf::from("/home/test/.local/share"));
        Ok(())
    }

    #[sinex_test]
    async fn test_atuin_resume_cursor_uses_rowid_anchor() -> xtask::sandbox::TestResult<()> {
        assert!(ATUIN_RESUME_CURSOR_SQL.contains("MAX(anchor_byte)"));
        assert!(ATUIN_RESUME_CURSOR_SQL.contains("offset_kind = 'rowid'"));
        assert!(ATUIN_LEGACY_RESUME_CURSOR_SQL.contains("offset_kind = 'row'"));
        assert!(ATUIN_HISTORY_SELECT_SQL.contains("SELECT ROWID"));
        assert!(ATUIN_HISTORY_SELECT_SQL.contains("ROWID > ?1"));
        Ok(())
    }

    #[sinex_test]
    async fn test_resolve_atuin_resume_row_id_rejects_legacy_row_cursor(
    ) -> xtask::sandbox::TestResult<()> {
        let error =
            resolve_atuin_resume_row_id(0, 42).expect_err("legacy row cursor must fail honestly");
        assert!(error
            .to_string()
            .contains("legacy row-index offsets"));
        Ok(())
    }
}

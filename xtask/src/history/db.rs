//! `SQLite` database operations for xtask history.

use color_eyre::eyre::{Result, WrapErr};

/// Opening half of the package-scoped supersession CTE used by diagnostic queries.
///
/// Finds the most recent successful/failed invocation that compiled each package.
/// An optional `AND i.command = ?N` clause may be injected before appending
/// `LATEST_PER_PACKAGE_CTE_CLOSE`.
const LATEST_PER_PACKAGE_CTE_OPEN: &str = "
    WITH latest_per_package AS (
        SELECT ip.package, MAX(i.id) as latest_inv_id
        FROM invocation_packages ip
        JOIN invocations i ON ip.invocation_id = i.id
        WHERE i.status IN ('success', 'failed')
";

/// Closing half of the package-scoped supersession CTE (GROUP BY + closing paren).
const LATEST_PER_PACKAGE_CTE_CLOSE: &str = "
        GROUP BY ip.package
    )
";
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sinex_primitives::temporal::Timestamp;
use std::collections::HashSet;
use std::path::Path;
use time::OffsetDateTime;

const HISTORY_DB_SCHEMA_VERSION: i32 = 3;

/// Status of a command invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InvocationStatus {
    Running,
    Success,
    Failed,
    Cancelled,
}

impl InvocationStatus {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Success => "success",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub(crate) fn try_from_str(s: &str) -> Result<Self> {
        match s {
            "running" => Ok(Self::Running),
            "success" => Ok(Self::Success),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(color_eyre::eyre::eyre!(
                "invalid invocation status in history DB: {s}"
            )),
        }
    }
}

/// Process lifecycle status for background jobs (separate from invocation success/failure).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobLifecycleStatus {
    Running,
    Completed,
    Orphaned,
    Killed,
}

impl JobLifecycleStatus {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Orphaned => "orphaned",
            Self::Killed => "killed",
        }
    }

    pub(crate) fn try_from_str(s: &str) -> Result<Self> {
        match s {
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "orphaned" => Ok(Self::Orphaned),
            "killed" => Ok(Self::Killed),
            _ => Err(color_eyre::eyre::eyre!("invalid job lifecycle status: {s}")),
        }
    }

    pub(crate) fn is_terminal(&self) -> bool {
        !matches!(self, Self::Running)
    }
}

pub(crate) fn parse_stored_invocation_status(
    status_str: String,
) -> rusqlite::Result<InvocationStatus> {
    InvocationStatus::try_from_str(&status_str).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid invocation status in history DB: {status_str}"),
            )),
        )
    })
}

/// A recorded command invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Invocation {
    pub id: i64,
    pub command: String,
    pub subcommand: Option<String>,
    pub profile: Option<String>,
    pub args_json: Option<String>,
    pub git_commit: Option<String>,
    pub git_dirty: bool,
    pub started_at: OffsetDateTime,
    pub finished_at: Option<OffsetDateTime>,
    pub duration_secs: Option<f64>,
    pub exit_code: Option<i32>,
    pub status: InvocationStatus,
    pub host: String,
    pub cwd: String,
    /// Currently executing pipeline stage (NULL when idle or finished).
    pub live_stage: Option<String>,
}

/// Emitted once per process (via `OnceLock`) when a read command accesses synthetic data.
static SYNTHETIC_WARNING_EMITTED: std::sync::OnceLock<()> = std::sync::OnceLock::new();

/// Handle to the history `SQLite` database.
pub struct HistoryDb {
    pub(super) conn: Connection,
    /// True if the database contains synthetic (seeded) data.
    pub is_synthetic: bool,
}

impl HistoryDb {
    /// Open or create the history database at the given path.
    pub fn open(path: &Path) -> Result<Self> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                let parent_display = parent.display();
                format!("failed to create directory: {parent_display}")
            })?;
        }

        // Detect and recover from corrupted (0-byte) database files.
        // SQLite treats a 0-byte file as valid (empty DB) but our WAL/schema
        // setup may leave it in an inconsistent state. Delete and recreate.
        if path.exists()
            && let Ok(meta) = std::fs::metadata(path)
            && meta.len() == 0
        {
            eprintln!(
                "⚠️  History database at {} is empty (0 bytes), recreating",
                path.display()
            );
            let _ = std::fs::remove_file(path);
        }

        let conn = Connection::open(path).with_context(|| {
            let path_display = path.display();
            format!("failed to open history database: {path_display}")
        })?;

        // WAL mode enables concurrent readers during writes (critical for
        // querying test history while a test run is in progress).
        // busy_timeout prevents SQLITE_BUSY on concurrent access from parallel xtask processes.
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA busy_timeout=5000;",
        )?;

        // Verify database integrity on open. If corrupted, delete and recreate.
        let integrity_ok = conn
            .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
            .is_ok_and(|result| result == "ok");
        if !integrity_ok {
            drop(conn);
            eprintln!(
                "⚠️  History database at {} failed integrity check, recreating",
                path.display()
            );
            let _ = std::fs::remove_file(path);
            // Remove WAL and SHM files too
            let wal_path = path.with_extension("db-wal");
            let shm_path = path.with_extension("db-shm");
            let _ = std::fs::remove_file(&wal_path);
            let _ = std::fs::remove_file(&shm_path);
            let conn = Connection::open(path).with_context(|| {
                format!("failed to recreate history database: {}", path.display())
            })?;
            conn.execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA busy_timeout=5000;",
            )?;
            let db = Self {
                conn,
                is_synthetic: false,
            };
            db.init_schema()?;
            db.set_schema_version(HISTORY_DB_SCHEMA_VERSION)?;
            return Ok(db);
        }

        let mut db = Self {
            conn,
            is_synthetic: false,
        };
        if db.schema_version()? < HISTORY_DB_SCHEMA_VERSION {
            db.init_schema()?;
            db.set_schema_version(HISTORY_DB_SCHEMA_VERSION)?;
        }
        db.is_synthetic = db.check_synthetic()?;
        db.cleanup_stale_invocations();
        Ok(db)
    }

    fn schema_version(&self) -> Result<i32> {
        self.conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .context("failed to read history DB schema version")
    }

    fn set_schema_version(&self, version: i32) -> Result<()> {
        self.conn
            .execute_batch(&format!("PRAGMA user_version = {version};"))
            .context("failed to persist history DB schema version")
    }

    /// Initialize the database schema.
    fn init_schema(&self) -> Result<()> {
        self.conn.execute_batch(
            r"
            -- Command invocations
            CREATE TABLE IF NOT EXISTS invocations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                command TEXT NOT NULL,
                subcommand TEXT,
                profile TEXT,
                args_json TEXT,
                git_commit TEXT,
                git_dirty INTEGER DEFAULT 0,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                duration_secs REAL,
                exit_code INTEGER,
                status TEXT NOT NULL DEFAULT 'running',
                host TEXT NOT NULL,
                cwd TEXT NOT NULL,
                pid INTEGER,
                is_background INTEGER DEFAULT 0,
                stdout_path TEXT,
                stderr_path TEXT,
                stdout_content TEXT,
                stderr_content TEXT,
                cpu_usage_avg REAL,
                memory_usage_max_mb REAL,
                tree_fingerprint TEXT,
                scope_key TEXT,
                test_total INTEGER,
                test_passed INTEGER,
                test_failed INTEGER,
                test_ignored INTEGER,
                test_completed INTEGER,
                test_last_name TEXT,
                test_progress_updated_at TEXT
            );

            -- Test results (per-test granularity)
            CREATE TABLE IF NOT EXISTS test_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER NOT NULL REFERENCES invocations(id) ON DELETE CASCADE,
                test_name TEXT NOT NULL,
                package TEXT NOT NULL,
                status TEXT NOT NULL,
                duration_secs REAL,
                attempt INTEGER DEFAULT 1,
                output TEXT,
                slot_name TEXT,
                slot_wait_ms INTEGER,
                cleanup_ms INTEGER,
                failure_message TEXT,
                failure_type TEXT,
                UNIQUE(invocation_id, test_name, attempt)
            );

            -- Build diagnostics (compiler errors/warnings)
            CREATE TABLE IF NOT EXISTS build_diagnostics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER NOT NULL REFERENCES invocations(id) ON DELETE CASCADE,
                level TEXT NOT NULL,
                code TEXT,
                message TEXT NOT NULL,
                file_path TEXT,
                line INTEGER,
                col INTEGER,
                rendered TEXT,
                package TEXT,
                fix_replacement TEXT,
                fix_applicability TEXT,
                fix_byte_start INTEGER,
                fix_byte_end INTEGER
            );

            -- Tracks which packages were compiled in each invocation (for package-scoped supersession)
            CREATE TABLE IF NOT EXISTS invocation_packages (
                invocation_id INTEGER NOT NULL REFERENCES invocations(id) ON DELETE CASCADE,
                package TEXT NOT NULL,
                PRIMARY KEY (invocation_id, package)
            );

            -- Per-stage timing within a command invocation (fmt, clippy, forbidden, compile, preflight)
            CREATE TABLE IF NOT EXISTS stage_timings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER NOT NULL REFERENCES invocations(id) ON DELETE CASCADE,
                stage_name TEXT NOT NULL,
                started_at TEXT NOT NULL,
                duration_secs REAL NOT NULL,
                success INTEGER NOT NULL DEFAULT 1
            );

            -- Unified invocation progress (phase tracking for long-running commands)
            CREATE TABLE IF NOT EXISTS invocation_progress (
                invocation_id INTEGER PRIMARY KEY REFERENCES invocations(id) ON DELETE CASCADE,
                phase TEXT,
                step TEXT,
                pct_done REAL,
                items_done INTEGER,
                items_total INTEGER,
                updated_at TEXT NOT NULL
            );

            -- ETA samples: persisted timing observations per (command, phase)
            CREATE TABLE IF NOT EXISTS invocation_eta_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER NOT NULL REFERENCES invocations(id) ON DELETE CASCADE,
                command TEXT NOT NULL,
                phase TEXT NOT NULL,
                duration_secs REAL NOT NULL,
                sampled_at TEXT NOT NULL
            );

            -- Structured trace events from HistoryTracingLayer (ERROR/WARN always; INFO from
            -- coordinator, preflight, cargo). DEBUG/TRACE are never persisted.
            CREATE TABLE IF NOT EXISTS trace_events (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE CASCADE,
                ts            TEXT    NOT NULL,
                level         TEXT    NOT NULL,
                target        TEXT    NOT NULL,
                event_kind    TEXT,
                message       TEXT    NOT NULL,
                fields        TEXT
            );

            -- Live stage column (nullable, shows currently executing pipeline stage)
            -- Added via ALTER TABLE for forward-compat with existing databases

            -- Background job process handles (operational, ephemeral)
            CREATE TABLE IF NOT EXISTS background_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE SET NULL,
                command TEXT NOT NULL,
                args_json TEXT,
                pid INTEGER,
                stdout_path TEXT,
                stderr_path TEXT,
                job_status TEXT NOT NULL DEFAULT 'running',
                exit_code INTEGER,
                started_at TEXT NOT NULL,
                finished_at TEXT
            );

            -- Durable archived stdout/stderr for completed background jobs
            CREATE TABLE IF NOT EXISTS background_job_logs (
                job_id INTEGER PRIMARY KEY REFERENCES background_jobs(id) ON DELETE CASCADE,
                stdout_content TEXT,
                stderr_content TEXT
            );
            ",
        )?;
        // SQLite doesn't support ADD COLUMN IF NOT EXISTS before 3.37.
        // Execute separately so it can be ignored if the column already exists.
        let _ = self
            .conn
            .execute_batch("ALTER TABLE invocations ADD COLUMN live_stage TEXT;");
        // L3: test_mode column for VM/bench/fuzz extensibility (default 'nextest').
        let _ = self
            .conn
            .execute_batch("ALTER TABLE test_results ADD COLUMN test_mode TEXT DEFAULT 'nextest';");
        // G3: fix session tracking — pre-fix diagnostic snapshot stored on each invocation.
        let _ = self
            .conn
            .execute_batch("ALTER TABLE invocations ADD COLUMN pre_fix_errors INTEGER;");
        let _ = self
            .conn
            .execute_batch("ALTER TABLE invocations ADD COLUMN pre_fix_warnings INTEGER;");
        let _ = self
            .conn
            .execute_batch("ALTER TABLE invocations ADD COLUMN pre_fix_fixable INTEGER;");
        // Phase 3: launch_mode distinguishes foreground vs background invocations.
        let _ = self
            .conn
            .execute_batch("ALTER TABLE invocations ADD COLUMN launch_mode TEXT DEFAULT 'foreground';"
        );
        self.conn.execute_batch(
            r"
            -- Indices for common queries
            CREATE INDEX IF NOT EXISTS idx_invocations_command ON invocations(command);
            CREATE INDEX IF NOT EXISTS idx_invocations_started ON invocations(started_at);
            CREATE INDEX IF NOT EXISTS idx_invocations_status ON invocations(status);
            -- Composite index for the most common query pattern (status --summary, history stats)
            CREATE INDEX IF NOT EXISTS idx_invocations_command_status_started
                ON invocations(command, status, started_at);
            CREATE INDEX IF NOT EXISTS idx_invocations_background
                ON invocations(is_background, status)
                WHERE is_background = 1;
            CREATE INDEX IF NOT EXISTS idx_invocations_fingerprint
                ON invocations(command, tree_fingerprint, scope_key);
            CREATE INDEX IF NOT EXISTS idx_test_results_name ON test_results(test_name);
            CREATE INDEX IF NOT EXISTS idx_test_results_status ON test_results(status);
            CREATE INDEX IF NOT EXISTS idx_test_results_invocation ON test_results(invocation_id);
            -- Dedup before creating the unique index: keep lowest rowid per identity tuple.
            -- Safe to run repeatedly; no-op when no duplicates exist.
            DELETE FROM build_diagnostics WHERE rowid NOT IN (
                SELECT MIN(rowid) FROM build_diagnostics
                GROUP BY
                    invocation_id,
                    level,
                    COALESCE(code, ''),
                    message,
                    COALESCE(file_path, ''),
                    COALESCE(line, -1),
                    COALESCE(col, -1),
                    COALESCE(rendered, ''),
                    COALESCE(package, ''),
                    COALESCE(fix_replacement, ''),
                    COALESCE(fix_applicability, ''),
                    COALESCE(fix_byte_start, -1),
                    COALESCE(fix_byte_end, -1)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_build_diagnostics_identity
                ON build_diagnostics(
                    invocation_id,
                    level,
                    COALESCE(code, ''),
                    message,
                    COALESCE(file_path, ''),
                    COALESCE(line, -1),
                    COALESCE(col, -1),
                    COALESCE(rendered, ''),
                    COALESCE(package, ''),
                    COALESCE(fix_replacement, ''),
                    COALESCE(fix_applicability, ''),
                    COALESCE(fix_byte_start, -1),
                    COALESCE(fix_byte_end, -1)
                );
            CREATE INDEX IF NOT EXISTS idx_diagnostics_invocation ON build_diagnostics(invocation_id);
            CREATE INDEX IF NOT EXISTS idx_stage_timings_invocation ON stage_timings(invocation_id);
            CREATE INDEX IF NOT EXISTS trace_events_invocation_idx  ON trace_events(invocation_id);
            CREATE INDEX IF NOT EXISTS trace_events_level_idx       ON trace_events(level);
            CREATE INDEX IF NOT EXISTS trace_events_event_kind_idx  ON trace_events(event_kind);
            CREATE INDEX IF NOT EXISTS trace_events_ts_idx          ON trace_events(ts);
            CREATE INDEX IF NOT EXISTS idx_background_jobs_status     ON background_jobs(job_status);
            CREATE INDEX IF NOT EXISTS idx_background_jobs_started    ON background_jobs(started_at);
            CREATE INDEX IF NOT EXISTS idx_background_jobs_invocation ON background_jobs(invocation_id);
            CREATE INDEX IF NOT EXISTS idx_eta_samples_command_phase ON invocation_eta_samples(command, phase);
            CREATE INDEX IF NOT EXISTS idx_invocation_progress_invocation ON invocation_progress(invocation_id);
            ",
        )?;
        // Metadata table: used to mark synthetic (seeded) databases.
        // Isolated from the main batch above so it can be ignored if already exists.
        let _ = self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT);",
        );
        Ok(())
    }

    /// Check whether this database contains synthetic (seeded) data.
    pub fn check_synthetic(&self) -> Result<bool> {
        let exists: bool = self
            .conn
            .query_row(
                "SELECT 1 FROM metadata WHERE key = 'synthetic' AND value = 'true' LIMIT 1",
                [],
                |_| Ok(true),
            )
            .unwrap_or(false);
        Ok(exists)
    }

    /// Mark the database as containing synthetic data.
    pub fn set_synthetic(&self) -> Result<()> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO metadata (key, value) VALUES ('synthetic', 'true')",
                [],
            )
            .context("failed to set synthetic marker")?;
        Ok(())
    }

    /// Clear the synthetic marker (called on first real `start_invocation`).
    fn clear_synthetic(&self) {
        let _ = self
            .conn
            .execute("DELETE FROM metadata WHERE key = 'synthetic'", []);
    }

    /// Print a one-time-per-process warning if this database is synthetic.
    ///
    /// Suppressed when `XTASK_SYNTHETIC_HISTORY=allow` is set (exercises use this).
    pub fn warn_if_synthetic(&self, path: &std::path::Path) {
        if !self.is_synthetic {
            return;
        }
        if std::env::var_os("XTASK_SYNTHETIC_HISTORY").as_deref()
            == Some(std::ffi::OsStr::new("allow"))
        {
            return;
        }
        SYNTHETIC_WARNING_EMITTED.get_or_init(|| {
            eprintln!(
                "\nWARNING: History database contains synthetic (seeded) data.\n  \
                Database: {}\n  \
                Seeded by: xtask exercise --seed or xtask history seed\n\n  \
                Results from history commands reflect fabricated data, not real usage.\n  \
                To start fresh: xtask reset --yes --history\n  \
                To suppress:    XTASK_SYNTHETIC_HISTORY=allow\n",
                path.display()
            );
        });
    }

    /// Start a new invocation record. Returns the invocation ID.
    pub fn start_invocation(
        &self,
        command: &str,
        subcommand: Option<&str>,
        profile: Option<&str>,
        args_json: Option<&str>,
    ) -> Result<i64> {
        let git_commit = get_git_commit();
        let git_dirty = is_git_dirty();
        let host = crate::config::config().hostname.clone();
        let cwd = std::env::current_dir()
            .map(|p| p.display().to_string())
            .unwrap_or_default();
        let started_at = Timestamp::now().format_rfc3339();

        // Transition from synthetic to real: clear the marker on first real write.
        if self.is_synthetic {
            self.clear_synthetic();
        }

        self.conn.execute(
            r"
            INSERT INTO invocations (command, subcommand, profile, args_json, git_commit, git_dirty, started_at, host, cwd, status)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'running')
            ",
            params![command, subcommand, profile, args_json, git_commit, git_dirty, started_at, host, cwd],
        )?;

        Ok(self.conn.last_insert_rowid())
    }

    /// Finish an invocation with the given status and exit code.
    pub fn finish_invocation(
        &self,
        id: i64,
        status: InvocationStatus,
        exit_code: Option<i32>,
        duration_secs: f64,
    ) -> Result<()> {
        let finished_at = Timestamp::now().format_rfc3339();

        self.conn.execute(
            r"
            UPDATE invocations
            SET finished_at = ?1, duration_secs = ?2, exit_code = ?3, status = ?4
            WHERE id = ?5
            ",
            params![finished_at, duration_secs, exit_code, status.as_str(), id],
        )?;

        Ok(())
    }

    /// Record timing for a pipeline stage (fmt, clippy, forbidden, compile, preflight).
    pub fn record_stage_timing(
        &self,
        invocation_id: i64,
        stage_name: &str,
        started_at: &str,
        duration_secs: f64,
        success: bool,
    ) -> Result<()> {
        self.conn.execute(
            r"
            INSERT INTO stage_timings (invocation_id, stage_name, started_at, duration_secs, success)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ",
            params![invocation_id, stage_name, started_at, duration_secs, i32::from(success)],
        )?;
        Ok(())
    }

    /// Set the currently executing pipeline stage for an in-flight invocation.
    ///
    /// This is written at `start_stage()` time and cleared at `finish_stage()` time,
    /// giving real-time visibility into what a running background job is doing.
    pub fn set_live_stage(&self, invocation_id: i64, stage: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE invocations SET live_stage = ?1 WHERE id = ?2",
            params![stage, invocation_id],
        )?;
        Ok(())
    }

    /// Clear the live stage field (called when a stage finishes).
    pub fn clear_live_stage(&self, invocation_id: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE invocations SET live_stage = NULL WHERE id = ?1",
            params![invocation_id],
        )?;
        Ok(())
    }

    /// Get the currently executing stage for a running invocation.
    pub fn get_live_stage(&self, invocation_id: i64) -> Result<Option<String>> {
        let stage = self
            .conn
            .query_row(
                "SELECT live_stage FROM invocations WHERE id = ?1",
                params![invocation_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(stage)
    }

    /// Get all recorded stage timings for an invocation, ordered by start time.
    pub fn get_stage_timings_for_invocation(&self, invocation_id: i64) -> Result<Vec<StageTiming>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT invocation_id, stage_name, started_at, duration_secs, success
            FROM stage_timings
            WHERE invocation_id = ?1
            ORDER BY started_at ASC
            ",
        )?;
        let rows = stmt
            .query_map(params![invocation_id], |row| {
                Ok(StageTiming {
                    invocation_id: row.get(0)?,
                    stage_name: row.get(1)?,
                    started_at: row.get(2)?,
                    duration_secs: row.get(3)?,
                    success: row.get::<_, i32>(4)? != 0,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Write or update the live progress snapshot for an invocation.
    ///
    /// Called by CommandContext::report_progress() on stage transitions and
    /// incremental progress updates (e.g. nextest test count changes).
    pub fn write_progress(
        &self,
        invocation_id: i64,
        phase: Option<&str>,
        step: Option<&str>,
        pct_done: Option<f64>,
        items_done: Option<i64>,
        items_total: Option<i64>,
    ) -> Result<()> {
        let updated_at = Timestamp::now().format_rfc3339();
        self.conn.execute(
            r"INSERT INTO invocation_progress
                  (invocation_id, phase, step, pct_done, items_done, items_total, updated_at)
              VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
              ON CONFLICT(invocation_id) DO UPDATE SET
                  phase = excluded.phase,
                  step = excluded.step,
                  pct_done = excluded.pct_done,
                  items_done = excluded.items_done,
                  items_total = excluded.items_total,
                  updated_at = excluded.updated_at",
            params![
                invocation_id,
                phase,
                step,
                pct_done,
                items_done,
                items_total,
                updated_at
            ],
        )?;
        Ok(())
    }

    /// Get the current progress snapshot for an invocation.
    pub fn get_progress(&self, invocation_id: i64) -> Result<Option<InvocationProgress>> {
        self.conn
            .query_row(
                r"SELECT invocation_id, phase, step, pct_done, items_done, items_total, updated_at
                  FROM invocation_progress WHERE invocation_id = ?1",
                params![invocation_id],
                |row| {
                    Ok(InvocationProgress {
                        invocation_id: row.get(0)?,
                        phase: row.get(1)?,
                        step: row.get(2)?,
                        pct_done: row.get(3)?,
                        items_done: row.get(4)?,
                        items_total: row.get(5)?,
                        updated_at: row.get(6)?,
                    })
                },
            )
            .optional()
            .context("failed to get invocation progress")
    }

    /// Record an ETA sample for a (command, phase) pair.
    ///
    /// Called by CommandContext::finish_stage() to accumulate timing data
    /// for future ETA estimates.
    pub fn record_eta_sample(
        &self,
        invocation_id: i64,
        command: &str,
        phase: &str,
        duration_secs: f64,
    ) -> Result<()> {
        let sampled_at = Timestamp::now().format_rfc3339();
        self.conn.execute(
            r"INSERT INTO invocation_eta_samples (invocation_id, command, phase, duration_secs, sampled_at)
              VALUES (?1, ?2, ?3, ?4, ?5)",
            params![invocation_id, command, phase, duration_secs, sampled_at],
        )?;
        Ok(())
    }

    /// Get the median duration for a (command, phase) pair over the last N samples.
    ///
    /// Returns None if fewer than 3 samples exist (insufficient data for a reliable estimate).
    pub fn get_eta_estimate(
        &self,
        command: &str,
        phase: &str,
        window: usize,
    ) -> Result<Option<f64>> {
        let limit = window.max(3);
        let mut stmt = self.conn.prepare(
            r"SELECT duration_secs FROM invocation_eta_samples
              WHERE command = ?1 AND phase = ?2
              ORDER BY sampled_at DESC
              LIMIT ?3",
        )?;
        let samples: Vec<f64> = stmt
            .query_map(params![command, phase, limit as i64], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?;

        if samples.len() < 3 {
            return Ok(None);
        }

        // Median: sort and take middle value
        let mut sorted = samples.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let mid = sorted.len() / 2;
        Ok(Some(sorted[mid]))
    }

    /// Prune ETA samples older than N days to keep the table small.
    pub fn prune_eta_samples(&self, older_than_days: u32) -> Result<usize> {
        let count = self.conn.execute(
            r"DELETE FROM invocation_eta_samples
              WHERE sampled_at < strftime('%Y-%m-%dT%H:%M:%SZ', 'now', ?1)",
            params![format!("-{older_than_days} days")],
        )?;
        Ok(count)
    }

    /// Mark invocations stuck in 'running' for over 10 minutes as 'cancelled'.
    ///
    /// Called on `open()` to prevent orphaned invocations from accumulating
    /// when a process crashes before calling `finish_invocation()`.
    ///
    /// The 10-minute threshold is aggressive enough to catch zombies quickly
    /// (preventing poisoned stats) while generous enough to avoid cancelling
    /// legitimate long-running operations. The `CommandContext` Drop guard
    /// handles most cases immediately; this is the safety net for SIGKILL.
    fn cleanup_stale_invocations(&self) {
        // First collect PIDs of stale background jobs before updating them
        let stale_pids: Vec<i64> = self
            .conn
            .prepare(
                r"
                SELECT pid FROM invocations
                WHERE status = 'running'
                  AND is_background = 1
                  AND pid IS NOT NULL
                  AND pid > 0
                  AND started_at < strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-10 minutes')
                ",
            )
            .and_then(|mut stmt| {
                stmt.query_map([], |row| row.get::<_, i64>(0))
                    .and_then(|rows| rows.collect::<Result<Vec<_>, _>>())
            })
            .unwrap_or_default();

        let cleaned = self.conn.execute(
            r"
            UPDATE invocations
            SET status = 'cancelled',
                finished_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                duration_secs = (julianday('now') - julianday(started_at)) * 86400
            WHERE status = 'running'
              AND started_at < strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-10 minutes')
            ",
            [],
        );
        if let Ok(count) = cleaned
            && count > 0
        {
            eprintln!("ℹ️  Cleaned up {count} stale 'running' invocation(s) older than 10 minutes");
        }

        // Kill stale background processes to reclaim CPU/memory.
        // Send SIGTERM to all immediately, then spawn a thread for SIGKILL after grace period.
        let live_pids: Vec<i64> = stale_pids
            .into_iter()
            .filter(|&pid| {
                let nix_pid = nix::unistd::Pid::from_raw(pid as i32);
                if nix::sys::signal::kill(nix_pid, None).is_ok() {
                    let _ = nix::sys::signal::killpg(nix_pid, nix::sys::signal::Signal::SIGTERM);
                    true
                } else {
                    false
                }
            })
            .collect();

        if !live_pids.is_empty() {
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_secs(2));
                for pid in live_pids {
                    let nix_pid = nix::unistd::Pid::from_raw(pid as i32);
                    if nix::sys::signal::kill(nix_pid, None).is_ok() {
                        let _ =
                            nix::sys::signal::killpg(nix_pid, nix::sys::signal::Signal::SIGKILL);
                    }
                }
            });
        }

        // Also mark orphaned background_jobs rows (separate table, Phase 3).
        let _ = self.conn.execute(
            r"UPDATE background_jobs
              SET job_status = 'orphaned', finished_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
              WHERE job_status = 'running'
                AND started_at < strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-10 minutes')",
            [],
        );
    }

    /// Finish a background job and archive its log content in `background_job_logs`.
    ///
    /// Updates `background_jobs` row and inserts into `background_job_logs`.
    /// The invocation lifecycle is managed separately by `finish_invocation()`.
    pub fn finish_background_job(
        &self,
        job_id: i64,
        job_status: JobLifecycleStatus,
        exit_code: Option<i32>,
        _duration_secs: f64,
        stdout_path: Option<&std::path::Path>,
        stderr_path: Option<&std::path::Path>,
    ) -> Result<()> {
        let finished_at = Timestamp::now().format_rfc3339();

        let stdout_content = stdout_path.and_then(|p| std::fs::read_to_string(p).ok());
        let stderr_content = stderr_path.and_then(|p| std::fs::read_to_string(p).ok());

        self.conn.execute(
            r"UPDATE background_jobs
              SET finished_at = ?1, exit_code = ?2, job_status = ?3
              WHERE id = ?4",
            params![finished_at, exit_code, job_status.as_str(), job_id],
        )?;

        // Archive log content into dedicated table.
        if stdout_content.is_some() || stderr_content.is_some() {
            self.conn.execute(
                r"INSERT OR REPLACE INTO background_job_logs (job_id, stdout_content, stderr_content)
                  VALUES (?1, ?2, ?3)",
                params![job_id, stdout_content, stderr_content],
            )?;
        }

        Ok(())
    }

    /// Get log content for a completed job (reads from `background_job_logs`).
    pub fn get_job_logs(&self, job_id: i64) -> Result<(Option<String>, Option<String>)> {
        let result = self
            .conn
            .query_row(
                "SELECT stdout_content, stderr_content FROM background_job_logs WHERE job_id = ?1",
                params![job_id],
                |row| Ok((row.get::<_, Option<String>>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()?
            .unwrap_or((None, None));
        Ok(result)
    }

    /// Get recent invocations, optionally filtered by command.
    pub fn get_recent(
        &self,
        limit: usize,
        command_filter: Option<&str>,
    ) -> Result<Vec<Invocation>> {
        let sql = if command_filter.is_some() {
            r"
            SELECT id, command, subcommand, profile, args_json, git_commit, git_dirty,
                   started_at, finished_at, duration_secs, exit_code, status, host, cwd, live_stage
            FROM invocations
            WHERE command = ?1
            ORDER BY started_at DESC
            LIMIT ?2
            "
        } else {
            r"
            SELECT id, command, subcommand, profile, args_json, git_commit, git_dirty,
                   started_at, finished_at, duration_secs, exit_code, status, host, cwd, live_stage
            FROM invocations
            ORDER BY started_at DESC
            LIMIT ?1
            "
        };

        let mut stmt = self.conn.prepare(sql)?;

        let rows = if let Some(cmd) = command_filter {
            stmt.query_map(params![cmd, limit], row_to_invocation)?
        } else {
            stmt.query_map(params![limit], row_to_invocation)?
        };

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect invocations")
    }

    /// Get recent invocations with filtering, sorting, and pagination (G5).
    ///
    /// - `since_rfc3339`: if provided, only invocations started after this timestamp
    /// - `sort_by`: "started" (default), "duration", or "status"
    /// - `offset`: skip N entries for pagination
    pub fn get_recent_filtered(
        &self,
        limit: usize,
        offset: usize,
        command_filter: Option<&str>,
        since_rfc3339: Option<&str>,
        sort_by: &str,
    ) -> Result<Vec<Invocation>> {
        let order = match sort_by {
            "duration" => "duration_secs DESC NULLS LAST",
            "status" => "status ASC",
            _ => "started_at DESC",
        };

        let mut conditions: Vec<String> = Vec::new();
        if command_filter.is_some() {
            conditions.push("command = ?1".into());
        }
        if since_rfc3339.is_some() {
            let n = conditions.len() + 1;
            conditions.push(format!("started_at >= ?{n}"));
        }
        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let offset_n = conditions.len() + 1;
        let limit_n = conditions.len() + 2;
        let sql = format!(
            r"
            SELECT id, command, subcommand, profile, args_json, git_commit, git_dirty,
                   started_at, finished_at, duration_secs, exit_code, status, host, cwd, live_stage
            FROM invocations
            {where_clause}
            ORDER BY {order}
            LIMIT ?{limit_n} OFFSET ?{offset_n}
            "
        );

        let mut stmt = self.conn.prepare(&sql)?;

        // Build params dynamically
        let mut param_values: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        if let Some(cmd) = command_filter {
            param_values.push(Box::new(cmd.to_string()));
        }
        if let Some(since) = since_rfc3339 {
            param_values.push(Box::new(since.to_string()));
        }
        param_values.push(Box::new(offset as i64));
        param_values.push(Box::new(limit as i64));

        let refs: Vec<&dyn rusqlite::ToSql> = param_values.iter().map(|b| b.as_ref()).collect();
        let rows = stmt.query_map(refs.as_slice(), row_to_invocation)?;
        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect filtered invocations")
    }

    /// Get diagnostic error/warning counts for a specific invocation (G5 --with-diagnostics).
    pub fn get_diagnostic_counts_for_invocation(
        &self,
        invocation_id: i64,
    ) -> Result<DiagnosticCounts> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT
                SUM(CASE WHEN level = 'error' THEN 1 ELSE 0 END),
                SUM(CASE WHEN level = 'warning' THEN 1 ELSE 0 END),
                SUM(CASE WHEN fix_applicability = 'MachineApplicable' THEN 1 ELSE 0 END)
            FROM build_diagnostics
            WHERE invocation_id = ?1
            ",
        )?;
        let (errors, warnings, fixable) = stmt
            .query_row(params![invocation_id], |row| {
                Ok((
                    row.get::<_, i64>(0).unwrap_or(0),
                    row.get::<_, i64>(1).unwrap_or(0),
                    row.get::<_, i64>(2).unwrap_or(0),
                ))
            })
            .unwrap_or((0, 0, 0));
        Ok(DiagnosticCounts {
            errors: errors as usize,
            warnings: warnings as usize,
            fixable: fixable as usize,
        })
    }

    /// Get the most recent invocation for a command.
    pub fn get_last(&self, command: &str) -> Result<Option<Invocation>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT id, command, subcommand, profile, args_json, git_commit, git_dirty,
                   started_at, finished_at, duration_secs, exit_code, status, host, cwd, live_stage
            FROM invocations
            WHERE command = ?1
            ORDER BY started_at DESC
            LIMIT 1
            ",
        )?;

        stmt.query_row(params![command], row_to_invocation)
            .optional()
            .context("failed to get last invocation")
    }

    /// Get statistics for a command.
    ///
    /// Only includes `success` and `failed` invocations — excludes `running`
    /// (incomplete) and `cancelled` (which have inflated durations from zombie
    /// cleanup, poisoning AVG calculations).
    pub fn get_stats(&self, command: &str, days: u32) -> Result<CommandStats> {
        let since = Timestamp::now() - time::Duration::days(i64::from(days));
        let since_str = since.format_rfc3339();

        let mut stmt = self.conn.prepare(
            r"
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successes,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failures,
                AVG(duration_secs) as avg_duration
            FROM invocations
            WHERE command = ?1 AND started_at >= ?2 AND status IN ('success', 'failed')
            ",
        )?;

        let stats = stmt.query_row(params![command, since_str], |row| {
            Ok(CommandStats {
                total: row.get(0)?,
                successes: row.get(1)?,
                failures: row.get(2)?,
                avg_duration_secs: row.get(3)?,
            })
        })?;

        Ok(stats)
    }

    /// Get the last completed invocation for a command that has a tree fingerprint.
    ///
    /// Used by the coordinator to check for "fresh" results.
    pub fn get_last_completed_with_fingerprint(
        &self,
        command: &str,
    ) -> Result<Option<InvocationWithFingerprint>> {
        self.conn
            .query_row(
                r"
                SELECT id, status, duration_secs, tree_fingerprint, scope_key
                FROM invocations
                WHERE command = ?1
                  AND status IN ('success', 'failed')
                  AND tree_fingerprint IS NOT NULL
                ORDER BY started_at DESC
                LIMIT 1
                ",
                params![command],
                |row| {
                    let status_str: String = row.get(1)?;
                    Ok(InvocationWithFingerprint {
                        id: row.get(0)?,
                        status: parse_stored_invocation_status(status_str)?,
                        duration_secs: row.get(2)?,
                        tree_fingerprint: row.get(3)?,
                        scope_key: row.get(4)?,
                    })
                },
            )
            .optional()
            .context("failed to get last completed invocation with fingerprint")
    }

    /// Update an invocation's tree fingerprint and scope key.
    ///
    /// Called after starting an invocation to record the coordination scope.
    pub fn update_invocation_fingerprint(
        &self,
        id: i64,
        tree_fingerprint: &str,
        scope_key: &str,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE invocations SET tree_fingerprint = ?1, scope_key = ?2 WHERE id = ?3",
            params![tree_fingerprint, scope_key, id],
        )?;
        Ok(())
    }

    /// Prune old invocations older than the given number of days.
    pub fn prune(&self, older_than_days: u32) -> Result<usize> {
        // If 0 days, don't prune anything (nothing is "older than right now")
        if older_than_days == 0 {
            return Ok(0);
        }

        let cutoff = Timestamp::now() - time::Duration::days(i64::from(older_than_days));
        let cutoff_str = cutoff.format_rfc3339();

        let deleted = self.conn.execute(
            "DELETE FROM invocations WHERE started_at < ?1",
            params![cutoff_str],
        )?;

        Ok(deleted)
    }

    /// Prune old background jobs. Alias for `prune()` for API consistency.
    pub fn prune_old_jobs(&self, older_than_days: u32) -> Result<usize> {
        self.prune(older_than_days)
    }

    /// Record a test result.
    ///
    /// `test_mode` distinguishes execution lanes: `"nextest"`, `"vm"`, `"bench"`, `"fuzz"`.
    pub fn record_test_result(
        &self,
        invocation_id: i64,
        test_name: &str,
        package: &str,
        status: &str,
        duration_secs: f64,
        output: Option<&str>,
        test_mode: &str,
    ) -> Result<()> {
        self.conn.execute(
            r"
            INSERT INTO test_results (invocation_id, test_name, package, status, duration_secs, output, test_mode)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ",
            params![invocation_id, test_name, package, status, duration_secs, output, test_mode],
        )?;
        Ok(())
    }

    /// Update semantic test progress snapshot for an invocation.
    pub fn update_test_progress_snapshot(
        &self,
        invocation_id: i64,
        total: Option<usize>,
        passed: usize,
        failed: usize,
        ignored: usize,
        last_test_name: Option<&str>,
    ) -> Result<()> {
        let completed = passed + failed + ignored;
        let updated_at = Timestamp::now().format_rfc3339();

        self.conn.execute(
            r"
            UPDATE invocations
            SET test_total = ?1,
                test_passed = ?2,
                test_failed = ?3,
                test_ignored = ?4,
                test_completed = ?5,
                test_last_name = ?6,
                test_progress_updated_at = ?7
            WHERE id = ?8
            ",
            params![
                total.map(|v| v as i64),
                passed as i64,
                failed as i64,
                ignored as i64,
                completed as i64,
                last_test_name,
                updated_at,
                invocation_id
            ],
        )?;
        Ok(())
    }

    /// Get semantic test progress for an invocation, if available.
    pub fn get_test_progress(&self, invocation_id: i64) -> Result<Option<TestProgress>> {
        let progress = self
            .conn
            .query_row(
                r"
                SELECT
                    test_total,
                    COALESCE(test_passed, 0),
                    COALESCE(test_failed, 0),
                    COALESCE(test_ignored, 0),
                    COALESCE(test_completed, 0),
                    test_last_name,
                    test_progress_updated_at
                FROM invocations
                WHERE id = ?1
                ",
                params![invocation_id],
                |row| {
                    let total: Option<i64> = row.get(0)?;
                    let passed: i64 = row.get(1)?;
                    let failed: i64 = row.get(2)?;
                    let ignored: i64 = row.get(3)?;
                    let completed: i64 = row.get(4)?;
                    let last_test_name: Option<String> = row.get(5)?;
                    let updated_at: Option<String> = row.get(6)?;

                    if total.is_none() && passed == 0 && failed == 0 && ignored == 0 {
                        return Ok(None);
                    }

                    Ok(Some(TestProgress {
                        total: total.map(|v| v.max(0) as usize),
                        passed: passed.max(0) as usize,
                        failed: failed.max(0) as usize,
                        ignored: ignored.max(0) as usize,
                        completed: completed.max(0) as usize,
                        last_test_name,
                        updated_at,
                    }))
                },
            )
            .optional()
            .context("failed to get test progress")?;

        Ok(progress.flatten())
    }

    /// Back-fill test outputs from JUnit XML for an invocation.
    ///
    /// Updates `test_results.output` for tests that currently have NULL output.
    /// This is used after parsing JUnit XML to populate passing test output,
    /// since `libtest-json-plus` only includes stdout for failed tests.
    pub fn backfill_test_outputs(
        &self,
        invocation_id: i64,
        outputs: &std::collections::HashMap<String, String>,
    ) -> Result<usize> {
        let mut updated = 0usize;
        let mut stmt = self.conn.prepare(
            r"
            UPDATE test_results
            SET output = ?1
            WHERE invocation_id = ?2 AND test_name LIKE ?3 AND output IS NULL
            ",
        )?;

        for (test_name, output) in outputs {
            // The JUnit `name` attribute is the test function path (e.g.,
            // "repositories::events::tests::test_basic") which matches the
            // libtest-json `name` field stored in test_results.test_name.
            // Use suffix match with LIKE to handle potential differences.
            let pattern = format!("%{test_name}");
            let rows = stmt.execute(params![output, invocation_id, pattern])?;
            updated += rows;
        }

        Ok(updated)
    }

    /// Back-fill test metadata from JUnit XML for an invocation.
    ///
    /// Enriches `test_results` with:
    /// - Output (for tests with NULL output — passing tests from libtest-json-plus)
    /// - Failure message/type from JUnit `<failure>` elements
    /// - Sandbox infrastructure metadata (slot name, timing) parsed from slog events
    /// - Package correction from JUnit `classname` attribute
    pub fn backfill_test_metadata(
        &self,
        invocation_id: i64,
        metadata: &std::collections::HashMap<String, crate::nextest::junit::JunitTestMeta>,
    ) -> Result<usize> {
        let mut updated = 0usize;

        // Phase 1: Back-fill output for tests that don't have it yet
        let mut output_stmt = self.conn.prepare(
            r"
            UPDATE test_results
            SET output = ?1
            WHERE invocation_id = ?2 AND test_name LIKE ?3 AND output IS NULL
            ",
        )?;

        // Phase 2: Update failure info and package from classname
        let mut meta_stmt = self.conn.prepare(
            r"
            UPDATE test_results
            SET failure_message = COALESCE(?1, failure_message),
                failure_type = COALESCE(?2, failure_type),
                package = COALESCE(?3, package)
            WHERE invocation_id = ?4 AND test_name LIKE ?5
            ",
        )?;

        for (test_name, meta) in metadata {
            let pattern = format!("%{test_name}");

            // Back-fill output if available and not already present
            if let Some(output) = &meta.output {
                let rows = output_stmt.execute(params![output, invocation_id, &pattern])?;
                updated += rows;
            }

            // Update failure info and classname-based package
            let has_meta = meta.failure_message.is_some()
                || meta.failure_type.is_some()
                || meta.classname.is_some();
            if has_meta {
                meta_stmt.execute(params![
                    meta.failure_message,
                    meta.failure_type,
                    meta.classname,
                    invocation_id,
                    &pattern,
                ])?;
            }
        }

        drop(output_stmt);
        drop(meta_stmt);

        // Phase 3: Parse slog events from output to extract sandbox metadata
        self.extract_sandbox_metadata(invocation_id)?;

        Ok(updated)
    }

    /// Extract sandbox infrastructure metadata from slog events in test output.
    ///
    /// Scans the `output` column for `[sandbox:*] event=slot_acquired` lines and
    /// extracts `slot`, `duration_ms`, `clean_ms` into dedicated columns.
    fn extract_sandbox_metadata(&self, invocation_id: i64) -> Result<()> {
        // Fetch all tests with output for this invocation
        let mut fetch_stmt = self.conn.prepare(
            r"
            SELECT id, output FROM test_results
            WHERE invocation_id = ?1 AND output IS NOT NULL AND slot_name IS NULL
            ",
        )?;

        let mut update_stmt = self.conn.prepare(
            r"
            UPDATE test_results
            SET slot_name = ?1, slot_wait_ms = ?2, cleanup_ms = ?3
            WHERE id = ?4
            ",
        )?;

        let rows: Vec<(i64, String)> = fetch_stmt
            .query_map([invocation_id], |row| Ok((row.get(0)?, row.get(1)?)))?
            .filter_map(Result::ok)
            .collect();

        for (id, output) in &rows {
            let meta = parse_sandbox_meta(output);
            if meta.slot_name.is_some() || meta.slot_wait_ms.is_some() {
                update_stmt.execute(params![
                    meta.slot_name,
                    meta.slot_wait_ms,
                    meta.cleanup_ms,
                    id,
                ])?;
            }
        }

        Ok(())
    }

    /// Record system resource metrics for an invocation.
    pub fn record_system_metrics(
        &self,
        invocation_id: i64,
        cpu_usage_avg: f32,
        memory_usage_max_mb: f64,
    ) -> Result<()> {
        self.conn.execute(
            r"
            UPDATE invocations
            SET cpu_usage_avg = ?1, memory_usage_max_mb = ?2
            WHERE id = ?3
            ",
            params![cpu_usage_avg, memory_usage_max_mb, invocation_id],
        )?;
        Ok(())
    }

    /// Get resource usage (CPU/memory) for recent invocations.
    pub fn get_resource_usage(
        &self,
        command_filter: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ResourceUsage>> {
        let mut query = String::from(
            r"SELECT command, started_at, duration_secs, cpu_usage_avg, memory_usage_max_mb
              FROM invocations
              WHERE status = 'success'
                AND cpu_usage_avg IS NOT NULL",
        );
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        let mut param_idx = 1usize;

        if let Some(cmd) = command_filter {
            query.push_str(&format!(" AND command = ?{param_idx}"));
            params_vec.push(Box::new(cmd.to_string()));
            param_idx += 1;
        }

        query.push_str(&format!(" ORDER BY id DESC LIMIT ?{param_idx}"));
        params_vec.push(Box::new(limit as i64));

        let mut stmt = self.conn.prepare(&query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(std::convert::AsRef::as_ref).collect();

        let rows = stmt.query_map(rusqlite::params_from_iter(params_refs), |row| {
            Ok(ResourceUsage {
                command: row.get(0)?,
                started_at: row.get(1)?,
                duration_secs: row.get(2)?,
                cpu_usage_avg: row.get(3)?,
                memory_usage_max_mb: row.get(4)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Get count of invocations.
    pub fn count(&self) -> Result<usize> {
        let count: usize = self
            .conn
            .query_row("SELECT COUNT(*) FROM invocations", [], |row| row.get(0))?;
        Ok(count)
    }

    // ============ Background Job Methods (Phase 3: Jobs Split) ============

    /// Start a background job. Creates both an invocation row and a background_jobs row.
    ///
    /// Returns `(invocation_id, job_id)`. The invocation is the durable execution record;
    /// the job is the process handle. The child process claims the invocation via
    /// `XTASK_BG_INVOCATION_ID`; the job_id is used for directory naming and coordinator tracking.
    pub fn start_background_job(
        &self,
        command: &str,
        args: &[String],
        pid: u32,
        stdout_path: &Path,
        stderr_path: &Path,
    ) -> Result<(i64, i64)> {
        let args_json = serde_json::to_string(args)?;
        let git_commit = get_git_commit();
        let git_dirty = is_git_dirty();
        let host = crate::config::config().hostname.clone();
        let cwd = std::env::current_dir()
            .map(|p| p.display().to_string())
            .unwrap_or_default();
        let started_at = Timestamp::now().format_rfc3339();

        // Create the durable invocation record.
        self.conn.execute(
            r"INSERT INTO invocations
                (command, args_json, git_commit, git_dirty, started_at, host, cwd, status, launch_mode, is_background)
              VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'running', 'background', 1)",
            params![command, args_json, git_commit, git_dirty, started_at, host, cwd],
        )?;
        let invocation_id = self.conn.last_insert_rowid();

        // Create the background job handle row.
        let stdout_str = if stdout_path == Path::new("") {
            None
        } else {
            Some(stdout_path.display().to_string())
        };
        let stderr_str = if stderr_path == Path::new("") {
            None
        } else {
            Some(stderr_path.display().to_string())
        };
        let pid_val = if pid == 0 { None } else { Some(pid) };
        self.conn.execute(
            r"INSERT INTO background_jobs
                (invocation_id, command, args_json, pid, stdout_path, stderr_path, job_status, started_at)
              VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'running', ?7)",
            params![invocation_id, command, args_json, pid_val, stdout_str, stderr_str, started_at],
        )?;
        let job_id = self.conn.last_insert_rowid();

        Ok((invocation_id, job_id))
    }

    /// Attach command metadata to a pre-created background invocation row.
    ///
    /// Background jobs are registered before spawning to reserve a stable ID.
    /// The child `xtask --fg` process then claims that row via `XTASK_BG_INVOCATION_ID`
    /// and records execution details on the same invocation.
    pub fn claim_background_invocation(
        &self,
        id: i64,
        command: &str,
        subcommand: Option<&str>,
        profile: Option<&str>,
        args_json: Option<&str>,
    ) -> Result<bool> {
        let updated = self.conn.execute(
            r"
            UPDATE invocations
            SET command = ?1,
                subcommand = ?2,
                profile = ?3,
                args_json = COALESCE(?4, args_json)
            WHERE id = ?5 AND is_background = 1
            ",
            params![command, subcommand, profile, args_json, id],
        )?;
        Ok(updated == 1)
    }

    /// Get all active (running) background jobs.
    pub fn get_active_background_jobs(&self) -> Result<Vec<BackgroundJob>> {
        let mut stmt = self.conn.prepare(
            r"SELECT id, invocation_id, command, args_json, started_at, pid, stdout_path, stderr_path, job_status, exit_code
              FROM background_jobs
              WHERE job_status = 'running'
              ORDER BY started_at DESC",
        )?;
        let rows = stmt.query_map([], row_to_background_job)?;
        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect background jobs")
    }

    /// Get a single background job by ID (O(1) direct SQL lookup).
    pub fn get_background_job_by_id(&self, id: i64) -> Result<Option<BackgroundJob>> {
        self.conn
            .query_row(
                r"SELECT id, invocation_id, command, args_json, started_at, pid, stdout_path, stderr_path, job_status, exit_code
                  FROM background_jobs WHERE id = ?1",
                params![id],
                row_to_background_job,
            )
            .optional()
            .context("failed to get background job by id")
    }

    /// Get all background job IDs (for prune orphan directory cleanup).
    pub fn get_all_background_job_ids(&self) -> Result<HashSet<i64>> {
        let mut stmt = self.conn.prepare("SELECT id FROM background_jobs")?;
        let rows = stmt.query_map([], |row| row.get::<_, i64>(0))?;
        let mut ids = HashSet::new();
        for id in rows {
            ids.insert(id?);
        }
        Ok(ids)
    }

    /// Get recent background jobs (including completed ones).
    pub fn get_recent_background_jobs(&self, limit: usize) -> Result<Vec<BackgroundJob>> {
        let mut stmt = self.conn.prepare(
            r"SELECT id, invocation_id, command, args_json, started_at, pid, stdout_path, stderr_path, job_status, exit_code
              FROM background_jobs
              ORDER BY started_at DESC
              LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], row_to_background_job)?;
        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect background jobs")
    }

    /// Update a background job's PID (used when process is spawned).
    pub fn update_job_pid(&self, job_id: i64, pid: u32) -> Result<()> {
        self.conn.execute(
            "UPDATE background_jobs SET pid = ?1 WHERE id = ?2",
            params![pid, job_id],
        )?;
        Ok(())
    }

    /// Update a background job's log file paths.
    pub fn update_job_paths(
        &self,
        job_id: i64,
        stdout_path: &std::path::Path,
        stderr_path: &std::path::Path,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE background_jobs SET stdout_path = ?1, stderr_path = ?2 WHERE id = ?3",
            params![
                stdout_path.display().to_string(),
                stderr_path.display().to_string(),
                job_id
            ],
        )?;
        Ok(())
    }

    /// Check if a background job's process is still running.
    pub fn is_job_running(&self, job_id: i64) -> Result<bool> {
        let pid: Option<u32> = self
            .conn
            .query_row(
                "SELECT pid FROM background_jobs WHERE id = ?1",
                params![job_id],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        Ok(pid.is_some_and(is_process_running))
    }

    // ============ Diagnostics Methods (Phase 4: Build Diagnostics Capture) ============

    /// Record a build diagnostic (warning/error).
    pub fn record_diagnostic(
        &self,
        invocation_id: i64,
        diag: &crate::cargo_diagnostics::CompilerDiagnostic,
    ) -> Result<()> {
        self.conn.execute(
            r"
            INSERT OR IGNORE INTO build_diagnostics
                (invocation_id, level, code, message, file_path, line, col, rendered,
                 package, fix_replacement, fix_applicability, fix_byte_start, fix_byte_end)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            ",
            params![
                invocation_id,
                diag.level,
                diag.code,
                diag.message,
                diag.file_path,
                diag.line,
                diag.column,
                diag.rendered,
                diag.package,
                diag.fix_replacement,
                diag.fix_applicability,
                diag.fix_byte_start,
                diag.fix_byte_end,
            ],
        )?;
        Ok(())
    }

    /// Record multiple diagnostics in a single transaction.
    ///
    /// Much more efficient than calling `record_diagnostic()` in a loop — uses a single
    /// prepared statement and wraps all inserts in one transaction.
    pub fn record_diagnostics_batch(
        &self,
        invocation_id: i64,
        diagnostics: &[crate::cargo_diagnostics::CompilerDiagnostic],
    ) -> Result<()> {
        if diagnostics.is_empty() {
            return Ok(());
        }
        let tx = self.conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare(
                r"
                INSERT OR IGNORE INTO build_diagnostics
                    (invocation_id, level, code, message, file_path, line, col, rendered,
                     package, fix_replacement, fix_applicability, fix_byte_start, fix_byte_end)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                ",
            )?;
            for diag in diagnostics {
                stmt.execute(params![
                    invocation_id,
                    diag.level,
                    diag.code,
                    diag.message,
                    diag.file_path,
                    diag.line,
                    diag.column,
                    diag.rendered,
                    diag.package,
                    diag.fix_replacement,
                    diag.fix_applicability,
                    diag.fix_byte_start,
                    diag.fix_byte_end,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Record which packages were compiled in an invocation (for package-scoped supersession).
    pub fn record_compiled_packages(
        &self,
        invocation_id: i64,
        packages: &std::collections::HashSet<String>,
    ) -> Result<()> {
        let mut stmt = self.conn.prepare(
            "INSERT OR IGNORE INTO invocation_packages (invocation_id, package) VALUES (?1, ?2)",
        )?;
        for pkg in packages {
            stmt.execute(params![invocation_id, pkg])?;
        }
        Ok(())
    }

    /// Get packages compiled in a specific invocation (H5 — fresh path scope context).
    pub fn get_compiled_packages_for_invocation(&self, invocation_id: i64) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT package FROM invocation_packages WHERE invocation_id = ?1 ORDER BY package",
        )?;
        let rows = stmt.query_map(params![invocation_id], |row| row.get::<_, String>(0))?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Get diagnostics for an invocation.
    pub fn get_diagnostics(&self, invocation_id: i64) -> Result<Vec<StoredDiagnostic>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT d.id, d.level, d.code, d.message, d.file_path, d.line, d.col, d.rendered,
                   d.package, d.fix_replacement, d.fix_applicability, d.fix_byte_start, d.fix_byte_end,
                   i.command, i.started_at
            FROM build_diagnostics d
            JOIN invocations i ON d.invocation_id = i.id
            WHERE d.invocation_id = ?1
            ORDER BY d.id
            ",
        )?;

        let rows = stmt.query_map(params![invocation_id], row_to_diagnostic_full)?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect diagnostics")
    }

    /// Get diagnostics from a specific invocation (by ID or "latest").
    pub fn get_diagnostics_for_invocation(
        &self,
        invocation: &str,
        command: Option<&str>,
    ) -> Result<Vec<StoredDiagnostic>> {
        let inv_id: Option<i64> = if invocation == "latest" {
            if let Some(cmd) = command {
                self.conn
                    .query_row(
                        r"
                        SELECT id FROM invocations
                        WHERE command = ?1 AND status IN ('success', 'failed')
                        ORDER BY started_at DESC LIMIT 1
                        ",
                        params![cmd],
                        |row| row.get(0),
                    )
                    .optional()?
            } else {
                self.conn
                    .query_row(
                        r"
                        SELECT id FROM invocations
                        WHERE status IN ('success', 'failed')
                        ORDER BY started_at DESC LIMIT 1
                        ",
                        [],
                        |row| row.get(0),
                    )
                    .optional()?
            }
        } else {
            // Parse as invocation ID
            invocation.parse::<i64>().ok()
        };

        match inv_id {
            Some(id) => self.get_diagnostics(id),
            None => Ok(vec![]),
        }
    }

    /// Get current diagnostics using package-scoped supersession.
    ///
    /// For each package, finds the most recent invocation that compiled it,
    /// and returns diagnostics from that invocation for that package only.
    /// This gives a "current state of the world" view — partial builds update
    /// only the packages they touched, preserving diagnostics from earlier runs
    /// for untouched packages.
    pub fn get_current_diagnostics(
        &self,
        level_filter: Option<&str>,
        file_pattern: Option<&str>,
        package_filter: Option<&str>,
        command_filter: Option<&str>,
        fixable_only: bool,
    ) -> Result<Vec<StoredDiagnostic>> {
        // Build the CTE query dynamically based on filters.
        // The CTE prefix is extracted as a const (shared with get_current_diagnostic_counts).
        let mut query = String::from(LATEST_PER_PACKAGE_CTE_OPEN);

        // Command filter in CTE
        let mut param_idx = 1;
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(cmd) = command_filter {
            query.push_str(&format!(" AND i.command = ?{param_idx}"));
            params_vec.push(Box::new(cmd.to_string()));
            param_idx += 1;
        }

        query.push_str(LATEST_PER_PACKAGE_CTE_CLOSE);
        query.push_str(
            r"
            SELECT d.id, d.level, d.code, d.message, d.file_path, d.line, d.col, d.rendered,
                   d.package, d.fix_replacement, d.fix_applicability, d.fix_byte_start, d.fix_byte_end,
                   i.command, i.started_at
            FROM build_diagnostics d
            JOIN invocations i ON d.invocation_id = i.id
            JOIN latest_per_package lpp ON d.package = lpp.package
                                       AND d.invocation_id = lpp.latest_inv_id
            WHERE 1=1
            ",
        );

        if let Some(level) = level_filter {
            query.push_str(&format!(" AND d.level = ?{param_idx}"));
            params_vec.push(Box::new(level.to_string()));
            param_idx += 1;
        }

        if let Some(pattern) = file_pattern {
            query.push_str(&format!(" AND d.file_path LIKE ?{param_idx}"));
            params_vec.push(Box::new(format!("%{pattern}%")));
            param_idx += 1;
        }

        if let Some(pkg) = package_filter {
            query.push_str(&format!(" AND d.package = ?{param_idx}"));
            params_vec.push(Box::new(pkg.to_string()));
            param_idx += 1;
        }

        if fixable_only {
            query.push_str(&format!(" AND d.fix_applicability = ?{param_idx}"));
            params_vec.push(Box::new("MachineApplicable".to_string()));
            let _ = param_idx; // suppress unused warning
        }

        query.push_str(" ORDER BY d.level ASC, d.package ASC, d.file_path ASC, d.line ASC");

        let mut stmt = self.conn.prepare(&query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(std::convert::AsRef::as_ref).collect();

        let rows = stmt.query_map(
            rusqlite::params_from_iter(params_refs),
            row_to_diagnostic_full,
        )?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Get current diagnostic counts by level (package-scoped supersession).
    ///
    /// Returns a map of level → count using the same CTE as `get_current_diagnostics`
    /// but only fetching aggregate counts. Lightweight enough for the status summary.
    pub fn get_current_diagnostic_counts(&self) -> Result<DiagnosticCounts> {
        let query = format!(
            "{LATEST_PER_PACKAGE_CTE_OPEN}
            {LATEST_PER_PACKAGE_CTE_CLOSE}
            SELECT
                COALESCE(SUM(CASE WHEN d.level = 'error' THEN 1 ELSE 0 END), 0) as errors,
                COALESCE(SUM(CASE WHEN d.level = 'warning' THEN 1 ELSE 0 END), 0) as warnings,
                COALESCE(SUM(CASE WHEN d.fix_applicability = 'MachineApplicable' THEN 1 ELSE 0 END), 0) as fixable
            FROM build_diagnostics d
            JOIN latest_per_package lpp ON d.package = lpp.package
                                       AND d.invocation_id = lpp.latest_inv_id"
        );

        let mut stmt = self.conn.prepare(&query)?;
        let counts = stmt.query_row([], |row| {
            Ok(DiagnosticCounts {
                errors: row.get::<_, i64>(0)? as usize,
                warnings: row.get::<_, i64>(1)? as usize,
                fixable: row.get::<_, i64>(2)? as usize,
            })
        })?;

        Ok(counts)
    }

    /// Get the count of auto-fixable diagnostics in the current package-scoped view (G3).
    pub fn get_fixable_diagnostic_count(&self) -> Result<usize> {
        Ok(self.get_current_diagnostic_counts()?.fixable)
    }

    /// Get diagnostic counts per invocation for trend analysis.
    ///
    /// Returns the most recent `limit` check/build invocations with their
    /// error and warning counts. Used by `--trend`.
    pub fn get_diagnostic_trend(&self, limit: usize) -> Result<Vec<DiagnosticTrendPoint>> {
        let query = r"
            SELECT
                i.id,
                i.command,
                i.started_at,
                i.status,
                COALESCE(SUM(CASE WHEN d.level = 'error' THEN 1 ELSE 0 END), 0) as errors,
                COALESCE(SUM(CASE WHEN d.level = 'warning' THEN 1 ELSE 0 END), 0) as warnings,
                COUNT(d.id) as total
            FROM invocations i
            LEFT JOIN build_diagnostics d ON d.invocation_id = i.id
            WHERE i.command IN ('check', 'build')
              AND i.status IN ('success', 'failed')
            GROUP BY i.id
            ORDER BY i.started_at DESC
            LIMIT ?1
        ";

        let mut stmt = self.conn.prepare(query)?;
        let rows = stmt.query_map(rusqlite::params![limit], |row| {
            let status_str: String = row.get(3)?;
            Ok(DiagnosticTrendPoint {
                invocation_id: row.get(0)?,
                command: row.get(1)?,
                started_at: row.get(2)?,
                status: parse_stored_invocation_status(status_str)?,
                errors: row.get::<_, i64>(4)? as usize,
                warnings: row.get::<_, i64>(5)? as usize,
                total: row.get::<_, i64>(6)? as usize,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        // Return in chronological order (oldest first)
        results.reverse();
        Ok(results)
    }

    /// Get recent diagnostics across all invocations (raw accumulated, used by `--all`).
    pub fn get_recent_diagnostics_all(
        &self,
        limit: usize,
        level_filter: Option<&str>,
        file_pattern: Option<&str>,
        command_filter: Option<&str>,
        package_filter: Option<&str>,
    ) -> Result<Vec<StoredDiagnostic>> {
        let mut query = String::from(
            r"
            SELECT d.id, d.level, d.code, d.message, d.file_path, d.line, d.col, d.rendered,
                   d.package, d.fix_replacement, d.fix_applicability, d.fix_byte_start, d.fix_byte_end,
                   i.command, i.started_at
            FROM build_diagnostics d
            JOIN invocations i ON d.invocation_id = i.id
            WHERE 1=1
            ",
        );

        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        let mut param_idx = 1;

        if let Some(level) = level_filter {
            query.push_str(&format!(" AND d.level = ?{param_idx}"));
            params_vec.push(Box::new(level.to_string()));
            param_idx += 1;
        }
        if let Some(pattern) = file_pattern {
            query.push_str(&format!(" AND d.file_path LIKE ?{param_idx}"));
            params_vec.push(Box::new(format!("%{pattern}%")));
            param_idx += 1;
        }
        if let Some(cmd) = command_filter {
            query.push_str(&format!(" AND i.command = ?{param_idx}"));
            params_vec.push(Box::new(cmd.to_string()));
            param_idx += 1;
        }
        if let Some(pkg) = package_filter {
            query.push_str(&format!(" AND d.package = ?{param_idx}"));
            params_vec.push(Box::new(pkg.to_string()));
            param_idx += 1;
        }

        query.push_str(&format!(
            " ORDER BY i.started_at DESC, d.id DESC LIMIT ?{param_idx}"
        ));
        params_vec.push(Box::new(limit as i64));

        let mut stmt = self.conn.prepare(&query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(std::convert::AsRef::as_ref).collect();

        let rows = stmt.query_map(
            rusqlite::params_from_iter(params_refs),
            row_to_diagnostic_full,
        )?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    // ──────────────────────────────────────────────────────────────────────
    // G1: Diagnostic Delta — compare two invocations' diagnostic sets
    // ──────────────────────────────────────────────────────────────────────

    /// Compute the diagnostic delta between two invocations.
    ///
    /// Matches diagnostics by their identity key: `(level, code, message, file_path, line, col, package)`.
    /// - `new`: diagnostics in `to_id` but not `from_id` (newly appeared)
    /// - `resolved`: diagnostics in `from_id` but not `to_id` (fixed)
    /// - `persistent`: diagnostics in both invocations
    pub fn get_diagnostic_delta(&self, from_id: i64, to_id: i64) -> Result<DiagnosticDelta> {
        fn identity_key(d: &StoredDiagnostic) -> String {
            format!(
                "{}|{}|{}|{}|{}|{}|{}",
                d.level,
                d.code.as_deref().unwrap_or(""),
                d.message,
                d.file_path.as_deref().unwrap_or(""),
                d.line.map(|v| v.to_string()).unwrap_or_default(),
                d.col.map(|v| v.to_string()).unwrap_or_default(),
                d.package.as_deref().unwrap_or(""),
            )
        }

        let from_diags = self.get_diagnostics(from_id)?;
        let to_diags = self.get_diagnostics(to_id)?;

        let from_keys: std::collections::HashSet<String> =
            from_diags.iter().map(identity_key).collect();
        let to_keys: std::collections::HashSet<String> =
            to_diags.iter().map(identity_key).collect();

        let new: Vec<StoredDiagnostic> = to_diags
            .iter()
            .filter(|d| !from_keys.contains(&identity_key(d)))
            .cloned()
            .collect();
        let resolved: Vec<StoredDiagnostic> = from_diags
            .iter()
            .filter(|d| !to_keys.contains(&identity_key(d)))
            .cloned()
            .collect();
        let persistent: Vec<StoredDiagnostic> = to_diags
            .iter()
            .filter(|d| from_keys.contains(&identity_key(d)))
            .cloned()
            .collect();

        Ok(DiagnosticDelta {
            new,
            resolved,
            persistent,
        })
    }

    // ──────────────────────────────────────────────────────────────────────
    // G2: Stage Analytics — slowest stages and per-stage trend
    // ──────────────────────────────────────────────────────────────────────

    /// Get aggregate stage timing statistics (G2 — slowest stages view).
    ///
    /// Returns stages sorted by average duration descending.
    pub fn get_slowest_stages(
        &self,
        command_filter: Option<&str>,
        limit: usize,
    ) -> Result<Vec<StageStats>> {
        let mut query = String::from(
            r"
            SELECT
                st.stage_name,
                AVG(st.duration_secs) as avg_duration,
                MAX(st.duration_secs) as max_duration,
                COUNT(*) as run_count
            FROM stage_timings st
            JOIN invocations i ON st.invocation_id = i.id
            WHERE 1=1
            ",
        );

        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        let mut param_idx = 1usize;

        if let Some(cmd) = command_filter {
            query.push_str(&format!(" AND i.command = ?{param_idx}"));
            params_vec.push(Box::new(cmd.to_string()));
            param_idx += 1;
        }

        query.push_str(&format!(
            " GROUP BY st.stage_name
              ORDER BY avg_duration DESC
              LIMIT ?{param_idx}"
        ));
        params_vec.push(Box::new(limit as i64));

        let mut stmt = self.conn.prepare(&query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(std::convert::AsRef::as_ref).collect();

        let rows = stmt.query_map(rusqlite::params_from_iter(params_refs), |row| {
            Ok(StageStats {
                stage_name: row.get(0)?,
                avg_duration_secs: row.get(1)?,
                max_duration_secs: row.get(2)?,
                run_count: row.get::<_, i64>(3)? as usize,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Get timing trend for a specific stage over recent invocations (G2).
    pub fn get_stage_trend(
        &self,
        stage_name: &str,
        command_filter: Option<&str>,
        window: usize,
    ) -> Result<Vec<StageTrendPoint>> {
        let mut query = String::from(
            r"
            SELECT
                st.invocation_id,
                i.started_at,
                st.duration_secs,
                st.success
            FROM stage_timings st
            JOIN invocations i ON st.invocation_id = i.id
            WHERE st.stage_name = ?1
            ",
        );

        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        params_vec.push(Box::new(stage_name.to_string()));
        let mut param_idx = 2usize;

        if let Some(cmd) = command_filter {
            query.push_str(&format!(" AND i.command = ?{param_idx}"));
            params_vec.push(Box::new(cmd.to_string()));
            param_idx += 1;
        }

        query.push_str(&format!(" ORDER BY i.started_at DESC LIMIT ?{param_idx}"));
        params_vec.push(Box::new(window as i64));

        let mut stmt = self.conn.prepare(&query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(std::convert::AsRef::as_ref).collect();

        let rows = stmt.query_map(rusqlite::params_from_iter(params_refs), |row| {
            let success_int: i32 = row.get(3)?;
            Ok(StageTrendPoint {
                invocation_id: row.get(0)?,
                started_at: row.get(1)?,
                duration_secs: row.get(2)?,
                success: success_int != 0,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        results.reverse(); // Chronological order
        Ok(results)
    }

    // ──────────────────────────────────────────────────────────────────────
    // G3: Fix Session Analytics
    // ──────────────────────────────────────────────────────────────────────

    /// Record a pre-fix diagnostic snapshot on an invocation (called before `xtask fix` runs).
    pub fn record_fix_session_snapshot(
        &self,
        invocation_id: i64,
        errors: i64,
        warnings: i64,
        fixable: i64,
    ) -> Result<()> {
        self.conn.execute(
            r"UPDATE invocations
              SET pre_fix_errors = ?2, pre_fix_warnings = ?3, pre_fix_fixable = ?4
              WHERE id = ?1",
            rusqlite::params![invocation_id, errors, warnings, fixable],
        )?;
        Ok(())
    }

    /// Get recent fix sessions with their pre-fix diagnostic counts (G3).
    pub fn get_fix_sessions(&self, limit: usize) -> Result<Vec<FixSession>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT
                id,
                started_at,
                duration_secs,
                pre_fix_errors,
                pre_fix_warnings,
                pre_fix_fixable
            FROM invocations
            WHERE command = 'fix'
            ORDER BY started_at DESC
            LIMIT ?1
            ",
        )?;

        let rows = stmt.query_map(rusqlite::params![limit as i64], |row| {
            Ok(FixSession {
                invocation_id: row.get(0)?,
                started_at: row.get(1)?,
                duration_secs: row.get(2)?,
                pre_fix_errors: row.get(3)?,
                pre_fix_warnings: row.get(4)?,
                pre_fix_fixable: row.get(5)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    // ──────────────────────────────────────────────────────────────────────
    // G4: Package Enumeration
    // ──────────────────────────────────────────────────────────────────────

    /// Get all package names that have appeared in diagnostics (G4).
    pub fn get_known_packages(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT DISTINCT package
            FROM build_diagnostics
            WHERE package IS NOT NULL
            ORDER BY package
            ",
        )?;

        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    // ──────────────────────────────────────────────────────────────────────
    // I: Semantic Query Intelligence
    // ──────────────────────────────────────────────────────────────────────

    /// I3: Get diagnostic lifecycle status across invocations.
    ///
    /// Classifies each unique (package, level, code, message) tuple as:
    /// - `new`: only appeared in the latest invocation for its package
    /// - `chronic`: present in 3+ invocations and still in the latest
    /// - `recurring`: appeared more than once but not chronic, still in latest
    /// - `resolved`: was present before but NOT in the latest invocation
    pub fn get_diagnostic_lifecycle(
        &self,
        package: Option<&str>,
        code: Option<&str>,
        level: Option<&str>,
        lifecycle_status: Option<&str>,
        limit: usize,
    ) -> Result<Vec<DiagnosticLifecycle>> {
        let mut sql = String::from(
            r"
            WITH latest_per_package AS (
                SELECT ip.package, MAX(i.id) as latest_inv_id
                FROM invocation_packages ip
                JOIN invocations i ON ip.invocation_id = i.id
                WHERE i.status IN ('success', 'failed')
                GROUP BY ip.package
            ),
            diag_occurrences AS (
                SELECT
                    bd.package,
                    bd.level,
                    bd.code,
                    bd.message,
                    COUNT(DISTINCT bd.invocation_id) as occurrence_count,
                    MIN(bd.invocation_id) as first_seen,
                    MAX(bd.invocation_id) as last_seen
                FROM build_diagnostics bd
                WHERE bd.package IS NOT NULL
                GROUP BY bd.package, bd.level, COALESCE(bd.code, ''), bd.message
            ),
            lifecycle AS (
                SELECT
                    d.package, d.level, d.code, d.message,
                    d.occurrence_count, d.first_seen, d.last_seen,
                    CASE
                        WHEN lpp.latest_inv_id IS NULL THEN 'resolved'
                        WHEN d.last_seen < lpp.latest_inv_id THEN 'resolved'
                        WHEN d.first_seen = d.last_seen THEN 'new'
                        WHEN d.occurrence_count >= 3 THEN 'chronic'
                        ELSE 'recurring'
                    END as status
                FROM diag_occurrences d
                LEFT JOIN latest_per_package lpp ON d.package = lpp.package
            )
            SELECT package, level, code, message, occurrence_count, first_seen, last_seen, status
            FROM lifecycle
            WHERE 1=1
            ",
        );

        let mut params: Vec<String> = Vec::new();
        let mut idx = 1usize;

        if let Some(pkg) = package {
            sql.push_str(&format!(" AND package = ?{idx}"));
            params.push(pkg.to_string());
            idx += 1;
        }
        if let Some(c) = code {
            sql.push_str(&format!(" AND COALESCE(code, '') = ?{idx}"));
            params.push(c.to_string());
            idx += 1;
        }
        if let Some(l) = level {
            sql.push_str(&format!(" AND level = ?{idx}"));
            params.push(l.to_string());
            idx += 1;
        }
        if let Some(s) = lifecycle_status {
            sql.push_str(&format!(" AND status = ?{idx}"));
            params.push(s.to_string());
            idx += 1;
        }
        let _ = idx;
        sql.push_str(" ORDER BY status, occurrence_count DESC, package");
        sql.push_str(&format!(" LIMIT {limit}"));

        let mut stmt = self
            .conn
            .prepare(&sql)
            .context("failed to prepare lifecycle query")?;
        let refs: Vec<&dyn rusqlite::ToSql> =
            params.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
        let rows = stmt
            .query_map(refs.as_slice(), |row| {
                let status_str: String = row.get(7)?;
                let status = match status_str.as_str() {
                    "new" => LifecycleStatus::New,
                    "chronic" => LifecycleStatus::Chronic,
                    "recurring" => LifecycleStatus::Recurring,
                    _ => LifecycleStatus::Resolved,
                };
                Ok(DiagnosticLifecycle {
                    package: row.get(0)?,
                    level: row.get(1)?,
                    code: row.get(2)?,
                    message: row.get(3)?,
                    occurrence_count: row.get::<_, i64>(4)? as usize,
                    first_seen: row.get(5)?,
                    last_seen: row.get(6)?,
                    status,
                })
            })
            .context("failed to execute lifecycle query")?
            .collect::<Result<Vec<_>, _>>()
            .context("failed to collect lifecycle results")?;
        Ok(rows)
    }

    /// I4: Get cross-invocation chronological timeline with diagnostic counts.
    pub fn get_invocation_timeline(
        &self,
        command: Option<&str>,
        days: u32,
        limit: usize,
    ) -> Result<Vec<InvocationTimelineEntry>> {
        let cutoff = {
            let dt = time::OffsetDateTime::now_utc() - time::Duration::days(i64::from(days));
            dt.format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default()
        };

        let mut sql = String::from(
            r"
            SELECT
                i.id,
                i.command,
                i.status,
                i.started_at,
                i.duration_secs,
                COALESCE(st.stage_count, 0) as stage_count,
                COALESCE(dc_err.error_count, 0) as error_count,
                COALESCE(dc_warn.warning_count, 0) as warning_count
            FROM invocations i
            LEFT JOIN (
                SELECT invocation_id, COUNT(*) as stage_count
                FROM stage_timings GROUP BY invocation_id
            ) st ON i.id = st.invocation_id
            LEFT JOIN (
                SELECT invocation_id, COUNT(*) as error_count
                FROM build_diagnostics WHERE level = 'error'
                GROUP BY invocation_id
            ) dc_err ON i.id = dc_err.invocation_id
            LEFT JOIN (
                SELECT invocation_id, COUNT(*) as warning_count
                FROM build_diagnostics WHERE level = 'warning'
                GROUP BY invocation_id
            ) dc_warn ON i.id = dc_warn.invocation_id
            WHERE i.status IN ('success', 'failed', 'cancelled')
              AND i.started_at >= ?1
            ",
        );

        let mut params: Vec<String> = vec![cutoff];
        let mut idx = 2usize;

        if let Some(cmd) = command {
            sql.push_str(&format!(" AND i.command = ?{idx}"));
            params.push(cmd.to_string());
            idx += 1;
        }
        let _ = idx;
        sql.push_str(&format!(" ORDER BY i.id DESC LIMIT {limit}"));

        let mut stmt = self
            .conn
            .prepare(&sql)
            .context("failed to prepare timeline query")?;
        let refs: Vec<&dyn rusqlite::ToSql> =
            params.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
        let mut entries: Vec<InvocationTimelineEntry> = stmt
            .query_map(refs.as_slice(), |row| {
                let status_str: String = row.get(2)?;
                Ok(InvocationTimelineEntry {
                    id: row.get(0)?,
                    command: row.get(1)?,
                    status: parse_stored_invocation_status(status_str)?,
                    started_at: row.get(3)?,
                    duration_secs: row.get(4)?,
                    stage_count: row.get::<_, i64>(5)? as usize,
                    error_count: row.get::<_, i64>(6)? as usize,
                    warning_count: row.get::<_, i64>(7)? as usize,
                    diagnostic_delta: 0,
                })
            })
            .context("failed to execute timeline query")?
            .collect::<Result<Vec<_>, _>>()
            .context("failed to collect timeline entries")?;

        // Reverse to chronological order for delta computation, then re-reverse.
        entries.reverse();
        for i in 0..entries.len() {
            let curr_total = (entries[i].error_count + entries[i].warning_count) as i64;
            entries[i].diagnostic_delta = if i == 0 {
                0
            } else {
                let prev_total = (entries[i - 1].error_count + entries[i - 1].warning_count) as i64;
                curr_total - prev_total
            };
        }
        entries.reverse();
        Ok(entries)
    }

    /// I6: Group invocations into working sessions (consecutive runs < gap_minutes apart).
    pub fn get_working_sessions(
        &self,
        limit: usize,
        gap_minutes: u32,
    ) -> Result<Vec<WorkingSession>> {
        struct Row {
            command: String,
            started_at: String,
            finished_at: Option<String>,
            duration_secs: Option<f64>,
            status: String,
        }

        let mut stmt = self.conn.prepare(
            r"
            SELECT command, started_at, finished_at, duration_secs, status
            FROM invocations
            WHERE status IN ('success', 'failed', 'cancelled')
            ORDER BY started_at ASC
            LIMIT 2000
            ",
        )?;

        let rows: Vec<Row> = stmt
            .query_map([], |row| {
                Ok(Row {
                    command: row.get(0)?,
                    started_at: row.get(1)?,
                    finished_at: row.get(2)?,
                    duration_secs: row.get(3)?,
                    status: row.get(4)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        let gap_secs = i64::from(gap_minutes) * 60;
        let mut sessions: Vec<WorkingSession> = Vec::new();
        let mut current: Option<WorkingSession> = None;
        let mut prev_started: Option<String> = None;

        for row in &rows {
            let gap_exceeded = prev_started.as_deref().map_or(true, |prev| {
                let gap = time::OffsetDateTime::parse(
                    &row.started_at,
                    &time::format_description::well_known::Rfc3339,
                )
                .ok()
                .and_then(|curr| {
                    time::OffsetDateTime::parse(
                        prev,
                        &time::format_description::well_known::Rfc3339,
                    )
                    .ok()
                    .map(|p| (curr - p).whole_seconds())
                })
                .unwrap_or(i64::MAX);
                gap > gap_secs
            });

            if gap_exceeded {
                if let Some(s) = current.take() {
                    sessions.push(s);
                }
                current = Some(WorkingSession {
                    session_index: 0,
                    first_started: row.started_at.clone(),
                    last_finished: row.finished_at.clone(),
                    invocation_count: 1,
                    commands: vec![row.command.clone()],
                    total_duration_secs: row.duration_secs.unwrap_or(0.0),
                    success_count: usize::from(row.status == "success"),
                    failure_count: usize::from(row.status == "failed"),
                });
            } else if let Some(s) = current.as_mut() {
                s.invocation_count += 1;
                if row.finished_at.is_some() {
                    s.last_finished.clone_from(&row.finished_at);
                }
                if !s.commands.contains(&row.command) {
                    s.commands.push(row.command.clone());
                }
                s.total_duration_secs += row.duration_secs.unwrap_or(0.0);
                if row.status == "success" {
                    s.success_count += 1;
                }
                if row.status == "failed" {
                    s.failure_count += 1;
                }
            }
            prev_started = Some(row.started_at.clone());
        }
        if let Some(s) = current {
            sessions.push(s);
        }

        // Most recent first, assign 1-based indices, truncate.
        sessions.reverse();
        for (i, s) in sessions.iter_mut().enumerate() {
            s.session_index = i + 1;
        }
        sessions.truncate(limit);
        Ok(sessions)
    }

    /// I7: Get complete single-invocation picture (invocation + stages + diagnostics).
    pub fn get_invocation_full(&self, id: i64) -> Result<Option<InvocationFull>> {
        let inv = self
            .conn
            .query_row(
                r"SELECT id, command, subcommand, profile, args_json, git_commit, git_dirty,
                         started_at, finished_at, duration_secs, exit_code, status, host, cwd,
                         live_stage
                  FROM invocations WHERE id = ?1",
                params![id],
                row_to_invocation,
            )
            .optional()
            .context("failed to fetch invocation")?;

        let inv = match inv {
            Some(i) => i,
            None => return Ok(None),
        };

        let stages = self.get_stage_timings_for_invocation(id)?;

        let mut diag_stmt = self.conn.prepare(
            r"SELECT id, level, code, message, file_path, line, col, rendered, package,
                     fix_replacement, fix_applicability, fix_byte_start, fix_byte_end,
                     NULL as source_command, NULL as source_time
              FROM build_diagnostics
              WHERE invocation_id = ?1
              ORDER BY level, package, file_path",
        )?;
        let diagnostics: Vec<StoredDiagnostic> = diag_stmt
            .query_map(params![id], row_to_diagnostic_full)?
            .collect::<Result<Vec<_>, _>>()?;

        let error_count = diagnostics.iter().filter(|d| d.level == "error").count();
        let warning_count = diagnostics.iter().filter(|d| d.level == "warning").count();
        Ok(Some(InvocationFull {
            invocation: inv,
            stages,
            diagnostics,
            error_count,
            warning_count,
        }))
    }

    /// I2: Execute a read-only SQL query and return rows as JSON objects.
    ///
    /// Only SELECT / WITH / PRAGMA statements are accepted (checked syntactically).
    /// Results are returned as a vector of JSON maps, keyed by column name.
    pub fn run_readonly_query(
        &self,
        sql: &str,
    ) -> Result<Vec<serde_json::Map<String, serde_json::Value>>> {
        let trimmed = sql.trim().to_uppercase();
        if !trimmed.starts_with("SELECT")
            && !trimmed.starts_with("WITH")
            && !trimmed.starts_with("PRAGMA")
        {
            return Err(color_eyre::eyre::eyre!(
                "Only SELECT, WITH, and PRAGMA queries are permitted (got: {})",
                &sql[..sql.len().min(40)]
            ));
        }
        let mut stmt = self.conn.prepare(sql).wrap_err("failed to prepare query")?;
        let col_names: Vec<String> = stmt
            .column_names()
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        let rows = stmt
            .query_map([], |row| {
                let mut map = serde_json::Map::new();
                for (i, name) in col_names.iter().enumerate() {
                    let val: rusqlite::types::Value = row.get(i)?;
                    let json_val = match val {
                        rusqlite::types::Value::Null => serde_json::Value::Null,
                        rusqlite::types::Value::Integer(n) => serde_json::Value::Number(n.into()),
                        rusqlite::types::Value::Real(f) => serde_json::Number::from_f64(f)
                            .map(serde_json::Value::Number)
                            .unwrap_or(serde_json::Value::Null),
                        rusqlite::types::Value::Text(s) => serde_json::Value::String(s),
                        rusqlite::types::Value::Blob(_) => {
                            serde_json::Value::String("<blob>".to_string())
                        }
                    };
                    map.insert(name.clone(), json_val);
                }
                Ok(map)
            })
            .wrap_err("failed to execute query")?
            .collect::<Result<Vec<_>, _>>()
            .wrap_err("failed to collect query results")?;
        Ok(rows)
    }

    /// I2: Dump (table_name, CREATE TABLE sql) pairs for the history database schema.
    pub fn get_schema_dump(&self) -> Result<Vec<(String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT name, sql FROM sqlite_schema WHERE type = 'table' AND sql IS NOT NULL ORDER BY name",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Resolve an invocation identifier ('latest' or numeric ID string) to a concrete ID.
    pub fn resolve_invocation_id(
        &self,
        id_or_latest: &str,
        command: Option<&str>,
    ) -> Result<Option<i64>> {
        if id_or_latest == "latest" {
            let id = if let Some(cmd) = command {
                self.conn
                    .query_row(
                        r"SELECT id FROM invocations WHERE status IN ('success', 'failed')
                          AND command = ?1 ORDER BY id DESC LIMIT 1",
                        params![cmd],
                        |row| row.get(0),
                    )
                    .optional()?
            } else {
                self.conn
                    .query_row(
                        r"SELECT id FROM invocations WHERE status IN ('success', 'failed')
                          ORDER BY id DESC LIMIT 1",
                        [],
                        |row| row.get(0),
                    )
                    .optional()?
            };
            Ok(id)
        } else {
            let id = id_or_latest.parse::<i64>().map_err(|_| {
                color_eyre::eyre::eyre!(
                    "invalid invocation ID: '{id_or_latest}' (expected a number or 'latest')"
                )
            })?;
            Ok(Some(id))
        }
    }

    /// Get the invocation ID immediately before `before_id` for the same command (if given).
    pub fn get_previous_invocation_id(
        &self,
        before_id: i64,
        command: Option<&str>,
    ) -> Result<Option<i64>> {
        let id = if let Some(cmd) = command {
            self.conn
                .query_row(
                    r"SELECT id FROM invocations WHERE status IN ('success', 'failed')
                      AND id < ?1 AND command = ?2 ORDER BY id DESC LIMIT 1",
                    params![before_id, cmd],
                    |row| row.get(0),
                )
                .optional()?
        } else {
            self.conn
                .query_row(
                    r"SELECT id FROM invocations WHERE status IN ('success', 'failed')
                      AND id < ?1 ORDER BY id DESC LIMIT 1",
                    params![before_id],
                    |row| row.get(0),
                )
                .optional()?
        };
        Ok(id)
    }
}

// ─── I: Semantic Query Intelligence types ─────────────────────────────────────

/// Lifecycle status of a diagnostic across invocations (I3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LifecycleStatus {
    /// Only appeared in the latest invocation for this package.
    New,
    /// Present in 3+ invocations and still in the latest.
    Chronic,
    /// Appeared more than once but not chronic; still in the latest.
    Recurring,
    /// Was present before but NOT in the latest invocation.
    Resolved,
}

/// A diagnostic with its lifecycle status across invocations (I3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticLifecycle {
    pub package: Option<String>,
    pub level: String,
    pub code: Option<String>,
    pub message: String,
    pub status: LifecycleStatus,
    pub first_seen: i64,
    pub last_seen: i64,
    pub occurrence_count: usize,
}

/// One entry in the cross-invocation timeline view (I4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationTimelineEntry {
    pub id: i64,
    pub command: String,
    pub status: InvocationStatus,
    pub started_at: String,
    pub duration_secs: Option<f64>,
    pub stage_count: usize,
    pub error_count: usize,
    pub warning_count: usize,
    /// Change in (error + warning) count vs the previous timeline entry.
    pub diagnostic_delta: i64,
}

/// A contiguous working session: invocations grouped by < N min gaps (I6).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkingSession {
    pub session_index: usize,
    pub first_started: String,
    pub last_finished: Option<String>,
    pub invocation_count: usize,
    pub commands: Vec<String>,
    pub total_duration_secs: f64,
    pub success_count: usize,
    pub failure_count: usize,
}

/// Complete invocation picture: record + stages + diagnostics (I7).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationFull {
    pub invocation: Invocation,
    pub stages: Vec<StageTiming>,
    pub diagnostics: Vec<StoredDiagnostic>,
    pub error_count: usize,
    pub warning_count: usize,
}

/// Map a full diagnostic row (15 columns) to `StoredDiagnostic`.
fn row_to_diagnostic_full(row: &rusqlite::Row) -> rusqlite::Result<StoredDiagnostic> {
    Ok(StoredDiagnostic {
        id: row.get(0)?,
        level: row.get(1)?,
        code: row.get(2)?,
        message: row.get(3)?,
        file_path: row.get(4)?,
        line: row.get(5)?,
        col: row.get(6)?,
        rendered: row.get(7)?,
        package: row.get(8)?,
        fix_replacement: row.get(9)?,
        fix_applicability: row.get(10)?,
        fix_byte_start: row.get(11)?,
        fix_byte_end: row.get(12)?,
        source_command: row.get(13)?,
        source_time: row.get(14)?,
    })
}

/// Live progress snapshot for a running invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationProgress {
    pub invocation_id: i64,
    pub phase: Option<String>,
    pub step: Option<String>,
    /// 0.0–100.0, None if indeterminate
    pub pct_done: Option<f64>,
    pub items_done: Option<i64>,
    pub items_total: Option<i64>,
    pub updated_at: String,
}

/// Recorded timing for a single pipeline stage within an invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageTiming {
    pub invocation_id: i64,
    pub stage_name: String,
    pub started_at: String,
    pub duration_secs: f64,
    pub success: bool,
}

/// Map a SQLite row to a `BackgroundJob`.
///
/// Expected column order (0-indexed):
///   0: id, 1: invocation_id, 2: command, 3: args_json, 4: started_at,
///   5: pid, 6: stdout_path, 7: stderr_path, 8: job_status, 9: exit_code
fn row_to_background_job(row: &rusqlite::Row<'_>) -> rusqlite::Result<BackgroundJob> {
    let args_json: Option<String> = row.get(3)?;
    let started_at_str: String = row.get(4)?;
    let pid: Option<u32> = row.get(5)?;
    let job_status_str: String = row.get(8)?;
    Ok(BackgroundJob {
        id: row.get(0)?,
        invocation_id: row.get(1)?,
        command: row.get(2)?,
        args: args_json
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default(),
        started_at: OffsetDateTime::parse(
            &started_at_str,
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap_or_else(|_| OffsetDateTime::now_utc()),
        pid: pid.unwrap_or(0),
        stdout_path: row.get(6)?,
        stderr_path: row.get(7)?,
        job_status: JobLifecycleStatus::try_from_str(&job_status_str)
            .unwrap_or(JobLifecycleStatus::Orphaned),
        exit_code: row.get(9)?,
    })
}

/// A background job record from the history database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackgroundJob {
    /// Background job ID (`background_jobs.id`) — the process handle.
    pub id: i64,
    /// Invocation ID (`invocations.id`) — the durable execution record.
    pub invocation_id: Option<i64>,
    pub command: String,
    pub args: Vec<String>,
    pub started_at: OffsetDateTime,
    pub pid: u32,
    pub stdout_path: Option<String>,
    pub stderr_path: Option<String>,
    /// Process lifecycle status (running/completed/orphaned/killed).
    pub job_status: JobLifecycleStatus,
    pub exit_code: Option<i32>,
}

/// Live semantic test progress for an invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestProgress {
    pub total: Option<usize>,
    pub passed: usize,
    pub failed: usize,
    pub ignored: usize,
    pub completed: usize,
    pub last_test_name: Option<String>,
    pub updated_at: Option<String>,
}

/// A stored build diagnostic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredDiagnostic {
    pub id: i64,
    pub level: String,
    pub code: Option<String>,
    pub message: String,
    pub file_path: Option<String>,
    pub line: Option<u32>,
    pub col: Option<u32>,
    pub rendered: Option<String>,
    pub package: Option<String>,
    pub fix_replacement: Option<String>,
    pub fix_applicability: Option<String>,
    pub fix_byte_start: Option<u32>,
    pub fix_byte_end: Option<u32>,
    /// Source command that produced this diagnostic (e.g. "check")
    pub source_command: Option<String>,
    /// When the source invocation ran
    pub source_time: Option<String>,
}

/// Aggregate diagnostic counts by level (used by `status --summary`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiagnosticCounts {
    pub errors: usize,
    pub warnings: usize,
    /// Count of auto-fixable diagnostics (MachineApplicable applicability).
    pub fixable: usize,
}

impl DiagnosticCounts {
    #[must_use]
    pub fn total(&self) -> usize {
        self.errors + self.warnings
    }
}

/// Delta between two invocations' diagnostic sets (G1).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiagnosticDelta {
    /// Diagnostics present in `to` but not in `from` (newly appeared).
    pub new: Vec<StoredDiagnostic>,
    /// Diagnostics present in `from` but not in `to` (resolved/fixed).
    pub resolved: Vec<StoredDiagnostic>,
    /// Diagnostics present in both (persistent).
    pub persistent: Vec<StoredDiagnostic>,
}

/// Resource usage snapshot for a single invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUsage {
    pub command: String,
    pub started_at: String,
    pub duration_secs: Option<f64>,
    pub cpu_usage_avg: Option<f64>,
    pub memory_usage_max_mb: Option<f64>,
}

/// Stage timing summary entry (G2 — slowest stages view).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageStats {
    pub stage_name: String,
    pub avg_duration_secs: f64,
    pub max_duration_secs: f64,
    pub run_count: usize,
}

/// A single data point in a stage timing trend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageTrendPoint {
    pub invocation_id: i64,
    pub started_at: String,
    pub duration_secs: f64,
    pub success: bool,
}

/// A fix session: an invocation of `xtask fix` with before/after diagnostic snapshot (G3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixSession {
    pub invocation_id: i64,
    pub started_at: String,
    pub duration_secs: Option<f64>,
    pub pre_fix_errors: Option<i64>,
    pub pre_fix_warnings: Option<i64>,
    pub pre_fix_fixable: Option<i64>,
}

/// A single point in the diagnostic trend timeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticTrendPoint {
    pub invocation_id: i64,
    pub command: String,
    pub started_at: String,
    pub status: InvocationStatus,
    pub errors: usize,
    pub warnings: usize,
    pub total: usize,
}

/// Check if a process with the given PID is still running.
fn is_process_running(pid: u32) -> bool {
    #[cfg(unix)]
    {
        // On Unix, sending signal 0 checks if process exists
        unsafe { libc::kill(pid as i32, 0) == 0 }
    }
    #[cfg(not(unix))]
    {
        // On other platforms, assume running (best effort)
        true
    }
}

/// An invocation with its coordination fingerprint data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationWithFingerprint {
    pub id: i64,
    pub status: InvocationStatus,
    pub duration_secs: Option<f64>,
    pub tree_fingerprint: Option<String>,
    pub scope_key: Option<String>,
}

impl HistoryDb {
    /// R3: Compute the probability that `to_command` follows `from_command` within
    /// `window_mins` minutes, based on the `limit` most recent `from_command` successes.
    ///
    /// Returns a value 0.0–100.0 (percentage). Used for predictive compilation prefetch:
    /// if check→test transition is >70% likely, pre-compile tests while the developer
    /// reviews check output.
    ///
    /// Returns 0.0 when there is insufficient history.
    pub fn get_transition_probability(
        &self,
        from_command: &str,
        to_command: &str,
        window_mins: u32,
        limit: u32,
    ) -> Result<f64> {
        let window_str = format!("+{} seconds", window_mins * 60);

        // CTE: recent `from_command` successes, then count how many were followed by `to_command`
        let (total, followed): (i64, i64) = self
            .conn
            .query_row(
                r"
                WITH recent_from AS (
                    SELECT id, finished_at
                    FROM invocations
                    WHERE command = ?1
                      AND status = 'success'
                      AND finished_at IS NOT NULL
                    ORDER BY id DESC
                    LIMIT ?2
                )
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN EXISTS (
                        SELECT 1 FROM invocations next
                        WHERE next.command = ?3
                          AND next.id > rf.id
                          AND next.started_at > rf.finished_at
                          AND next.started_at <= datetime(rf.finished_at, ?4)
                    ) THEN 1 ELSE 0 END) AS followed
                FROM recent_from rf
                ",
                rusqlite::params![from_command, limit, to_command, window_str],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Option<i64>>(1)?.unwrap_or(0),
                    ))
                },
            )
            .unwrap_or((0, 0));

        if total == 0 {
            return Ok(0.0);
        }

        Ok((followed as f64 / total as f64) * 100.0)
    }
}

/// Statistics for a command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandStats {
    pub total: i64,
    pub successes: i64,
    pub failures: i64,
    pub avg_duration_secs: Option<f64>,
}

fn row_to_invocation(row: &rusqlite::Row) -> rusqlite::Result<Invocation> {
    let started_at_str: String = row.get(7)?;
    let finished_at_str: Option<String> = row.get(8)?;
    let status_str: String = row.get(11)?;

    Ok(Invocation {
        id: row.get(0)?,
        command: row.get(1)?,
        subcommand: row.get(2)?,
        profile: row.get(3)?,
        args_json: row.get(4)?,
        git_commit: row.get(5)?,
        git_dirty: row.get::<_, i32>(6)? != 0,
        started_at: OffsetDateTime::parse(
            &started_at_str,
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap_or_else(|_| OffsetDateTime::now_utc()),
        finished_at: finished_at_str.and_then(|s| {
            OffsetDateTime::parse(&s, &time::format_description::well_known::Rfc3339).ok()
        }),
        duration_secs: row.get(9)?,
        exit_code: row.get(10)?,
        status: parse_stored_invocation_status(status_str)?,
        host: row.get(12)?,
        cwd: row.get(13)?,
        live_stage: row.get(14)?,
    })
}

/// Get current git commit hash (short form).
fn get_git_commit() -> Option<String> {
    std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
}

/// Check if the git working directory has uncommitted changes.
fn is_git_dirty() -> bool {
    std::process::Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .is_some_and(|o| !o.stdout.is_empty())
}

/// Sandbox infrastructure metadata extracted from slog events in test output.
#[derive(Debug, Default)]
struct SandboxMeta {
    slot_name: Option<String>,
    slot_wait_ms: Option<i64>,
    cleanup_ms: Option<i64>,
}

/// Parse sandbox slog events from test output to extract infrastructure metadata.
///
/// Looks for `[sandbox:*] event=slot_acquired` lines and extracts:
/// - `slot` → slot_name (e.g., "sinex_test_pool_13")
/// - `duration_ms` → slot_wait_ms (total acquisition time including cleanup)
/// - `clean_ms` → cleanup_ms (cleanup time for dirty slots, absent for clean slots)
fn parse_sandbox_meta(output: &str) -> SandboxMeta {
    let mut meta = SandboxMeta::default();

    for line in output.lines() {
        if !line.contains("event=slot_acquired") {
            continue;
        }

        // Parse key=value pairs from the slog line
        for part in line.split_whitespace() {
            if let Some(val) = part.strip_prefix("slot=") {
                meta.slot_name = Some(val.to_string());
            } else if let Some(val) = part.strip_prefix("duration_ms=") {
                meta.slot_wait_ms = val.parse().ok();
            } else if let Some(val) = part.strip_prefix("clean_ms=") {
                meta.cleanup_ms = val.parse().ok();
            }
        }

        // Take the first slot_acquired event (the test's primary database)
        break;
    }

    meta
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn test_history_db_lifecycle() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-history.db");

        let db = HistoryDb::open(&db_path)?;

        // Start an invocation
        let id = db.start_invocation("test", Some("fast"), Some("fast"), None)?;
        assert!(id > 0);

        // Finish it
        db.finish_invocation(id, InvocationStatus::Success, Some(0), 1.5)?;

        // Query it
        let recent = db.get_recent(10, None)?;
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].command, "test");
        assert_eq!(recent[0].status, InvocationStatus::Success);

        // Get last
        let last = db.get_last("test")?;
        assert!(last.is_some());
        assert_eq!(last.unwrap().id, id);

        // Stats
        let stats = db.get_stats("test", 7)?;
        assert_eq!(stats.total, 1);
        assert_eq!(stats.successes, 1);
        Ok(())
    }

    #[sinex_test]
    async fn test_prune() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-prune.db");

        let db = HistoryDb::open(&db_path)?;

        // Create some invocations
        for _ in 0..5 {
            let id = db.start_invocation("check", None, None, None)?;
            db.finish_invocation(id, InvocationStatus::Success, Some(0), 0.1)?;
        }

        assert_eq!(db.count()?, 5);

        // Prune with 0 days should remove nothing (they're all recent)
        let pruned = db.prune(0)?;
        // All were created just now, so none should be pruned
        assert_eq!(pruned, 0);
        Ok(())
    }

    #[sinex_test]
    async fn test_get_recent_with_command_filter() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-filter.db");
        let db = HistoryDb::open(&db_path)?;

        // Create invocations with different commands
        let check_id = db.start_invocation("check", None, None, None)?;
        db.finish_invocation(check_id, InvocationStatus::Success, Some(0), 0.5)?;

        let test_id = db.start_invocation("test", None, None, None)?;
        db.finish_invocation(test_id, InvocationStatus::Success, Some(0), 1.0)?;

        let build_id = db.start_invocation("build", None, None, None)?;
        db.finish_invocation(build_id, InvocationStatus::Success, Some(0), 2.0)?;

        // Query without filter should return all 3
        let all = db.get_recent(10, None)?;
        assert_eq!(all.len(), 3);

        // Query with "test" filter should return only test invocation
        let test_only = db.get_recent(10, Some("test"))?;
        assert_eq!(test_only.len(), 1);
        assert_eq!(test_only[0].command, "test");

        // Query with "check" filter should return only check invocation
        let check_only = db.get_recent(10, Some("check"))?;
        assert_eq!(check_only.len(), 1);
        assert_eq!(check_only[0].command, "check");
        Ok(())
    }

    #[sinex_test]
    async fn test_get_last_returns_most_recent() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-last.db");
        let db = HistoryDb::open(&db_path)?;

        // Create 3 invocations for "check" command
        let id1 = db.start_invocation("check", None, None, None)?;
        db.finish_invocation(id1, InvocationStatus::Success, Some(0), 0.1)?;

        let id2 = db.start_invocation("check", None, None, None)?;
        db.finish_invocation(id2, InvocationStatus::Failed, Some(1), 0.2)?;

        let id3 = db.start_invocation("check", None, None, None)?;
        db.finish_invocation(id3, InvocationStatus::Success, Some(0), 0.3)?;

        // get_last should return the most recent (id3)
        let last = db.get_last("check")?;
        assert!(last.is_some());
        assert_eq!(last.unwrap().id, id3);
        Ok(())
    }

    #[sinex_test]
    async fn test_get_last_returns_none_for_unknown_command() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-last-none.db");
        let db = HistoryDb::open(&db_path)?;

        // Query for a command that doesn't exist
        let result = db.get_last("nonexistent")?;
        assert!(result.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_get_stats_counts_correctly() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-stats.db");
        let db = HistoryDb::open(&db_path)?;

        // Create 3 successful invocations
        for _ in 0..3 {
            let id = db.start_invocation("build", None, None, None)?;
            db.finish_invocation(id, InvocationStatus::Success, Some(0), 0.5)?;
        }

        // Create 2 failed invocations
        for _ in 0..2 {
            let id = db.start_invocation("build", None, None, None)?;
            db.finish_invocation(id, InvocationStatus::Failed, Some(1), 0.8)?;
        }

        // Get stats for last 7 days
        let stats = db.get_stats("build", 7)?;
        assert_eq!(stats.total, 5);
        assert_eq!(stats.successes, 3);
        assert_eq!(stats.failures, 2);
        assert!(stats.avg_duration_secs.is_some());
        Ok(())
    }

    #[sinex_test]
    async fn test_background_job_lifecycle() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-bg-job.db");
        let db = HistoryDb::open(&db_path)?;

        let stdout_path = dir.path().join("job1_stdout.log");
        let stderr_path = dir.path().join("job1_stderr.log");

        // Start a background job
        let (_inv_id, job_id) = db.start_background_job(
            "check",
            &["--all".to_string()],
            99999,
            &stdout_path,
            &stderr_path,
        )?;
        assert!(job_id > 0);

        // Should appear in active jobs
        let active = db.get_active_background_jobs()?;
        assert!(active.iter().any(|j| j.id == job_id));

        // Finish the job
        db.finish_background_job(job_id, JobLifecycleStatus::Completed, Some(0), 1.5, None, None)?;

        // Should no longer appear in active jobs
        let active = db.get_active_background_jobs()?;
        assert!(!active.iter().any(|j| j.id == job_id));
        Ok(())
    }

    #[sinex_test]
    async fn test_background_job_by_id() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-bg-id.db");
        let db = HistoryDb::open(&db_path)?;

        let stdout_path = dir.path().join("job2_stdout.log");
        let stderr_path = dir.path().join("job2_stderr.log");

        let (_inv_id, job_id) = db.start_background_job(
            "test",
            &["-p".to_string(), "sinex-primitives".to_string()],
            88888,
            &stdout_path,
            &stderr_path,
        )?;

        // Get job by id
        let job = db.get_background_job_by_id(job_id)?;
        assert!(job.is_some());
        assert_eq!(job.unwrap().id, job_id);

        // Non-existent id returns None
        let nonexistent = db.get_background_job_by_id(99999)?;
        assert!(nonexistent.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_background_job_logs() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-bg-logs.db");
        let db = HistoryDb::open(&db_path)?;

        let stdout_path = dir.path().join("job3_stdout.log");
        let stderr_path = dir.path().join("job3_stderr.log");

        // Create log files with content
        std::fs::write(&stdout_path, "test stdout output\nmultiline output")?;
        std::fs::write(&stderr_path, "test stderr output\nerror line")?;

        let (_inv_id, job_id) =
            db.start_background_job("check", &[], 77777, &stdout_path, &stderr_path)?;

        // Finish job with log files
        db.finish_background_job(
            job_id,
            JobLifecycleStatus::Completed,
            Some(0),
            0.5,
            Some(&stdout_path),
            Some(&stderr_path),
        )?;

        // Get logs
        let (stdout, stderr) = db.get_job_logs(job_id)?;
        assert!(stdout.is_some());
        assert!(stderr.is_some());
        assert_eq!(stdout.unwrap(), "test stdout output\nmultiline output");
        assert_eq!(stderr.unwrap(), "test stderr output\nerror line");
        Ok(())
    }

    #[sinex_test]
    async fn test_get_all_background_job_ids() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-all-ids.db");
        let db = HistoryDb::open(&db_path)?;

        // Start 3 background jobs
        let ids: Vec<i64> = (0..3)
            .map(|i| {
                let stdout = dir.path().join(format!("job{i}_stdout.log"));
                let stderr = dir.path().join(format!("job{i}_stderr.log"));
                let (_inv_id, job_id) =
                    db.start_background_job("build", &[], 66666 + i as u32, &stdout, &stderr)
                        .unwrap();
                job_id
            })
            .collect();

        // Get all job IDs
        let all_ids = db.get_all_background_job_ids()?;
        assert_eq!(all_ids.len(), 3);
        for id in ids {
            assert!(all_ids.contains(&id));
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_get_recent_background_jobs_respects_limit() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-recent-limit.db");
        let db = HistoryDb::open(&db_path)?;

        // Start 5 background jobs
        for i in 0..5 {
            let stdout = dir.path().join(format!("job5_{i}_stdout.log"));
            let stderr = dir.path().join(format!("job5_{i}_stderr.log"));
            db.start_background_job("test", &[], 55555 + i as u32, &stdout, &stderr)?; // returns (inv_id, job_id)
        }

        // Get only 3 most recent
        let recent = db.get_recent_background_jobs(3)?;
        assert_eq!(recent.len(), 3);

        // Get all 5
        let all = db.get_recent_background_jobs(10)?;
        assert_eq!(all.len(), 5);
        Ok(())
    }

    #[sinex_test]
    async fn test_record_and_get_diagnostics() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-diagnostics.db");
        let db = HistoryDb::open(&db_path)?;

        let inv_id = db.start_invocation("check", None, None, None)?;
        db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 0.5)?;

        // Record 3 diagnostics
        use crate::cargo_diagnostics::CompilerDiagnostic;
        db.record_diagnostic(
            inv_id,
            &CompilerDiagnostic {
                level: "warning".into(),
                code: Some("W001".into()),
                message: "unused variable".into(),
                file_path: Some("src/main.rs".into()),
                line: Some(10),
                column: Some(5),
                ..Default::default()
            },
        )?;

        db.record_diagnostic(
            inv_id,
            &CompilerDiagnostic {
                level: "error".into(),
                code: Some("E001".into()),
                message: "type mismatch".into(),
                file_path: Some("src/lib.rs".into()),
                line: Some(20),
                column: Some(15),
                ..Default::default()
            },
        )?;

        db.record_diagnostic(
            inv_id,
            &CompilerDiagnostic {
                level: "info".into(),
                message: "build complete".into(),
                ..Default::default()
            },
        )?;

        // Get all diagnostics
        let diags = db.get_diagnostics(inv_id)?;
        assert_eq!(diags.len(), 3);
        assert_eq!(diags[0].level, "warning");
        assert_eq!(diags[1].level, "error");
        assert_eq!(diags[2].level, "info");
        Ok(())
    }

    #[sinex_test]
    async fn test_get_recent_diagnostics_with_level_filter() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-diag-filter.db");
        let db = HistoryDb::open(&db_path)?;

        let inv_id = db.start_invocation("test", None, None, None)?;
        db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 1.0)?;

        // Record mixed diagnostics
        use crate::cargo_diagnostics::CompilerDiagnostic;
        db.record_diagnostic(
            inv_id,
            &CompilerDiagnostic {
                level: "warning".into(),
                message: "warning 1".into(),
                ..Default::default()
            },
        )?;
        db.record_diagnostic(
            inv_id,
            &CompilerDiagnostic {
                level: "error".into(),
                message: "error 1".into(),
                ..Default::default()
            },
        )?;
        db.record_diagnostic(
            inv_id,
            &CompilerDiagnostic {
                level: "error".into(),
                message: "error 2".into(),
                ..Default::default()
            },
        )?;
        db.record_diagnostic(
            inv_id,
            &CompilerDiagnostic {
                level: "info".into(),
                message: "info 1".into(),
                ..Default::default()
            },
        )?;

        // Get only errors
        let errors = db.get_recent_diagnostics_all(10, Some("error"), None, None, None)?;
        assert_eq!(errors.len(), 2);
        assert!(errors.iter().all(|d| d.level == "error"));

        // Get only warnings
        let warnings = db.get_recent_diagnostics_all(10, Some("warning"), None, None, None)?;
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].level, "warning");
        Ok(())
    }

    #[sinex_test]
    async fn test_get_recent_diagnostics_filtered_by_file_pattern() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-diag-file.db");
        let db = HistoryDb::open(&db_path)?;

        let inv_id = db.start_invocation("build", None, None, None)?;
        db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 2.0)?;

        // Record diagnostics with various file paths
        use crate::cargo_diagnostics::CompilerDiagnostic;
        db.record_diagnostic(
            inv_id,
            &CompilerDiagnostic {
                level: "error".into(),
                message: "error in main".into(),
                file_path: Some("src/main.rs".into()),
                line: Some(5),
                ..Default::default()
            },
        )?;

        db.record_diagnostic(
            inv_id,
            &CompilerDiagnostic {
                level: "error".into(),
                message: "error in lib".into(),
                file_path: Some("src/lib.rs".into()),
                line: Some(10),
                ..Default::default()
            },
        )?;

        db.record_diagnostic(
            inv_id,
            &CompilerDiagnostic {
                level: "warning".into(),
                message: "warning in tests".into(),
                file_path: Some("tests/integration.rs".into()),
                line: Some(15),
                ..Default::default()
            },
        )?;

        // Filter by "main" file pattern and error level
        let main_errors =
            db.get_recent_diagnostics_all(10, Some("error"), Some("main"), None, None)?;
        assert_eq!(main_errors.len(), 1);
        assert!(main_errors[0].file_path.as_ref().unwrap().contains("main"));

        // Filter by "src" pattern
        let src_diags = db.get_recent_diagnostics_all(10, None, Some("src"), None, None)?;
        assert_eq!(src_diags.len(), 2);
        assert!(
            src_diags
                .iter()
                .all(|d| d.file_path.as_ref().unwrap().contains("src"))
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_record_and_get_diagnostics_with_package_and_fix() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-diag-pkg-fix.db");
        let db = HistoryDb::open(&db_path)?;

        let inv_id = db.start_invocation("check", None, None, None)?;
        db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 1.0)?;

        // Record compiled packages so package-scoped supersession works
        db.record_compiled_packages(inv_id, &HashSet::from(["sinex-db".to_string()]))?;

        // Record a diagnostic with package and fix metadata
        use crate::cargo_diagnostics::CompilerDiagnostic;
        db.record_diagnostic(
            inv_id,
            &CompilerDiagnostic {
                level: "warning".into(),
                code: Some("W0042".into()),
                message: "unused import".into(),
                file_path: Some("crate/lib/sinex-db/src/lib.rs".into()),
                line: Some(10),
                column: Some(1),
                rendered: Some("warning[W0042]: unused import".into()),
                package: Some("sinex-db".into()),
                fix_replacement: Some(String::new()),
                fix_applicability: Some("MachineApplicable".into()),
                fix_byte_start: Some(42),
                fix_byte_end: Some(55),
                ..Default::default()
            },
        )?;

        // get_diagnostics: package and fix fields must be populated
        let diags = db.get_diagnostics(inv_id)?;
        assert_eq!(diags.len(), 1);
        let d = &diags[0];
        assert_eq!(d.package.as_deref(), Some("sinex-db"));
        assert_eq!(d.fix_replacement.as_deref(), Some(""));
        assert_eq!(d.fix_applicability.as_deref(), Some("MachineApplicable"));
        assert_eq!(d.fix_byte_start, Some(42));
        assert_eq!(d.fix_byte_end, Some(55));

        // get_current_diagnostics filtered by package
        let pkg_diags = db.get_current_diagnostics(None, None, Some("sinex-db"), None, false)?;
        assert_eq!(pkg_diags.len(), 1);
        assert_eq!(pkg_diags[0].package.as_deref(), Some("sinex-db"));

        // get_current_diagnostics fixable_only=true — should include this diagnostic
        let fixable = db.get_current_diagnostics(None, None, None, None, true)?;
        assert_eq!(fixable.len(), 1);
        assert_eq!(
            fixable[0].fix_applicability.as_deref(),
            Some("MachineApplicable")
        );

        Ok(())
    }

    #[sinex_test]
    async fn test_record_diagnostic_ignores_exact_duplicates() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-diag-no-duplicates.db");
        let db = HistoryDb::open(&db_path)?;

        let inv_id = db.start_invocation("check", None, None, None)?;
        db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 1.0)?;
        db.record_compiled_packages(inv_id, &HashSet::from(["sinex-db".to_string()]))?;

        use crate::cargo_diagnostics::CompilerDiagnostic;
        let diag = CompilerDiagnostic {
            level: "warning".into(),
            code: Some("async_fn_in_trait".into()),
            message: "duplicate warning".into(),
            file_path: Some("crate/lib/sinex-db/src/repositories/common.rs".into()),
            line: Some(112),
            column: Some(5),
            package: Some("sinex-db".into()),
            ..Default::default()
        };

        db.record_diagnostic(inv_id, &diag)?;
        db.record_diagnostic(inv_id, &diag)?;
        db.record_diagnostic(inv_id, &diag)?;

        assert_eq!(db.get_diagnostics(inv_id)?.len(), 1);
        assert_eq!(
            db.get_current_diagnostics(None, None, Some("sinex-db"), None, false)?
                .len(),
            1
        );

        let counts = db.get_current_diagnostic_counts()?;
        assert_eq!(counts.warnings, 1);
        assert_eq!(counts.errors, 0);
        Ok(())
    }

    #[sinex_test]
    async fn test_record_test_result() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-result.db");
        let db = HistoryDb::open(&db_path)?;

        let inv_id = db.start_invocation("test", None, None, None)?;
        db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 5.0)?;

        // Record a test result
        db.record_test_result(
            inv_id,
            "test_parsing",
            "sinex-primitives",
            "passed",
            0.5,
            Some("output log"),
            "nextest",
        )?;

        // Verify it was stored via direct SQL query
        let count: i64 = db.conn.query_row(
            "SELECT COUNT(*) FROM test_results WHERE invocation_id = ?1",
            params![inv_id],
            |row| row.get(0),
        )?;
        assert_eq!(count, 1);

        // Verify the stored data
        let (test_name, package, status): (String, String, String) = db.conn.query_row(
            "SELECT test_name, package, status FROM test_results WHERE invocation_id = ?1",
            params![inv_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        assert_eq!(test_name, "test_parsing");
        assert_eq!(package, "sinex-primitives");
        assert_eq!(status, "passed");
        Ok(())
    }

    #[sinex_test]
    async fn test_update_job_pid_and_paths() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-update-job.db");
        let db = HistoryDb::open(&db_path)?;

        let original_stdout = dir.path().join("original_stdout.log");
        let original_stderr = dir.path().join("original_stderr.log");

        let (_inv_id, job_id) =
            db.start_background_job("build", &[], 33333, &original_stdout, &original_stderr)?;

        // Update pid
        db.update_job_pid(job_id, 44444)?;

        // Update paths
        let new_stdout = dir.path().join("new_stdout.log");
        let new_stderr = dir.path().join("new_stderr.log");
        db.update_job_paths(job_id, &new_stdout, &new_stderr)?;

        // Retrieve and verify updates
        let job = db.get_background_job_by_id(job_id)?.unwrap();
        assert_eq!(job.pid, 44444);
        assert_eq!(
            job.stdout_path.as_ref().unwrap(),
            &new_stdout.display().to_string()
        );
        assert_eq!(
            job.stderr_path.as_ref().unwrap(),
            &new_stderr.display().to_string()
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_sandbox_meta_slot_acquired() -> TestResult<()> {
        // Clean slot (no clean_ms field)
        let output = "[sandbox:INFO] event=slot_acquired slot=sinex_test_pool_5 duration_ms=42 pid=12345 clean=true\ntest output here";
        let meta = parse_sandbox_meta(output);
        assert_eq!(meta.slot_name.as_deref(), Some("sinex_test_pool_5"));
        assert_eq!(meta.slot_wait_ms, Some(42));
        assert!(meta.cleanup_ms.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_sandbox_meta_dirty_slot() -> TestResult<()> {
        // Dirty slot with cleanup time
        let output = "some earlier output\n[sandbox:INFO] event=slot_acquired slot=sinex_test_pool_13 duration_ms=381 clean_ms=352 pid=917199 clean=false\nmore output";
        let meta = parse_sandbox_meta(output);
        assert_eq!(meta.slot_name.as_deref(), Some("sinex_test_pool_13"));
        assert_eq!(meta.slot_wait_ms, Some(381));
        assert_eq!(meta.cleanup_ms, Some(352));
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_sandbox_meta_no_slog_events() -> TestResult<()> {
        let output = "plain test output\nno sandbox events here";
        let meta = parse_sandbox_meta(output);
        assert!(meta.slot_name.is_none());
        assert!(meta.slot_wait_ms.is_none());
        assert!(meta.cleanup_ms.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_test_metadata_columns_available() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-meta-columns.db");
        let db = HistoryDb::open(&db_path)?;

        // Verify we can insert with the new columns
        let id = db.start_invocation("test", None, None, None)?;
        db.record_test_result(id, "my_test", "my_pkg", "pass", 1.0, None, "nextest")?;

        // Back-fill with metadata
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            "my_test".to_string(),
            crate::nextest::junit::JunitTestMeta {
                output: Some("[sandbox:INFO] event=slot_acquired slot=pool_1 duration_ms=50 pid=1 clean=true\ntest out".to_string()),
                classname: Some("my-crate".to_string()),
                failure_message: None,
                failure_type: None,
            },
        );
        let updated = db.backfill_test_metadata(id, &metadata)?;
        assert_eq!(updated, 1);

        // Verify the sandbox metadata was extracted
        let slot_name: Option<String> = db.conn.query_row(
            "SELECT slot_name FROM test_results WHERE invocation_id = ?1",
            params![id],
            |row| row.get(0),
        )?;
        assert_eq!(slot_name.as_deref(), Some("pool_1"));

        let slot_wait: Option<i64> = db.conn.query_row(
            "SELECT slot_wait_ms FROM test_results WHERE invocation_id = ?1",
            params![id],
            |row| row.get(0),
        )?;
        assert_eq!(slot_wait, Some(50));

        // Verify classname updated the package
        let pkg: String = db.conn.query_row(
            "SELECT package FROM test_results WHERE invocation_id = ?1",
            params![id],
            |row| row.get(0),
        )?;
        assert_eq!(pkg, "my-crate");

        Ok(())
    }

    #[sinex_test]
    async fn invalid_invocation_status_is_rejected() -> TestResult<()> {
        let err = InvocationStatus::try_from_str("mystery").expect_err("should fail");
        assert!(err.to_string().contains("invalid invocation status"));
        Ok(())
    }
}

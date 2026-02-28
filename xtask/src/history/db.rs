//! `SQLite` database operations for xtask history.

use color_eyre::eyre::{Result, WrapErr};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sinex_primitives::temporal::Timestamp;
use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use time::OffsetDateTime;

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
    fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Success => "success",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    fn from_str(s: &str) -> Self {
        match s {
            "running" => Self::Running,
            "success" => Self::Success,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            _ => Self::Failed,
        }
    }
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
}

/// Handle to the history `SQLite` database.
pub struct HistoryDb {
    pub(super) conn: Connection,
    /// Guard to ensure job columns migration runs at most once per instance.
    job_columns_ensured: AtomicBool,
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
                && meta.len() == 0 {
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
                job_columns_ensured: AtomicBool::new(false),
            };
            db.init_schema()?;
            return Ok(db);
        }

        let db = Self {
            conn,
            job_columns_ensured: AtomicBool::new(false),
        };
        db.init_schema()?;
        db.cleanup_stale_invocations();
        Ok(db)
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
                cwd TEXT NOT NULL
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

            -- Background job tracking columns (added for jobs unification)
            -- Note: These columns may not exist in older DBs, so we use ALTER TABLE conditionally

            -- Per-stage timing within a command invocation (fmt, clippy, forbidden, compile, preflight)
            CREATE TABLE IF NOT EXISTS stage_timings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER NOT NULL REFERENCES invocations(id) ON DELETE CASCADE,
                stage_name TEXT NOT NULL,
                started_at TEXT NOT NULL,
                duration_secs REAL NOT NULL,
                success INTEGER NOT NULL DEFAULT 1
            );

            -- Indices for common queries
            CREATE INDEX IF NOT EXISTS idx_invocations_command ON invocations(command);
            CREATE INDEX IF NOT EXISTS idx_invocations_started ON invocations(started_at);
            CREATE INDEX IF NOT EXISTS idx_invocations_status ON invocations(status);
            -- Composite index for the most common query pattern (status --summary, history stats)
            CREATE INDEX IF NOT EXISTS idx_invocations_command_status_started
                ON invocations(command, status, started_at);
            CREATE INDEX IF NOT EXISTS idx_test_results_name ON test_results(test_name);
            CREATE INDEX IF NOT EXISTS idx_test_results_status ON test_results(status);
            CREATE INDEX IF NOT EXISTS idx_test_results_invocation ON test_results(invocation_id);
            CREATE INDEX IF NOT EXISTS idx_diagnostics_invocation ON build_diagnostics(invocation_id);
            CREATE INDEX IF NOT EXISTS idx_stage_timings_invocation ON stage_timings(invocation_id);
            ",
        )?;
        Ok(())
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
        let host = gethostname::gethostname().to_string_lossy().into_owned();
        let cwd = std::env::current_dir()
            .map(|p| p.display().to_string())
            .unwrap_or_default();
        let started_at = Timestamp::now().format_rfc3339();

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
                  AND started_at < datetime('now', '-10 minutes')
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
                finished_at = datetime('now'),
                duration_secs = (julianday('now') - julianday(started_at)) * 86400
            WHERE status = 'running'
              AND started_at < datetime('now', '-10 minutes')
            ",
            [],
        );
        if let Ok(count) = cleaned
            && count > 0 {
                eprintln!(
                    "ℹ️  Cleaned up {count} stale 'running' invocation(s) older than 10 minutes"
                );
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
    }

    /// Finish a background job and store its log content in the DB.
    ///
    /// This reads the log files and stores content in DB. Log files are preserved
    /// on disk for direct inspection and are only removed by `xtask jobs prune`.
    pub fn finish_background_job(
        &self,
        id: i64,
        status: InvocationStatus,
        exit_code: Option<i32>,
        duration_secs: f64,
        stdout_path: Option<&std::path::Path>,
        stderr_path: Option<&std::path::Path>,
    ) -> Result<()> {
        let finished_at = Timestamp::now().format_rfc3339();

        // Read log files
        let stdout_content = stdout_path.and_then(|p| std::fs::read_to_string(p).ok());
        let stderr_content = stderr_path.and_then(|p| std::fs::read_to_string(p).ok());

        self.conn.execute(
            r"
            UPDATE invocations
            SET finished_at = ?1, duration_secs = ?2, exit_code = ?3, status = ?4,
                stdout_content = ?5, stderr_content = ?6
            WHERE id = ?7
            ",
            params![
                finished_at,
                duration_secs,
                exit_code,
                status.as_str(),
                stdout_content,
                stderr_content,
                id
            ],
        )?;

        // Keep log files on disk alongside DB storage for direct inspection.
        // Files are only removed by `xtask jobs prune`.

        Ok(())
    }

    /// Get log content for a completed job.
    pub fn get_job_logs(&self, id: i64) -> Result<(Option<String>, Option<String>)> {
        self.ensure_job_columns()?;
        let result = self.conn.query_row(
            "SELECT stdout_content, stderr_content FROM invocations WHERE id = ?1",
            params![id],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                ))
            },
        )?;
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
                   started_at, finished_at, duration_secs, exit_code, status, host, cwd
            FROM invocations
            WHERE command = ?1
            ORDER BY started_at DESC
            LIMIT ?2
            "
        } else {
            r"
            SELECT id, command, subcommand, profile, args_json, git_commit, git_dirty,
                   started_at, finished_at, duration_secs, exit_code, status, host, cwd
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

    /// Get the most recent invocation for a command.
    pub fn get_last(&self, command: &str) -> Result<Option<Invocation>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT id, command, subcommand, profile, args_json, git_commit, git_dirty,
                   started_at, finished_at, duration_secs, exit_code, status, host, cwd
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
        self.ensure_job_columns()?;

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
                        status: InvocationStatus::from_str(&status_str),
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
        self.ensure_job_columns()?;
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
    pub fn record_test_result(
        &self,
        invocation_id: i64,
        test_name: &str,
        package: &str,
        status: &str,
        duration_secs: f64,
        output: Option<&str>,
    ) -> Result<()> {
        self.conn.execute(
            r"
            INSERT INTO test_results (invocation_id, test_name, package, status, duration_secs, output)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ",
            params![invocation_id, test_name, package, status, duration_secs, output],
        )?;
        Ok(())
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

    /// Record system resource metrics for an invocation.
    pub fn record_system_metrics(
        &self,
        invocation_id: i64,
        cpu_usage_avg: f32,
        memory_usage_max_mb: f64,
    ) -> Result<()> {
        // The columns might not exist if migration failed/was skipped, so we ignore errors here for robustness
        // or we rely on the fact that we ran the migration manually via sqlite3.
        let _ = self.conn.execute(
            r"
            UPDATE invocations
            SET cpu_usage_avg = ?1, memory_usage_max_mb = ?2
            WHERE id = ?3
            ",
            params![cpu_usage_avg, memory_usage_max_mb, invocation_id],
        );
        Ok(())
    }

    /// Get count of invocations.
    pub fn count(&self) -> Result<usize> {
        let count: usize = self
            .conn
            .query_row("SELECT COUNT(*) FROM invocations", [], |row| row.get(0))?;
        Ok(count)
    }

    // ============ Background Job Methods (Phase 3: Jobs Unification) ============

    /// Ensure the background job columns exist (for schema migration).
    ///
    /// Runs at most once per `HistoryDb` instance to avoid repeated ALTER TABLE overhead.
    pub fn ensure_job_columns(&self) -> Result<()> {
        // Fast path: already ensured this instance
        if self.job_columns_ensured.load(Ordering::Relaxed) {
            return Ok(());
        }

        // Add columns if they don't exist (SQLite doesn't support IF NOT EXISTS for columns)
        let columns_to_add = [
            ("pid", "INTEGER"),
            ("is_background", "INTEGER DEFAULT 0"),
            ("stdout_path", "TEXT"),
            ("stderr_path", "TEXT"),
            ("stdout_content", "TEXT"),
            ("stderr_content", "TEXT"),
            ("cpu_usage_avg", "REAL"),
            ("memory_usage_max_mb", "REAL"),
            ("tree_fingerprint", "TEXT"),
            ("scope_key", "TEXT"),
        ];

        for (col_name, col_type) in columns_to_add {
            let _ = self.conn.execute(
                &format!("ALTER TABLE invocations ADD COLUMN {col_name} {col_type}"),
                [],
            );
            // Ignore errors (column likely already exists)
        }

        // Create index for background job queries
        let _ = self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_invocations_background ON invocations(is_background, status) WHERE is_background = 1",
            [],
        );

        // Create index for coordinator fingerprint lookups
        let _ = self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_invocations_fingerprint ON invocations(command, tree_fingerprint, scope_key)",
            [],
        );

        self.job_columns_ensured.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Start a background job invocation. Returns the invocation ID.
    pub fn start_background_job(
        &self,
        command: &str,
        args: &[String],
        pid: u32,
        stdout_path: &Path,
        stderr_path: &Path,
    ) -> Result<i64> {
        self.ensure_job_columns()?;

        let args_json = serde_json::to_string(args)?;
        let git_commit = get_git_commit();
        let git_dirty = is_git_dirty();
        let host = gethostname::gethostname().to_string_lossy().into_owned();
        let cwd = std::env::current_dir()
            .map(|p| p.display().to_string())
            .unwrap_or_default();
        let started_at = Timestamp::now().format_rfc3339();

        self.conn.execute(
            r"
            INSERT INTO invocations (command, args_json, git_commit, git_dirty, started_at, host, cwd, status, pid, is_background, stdout_path, stderr_path)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'running', ?8, 1, ?9, ?10)
            ",
            params![
                command,
                args_json,
                git_commit,
                git_dirty,
                started_at,
                host,
                cwd,
                pid,
                stdout_path.display().to_string(),
                stderr_path.display().to_string()
            ],
        )?;

        Ok(self.conn.last_insert_rowid())
    }

    /// Get all active (running) background jobs.
    pub fn get_active_background_jobs(&self) -> Result<Vec<BackgroundJob>> {
        self.ensure_job_columns()?;

        let mut stmt = self.conn.prepare(
            r"
            SELECT id, command, args_json, started_at, pid, stdout_path, stderr_path, status, exit_code
            FROM invocations
            WHERE is_background = 1 AND status = 'running'
            ORDER BY started_at DESC
            ",
        )?;

        let rows = stmt.query_map([], |row| {
            let args_json: Option<String> = row.get(2)?;
            let started_at_str: String = row.get(3)?;
            let pid: Option<u32> = row.get(4)?;

            Ok(BackgroundJob {
                id: row.get(0)?,
                command: row.get(1)?,
                args: args_json
                    .and_then(|s| serde_json::from_str(&s).ok())
                    .unwrap_or_default(),
                started_at: OffsetDateTime::parse(
                    &started_at_str,
                    &time::format_description::well_known::Rfc3339,
                )
                .unwrap_or_else(|_| OffsetDateTime::now_utc()),
                pid: pid.unwrap_or(0),
                stdout_path: row.get(5)?,
                stderr_path: row.get(6)?,
                status: InvocationStatus::from_str(&row.get::<_, String>(7)?),
                exit_code: row.get(8)?,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect background jobs")
    }

    /// Get a single background job by ID (O(1) direct SQL lookup).
    pub fn get_background_job_by_id(&self, id: i64) -> Result<Option<BackgroundJob>> {
        self.ensure_job_columns()?;

        self.conn
            .query_row(
                r"
            SELECT id, command, args_json, started_at, pid, stdout_path, stderr_path, status, exit_code
            FROM invocations
            WHERE id = ?1 AND is_background = 1
            ",
                params![id],
                |row| {
                    let args_json: Option<String> = row.get(2)?;
                    let started_at_str: String = row.get(3)?;
                    let pid: Option<u32> = row.get(4)?;

                    Ok(BackgroundJob {
                        id: row.get(0)?,
                        command: row.get(1)?,
                        args: args_json
                            .and_then(|s| serde_json::from_str(&s).ok())
                            .unwrap_or_default(),
                        started_at: OffsetDateTime::parse(
                            &started_at_str,
                            &time::format_description::well_known::Rfc3339,
                        )
                        .unwrap_or_else(|_| OffsetDateTime::now_utc()),
                        pid: pid.unwrap_or(0),
                        stdout_path: row.get(5)?,
                        stderr_path: row.get(6)?,
                        status: InvocationStatus::from_str(&row.get::<_, String>(7)?),
                        exit_code: row.get(8)?,
                    })
                },
            )
            .optional()
            .context("failed to get background job by id")
    }

    /// Get all background job IDs (for prune orphan directory cleanup).
    pub fn get_all_background_job_ids(&self) -> Result<HashSet<i64>> {
        self.ensure_job_columns()?;

        let mut stmt = self
            .conn
            .prepare("SELECT id FROM invocations WHERE is_background = 1")?;

        let rows = stmt.query_map([], |row| row.get::<_, i64>(0))?;

        let mut ids = HashSet::new();
        for id in rows {
            ids.insert(id?);
        }
        Ok(ids)
    }

    /// Get recent background jobs (including completed ones).
    pub fn get_recent_background_jobs(&self, limit: usize) -> Result<Vec<BackgroundJob>> {
        self.ensure_job_columns()?;

        let mut stmt = self.conn.prepare(
            r"
            SELECT id, command, args_json, started_at, pid, stdout_path, stderr_path, status, exit_code
            FROM invocations
            WHERE is_background = 1
            ORDER BY started_at DESC
            LIMIT ?1
            ",
        )?;

        let rows = stmt.query_map(params![limit], |row| {
            let args_json: Option<String> = row.get(2)?;
            let started_at_str: String = row.get(3)?;
            let pid: Option<u32> = row.get(4)?;

            Ok(BackgroundJob {
                id: row.get(0)?,
                command: row.get(1)?,
                args: args_json
                    .and_then(|s| serde_json::from_str(&s).ok())
                    .unwrap_or_default(),
                started_at: OffsetDateTime::parse(
                    &started_at_str,
                    &time::format_description::well_known::Rfc3339,
                )
                .unwrap_or_else(|_| OffsetDateTime::now_utc()),
                pid: pid.unwrap_or(0),
                stdout_path: row.get(5)?,
                stderr_path: row.get(6)?,
                status: InvocationStatus::from_str(&row.get::<_, String>(7)?),
                exit_code: row.get(8)?,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect background jobs")
    }

    /// Update a background job's PID (used when process is spawned).
    pub fn update_job_pid(&self, id: i64, pid: u32) -> Result<()> {
        self.conn.execute(
            "UPDATE invocations SET pid = ?1 WHERE id = ?2",
            params![pid, id],
        )?;
        Ok(())
    }

    /// Update a background job's log file paths.
    pub fn update_job_paths(
        &self,
        id: i64,
        stdout_path: &std::path::Path,
        stderr_path: &std::path::Path,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE invocations SET stdout_path = ?1, stderr_path = ?2 WHERE id = ?3",
            params![
                stdout_path.display().to_string(),
                stderr_path.display().to_string(),
                id
            ],
        )?;
        Ok(())
    }

    /// Check if a background job's process is still running.
    pub fn is_job_running(&self, id: i64) -> Result<bool> {
        let pid: Option<u32> = self.conn.query_row(
            "SELECT pid FROM invocations WHERE id = ?1 AND is_background = 1",
            params![id],
            |row| row.get(0),
        )?;

        if let Some(pid) = pid {
            // Check if process is still running
            Ok(is_process_running(pid))
        } else {
            Ok(false)
        }
    }

    // ============ Diagnostics Methods (Phase 4: Build Diagnostics Capture) ============

    /// Ensure diagnostic columns exist (for schema migration from older DBs).
    ///
    /// Adds the package and fix metadata columns to `build_diagnostics`, and creates the
    /// `invocation_packages` table if it doesn't exist.
    pub fn ensure_diagnostic_columns(&self) -> Result<()> {
        let columns_to_add = [
            ("package", "TEXT"),
            ("fix_replacement", "TEXT"),
            ("fix_applicability", "TEXT"),
            ("fix_byte_start", "INTEGER"),
            ("fix_byte_end", "INTEGER"),
        ];

        for (col_name, col_type) in columns_to_add {
            let _ = self.conn.execute(
                &format!("ALTER TABLE build_diagnostics ADD COLUMN {col_name} {col_type}"),
                [],
            );
        }

        self.conn.execute_batch(
            r"
            CREATE TABLE IF NOT EXISTS invocation_packages (
                invocation_id INTEGER NOT NULL REFERENCES invocations(id) ON DELETE CASCADE,
                package TEXT NOT NULL,
                PRIMARY KEY (invocation_id, package)
            );
            ",
        )?;

        Ok(())
    }

    /// Record a build diagnostic (warning/error).
    pub fn record_diagnostic(
        &self,
        invocation_id: i64,
        level: &str,
        code: Option<&str>,
        message: &str,
        file_path: Option<&str>,
        line: Option<u32>,
        col: Option<u32>,
        rendered: Option<&str>,
        package: Option<&str>,
        fix_replacement: Option<&str>,
        fix_applicability: Option<&str>,
        fix_byte_start: Option<u32>,
        fix_byte_end: Option<u32>,
    ) -> Result<()> {
        self.conn.execute(
            r"
            INSERT INTO build_diagnostics
                (invocation_id, level, code, message, file_path, line, col, rendered,
                 package, fix_replacement, fix_applicability, fix_byte_start, fix_byte_end)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            ",
            params![
                invocation_id, level, code, message, file_path, line, col, rendered,
                package, fix_replacement, fix_applicability, fix_byte_start, fix_byte_end,
            ],
        )?;
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
        // Build the CTE query dynamically based on filters
        let mut query = String::from(
            r"
            WITH latest_per_package AS (
                SELECT ip.package, MAX(i.id) as latest_inv_id
                FROM invocation_packages ip
                JOIN invocations i ON ip.invocation_id = i.id
                WHERE i.status IN ('success', 'failed')
            ",
        );

        // Command filter in CTE
        let mut param_idx = 1;
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(cmd) = command_filter {
            query.push_str(&format!(" AND i.command = ?{param_idx}"));
            params_vec.push(Box::new(cmd.to_string()));
            param_idx += 1;
        }

        query.push_str(
            r"
                GROUP BY ip.package
            )
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
        let params_refs: Vec<&dyn rusqlite::ToSql> = params_vec
            .iter()
            .map(std::convert::AsRef::as_ref)
            .collect();

        let rows = stmt.query_map(rusqlite::params_from_iter(params_refs), row_to_diagnostic_full)?;
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
        let query = r"
            WITH latest_per_package AS (
                SELECT ip.package, MAX(i.id) as latest_inv_id
                FROM invocation_packages ip
                JOIN invocations i ON ip.invocation_id = i.id
                WHERE i.status IN ('success', 'failed')
                GROUP BY ip.package
            )
            SELECT
                COALESCE(SUM(CASE WHEN d.level = 'error' THEN 1 ELSE 0 END), 0) as errors,
                COALESCE(SUM(CASE WHEN d.level = 'warning' THEN 1 ELSE 0 END), 0) as warnings
            FROM build_diagnostics d
            JOIN latest_per_package lpp ON d.package = lpp.package
                                       AND d.invocation_id = lpp.latest_inv_id
        ";

        let mut stmt = self.conn.prepare(query)?;
        let counts = stmt.query_row([], |row| {
            Ok(DiagnosticCounts {
                errors: row.get::<_, i64>(0)? as usize,
                warnings: row.get::<_, i64>(1)? as usize,
            })
        })?;

        Ok(counts)
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
            Ok(DiagnosticTrendPoint {
                invocation_id: row.get(0)?,
                command: row.get(1)?,
                started_at: row.get(2)?,
                status: row.get(3)?,
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

        query.push_str(&format!(" ORDER BY i.started_at DESC, d.id DESC LIMIT ?{param_idx}"));
        params_vec.push(Box::new(limit as i64));

        let mut stmt = self.conn.prepare(&query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> = params_vec
            .iter()
            .map(std::convert::AsRef::as_ref)
            .collect();

        let rows = stmt.query_map(rusqlite::params_from_iter(params_refs), row_to_diagnostic_full)?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }
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

/// A background job record from the history database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackgroundJob {
    pub id: i64,
    pub command: String,
    pub args: Vec<String>,
    pub started_at: OffsetDateTime,
    pub pid: u32,
    pub stdout_path: Option<String>,
    pub stderr_path: Option<String>,
    pub status: InvocationStatus,
    pub exit_code: Option<i32>,
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
}

impl DiagnosticCounts {
    pub fn total(&self) -> usize {
        self.errors + self.warnings
    }
}

/// A single point in the diagnostic trend timeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticTrendPoint {
    pub invocation_id: i64,
    pub command: String,
    pub started_at: String,
    pub status: String,
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
        status: InvocationStatus::from_str(&status_str),
        host: row.get(12)?,
        cwd: row.get(13)?,
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    fn test_history_db_lifecycle() -> TestResult<()> {
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
    fn test_prune() -> TestResult<()> {
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
    fn test_get_recent_with_command_filter() -> TestResult<()> {
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
    fn test_get_last_returns_most_recent() -> TestResult<()> {
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
    fn test_get_last_returns_none_for_unknown_command() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-last-none.db");
        let db = HistoryDb::open(&db_path)?;

        // Query for a command that doesn't exist
        let result = db.get_last("nonexistent")?;
        assert!(result.is_none());
        Ok(())
    }

    #[sinex_test]
    fn test_get_stats_counts_correctly() -> TestResult<()> {
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
    fn test_background_job_lifecycle() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-bg-job.db");
        let db = HistoryDb::open(&db_path)?;

        let stdout_path = dir.path().join("job1_stdout.log");
        let stderr_path = dir.path().join("job1_stderr.log");

        // Start a background job
        let job_id = db.start_background_job(
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
        db.finish_background_job(job_id, InvocationStatus::Success, Some(0), 1.5, None, None)?;

        // Should no longer appear in active jobs
        let active = db.get_active_background_jobs()?;
        assert!(!active.iter().any(|j| j.id == job_id));
        Ok(())
    }

    #[sinex_test]
    fn test_background_job_by_id() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-bg-id.db");
        let db = HistoryDb::open(&db_path)?;

        let stdout_path = dir.path().join("job2_stdout.log");
        let stderr_path = dir.path().join("job2_stderr.log");

        let job_id = db.start_background_job(
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
    fn test_background_job_logs() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-bg-logs.db");
        let db = HistoryDb::open(&db_path)?;

        let stdout_path = dir.path().join("job3_stdout.log");
        let stderr_path = dir.path().join("job3_stderr.log");

        // Create log files with content
        std::fs::write(&stdout_path, "test stdout output\nmultiline output")?;
        std::fs::write(&stderr_path, "test stderr output\nerror line")?;

        let job_id = db.start_background_job("check", &[], 77777, &stdout_path, &stderr_path)?;

        // Finish job with log files
        db.finish_background_job(
            job_id,
            InvocationStatus::Success,
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
    fn test_get_all_background_job_ids() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-all-ids.db");
        let db = HistoryDb::open(&db_path)?;

        // Start 3 background jobs
        let ids: Vec<i64> = (0..3)
            .map(|i| {
                let stdout = dir.path().join(format!("job{i}_stdout.log"));
                let stderr = dir.path().join(format!("job{i}_stderr.log"));
                db.start_background_job("build", &[], 66666 + i as u32, &stdout, &stderr)
                    .unwrap()
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
    fn test_get_recent_background_jobs_respects_limit() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-recent-limit.db");
        let db = HistoryDb::open(&db_path)?;

        // Start 5 background jobs
        for i in 0..5 {
            let stdout = dir.path().join(format!("job5_{i}_stdout.log"));
            let stderr = dir.path().join(format!("job5_{i}_stderr.log"));
            db.start_background_job("test", &[], 55555 + i as u32, &stdout, &stderr)?;
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
    fn test_record_and_get_diagnostics() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-diagnostics.db");
        let db = HistoryDb::open(&db_path)?;

        let inv_id = db.start_invocation("check", None, None, None)?;
        db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 0.5)?;

        // Record 3 diagnostics
        db.record_diagnostic(
            inv_id,
            "warning",
            Some("W001"),
            "unused variable",
            Some("src/main.rs"),
            Some(10),
            Some(5),
            None,
            None,
            None,
            None,
            None,
            None,
        )?;

        db.record_diagnostic(
            inv_id,
            "error",
            Some("E001"),
            "type mismatch",
            Some("src/lib.rs"),
            Some(20),
            Some(15),
            None,
            None,
            None,
            None,
            None,
            None,
        )?;

        db.record_diagnostic(
            inv_id,
            "info",
            None,
            "build complete",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
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
    fn test_get_recent_diagnostics_with_level_filter() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-diag-filter.db");
        let db = HistoryDb::open(&db_path)?;

        let inv_id = db.start_invocation("test", None, None, None)?;
        db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 1.0)?;

        // Record mixed diagnostics
        db.record_diagnostic(inv_id, "warning", None, "warning 1", None, None, None, None, None, None, None, None, None)?;
        db.record_diagnostic(inv_id, "error", None, "error 1", None, None, None, None, None, None, None, None, None)?;
        db.record_diagnostic(inv_id, "error", None, "error 2", None, None, None, None, None, None, None, None, None)?;
        db.record_diagnostic(inv_id, "info", None, "info 1", None, None, None, None, None, None, None, None, None)?;

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
    fn test_get_recent_diagnostics_filtered_by_file_pattern() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-diag-file.db");
        let db = HistoryDb::open(&db_path)?;

        let inv_id = db.start_invocation("build", None, None, None)?;
        db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 2.0)?;

        // Record diagnostics with various file paths
        db.record_diagnostic(
            inv_id,
            "error",
            None,
            "error in main",
            Some("src/main.rs"),
            Some(5),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;

        db.record_diagnostic(
            inv_id,
            "error",
            None,
            "error in lib",
            Some("src/lib.rs"),
            Some(10),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;

        db.record_diagnostic(
            inv_id,
            "warning",
            None,
            "warning in tests",
            Some("tests/integration.rs"),
            Some(15),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;

        // Filter by "main" file pattern and error level
        let main_errors = db.get_recent_diagnostics_all(10, Some("error"), Some("main"), None, None)?;
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
    fn test_record_and_get_diagnostics_with_package_and_fix() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-diag-pkg-fix.db");
        let db = HistoryDb::open(&db_path)?;

        let inv_id = db.start_invocation("check", None, None, None)?;
        db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 1.0)?;

        // Record compiled packages so package-scoped supersession works
        db.record_compiled_packages(inv_id, &HashSet::from(["sinex-db".to_string()]))?;

        // Record a diagnostic with package and fix metadata
        db.record_diagnostic(
            inv_id,
            "warning",
            Some("W0042"),
            "unused import",
            Some("crate/lib/sinex-db/src/lib.rs"),
            Some(10),
            Some(1),
            Some("warning[W0042]: unused import"),
            Some("sinex-db"),
            Some(""),
            Some("MachineApplicable"),
            Some(42),
            Some(55),
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
        assert_eq!(fixable[0].fix_applicability.as_deref(), Some("MachineApplicable"));

        Ok(())
    }

    #[sinex_test]
    fn test_record_test_result() -> TestResult<()> {
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
    fn test_ensure_job_columns_idempotent() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-ensure-columns.db");
        let db = HistoryDb::open(&db_path)?;

        // Call ensure_job_columns multiple times - should not error
        db.ensure_job_columns()?;
        db.ensure_job_columns()?;
        db.ensure_job_columns()?;

        // Verify we can still use background job functionality
        let stdout = dir.path().join("ensure_stdout.log");
        let stderr = dir.path().join("ensure_stderr.log");
        let job_id = db.start_background_job("check", &[], 44444, &stdout, &stderr)?;
        assert!(job_id > 0);
        Ok(())
    }

    #[sinex_test]
    fn test_update_job_pid_and_paths() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-update-job.db");
        let db = HistoryDb::open(&db_path)?;

        let original_stdout = dir.path().join("original_stdout.log");
        let original_stderr = dir.path().join("original_stderr.log");

        let job_id =
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
}

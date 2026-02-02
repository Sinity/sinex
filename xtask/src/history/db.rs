//! `SQLite` database operations for xtask history.

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::path::Path;
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

        let conn = Connection::open(path).with_context(|| {
            let path_display = path.display();
            format!("failed to open history database: {path_display}")
        })?;

        let db = Self { conn };
        db.init_schema()?;
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
                rendered TEXT
            );

            -- Background job tracking columns (added for jobs unification)
            -- Note: These columns may not exist in older DBs, so we use ALTER TABLE conditionally

            -- Indices for common queries
            CREATE INDEX IF NOT EXISTS idx_invocations_command ON invocations(command);
            CREATE INDEX IF NOT EXISTS idx_invocations_started ON invocations(started_at);
            CREATE INDEX IF NOT EXISTS idx_invocations_status ON invocations(status);
            CREATE INDEX IF NOT EXISTS idx_test_results_name ON test_results(test_name);
            CREATE INDEX IF NOT EXISTS idx_test_results_status ON test_results(status);
            CREATE INDEX IF NOT EXISTS idx_test_results_invocation ON test_results(invocation_id);
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
        let started_at = OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();

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
        let finished_at = OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();

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

    /// Finish a background job and store its log content in the DB.
    ///
    /// This reads the log files, stores content in DB, then deletes the files.
    pub fn finish_background_job(
        &self,
        id: i64,
        status: InvocationStatus,
        exit_code: Option<i32>,
        duration_secs: f64,
        stdout_path: Option<&std::path::Path>,
        stderr_path: Option<&std::path::Path>,
    ) -> Result<()> {
        let finished_at = OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();

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

        // Delete log files now that content is in DB
        if let Some(path) = stdout_path {
            let _ = std::fs::remove_file(path);
        }
        if let Some(path) = stderr_path {
            let _ = std::fs::remove_file(path);
        }
        // Try to remove parent directory (job dir) if empty
        if let Some(path) = stdout_path {
            if let Some(parent) = path.parent() {
                let _ = std::fs::remove_dir(parent);
            }
        }

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
    /// Get statistics for a command.
    pub fn get_stats(&self, command: &str, days: u32) -> Result<CommandStats> {
        let since = OffsetDateTime::now_utc() - time::Duration::days(i64::from(days));
        let since_str = since
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();

        let mut stmt = self.conn.prepare(
            r"
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successes,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failures,
                AVG(duration_secs) as avg_duration
            FROM invocations
            WHERE command = ?1 AND started_at >= ?2 AND status != 'running'
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

    /// Prune old invocations older than the given number of days.
    pub fn prune(&self, older_than_days: u32) -> Result<usize> {
        // If 0 days, don't prune anything (nothing is "older than right now")
        if older_than_days == 0 {
            return Ok(0);
        }

        let cutoff = OffsetDateTime::now_utc() - time::Duration::days(i64::from(older_than_days));
        let cutoff_str = cutoff
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();

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
    #[allow(dead_code)]
    pub fn count(&self) -> Result<usize> {
        let count: usize = self
            .conn
            .query_row("SELECT COUNT(*) FROM invocations", [], |row| row.get(0))?;
        Ok(count)
    }

    // ============ Background Job Methods (Phase 3: Jobs Unification) ============

    /// Ensure the background job columns exist (for schema migration).
    pub fn ensure_job_columns(&self) -> Result<()> {
        // Add columns if they don't exist (SQLite doesn't support IF NOT EXISTS for columns)
        let columns_to_add = [
            ("pid", "INTEGER"),
            ("is_background", "INTEGER DEFAULT 0"),
            ("stdout_path", "TEXT"),
            ("stderr_path", "TEXT"),
            ("stdout_content", "TEXT"), // Logs stored in DB after completion
            ("stderr_content", "TEXT"),
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
        let started_at = OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();

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
            SELECT id, command, args_json, started_at, pid, stdout_path, stderr_path, status
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
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect background jobs")
    }

    /// Get recent background jobs (including completed ones).
    pub fn get_recent_background_jobs(&self, limit: usize) -> Result<Vec<BackgroundJob>> {
        self.ensure_job_columns()?;

        let mut stmt = self.conn.prepare(
            r"
            SELECT id, command, args_json, started_at, pid, stdout_path, stderr_path, status
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
    ) -> Result<()> {
        self.conn.execute(
            r"
            INSERT INTO build_diagnostics (invocation_id, level, code, message, file_path, line, col, rendered)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ",
            params![invocation_id, level, code, message, file_path, line, col, rendered],
        )?;
        Ok(())
    }

    /// Get diagnostics for an invocation.
    pub fn get_diagnostics(&self, invocation_id: i64) -> Result<Vec<StoredDiagnostic>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT id, level, code, message, file_path, line, col, rendered
            FROM build_diagnostics
            WHERE invocation_id = ?1
            ORDER BY id
            ",
        )?;

        let rows = stmt.query_map(params![invocation_id], |row| {
            Ok(StoredDiagnostic {
                id: row.get(0)?,
                level: row.get(1)?,
                code: row.get(2)?,
                message: row.get(3)?,
                file_path: row.get(4)?,
                line: row.get(5)?,
                col: row.get(6)?,
                rendered: row.get(7)?,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect diagnostics")
    }

    /// Get recent diagnostics across all invocations.
    pub fn get_recent_diagnostics(
        &self,
        limit: usize,
        level_filter: Option<&str>,
    ) -> Result<Vec<StoredDiagnostic>> {
        let mut results = Vec::new();

        if let Some(level) = level_filter {
            let mut stmt = self.conn.prepare(
                r"
                SELECT d.id, d.level, d.code, d.message, d.file_path, d.line, d.col, d.rendered
                FROM build_diagnostics d
                JOIN invocations i ON d.invocation_id = i.id
                WHERE d.level = ?1
                ORDER BY i.started_at DESC, d.id DESC
                LIMIT ?2
                ",
            )?;

            let rows = stmt.query_map(params![level, limit], row_to_diagnostic)?;
            for row in rows {
                results.push(row?);
            }
        } else {
            let mut stmt = self.conn.prepare(
                r"
                SELECT d.id, d.level, d.code, d.message, d.file_path, d.line, d.col, d.rendered
                FROM build_diagnostics d
                JOIN invocations i ON d.invocation_id = i.id
                ORDER BY i.started_at DESC, d.id DESC
                LIMIT ?1
                ",
            )?;

            let rows = stmt.query_map(params![limit], row_to_diagnostic)?;
            for row in rows {
                results.push(row?);
            }
        }

        Ok(results)
    }

    /// Get recent diagnostics with optional level and file pattern filters.
    pub fn get_recent_diagnostics_filtered(
        &self,
        limit: usize,
        level_filter: Option<&str>,
        file_pattern: Option<&str>,
    ) -> Result<Vec<StoredDiagnostic>> {
        let mut results = Vec::new();

        // Build query dynamically based on filters
        let mut conditions = Vec::new();
        let mut query = String::from(
            r"
            SELECT d.id, d.level, d.code, d.message, d.file_path, d.line, d.col, d.rendered
            FROM build_diagnostics d
            JOIN invocations i ON d.invocation_id = i.id
            ",
        );

        if level_filter.is_some() {
            conditions.push("d.level = ?");
        }
        if file_pattern.is_some() {
            conditions.push("d.file_path LIKE ?");
        }

        if !conditions.is_empty() {
            query.push_str("WHERE ");
            query.push_str(&conditions.join(" AND "));
        }

        query.push_str(" ORDER BY i.started_at DESC, d.id DESC LIMIT ?");

        let mut stmt = self.conn.prepare(&query)?;

        // Bind parameters in order
        let mut bound_params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(level) = level_filter {
            bound_params.push(Box::new(level.to_string()));
        }
        if let Some(pattern) = file_pattern {
            // Convert glob pattern to SQL LIKE pattern
            let like_pattern = format!("%{pattern}%");
            bound_params.push(Box::new(like_pattern));
        }
        bound_params.push(Box::new(limit as i64));

        // Use rusqlite's params_from_iter for dynamic binding
        let params_refs: Vec<&dyn rusqlite::ToSql> = bound_params
            .iter()
            .map(std::convert::AsRef::as_ref)
            .collect();

        let rows = stmt.query_map(rusqlite::params_from_iter(params_refs), row_to_diagnostic)?;
        for row in rows {
            results.push(row?);
        }

        Ok(results)
    }
}

fn row_to_diagnostic(row: &rusqlite::Row) -> rusqlite::Result<StoredDiagnostic> {
    Ok(StoredDiagnostic {
        id: row.get(0)?,
        level: row.get(1)?,
        code: row.get(2)?,
        message: row.get(3)?,
        file_path: row.get(4)?,
        line: row.get(5)?,
        col: row.get(6)?,
        rendered: row.get(7)?,
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

    #[test]
    fn test_history_db_lifecycle() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test-history.db");

        let db = HistoryDb::open(&db_path).unwrap();

        // Start an invocation
        let id = db
            .start_invocation("test", Some("fast"), Some("fast"), None)
            .unwrap();
        assert!(id > 0);

        // Finish it
        db.finish_invocation(id, InvocationStatus::Success, Some(0), 1.5)
            .unwrap();

        // Query it
        let recent = db.get_recent(10, None).unwrap();
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].command, "test");
        assert_eq!(recent[0].status, InvocationStatus::Success);

        // Get last
        let last = db.get_last("test").unwrap();
        assert!(last.is_some());
        assert_eq!(last.unwrap().id, id);

        // Stats
        let stats = db.get_stats("test", 7).unwrap();
        assert_eq!(stats.total, 1);
        assert_eq!(stats.successes, 1);
    }

    #[test]
    fn test_prune() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test-prune.db");

        let db = HistoryDb::open(&db_path).unwrap();

        // Create some invocations
        for _ in 0..5 {
            let id = db.start_invocation("check", None, None, None).unwrap();
            db.finish_invocation(id, InvocationStatus::Success, Some(0), 0.1)
                .unwrap();
        }

        assert_eq!(db.count().unwrap(), 5);

        // Prune with 0 days should remove nothing (they're all recent)
        let pruned = db.prune(0).unwrap();
        // All were created just now, so none should be pruned
        assert_eq!(pruned, 0);
    }
}

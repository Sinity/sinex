//! `SQLite` database operations for xtask history.

use super::query::InvocationQuery;
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

pub(crate) fn non_zombie_cancel_filter(column_prefix: &str) -> String {
    format!(
        "NOT ({column_prefix}status = 'cancelled' \
        AND ({column_prefix}cancel_reason IS NULL \
             OR {column_prefix}cancel_reason IN (\
                'stale_pid', \
                'watchdog_timeout', \
                'zombie_reaped', \
                'zombie_escaped_watchdog'\
             )))"
    )
}
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};

mod background;
mod diagnostics;
mod git;
mod impact;
mod integrity;
mod process;
mod rows;
mod sandbox_meta;
mod schema;
mod types;
use diagnostics::row_to_diagnostic_full;
pub use diagnostics::{
    DiagnosticCounts, DiagnosticDelta, DiagnosticLifecycle, DiagnosticTrendPoint, LifecycleStatus,
    StoredDiagnostic,
};
use git::current_git_snapshot;
use integrity::{
    format_preserved_history_artifact_destinations, preserve_history_artifacts_for_recreation,
    refresh_history_integrity_stamp, should_run_history_integrity_check,
};
use process::{
    StaleInvocationCandidate, background_watchdog_escape_threshold_secs, history_process_is_alive,
    is_process_running, try_reap_zombie_pid,
};
pub(crate) use rows::parse_stored_invocation_status;
pub(super) use rows::row_to_invocation;
use rows::{
    format_history_timestamp, format_invocation_timestamp, parse_invocation_timestamp,
    row_to_background_job, row_to_proof_evidence, row_to_test_proof_unit,
};
use sandbox_meta::parse_sandbox_meta;
pub(super) use schema::{HISTORY_DB_SCHEMA_VERSION, HistoryDbOpenMode};
use schema::{
    SQLITE_LOCK_RETRY_ATTEMPTS, SQLITE_LOCK_RETRY_BASE_DELAY, SQLITE_LOCK_RETRY_MAX_DELAY,
    SQLITE_STALE_CLEANUP_BUSY_TIMEOUT,
};
use sinex_primitives::temporal::Timestamp;
pub use types::{
    BackgroundJob, CommandStats, DriftGuardBypass, ExerciseResultRow, ExerciseRunRow, FixSession,
    ImpactAuditRunRow, Invocation, InvocationFull, InvocationProgress, InvocationStatus,
    InvocationTimelineEntry, InvocationWithFingerprint, JobLifecycleStatus, ProofEvidence,
    ResourceUsage, StagePressure, StageStats, StageTiming, StageTrendPoint, TestProofUnit,
    TraceEventRow, WorkingSession, WrapperEventRow,
};

use std::collections::HashSet;
use std::path::Path;
use time::OffsetDateTime;

fn capture_working_directory(current_dir: std::io::Result<std::path::PathBuf>) -> String {
    match current_dir {
        Ok(path) => path.display().to_string(),
        Err(error) => format!("<unavailable: {error}>"),
    }
}

fn validate_finite_duration_secs(context: &str, duration_secs: f64) -> Result<()> {
    if duration_secs.is_finite() {
        return Ok(());
    }

    Err(color_eyre::eyre::eyre!(
        "{context} has non-finite duration_secs: {duration_secs}"
    ))
}

fn normalize_junit_classname_package(classname: &str) -> Option<String> {
    classname
        .split("::")
        .next()
        .map(str::trim)
        .map(|package| package.replace('_', "-"))
        .filter(|package| !package.is_empty())
}

fn is_sqlite_lock_error(error: &color_eyre::Report) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<rusqlite::Error>()
            .and_then(|error| match error {
                rusqlite::Error::SqliteFailure(inner, _) => Some(inner.code),
                _ => None,
            })
            .is_some_and(|code| {
                matches!(
                    code,
                    rusqlite::ffi::ErrorCode::DatabaseBusy
                        | rusqlite::ffi::ErrorCode::DatabaseLocked
                )
            })
    })
}

fn is_recoverable_history_schema_version_error(error: &color_eyre::Report) -> bool {
    error
        .chain()
        .any(|cause| match cause.downcast_ref::<rusqlite::Error>() {
            Some(rusqlite::Error::SqliteFailure(inner, _)) => matches!(
                inner.code,
                rusqlite::ffi::ErrorCode::DatabaseCorrupt | rusqlite::ffi::ErrorCode::NotADatabase
            ),
            Some(rusqlite::Error::FromSqlConversionFailure(..)) => true,
            _ => false,
        })
}

fn sqlite_integrity_pragma_ok(conn: &Connection, pragma: &str) -> bool {
    conn.query_row(pragma, [], |row| row.get::<_, String>(0))
        .is_ok_and(|result| result == "ok")
}

fn with_sqlite_lock_retry<T, F>(action: &str, mut operation: F) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    let mut delay = SQLITE_LOCK_RETRY_BASE_DELAY;
    let mut last_lock_error = None;

    for attempt in 0..SQLITE_LOCK_RETRY_ATTEMPTS {
        match operation() {
            Ok(result) => return Ok(result),
            Err(error) if is_sqlite_lock_error(&error) => {
                last_lock_error = Some(error);
                if attempt + 1 == SQLITE_LOCK_RETRY_ATTEMPTS {
                    break;
                }
                std::thread::sleep(delay);
                delay = std::cmp::min(delay.saturating_mul(2), SQLITE_LOCK_RETRY_MAX_DELAY);
            }
            Err(error) => return Err(error).wrap_err_with(|| format!("failed to {action}")),
        }
    }

    Err(last_lock_error.expect("lock retry should preserve last error")).wrap_err_with(|| {
        format!("failed to {action} after {SQLITE_LOCK_RETRY_ATTEMPTS} lock retries")
    })
}

/// Emitted once per process (via `OnceLock`) when a read command accesses synthetic data.
static SYNTHETIC_WARNING_EMITTED: std::sync::OnceLock<()> = std::sync::OnceLock::new();

/// Handle to the history `SQLite` database.
pub struct HistoryDb {
    pub(super) conn: Connection,
    /// True if the database contains synthetic (seeded) data.
    pub is_synthetic: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StaleCleanupOutcome {
    Ran,
    SkippedLockHeld,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InvocationSelector {
    Latest,
    Previous,
    Current,
    InvocationId(i64),
    BackgroundJobId(i64),
}

impl HistoryDb {
    /// Open or create the history database at the given path.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_schema_version_probe(
            path,
            HistoryDbOpenMode::Persistent,
            Self::schema_version,
        )
    }

    /// Open an existing history database for read-only observational queries.
    ///
    /// Query surfaces like `xtask status`, `xtask history`, and `xtask analytics`
    /// should not pay integrity sweeps or stale-cleanup work just to read recent
    /// rows. If the database does not exist yet, return an empty in-memory view.
    pub fn open_query(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Self::open_in_memory();
        }

        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .with_context(|| {
                let path_display = path.display();
                format!("failed to open history database for query: {path_display}")
            })?;
        Self::configure_connection(&conn, HistoryDbOpenMode::Query)?;

        let mut db = Self {
            conn,
            is_synthetic: false,
        };
        let current_version = db
            .schema_version()
            .context("failed to read history DB schema version for query")?;
        if current_version != HISTORY_DB_SCHEMA_VERSION {
            color_eyre::eyre::bail!(
                "history DB schema v{current_version} != v{HISTORY_DB_SCHEMA_VERSION}; query open requires a compatible database"
            );
        }
        db.is_synthetic = db.check_synthetic()?;
        Ok(db)
    }

    /// Open an isolated in-memory history database.
    ///
    /// This keeps test and scratch workflows off the filesystem durability
    /// path while still exercising the real schema and query logic.
    pub fn open_in_memory() -> Result<Self> {
        let conn =
            Connection::open_in_memory().context("failed to open in-memory history database")?;
        Self::configure_connection(&conn, HistoryDbOpenMode::Ephemeral)?;
        let db = Self {
            conn,
            is_synthetic: false,
        };
        db.init_schema()?;
        db.ensure_compat_schema()?;
        db.set_schema_version(HISTORY_DB_SCHEMA_VERSION)?;
        Ok(db)
    }

    fn open_with_schema_version_probe<F>(
        path: &Path,
        mode: HistoryDbOpenMode,
        schema_version_probe: F,
    ) -> Result<Self>
    where
        F: FnOnce(&Self) -> Result<i32>,
    {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                let parent_display = parent.display();
                format!("failed to create directory: {parent_display}")
            })?;
        }

        let mut db_existed = path.exists();

        // Detect and recover from corrupted (0-byte) database files.
        // SQLite treats a 0-byte file as valid (empty DB) but our WAL/schema
        // setup may leave it in an inconsistent state. Preserve and recreate.
        if db_existed
            && let Ok(meta) = std::fs::metadata(path)
            && meta.len() == 0
        {
            let backups = preserve_history_artifacts_for_recreation(path, "empty-history-db")?;
            eprintln!(
                "⚠️  History database at {} is empty (0 bytes); preserved artifacts at [{}] and recreating",
                path.display(),
                format_preserved_history_artifact_destinations(&backups)
            );
            db_existed = false;
        }

        let conn = Connection::open(path).with_context(|| {
            let path_display = path.display();
            format!("failed to open history database: {path_display}")
        })?;

        Self::configure_connection(&conn, mode)?;

        // Fresh databases do not need integrity sweeps, schema-version probing,
        // or stale-invocation cleanup. This fast path keeps temp/ephemeral
        // history stores cheap and deterministic.
        if !db_existed {
            let db = Self {
                conn,
                is_synthetic: false,
            };
            db.init_schema()?;
            db.ensure_compat_schema()?;
            db.set_schema_version(HISTORY_DB_SCHEMA_VERSION)?;
            refresh_history_integrity_stamp(path, OffsetDateTime::now_utc());
            return Ok(db);
        }

        let now = OffsetDateTime::now_utc();
        if should_run_history_integrity_check(path, now) {
            // Integrity checks are expensive on large history databases. Run them
            // only on a periodic maintenance cadence instead of taxing every
            // tracked xtask command.
            let integrity_ok = sqlite_integrity_pragma_ok(&conn, "PRAGMA quick_check")
                || sqlite_integrity_pragma_ok(&conn, "PRAGMA integrity_check");
            if !integrity_ok {
                drop(conn);
                let backups =
                    preserve_history_artifacts_for_recreation(path, "integrity-check-failure")?;
                eprintln!(
                    "⚠️  History database at {} failed integrity check; preserved artifacts at [{}] and recreating",
                    path.display(),
                    format_preserved_history_artifact_destinations(&backups)
                );
                let conn = Connection::open(path).with_context(|| {
                    format!("failed to recreate history database: {}", path.display())
                })?;
                Self::configure_connection(&conn, mode)?;
                let db = Self {
                    conn,
                    is_synthetic: false,
                };
                db.init_schema()?;
                db.ensure_compat_schema()?;
                db.set_schema_version(HISTORY_DB_SCHEMA_VERSION)?;
                refresh_history_integrity_stamp(path, OffsetDateTime::now_utc());
                return Ok(db);
            }
            refresh_history_integrity_stamp(path, now);
        }

        let mut db = Self {
            conn,
            is_synthetic: false,
        };
        let current_version = match schema_version_probe(&db) {
            Ok(version) => version,
            Err(error) if is_recoverable_history_schema_version_error(&error) => {
                drop(db);
                let backups =
                    preserve_history_artifacts_for_recreation(path, "schema-version-read-failure")?;
                eprintln!(
                    "⚠️  History DB schema version read failed at {}; preserved artifacts at [{}] and recreating",
                    path.display(),
                    format_preserved_history_artifact_destinations(&backups)
                );
                let conn = Connection::open(path).with_context(|| {
                    format!(
                        "failed to recreate history database after unreadable schema version: {}",
                        path.display()
                    )
                })?;
                Self::configure_connection(&conn, mode)?;
                let recreated = Self {
                    conn,
                    is_synthetic: false,
                };
                recreated.init_schema()?;
                recreated.ensure_compat_schema()?;
                recreated.set_schema_version(HISTORY_DB_SCHEMA_VERSION)?;
                refresh_history_integrity_stamp(path, OffsetDateTime::now_utc());
                return Ok(recreated);
            }
            Err(error) => {
                return Err(error).context("failed to read history DB schema version");
            }
        };
        if current_version != HISTORY_DB_SCHEMA_VERSION {
            // Schema mismatch — preserve the prior DB by renaming it to a
            // versioned backup, then create a fresh DB at the live path.
            // History accumulates the user's dev-loop record across weeks
            // and months; never silently overwrite or drop it.  The rename
            // also moves the original off the live path atomically so the
            // recreated DB starts on a clean inode without inheriting any
            // mid-corruption state from the previous schema.
            if current_version != 0 {
                drop(db);
                let backup_path = path.with_extension(format!("db.v{current_version}.bak"));
                match std::fs::rename(path, &backup_path) {
                    Ok(()) => eprintln!(
                        "⚠️  History DB schema v{current_version} != v{HISTORY_DB_SCHEMA_VERSION}, \
                         renamed to {} and creating fresh DB",
                        backup_path.display()
                    ),
                    Err(e) => {
                        eprintln!(
                            "⚠️  History DB schema v{current_version} != v{HISTORY_DB_SCHEMA_VERSION}, \
                             rename to backup failed ({e}); refusing to drop the live DB"
                        );
                        return Err(e).context(
                            "could not preserve history DB before schema upgrade; refusing to wipe",
                        );
                    }
                }
                // Move auxiliary SQLite/runtime artifacts so the recreated
                // DB does not pick up the old WAL/SHM contents.
                for ext in ["db-wal", "db-shm", "db.integrity.json", "cleanup.lock"] {
                    let aux = path.with_extension(ext);
                    if aux.exists() {
                        let _ = std::fs::rename(
                            &aux,
                            aux.with_extension(format!("{ext}.v{current_version}.bak")),
                        );
                    }
                }
                let conn = Connection::open(path).with_context(|| {
                    format!(
                        "failed to create fresh history database after schema upgrade: {}",
                        path.display()
                    )
                })?;
                Self::configure_connection(&conn, mode)?;
                let recreated = Self {
                    conn,
                    is_synthetic: false,
                };
                recreated.init_schema()?;
                recreated.ensure_compat_schema()?;
                recreated.set_schema_version(HISTORY_DB_SCHEMA_VERSION)?;
                refresh_history_integrity_stamp(path, OffsetDateTime::now_utc());
                db = recreated;
            } else {
                // current_version == 0 means the DB was just created and is empty;
                // nothing to preserve.
                db.init_schema()?;
                db.ensure_compat_schema()?;
                db.set_schema_version(HISTORY_DB_SCHEMA_VERSION)?;
                refresh_history_integrity_stamp(path, OffsetDateTime::now_utc());
            }
        }
        db.ensure_compat_schema()?;
        db.is_synthetic = db.check_synthetic()?;
        if let Err(error) = db.with_busy_timeout(
            SQLITE_STALE_CLEANUP_BUSY_TIMEOUT,
            HistoryDbOpenMode::Persistent,
            || db.cleanup_stale_invocations_on_open(path),
        ) {
            if is_sqlite_lock_error(&error) {
                eprintln!(
                    "⚠️  History DB is busy; skipping stale invocation cleanup for now: {error:#}"
                );
            } else {
                return Err(error).context("failed to clean up stale invocations");
            }
        }
        Ok(db)
    }

    fn cleanup_stale_invocations_on_open(&self, path: &Path) -> Result<StaleCleanupOutcome> {
        let lock_path = path.with_extension("cleanup.lock");
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path);

        let lock_file = match lock_file {
            Ok(file) => file,
            Err(error) => {
                eprintln!(
                    "⚠️  Could not open history cleanup lock ({}): {error}; proceeding without lock",
                    lock_path.display()
                );
                self.cleanup_stale_invocations()?;
                return Ok(StaleCleanupOutcome::Ran);
            }
        };

        use std::os::fd::AsRawFd;
        let lock_result =
            unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if lock_result != 0 {
            return Ok(StaleCleanupOutcome::SkippedLockHeld);
        }

        self.repair_open_time_sweep_durations()?;
        self.cleanup_stale_invocations()?;
        drop(lock_file);
        Ok(StaleCleanupOutcome::Ran)
    }

    fn repair_open_time_sweep_durations(&self) -> Result<usize> {
        let repaired = self
            .conn
            .execute(
                r"
                UPDATE invocations
                SET duration_secs = NULL
                WHERE status = 'cancelled'
                  AND cancel_reason = 'stale_pid'
                  AND cancelled_by = 'open_time_sweep'
                  AND duration_secs IS NOT NULL
                ",
                [],
            )
            .context("failed to repair stale open-time-sweep invocation durations")?;

        if repaired > 0 {
            eprintln!(
                "ℹ️  Repaired {repaired} stale history duration(s): dead-PID cleanup rows have unknown runtime"
            );
        }

        Ok(repaired)
    }

    /// Check whether this database contains synthetic (seeded) data.
    pub fn check_synthetic(&self) -> Result<bool> {
        let exists = self
            .conn
            .query_row(
                "SELECT 1 FROM metadata WHERE key = 'synthetic' AND value = 'true' LIMIT 1",
                [],
                |_| Ok(true),
            )
            .optional()
            .context("failed to query synthetic history marker")?;
        Ok(exists.unwrap_or(false))
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

    /// Record a drift guard bypass event (#1565).
    ///
    /// Called by the pre-push hook when `SINEX_SKIP_DRIFT_GUARD=1` is used.
    /// `push_succeeded` is set later (unknown at bypass time), so callers
    /// pass `None` initially and update after the push completes.
    pub fn record_drift_guard_bypass(
        &self,
        git_branch: Option<&str>,
        head_sha: Option<&str>,
        push_succeeded: Option<bool>,
    ) -> Result<i64> {
        let mut stmt = self.conn.prepare(
            "INSERT INTO drift_guard_bypasses (git_branch, head_sha, push_succeeded) VALUES (?1, ?2, ?3)",
        )?;
        let id = stmt.insert(params![git_branch, head_sha, push_succeeded])?;
        Ok(id)
    }

    /// Update an existing bypass row with the push outcome.
    pub fn update_drift_guard_bypass_outcome(&self, id: i64, push_succeeded: bool) -> Result<()> {
        self.conn.execute(
            "UPDATE drift_guard_bypasses SET push_succeeded = ?1 WHERE id = ?2",
            params![push_succeeded, id],
        )?;
        Ok(())
    }

    /// Return the number of drift guard bypasses recorded in the last `days` days.
    pub fn get_drift_guard_bypass_count(&self, days: i32) -> Result<usize> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM drift_guard_bypasses WHERE recorded_at >= datetime('now', ?1)",
            params![format!("-{days} days")],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    /// Return the most recent drift guard bypass, if any.
    pub fn get_drift_guard_bypass_latest(&self) -> Result<Option<DriftGuardBypass>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, recorded_at, git_branch, head_sha, push_succeeded
             FROM drift_guard_bypasses
             ORDER BY recorded_at DESC LIMIT 1",
        )?;
        let row = stmt
            .query_row([], |row| {
                Ok(DriftGuardBypass {
                    id: row.get(0)?,
                    recorded_at: row.get::<_, String>(1)?,
                    git_branch: row.get(2)?,
                    head_sha: row.get(3)?,
                    push_succeeded: row.get(4)?,
                })
            })
            .optional()?;
        Ok(row)
    }

    /// List the most recent drift-guard bypasses (security/hygiene audit trail).
    ///
    /// Surfaced via `xtask history view drift-guard-bypasses` so this table no
    /// longer requires a raw `sqlite3` query to inspect.
    pub fn get_drift_guard_bypasses(&self, limit: usize) -> Result<Vec<DriftGuardBypass>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, recorded_at, git_branch, head_sha, push_succeeded
             FROM drift_guard_bypasses
             ORDER BY recorded_at DESC LIMIT ?1",
        )?;
        let rows = stmt
            .query_map(params![limit as i64], |row| {
                Ok(DriftGuardBypass {
                    id: row.get(0)?,
                    recorded_at: row.get::<_, String>(1)?,
                    git_branch: row.get(2)?,
                    head_sha: row.get(3)?,
                    push_succeeded: row.get(4)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Recent impact-plan audit runs (skip-accuracy / false-negative evidence).
    pub fn get_impact_audit_runs(&self, limit: usize) -> Result<Vec<ImpactAuditRunRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, invocation_id, sample_size, status, false_negative_count, created_at
             FROM impact_audit_runs ORDER BY created_at DESC LIMIT ?1",
        )?;
        let rows = stmt
            .query_map(params![limit as i64], |row| {
                Ok(ImpactAuditRunRow {
                    id: row.get(0)?,
                    invocation_id: row.get(1)?,
                    sample_size: row.get(2)?,
                    status: row.get(3)?,
                    false_negative_count: row.get(4)?,
                    created_at: row.get(5)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Most recent internal trace events (newest first).
    pub fn get_recent_trace_events(&self, limit: usize) -> Result<Vec<TraceEventRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, invocation_id, ts, level, target, message
             FROM trace_events ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt
            .query_map(params![limit as i64], |row| {
                Ok(TraceEventRow {
                    id: row.get(0)?,
                    invocation_id: row.get(1)?,
                    ts: row.get(2)?,
                    level: row.get(3)?,
                    target: row.get(4)?,
                    message: row.get(5)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Start a new invocation record. Returns the invocation ID.
    pub fn start_invocation(
        &self,
        command: &str,
        subcommand: Option<&str>,
        profile: Option<&str>,
        args_json: Option<&str>,
    ) -> Result<i64> {
        let git_snapshot = current_git_snapshot();
        let git_commit = git_snapshot.commit.clone();
        let git_dirty = git_snapshot.dirty;
        let host = crate::config::config().hostname.clone();
        let cwd = capture_working_directory(std::env::current_dir());
        let started_at = Timestamp::now().format_rfc3339();

        // Transition from synthetic to real: clear the marker and insert the
        // invocation row atomically so a crash between the two cannot leave the
        // DB in a state where the synthetic marker is gone but no real row exists.
        let is_synthetic = self.is_synthetic;
        with_sqlite_lock_retry("start invocation history row", || {
            self.conn.execute("BEGIN", [])?;
            if is_synthetic {
                match self
                    .conn
                    .execute("DELETE FROM metadata WHERE key = 'synthetic'", [])
                {
                    Ok(_) => {}
                    Err(err) => {
                        let _ = self.conn.execute("ROLLBACK", []);
                        return Err(color_eyre::eyre::Report::from(err))
                            .wrap_err("failed to clear synthetic marker");
                    }
                }
            }
            let result = self.conn.execute(
                r"
                INSERT INTO invocations (command, subcommand, profile, args_json, git_commit, git_dirty, started_at, host, cwd, status)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'running')
                ",
                params![command, subcommand, profile, args_json, git_commit, git_dirty, started_at, host, cwd],
            );
            match result {
                Ok(_) => {
                    self.conn.execute("COMMIT", [])?;
                }
                Err(err) => {
                    let _ = self.conn.execute("ROLLBACK", []);
                    return Err(err.into());
                }
            }
            Ok(())
        })?;

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

        with_sqlite_lock_retry("finish invocation history row", || {
            self.conn.execute(
                r"
                UPDATE invocations
                SET finished_at = ?1, duration_secs = ?2, exit_code = ?3, status = ?4
                WHERE id = ?5
                ",
                params![finished_at, duration_secs, exit_code, status.as_str(), id],
            )?;
            self.conn.execute(
                r"
                UPDATE proof_evidence
                SET finished_at = ?1, duration_secs = ?2, status = ?3
                WHERE invocation_id = ?4
                ",
                params![finished_at, duration_secs, status.as_str(), id],
            )?;
            self.conn.execute(
                r"
                UPDATE test_proof_units
                SET finished_at = ?1, duration_secs = ?2, status = ?3
                WHERE invocation_id = ?4
                ",
                params![finished_at, duration_secs, status.as_str(), id],
            )?;
            Ok(())
        })?;

        Ok(())
    }

    /// Finish a cancelled invocation and record why it was cancelled.
    pub fn finish_invocation_cancelled(
        &self,
        id: i64,
        exit_code: Option<i32>,
        duration_secs: f64,
        cancel_reason: &str,
        cancelled_by: &str,
    ) -> Result<()> {
        let finished_at = Timestamp::now().format_rfc3339();

        with_sqlite_lock_retry("finish cancelled invocation history row", || {
            self.conn.execute(
                r"
                UPDATE invocations
                SET finished_at = ?1,
                    duration_secs = ?2,
                    exit_code = ?3,
                    status = 'cancelled',
                    cancel_reason = ?4,
                    cancelled_by = ?5
                WHERE id = ?6
                ",
                params![
                    finished_at,
                    duration_secs,
                    exit_code,
                    cancel_reason,
                    cancelled_by,
                    id
                ],
            )?;
            Ok(())
        })?;

        Ok(())
    }

    /// Return cancellation metadata for an invocation, when present.
    pub fn get_invocation_cancel_metadata(
        &self,
        id: i64,
    ) -> Result<Option<(Option<String>, Option<String>)>> {
        self.conn
            .query_row(
                "SELECT cancel_reason, cancelled_by FROM invocations WHERE id = ?1",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .context("failed to read invocation cancellation metadata")
    }

    /// Record timing for a pipeline stage (fmt, clippy, forbidden, compile, preflight).
    pub fn record_stage_timing(
        &self,
        invocation_id: i64,
        stage_name: &str,
        started_at: &str,
        duration_secs: f64,
        success: bool,
        pressure: StagePressure,
    ) -> Result<()> {
        with_sqlite_lock_retry("record stage timing", || {
            self.conn.execute(
                r"
                INSERT INTO stage_timings (
                    invocation_id, stage_name, started_at, duration_secs, success,
                    io_full_avg10, cpu_some_avg10, memory_some_avg10,
                    io_full_stall_us, cpu_some_stall_us, memory_some_stall_us
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                ",
                params![
                    invocation_id,
                    stage_name,
                    started_at,
                    duration_secs,
                    i32::from(success),
                    pressure.io_full_avg10,
                    pressure.cpu_some_avg10,
                    pressure.memory_some_avg10,
                    pressure.io_full_stall_us,
                    pressure.cpu_some_stall_us,
                    pressure.memory_some_stall_us,
                ],
            )?;
            Ok(())
        })?;
        Ok(())
    }

    /// Set the currently executing pipeline stage for an in-flight invocation.
    ///
    /// This is written at `start_stage()` time and cleared at `finish_stage()` time,
    /// giving real-time visibility into what a running background job is doing.
    pub fn set_live_stage(&self, invocation_id: i64, stage: &str) -> Result<()> {
        with_sqlite_lock_retry("set live stage", || {
            self.conn.execute(
                "UPDATE invocations SET live_stage = ?1 WHERE id = ?2",
                params![stage, invocation_id],
            )?;
            Ok(())
        })?;
        Ok(())
    }

    /// Clear the live stage field (called when a stage finishes).
    pub fn clear_live_stage(&self, invocation_id: i64) -> Result<()> {
        with_sqlite_lock_retry("clear live stage", || {
            self.conn.execute(
                "UPDATE invocations SET live_stage = NULL WHERE id = ?1",
                params![invocation_id],
            )?;
            Ok(())
        })?;
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
            SELECT invocation_id, stage_name, started_at, duration_secs, success,
                   io_full_avg10, cpu_some_avg10, memory_some_avg10,
                   io_full_stall_us, cpu_some_stall_us, memory_some_stall_us
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
                    io_full_avg10: row.get(5)?,
                    cpu_some_avg10: row.get(6)?,
                    memory_some_avg10: row.get(7)?,
                    io_full_stall_us: row.get(8)?,
                    cpu_some_stall_us: row.get(9)?,
                    memory_some_stall_us: row.get(10)?,
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
        self.write_progress_full(
            invocation_id,
            phase,
            step,
            pct_done,
            items_done,
            items_total,
            Some("indeterminate"),
            None,
            None,
            Some("none"),
            None,
        )
    }

    /// Write or update the live progress snapshot with full field set.
    ///
    /// Extends write_progress with mode, unit_kind, rate_per_sec, eta_confidence,
    /// and terminal_summary for richer progress reporting (e.g. determinate compilation).
    #[allow(clippy::too_many_arguments)]
    pub fn write_progress_full(
        &self,
        invocation_id: i64,
        phase: Option<&str>,
        step: Option<&str>,
        pct_done: Option<f64>,
        items_done: Option<i64>,
        items_total: Option<i64>,
        mode: Option<&str>,
        unit_kind: Option<&str>,
        rate_per_sec: Option<f64>,
        eta_confidence: Option<&str>,
        terminal_summary: Option<&str>,
    ) -> Result<()> {
        let updated_at = Timestamp::now().format_rfc3339();
        with_sqlite_lock_retry("write invocation progress", || {
            self.conn.execute(
                r"INSERT INTO invocation_progress
                      (invocation_id, phase, step, pct_done, items_done, items_total, updated_at,
                       mode, unit_kind, rate_per_sec, eta_confidence, terminal_summary)
                  VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                  ON CONFLICT(invocation_id) DO UPDATE SET
                      phase = excluded.phase,
                      step = excluded.step,
                      pct_done = excluded.pct_done,
                      items_done = excluded.items_done,
                      items_total = excluded.items_total,
                      updated_at = excluded.updated_at,
                      mode = excluded.mode,
                      unit_kind = excluded.unit_kind,
                      rate_per_sec = excluded.rate_per_sec,
                      eta_confidence = excluded.eta_confidence,
                      terminal_summary = excluded.terminal_summary",
                params![
                    invocation_id,
                    phase,
                    step,
                    pct_done,
                    items_done,
                    items_total,
                    updated_at,
                    mode,
                    unit_kind,
                    rate_per_sec,
                    eta_confidence,
                    terminal_summary,
                ],
            )?;
            Ok(())
        })?;
        Ok(())
    }

    /// Get the current progress snapshot for an invocation.
    pub fn get_progress(&self, invocation_id: i64) -> Result<Option<InvocationProgress>> {
        self.conn
            .query_row(
                r"SELECT invocation_id, phase, step, pct_done, items_done, items_total, updated_at,
                         mode, unit_kind, rate_per_sec, eta_confidence, terminal_summary
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
                        mode: row.get(7)?,
                        unit_kind: row.get(8)?,
                        rate_per_sec: row.get(9)?,
                        eta_confidence: row.get(10)?,
                        terminal_summary: row.get(11)?,
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
        with_sqlite_lock_retry("record eta sample", || {
            self.conn.execute(
                r"INSERT INTO invocation_eta_samples (invocation_id, command, phase, duration_secs, sampled_at)
                  VALUES (?1, ?2, ?3, ?4, ?5)",
                params![invocation_id, command, phase, duration_secs, sampled_at],
            )?;
            Ok(())
        })?;
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

    /// Get all distinct (phase, median_duration_secs) pairs for a command.
    ///
    /// Returns a list of `(phase, median_secs, sample_count)` tuples sorted by phase name.
    /// Phases with fewer than 3 samples are included but flagged via sample_count.
    pub fn get_eta_phases(&self, command: &str) -> Result<Vec<(String, Option<f64>, usize)>> {
        let mut stmt = self.conn.prepare(
            r"SELECT phase, duration_secs FROM invocation_eta_samples
              WHERE command = ?1
              ORDER BY phase, sampled_at DESC",
        )?;
        let rows: Vec<(String, f64)> = stmt
            .query_map(params![command], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<Vec<_>, _>>()?;

        // Group by phase
        let mut by_phase: std::collections::BTreeMap<String, Vec<f64>> =
            std::collections::BTreeMap::new();
        for (phase, dur) in rows {
            by_phase.entry(phase).or_default().push(dur);
        }

        let result = by_phase
            .into_iter()
            .map(|(phase, mut samples)| {
                let count = samples.len();
                let median = if count >= 3 {
                    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                    Some(samples[count / 2])
                } else {
                    None
                };
                (phase, median, count)
            })
            .collect();
        Ok(result)
    }

    /// Get recent invocations, optionally filtered by command.
    pub fn get_recent(
        &self,
        limit: usize,
        command_filter: Option<&str>,
    ) -> Result<Vec<Invocation>> {
        let mut query = InvocationQuery::new().limit(limit);
        if let Some(command_filter) = command_filter {
            query = query.command(command_filter);
        }
        query.run(self)
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
        let mut query = InvocationQuery::new().limit(limit).offset(offset);
        if let Some(command_filter) = command_filter {
            query = query.command(command_filter);
        }
        if let Some(since_rfc3339) = since_rfc3339 {
            query = query.since_rfc3339(since_rfc3339);
        }
        query = match sort_by {
            "duration" => query.sort_duration(),
            "status" => query.sort_status(),
            _ => query.sort_started(),
        };
        query.run(self)
    }

    /// Get a specific invocation by database ID.
    pub fn get_invocation(&self, invocation_id: i64) -> Result<Option<Invocation>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT id, command, subcommand, profile, args_json, git_commit, git_dirty,
                   started_at, finished_at, duration_secs, exit_code, status, host, cwd, live_stage
            FROM invocations
            WHERE id = ?1
            LIMIT 1
            ",
        )?;

        stmt.query_row(params![invocation_id], row_to_invocation)
            .optional()
            .context("failed to get invocation by id")
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

    /// Get the newest successful invocation matching an exact freshness key.
    ///
    /// This is stricter than `get_last_completed_with_fingerprint`: it can find
    /// an older valid proof even when a newer invocation for the same command
    /// ran a different scope, and it never returns failed evidence.
    pub fn get_successful_invocation_by_fingerprint(
        &self,
        command: &str,
        tree_fingerprint: &str,
        scope_key: &str,
    ) -> Result<Option<InvocationWithFingerprint>> {
        self.conn
            .query_row(
                r"
                SELECT id, status, duration_secs, tree_fingerprint, scope_key
                FROM invocations
                WHERE command = ?1
                  AND status = 'success'
                  AND tree_fingerprint = ?2
                  AND scope_key = ?3
                ORDER BY started_at DESC
                LIMIT 1
                ",
                params![command, tree_fingerprint, scope_key],
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
            .context("failed to get successful invocation by fingerprint")
    }

    /// Get the newest successful proof evidence row matching an exact key.
    pub fn get_successful_proof_evidence(
        &self,
        command: &str,
        proof_kind: &str,
        input_fingerprint: &str,
        scope_key: &str,
    ) -> Result<Option<ProofEvidence>> {
        self.conn
            .query_row(
                r"
                SELECT
                    id,
                    invocation_id,
                    command,
                    proof_kind,
                    scope_key,
                    input_fingerprint,
                    status,
                    started_at,
                    finished_at,
                    duration_secs,
                    scope_json,
                    artifact_json
                FROM proof_evidence
                WHERE command = ?1
                  AND proof_kind = ?2
                  AND input_fingerprint = ?3
                  AND scope_key = ?4
                  AND status = 'success'
                ORDER BY finished_at DESC, id DESC
                LIMIT 1
                ",
                params![command, proof_kind, input_fingerprint, scope_key],
                row_to_proof_evidence,
            )
            .optional()
            .context("failed to get successful proof evidence")
    }

    /// Get the newest successful reusable test proof unit matching an exact key.
    pub fn get_successful_reusable_test_proof_unit(
        &self,
        proof_kind: &str,
        input_fingerprint: &str,
        scope_key: &str,
    ) -> Result<Option<TestProofUnit>> {
        self.conn
            .query_row(
                r"
                SELECT
                    id,
                    invocation_id,
                    proof_kind,
                    scope_key,
                    input_fingerprint,
                    manifest_json,
                    reusable,
                    status,
                    started_at,
                    finished_at,
                    duration_secs,
                    test_filter
                FROM test_proof_units
                WHERE proof_kind = ?1
                  AND input_fingerprint = ?2
                  AND scope_key = ?3
                  AND reusable = 1
                  AND status = 'success'
                ORDER BY finished_at DESC, id DESC
                LIMIT 1
                ",
                params![proof_kind, input_fingerprint, scope_key],
                row_to_test_proof_unit,
            )
            .optional()
            .context("failed to get successful reusable test proof unit")
    }

    /// Look up any test proof unit for a given scope key, ignoring the fingerprint.
    ///
    /// Used to detect stale proofs: a proof existed for this scope in a prior run
    /// but the current input fingerprint no longer matches (source/tooling changed).
    pub fn get_any_successful_test_proof_for_scope(
        &self,
        proof_kind: &str,
        scope_key: &str,
    ) -> Result<Option<TestProofUnit>> {
        self.conn
            .query_row(
                r"
                SELECT
                    id,
                    invocation_id,
                    proof_kind,
                    scope_key,
                    input_fingerprint,
                    manifest_json,
                    reusable,
                    status,
                    started_at,
                    finished_at,
                    duration_secs,
                    test_filter
                FROM test_proof_units
                WHERE proof_kind = ?1
                  AND scope_key = ?2
                  AND reusable = 1
                  AND status = 'success'
                ORDER BY finished_at DESC, id DESC
                LIMIT 1
                ",
                params![proof_kind, scope_key],
                row_to_test_proof_unit,
            )
            .optional()
            .context("failed to get any successful test proof for scope")
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

    /// Record the proof unit represented by a coordinated invocation.
    pub fn record_proof_evidence(
        &self,
        invocation_id: i64,
        command: &str,
        proof_kind: &str,
        scope_key: &str,
        input_fingerprint: &str,
        scope_json: Option<&str>,
        artifact_json: Option<&str>,
    ) -> Result<()> {
        with_sqlite_lock_retry("record proof evidence", || {
            self.conn.execute(
                r"
                INSERT INTO proof_evidence (
                    invocation_id,
                    command,
                    proof_kind,
                    scope_key,
                    input_fingerprint,
                    status,
                    started_at,
                    finished_at,
                    duration_secs,
                    scope_json,
                    artifact_json
                )
                SELECT
                    id,
                    ?2,
                    ?3,
                    ?4,
                    ?5,
                    status,
                    started_at,
                    finished_at,
                    duration_secs,
                    ?6,
                    ?7
                FROM invocations
                WHERE id = ?1
                ON CONFLICT(invocation_id, proof_kind, scope_key, input_fingerprint)
                DO UPDATE SET
                    command = excluded.command,
                    status = excluded.status,
                    started_at = excluded.started_at,
                    finished_at = excluded.finished_at,
                    duration_secs = excluded.duration_secs,
                    scope_json = excluded.scope_json,
                    artifact_json = excluded.artifact_json
                ",
                params![
                    invocation_id,
                    command,
                    proof_kind,
                    scope_key,
                    input_fingerprint,
                    scope_json,
                    artifact_json
                ],
            )?;
            Ok(())
        })
    }

    /// Record the resolved test manifest as a proof unit.
    /// Store the effective test filter for a proof unit, enabling per-test-name
    /// granularity evidence lookups (#1393 Phase 3).
    pub fn set_test_proof_filter(
        &self,
        invocation_id: i64,
        proof_kind: &str,
        scope_key: &str,
        input_fingerprint: &str,
        test_filter: &str,
    ) -> Result<()> {
        with_sqlite_lock_retry("set test proof filter", || {
            self.conn.execute(
                "UPDATE test_proof_units SET test_filter = ?5
                 WHERE invocation_id = ?1 AND proof_kind = ?2
                   AND scope_key = ?3 AND input_fingerprint = ?4",
                params![
                    invocation_id,
                    proof_kind,
                    scope_key,
                    input_fingerprint,
                    test_filter
                ],
            )?;
            Ok(())
        })
    }

    pub fn record_test_proof_unit(
        &self,
        invocation_id: i64,
        proof_kind: &str,
        scope_key: &str,
        input_fingerprint: &str,
        manifest_json: &str,
        reusable: bool,
    ) -> Result<()> {
        with_sqlite_lock_retry("record test proof unit", || {
            self.conn.execute(
                r"
                INSERT INTO test_proof_units (
                    invocation_id,
                    proof_kind,
                    scope_key,
                    input_fingerprint,
                    manifest_json,
                    reusable,
                    status,
                    started_at,
                    finished_at,
                    duration_secs
                )
                SELECT
                    id,
                    ?2,
                    ?3,
                    ?4,
                    ?5,
                    ?6,
                    status,
                    started_at,
                    finished_at,
                    duration_secs
                FROM invocations
                WHERE id = ?1
                ON CONFLICT(invocation_id, proof_kind, scope_key, input_fingerprint)
                DO UPDATE SET
                    manifest_json = excluded.manifest_json,
                    reusable = excluded.reusable,
                    status = excluded.status,
                    started_at = excluded.started_at,
                    finished_at = excluded.finished_at,
                    duration_secs = excluded.duration_secs
                ",
                params![
                    invocation_id,
                    proof_kind,
                    scope_key,
                    input_fingerprint,
                    manifest_json,
                    i64::from(reusable)
                ],
            )?;
            Ok(())
        })
    }

    /// Update an invocation's semantic workload arguments.
    pub fn update_invocation_args(&self, id: i64, args_json: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE invocations SET args_json = ?1 WHERE id = ?2",
            params![args_json, id],
        )?;
        Ok(())
    }

    /// Prune old background job handles (from `background_jobs`) older than `older_than_days`.
    ///
    /// This removes operational job handles and their cached logs, but does NOT touch the
    /// `invocations` table. Durable execution history survives independently of job pruning.
    pub fn prune_background_jobs(&self, older_than_days: u32) -> Result<usize> {
        if older_than_days == 0 {
            return Ok(0);
        }
        let interval = format!("-{older_than_days} days");
        let deleted = self.conn.execute(
            r"DELETE FROM background_jobs
              WHERE finished_at IS NOT NULL
                AND finished_at < strftime('%Y-%m-%dT%H:%M:%SZ', 'now', ?1)",
            rusqlite::params![interval],
        )?;
        Ok(deleted)
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

    /// Attach NATS consumer snapshot context to a test result record.
    ///
    /// D8: stores serialized `ConsumerSnapshot` JSON on failing tests so that
    /// `xtask history tests failures --output` can surface NATS consumer state
    /// at the time of failure, helping debug pipeline test failures caused by
    /// consumer lag or delivery ordering issues.
    ///
    /// Matches by `test_name` within `invocation_id`. No-op if the test isn't found.
    pub fn record_test_nats_context(
        &self,
        invocation_id: i64,
        test_name: &str,
        context: &serde_json::Value,
    ) -> Result<()> {
        let json = serde_json::to_string(context).context("failed to serialize NATS context")?;
        self.conn.execute(
            r"UPDATE test_results SET nats_context = ?1
              WHERE invocation_id = ?2 AND test_name = ?3",
            params![json, invocation_id, test_name],
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
        let mut updated_tests = 0usize;

        // Back-fill output for tests that do not have it yet.
        let mut output_stmt = self.conn.prepare(
            r"
            UPDATE test_results
            SET output = ?1
            WHERE invocation_id = ?2 AND test_name LIKE ?3 AND output IS NULL
            ",
        )?;

        // Update failure info and package from classname.
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
            let mut touched = false;

            // Back-fill output if available and not already present
            if let Some(output) = &meta.output {
                let rows = output_stmt.execute(params![output, invocation_id, &pattern])?;
                touched |= rows > 0;
            }

            // Update failure info and classname-based package
            let has_meta = meta.failure_message.is_some()
                || meta.failure_type.is_some()
                || meta.classname.is_some();
            if has_meta {
                let normalized_package = meta
                    .classname
                    .as_deref()
                    .and_then(normalize_junit_classname_package);
                meta_stmt.execute(params![
                    meta.failure_message,
                    meta.failure_type,
                    normalized_package,
                    invocation_id,
                    &pattern,
                ])?;
                touched = true;
            }

            if touched {
                updated_tests += 1;
            }
        }

        drop(output_stmt);
        drop(meta_stmt);

        // Parse slog events from output to extract sandbox metadata.
        self.extract_sandbox_metadata(invocation_id)?;

        Ok(updated_tests)
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
            .collect::<std::result::Result<Vec<_>, _>>()
            .wrap_err_with(|| {
                format!(
                    "failed to read stored sandbox metadata rows for invocation {invocation_id}"
                )
            })?;

        for (id, output) in &rows {
            let meta = parse_sandbox_meta(output).wrap_err_with(|| {
                format!("failed to parse sandbox metadata for stored test result row {id}")
            })?;
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

    /// Record host-level resource metrics for an invocation.
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

    /// Record invocation-local resource metrics for an invocation.
    pub fn record_resource_metrics(
        &self,
        invocation_id: i64,
        metrics: &crate::process::InvocationResourceMetrics,
    ) -> Result<()> {
        self.ensure_compat_schema()?;
        self.conn.execute(
            r"
            UPDATE invocations
            SET process_cpu_usage_avg = ?1,
                process_memory_usage_max_mb = ?2,
                root_process_cpu_usage_avg = ?3,
                root_process_memory_usage_max_mb = ?4,
                shared_nix_daemon_cpu_usage_avg = ?5,
                shared_nix_daemon_memory_usage_max_mb = ?6,
                shared_nix_build_slice_cpu_usage_avg = ?7,
                shared_nix_build_slice_memory_usage_max_mb = ?8,
                shared_background_slice_cpu_usage_avg = ?9,
                shared_background_slice_memory_usage_max_mb = ?10,
                host_cpu_pressure_some_avg10_max = ?11,
                host_io_pressure_some_avg10_max = ?12,
                host_io_pressure_full_avg10_max = ?13,
                host_memory_pressure_some_avg10_max = ?14,
                host_memory_pressure_full_avg10_max = ?15,
                host_block_read_mib_delta = ?16,
                host_block_write_mib_delta = ?17,
                host_block_read_iops_avg = ?18,
                host_block_write_iops_avg = ?19,
                host_block_busiest_device = ?20,
                host_block_busiest_device_total_mib_delta = ?21,
                host_block_busiest_device_read_iops_avg = ?22,
                host_block_busiest_device_write_iops_avg = ?23,
                host_block_busiest_device_weighted_io_ms_per_s = ?24,
                shm_free_min_mb = ?25,
                shm_used_max_mb = ?26,
                process_count_max = ?27,
                resource_sample_count = ?28
            WHERE id = ?29
            ",
            params![
                metrics.process_tree.cpu_usage_avg,
                metrics.process_tree.memory_usage_max_mb,
                metrics.process_tree.root_cpu_usage_avg,
                metrics.process_tree.root_memory_usage_max_mb,
                metrics.shared_build.shared_nix_daemon_cpu_usage_avg,
                metrics.shared_build.shared_nix_daemon_memory_usage_max_mb,
                metrics.shared_build.shared_nix_build_slice_cpu_usage_avg,
                metrics
                    .shared_build
                    .shared_nix_build_slice_memory_usage_max_mb,
                metrics.shared_build.shared_background_slice_cpu_usage_avg,
                metrics
                    .shared_build
                    .shared_background_slice_memory_usage_max_mb,
                metrics.host_pressure.cpu_some_avg10_max,
                metrics.host_pressure.io_some_avg10_max,
                metrics.host_pressure.io_full_avg10_max,
                metrics.host_pressure.memory_some_avg10_max,
                metrics.host_pressure.memory_full_avg10_max,
                metrics.host_block_io.read_mib_delta,
                metrics.host_block_io.write_mib_delta,
                metrics.host_block_io.read_iops_avg,
                metrics.host_block_io.write_iops_avg,
                metrics.host_block_io.busiest_device.clone(),
                metrics.host_block_io.busiest_device_total_mib_delta,
                metrics.host_block_io.busiest_device_read_iops_avg,
                metrics.host_block_io.busiest_device_write_iops_avg,
                metrics.host_block_io.busiest_device_weighted_io_ms_per_s,
                metrics.host_pressure.shm_free_min_mb,
                metrics.host_pressure.shm_used_max_mb,
                metrics.process_tree.process_count_max.map(i64::from),
                i64::from(metrics.process_tree.sample_count),
                invocation_id
            ],
        )?;
        Ok(())
    }

    fn invocation_columns(&self) -> Result<HashSet<String>> {
        let mut stmt = self
            .conn
            .prepare("PRAGMA table_info(invocations)")
            .context("failed to inspect invocation history schema")?;
        let mut columns = HashSet::new();
        let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
        for row in rows {
            columns.insert(row?);
        }
        Ok(columns)
    }

    /// Get resource usage (CPU/memory) for recent invocations.
    pub fn get_resource_usage(
        &self,
        command_filter: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ResourceUsage>> {
        self.get_resource_usage_with_zombies(command_filter, limit, false)
    }

    /// Get resource usage (CPU/memory) for recent invocations, optionally including zombie cancellations.
    pub fn get_resource_usage_with_zombies(
        &self,
        command_filter: Option<&str>,
        limit: usize,
        include_zombies: bool,
    ) -> Result<Vec<ResourceUsage>> {
        let columns = self.invocation_columns()?;
        let process_cpu_expr = if columns.contains("process_cpu_usage_avg") {
            "process_cpu_usage_avg"
        } else {
            "NULL"
        };
        let process_mem_expr = if columns.contains("process_memory_usage_max_mb") {
            "process_memory_usage_max_mb"
        } else {
            "NULL"
        };
        let root_process_cpu_expr = if columns.contains("root_process_cpu_usage_avg") {
            "root_process_cpu_usage_avg"
        } else {
            "NULL"
        };
        let root_process_mem_expr = if columns.contains("root_process_memory_usage_max_mb") {
            "root_process_memory_usage_max_mb"
        } else {
            "NULL"
        };
        let shared_nix_daemon_cpu_expr = if columns.contains("shared_nix_daemon_cpu_usage_avg") {
            "shared_nix_daemon_cpu_usage_avg"
        } else {
            "NULL"
        };
        let shared_nix_daemon_mem_expr =
            if columns.contains("shared_nix_daemon_memory_usage_max_mb") {
                "shared_nix_daemon_memory_usage_max_mb"
            } else {
                "NULL"
            };
        let shared_nix_build_cpu_expr = if columns.contains("shared_nix_build_slice_cpu_usage_avg")
        {
            "shared_nix_build_slice_cpu_usage_avg"
        } else {
            "NULL"
        };
        let shared_nix_build_mem_expr =
            if columns.contains("shared_nix_build_slice_memory_usage_max_mb") {
                "shared_nix_build_slice_memory_usage_max_mb"
            } else {
                "NULL"
            };
        let shared_background_cpu_expr =
            if columns.contains("shared_background_slice_cpu_usage_avg") {
                "shared_background_slice_cpu_usage_avg"
            } else {
                "NULL"
            };
        let shared_background_mem_expr =
            if columns.contains("shared_background_slice_memory_usage_max_mb") {
                "shared_background_slice_memory_usage_max_mb"
            } else {
                "NULL"
            };
        let process_count_expr = if columns.contains("process_count_max") {
            "process_count_max"
        } else {
            "NULL"
        };
        let sample_count_expr = if columns.contains("resource_sample_count") {
            "resource_sample_count"
        } else {
            "NULL"
        };
        let host_cpu_pressure_expr = if columns.contains("host_cpu_pressure_some_avg10_max") {
            "host_cpu_pressure_some_avg10_max"
        } else {
            "NULL"
        };
        let host_io_pressure_some_expr = if columns.contains("host_io_pressure_some_avg10_max") {
            "host_io_pressure_some_avg10_max"
        } else {
            "NULL"
        };
        let host_io_pressure_full_expr = if columns.contains("host_io_pressure_full_avg10_max") {
            "host_io_pressure_full_avg10_max"
        } else {
            "NULL"
        };
        let host_memory_pressure_some_expr =
            if columns.contains("host_memory_pressure_some_avg10_max") {
                "host_memory_pressure_some_avg10_max"
            } else {
                "NULL"
            };
        let host_memory_pressure_full_expr =
            if columns.contains("host_memory_pressure_full_avg10_max") {
                "host_memory_pressure_full_avg10_max"
            } else {
                "NULL"
            };
        let host_block_read_mib_expr = if columns.contains("host_block_read_mib_delta") {
            "host_block_read_mib_delta"
        } else {
            "NULL"
        };
        let host_block_write_mib_expr = if columns.contains("host_block_write_mib_delta") {
            "host_block_write_mib_delta"
        } else {
            "NULL"
        };
        let host_block_read_iops_expr = if columns.contains("host_block_read_iops_avg") {
            "host_block_read_iops_avg"
        } else {
            "NULL"
        };
        let host_block_write_iops_expr = if columns.contains("host_block_write_iops_avg") {
            "host_block_write_iops_avg"
        } else {
            "NULL"
        };
        let host_block_busiest_device_expr = if columns.contains("host_block_busiest_device") {
            "host_block_busiest_device"
        } else {
            "NULL"
        };
        let host_block_busiest_total_mib_expr =
            if columns.contains("host_block_busiest_device_total_mib_delta") {
                "host_block_busiest_device_total_mib_delta"
            } else {
                "NULL"
            };
        let host_block_busiest_read_iops_expr =
            if columns.contains("host_block_busiest_device_read_iops_avg") {
                "host_block_busiest_device_read_iops_avg"
            } else {
                "NULL"
            };
        let host_block_busiest_write_iops_expr =
            if columns.contains("host_block_busiest_device_write_iops_avg") {
                "host_block_busiest_device_write_iops_avg"
            } else {
                "NULL"
            };
        let host_block_busiest_weighted_expr =
            if columns.contains("host_block_busiest_device_weighted_io_ms_per_s") {
                "host_block_busiest_device_weighted_io_ms_per_s"
            } else {
                "NULL"
            };
        let shm_free_expr = if columns.contains("shm_free_min_mb") {
            "shm_free_min_mb"
        } else {
            "NULL"
        };
        let shm_used_expr = if columns.contains("shm_used_max_mb") {
            "shm_used_max_mb"
        } else {
            "NULL"
        };
        let mut query = String::from(&format!(
            r"SELECT command,
                         status,
                         started_at,
                         duration_secs,
                         {process_cpu_expr},
                         {process_mem_expr},
                         {root_process_cpu_expr},
                         {root_process_mem_expr},
                         {shared_nix_daemon_cpu_expr},
                         {shared_nix_daemon_mem_expr},
                         {shared_nix_build_cpu_expr},
                         {shared_nix_build_mem_expr},
                         {shared_background_cpu_expr},
                         {shared_background_mem_expr},
                         {process_count_expr},
                         {sample_count_expr},
                         cpu_usage_avg,
                         memory_usage_max_mb,
                         {host_cpu_pressure_expr},
                         {host_io_pressure_some_expr},
                         {host_io_pressure_full_expr},
                         {host_memory_pressure_some_expr},
                         {host_memory_pressure_full_expr},
                         {host_block_read_mib_expr},
                         {host_block_write_mib_expr},
                         {host_block_read_iops_expr},
                         {host_block_write_iops_expr},
                         {host_block_busiest_device_expr},
                         {host_block_busiest_total_mib_expr},
                         {host_block_busiest_read_iops_expr},
                         {host_block_busiest_write_iops_expr},
                         {host_block_busiest_weighted_expr},
                         {shm_free_expr},
                         {shm_used_expr}
              FROM invocations
              WHERE status != 'running'
               AND ({process_cpu_expr} IS NOT NULL
                     OR {process_mem_expr} IS NOT NULL
                     OR {root_process_cpu_expr} IS NOT NULL
                     OR {root_process_mem_expr} IS NOT NULL
                     OR {shared_nix_daemon_cpu_expr} IS NOT NULL
                     OR {shared_nix_daemon_mem_expr} IS NOT NULL
                     OR {shared_nix_build_cpu_expr} IS NOT NULL
                     OR {shared_nix_build_mem_expr} IS NOT NULL
                     OR {shared_background_cpu_expr} IS NOT NULL
                     OR {shared_background_mem_expr} IS NOT NULL
                     OR {host_cpu_pressure_expr} IS NOT NULL
                     OR {host_io_pressure_some_expr} IS NOT NULL
                     OR {host_io_pressure_full_expr} IS NOT NULL
                     OR {host_memory_pressure_some_expr} IS NOT NULL
                     OR {host_memory_pressure_full_expr} IS NOT NULL
                     OR {host_block_read_mib_expr} IS NOT NULL
                     OR {host_block_write_mib_expr} IS NOT NULL
                     OR {host_block_read_iops_expr} IS NOT NULL
                     OR {host_block_write_iops_expr} IS NOT NULL
                     OR {host_block_busiest_device_expr} IS NOT NULL
                     OR {host_block_busiest_total_mib_expr} IS NOT NULL
                     OR {host_block_busiest_read_iops_expr} IS NOT NULL
                     OR {host_block_busiest_write_iops_expr} IS NOT NULL
                     OR {host_block_busiest_weighted_expr} IS NOT NULL
                     OR {shm_free_expr} IS NOT NULL
                     OR {shm_used_expr} IS NOT NULL
                     OR cpu_usage_avg IS NOT NULL
                     OR memory_usage_max_mb IS NOT NULL)",
        ));
        if !include_zombies {
            query.push_str(" AND ");
            query.push_str(&non_zombie_cancel_filter(""));
        }
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
                status: row.get(1)?,
                started_at: row.get(2)?,
                duration_secs: row.get(3)?,
                process_cpu_usage_avg: row.get(4)?,
                process_memory_usage_max_mb: row.get(5)?,
                root_process_cpu_usage_avg: row.get(6)?,
                root_process_memory_usage_max_mb: row.get(7)?,
                shared_nix_daemon_cpu_usage_avg: row.get(8)?,
                shared_nix_daemon_memory_usage_max_mb: row.get(9)?,
                shared_nix_build_slice_cpu_usage_avg: row.get(10)?,
                shared_nix_build_slice_memory_usage_max_mb: row.get(11)?,
                shared_background_slice_cpu_usage_avg: row.get(12)?,
                shared_background_slice_memory_usage_max_mb: row.get(13)?,
                process_count_max: row.get::<_, Option<i64>>(14)?.map(|value| value as u32),
                sample_count: row.get::<_, Option<i64>>(15)?.map(|value| value as u32),
                host_cpu_usage_avg: row.get(16)?,
                host_memory_usage_max_mb: row.get(17)?,
                host_cpu_pressure_some_avg10_max: row.get(18)?,
                host_io_pressure_some_avg10_max: row.get(19)?,
                host_io_pressure_full_avg10_max: row.get(20)?,
                host_memory_pressure_some_avg10_max: row.get(21)?,
                host_memory_pressure_full_avg10_max: row.get(22)?,
                host_block_read_mib_delta: row.get(23)?,
                host_block_write_mib_delta: row.get(24)?,
                host_block_read_iops_avg: row.get(25)?,
                host_block_write_iops_avg: row.get(26)?,
                host_block_busiest_device: row.get(27)?,
                host_block_busiest_device_total_mib_delta: row.get(28)?,
                host_block_busiest_device_read_iops_avg: row.get(29)?,
                host_block_busiest_device_write_iops_avg: row.get(30)?,
                host_block_busiest_device_weighted_io_ms_per_s: row.get(31)?,
                shm_free_min_mb: row.get(32)?,
                shm_used_max_mb: row.get(33)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Get resource usage (CPU/memory/process count) for a specific invocation.
    pub fn get_resource_usage_for_invocation(
        &self,
        invocation_id: i64,
    ) -> Result<Option<ResourceUsage>> {
        let columns = self.invocation_columns()?;
        let process_cpu_expr = if columns.contains("process_cpu_usage_avg") {
            "process_cpu_usage_avg"
        } else {
            "NULL"
        };
        let process_mem_expr = if columns.contains("process_memory_usage_max_mb") {
            "process_memory_usage_max_mb"
        } else {
            "NULL"
        };
        let root_process_cpu_expr = if columns.contains("root_process_cpu_usage_avg") {
            "root_process_cpu_usage_avg"
        } else {
            "NULL"
        };
        let root_process_mem_expr = if columns.contains("root_process_memory_usage_max_mb") {
            "root_process_memory_usage_max_mb"
        } else {
            "NULL"
        };
        let shared_nix_daemon_cpu_expr = if columns.contains("shared_nix_daemon_cpu_usage_avg") {
            "shared_nix_daemon_cpu_usage_avg"
        } else {
            "NULL"
        };
        let shared_nix_daemon_mem_expr =
            if columns.contains("shared_nix_daemon_memory_usage_max_mb") {
                "shared_nix_daemon_memory_usage_max_mb"
            } else {
                "NULL"
            };
        let shared_nix_build_cpu_expr = if columns.contains("shared_nix_build_slice_cpu_usage_avg")
        {
            "shared_nix_build_slice_cpu_usage_avg"
        } else {
            "NULL"
        };
        let shared_nix_build_mem_expr =
            if columns.contains("shared_nix_build_slice_memory_usage_max_mb") {
                "shared_nix_build_slice_memory_usage_max_mb"
            } else {
                "NULL"
            };
        let shared_background_cpu_expr =
            if columns.contains("shared_background_slice_cpu_usage_avg") {
                "shared_background_slice_cpu_usage_avg"
            } else {
                "NULL"
            };
        let shared_background_mem_expr =
            if columns.contains("shared_background_slice_memory_usage_max_mb") {
                "shared_background_slice_memory_usage_max_mb"
            } else {
                "NULL"
            };
        let process_count_expr = if columns.contains("process_count_max") {
            "process_count_max"
        } else {
            "NULL"
        };
        let sample_count_expr = if columns.contains("resource_sample_count") {
            "resource_sample_count"
        } else {
            "NULL"
        };
        let host_cpu_pressure_expr = if columns.contains("host_cpu_pressure_some_avg10_max") {
            "host_cpu_pressure_some_avg10_max"
        } else {
            "NULL"
        };
        let host_io_pressure_some_expr = if columns.contains("host_io_pressure_some_avg10_max") {
            "host_io_pressure_some_avg10_max"
        } else {
            "NULL"
        };
        let host_io_pressure_full_expr = if columns.contains("host_io_pressure_full_avg10_max") {
            "host_io_pressure_full_avg10_max"
        } else {
            "NULL"
        };
        let host_memory_pressure_some_expr =
            if columns.contains("host_memory_pressure_some_avg10_max") {
                "host_memory_pressure_some_avg10_max"
            } else {
                "NULL"
            };
        let host_memory_pressure_full_expr =
            if columns.contains("host_memory_pressure_full_avg10_max") {
                "host_memory_pressure_full_avg10_max"
            } else {
                "NULL"
            };
        let host_block_read_mib_expr = if columns.contains("host_block_read_mib_delta") {
            "host_block_read_mib_delta"
        } else {
            "NULL"
        };
        let host_block_write_mib_expr = if columns.contains("host_block_write_mib_delta") {
            "host_block_write_mib_delta"
        } else {
            "NULL"
        };
        let host_block_read_iops_expr = if columns.contains("host_block_read_iops_avg") {
            "host_block_read_iops_avg"
        } else {
            "NULL"
        };
        let host_block_write_iops_expr = if columns.contains("host_block_write_iops_avg") {
            "host_block_write_iops_avg"
        } else {
            "NULL"
        };
        let host_block_busiest_device_expr = if columns.contains("host_block_busiest_device") {
            "host_block_busiest_device"
        } else {
            "NULL"
        };
        let host_block_busiest_total_mib_expr =
            if columns.contains("host_block_busiest_device_total_mib_delta") {
                "host_block_busiest_device_total_mib_delta"
            } else {
                "NULL"
            };
        let host_block_busiest_read_iops_expr =
            if columns.contains("host_block_busiest_device_read_iops_avg") {
                "host_block_busiest_device_read_iops_avg"
            } else {
                "NULL"
            };
        let host_block_busiest_write_iops_expr =
            if columns.contains("host_block_busiest_device_write_iops_avg") {
                "host_block_busiest_device_write_iops_avg"
            } else {
                "NULL"
            };
        let host_block_busiest_weighted_expr =
            if columns.contains("host_block_busiest_device_weighted_io_ms_per_s") {
                "host_block_busiest_device_weighted_io_ms_per_s"
            } else {
                "NULL"
            };
        let shm_free_expr = if columns.contains("shm_free_min_mb") {
            "shm_free_min_mb"
        } else {
            "NULL"
        };
        let shm_used_expr = if columns.contains("shm_used_max_mb") {
            "shm_used_max_mb"
        } else {
            "NULL"
        };

        let query = format!(
            r"SELECT command,
                     status,
                     started_at,
                     duration_secs,
                     {process_cpu_expr},
                     {process_mem_expr},
                     {root_process_cpu_expr},
                     {root_process_mem_expr},
                     {shared_nix_daemon_cpu_expr},
                     {shared_nix_daemon_mem_expr},
                     {shared_nix_build_cpu_expr},
                     {shared_nix_build_mem_expr},
                     {shared_background_cpu_expr},
                     {shared_background_mem_expr},
                     {process_count_expr},
                     {sample_count_expr},
                     cpu_usage_avg,
                     memory_usage_max_mb,
                     {host_cpu_pressure_expr},
                     {host_io_pressure_some_expr},
                     {host_io_pressure_full_expr},
                     {host_memory_pressure_some_expr},
                     {host_memory_pressure_full_expr},
                     {host_block_read_mib_expr},
                     {host_block_write_mib_expr},
                     {host_block_read_iops_expr},
                     {host_block_write_iops_expr},
                     {host_block_busiest_device_expr},
                     {host_block_busiest_total_mib_expr},
                     {host_block_busiest_read_iops_expr},
                     {host_block_busiest_write_iops_expr},
                     {host_block_busiest_weighted_expr},
                     {shm_free_expr},
                     {shm_used_expr}
              FROM invocations
              WHERE id = ?1
              LIMIT 1"
        );

        let usage = self
            .conn
            .query_row(&query, params![invocation_id], |row| {
                Ok(ResourceUsage {
                    command: row.get(0)?,
                    status: row.get(1)?,
                    started_at: row.get(2)?,
                    duration_secs: row.get(3)?,
                    process_cpu_usage_avg: row.get(4)?,
                    process_memory_usage_max_mb: row.get(5)?,
                    root_process_cpu_usage_avg: row.get(6)?,
                    root_process_memory_usage_max_mb: row.get(7)?,
                    shared_nix_daemon_cpu_usage_avg: row.get(8)?,
                    shared_nix_daemon_memory_usage_max_mb: row.get(9)?,
                    shared_nix_build_slice_cpu_usage_avg: row.get(10)?,
                    shared_nix_build_slice_memory_usage_max_mb: row.get(11)?,
                    shared_background_slice_cpu_usage_avg: row.get(12)?,
                    shared_background_slice_memory_usage_max_mb: row.get(13)?,
                    process_count_max: row.get::<_, Option<i64>>(14)?.map(|value| value as u32),
                    sample_count: row.get::<_, Option<i64>>(15)?.map(|value| value as u32),
                    host_cpu_usage_avg: row.get(16)?,
                    host_memory_usage_max_mb: row.get(17)?,
                    host_cpu_pressure_some_avg10_max: row.get(18)?,
                    host_io_pressure_some_avg10_max: row.get(19)?,
                    host_io_pressure_full_avg10_max: row.get(20)?,
                    host_memory_pressure_some_avg10_max: row.get(21)?,
                    host_memory_pressure_full_avg10_max: row.get(22)?,
                    host_block_read_mib_delta: row.get(23)?,
                    host_block_write_mib_delta: row.get(24)?,
                    host_block_read_iops_avg: row.get(25)?,
                    host_block_write_iops_avg: row.get(26)?,
                    host_block_busiest_device: row.get(27)?,
                    host_block_busiest_device_total_mib_delta: row.get(28)?,
                    host_block_busiest_device_read_iops_avg: row.get(29)?,
                    host_block_busiest_device_write_iops_avg: row.get(30)?,
                    host_block_busiest_device_weighted_io_ms_per_s: row.get(31)?,
                    shm_free_min_mb: row.get(32)?,
                    shm_used_max_mb: row.get(33)?,
                })
            })
            .optional()
            .context("failed to get resource usage for invocation")?;

        Ok(usage.filter(ResourceUsage::has_samples))
    }

    /// Get count of invocations.
    pub fn count(&self) -> Result<usize> {
        let count: usize = self
            .conn
            .query_row("SELECT COUNT(*) FROM invocations", [], |row| row.get(0))?;
        Ok(count)
    }

    // ──────────────────────────────────────────────────────────────────────
    // G2: Stage Analytics — slowest stages and per-stage trend    // ──────────────────────────────────────────────────────────────────────
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
    /// Record a completed exercise run into `exercise_runs` + `exercise_results`.
    ///
    /// Stores tier breakdown, pass/fail counts, duration, full report JSON, and
    /// per-exercise results so `xtask history exercise` can surface regressions.
    /// Called best-effort from `ExerciseCommand::execute()` via `ctx.with_history_db`.
    pub fn record_exercise_run(
        &self,
        invocation_id: i64,
        report: &crate::commands::exercise::ExerciseReport,
    ) -> Result<()> {
        validate_finite_duration_secs("exercise report", report.duration_secs)?;
        for entry in &report.results {
            validate_finite_duration_secs(
                &format!("exercise result '{}'", entry.id),
                entry.duration_secs,
            )?;
            for step in &entry.steps {
                validate_finite_duration_secs(
                    &format!("exercise step '{}' for '{}'", step.label, entry.id),
                    step.duration_secs,
                )?;
            }
        }

        let report_json = serde_json::to_string(report)
            .wrap_err("failed to serialize exercise report for history persistence")?;
        // Infer tier from results: if mixed, leave NULL (multi-tier run).
        let tier: Option<&str> = {
            let tiers: std::collections::HashSet<&str> =
                report.results.iter().map(|r| r.tier.as_str()).collect();
            if tiers.len() == 1 {
                tiers.into_iter().next()
            } else {
                None
            }
        };

        let run_id = self
            .conn
            .query_row(
                r"INSERT INTO exercise_runs
                (invocation_id, tier, total, passed, failed, skipped, duration_secs, report_json)
              VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
              RETURNING id",
                rusqlite::params![
                    invocation_id,
                    tier,
                    report.total as i64,
                    report.passed as i64,
                    report.failed as i64,
                    report.skipped as i64,
                    report.duration_secs,
                    Some(report_json),
                ],
                |row| row.get::<_, i64>(0),
            )
            .context("failed to insert exercise_run row")?;

        for entry in &report.results {
            self.conn
                .execute(
                    r"INSERT INTO exercise_results
                    (run_id, exercise_id, exercise_tier, passed, duration_secs, error, step_count)
                  VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    rusqlite::params![
                        run_id,
                        entry.id,
                        entry.tier,
                        i64::from(entry.passed),
                        entry.duration_secs,
                        entry.error,
                        entry.steps.len() as i64,
                    ],
                )
                .context("failed to insert exercise_result row")?;
        }

        Ok(())
    }

    /// Fetch recent exercise runs for `xtask history exercise`.
    pub fn get_exercise_runs(&self, limit: usize) -> Result<Vec<ExerciseRunRow>> {
        let mut stmt = self.conn.prepare(
            r"SELECT er.id, er.invocation_id, er.tier, er.total, er.passed, er.failed,
                     er.skipped, er.duration_secs, er.recorded_at,
                     inv.status, inv.git_commit
              FROM exercise_runs er
              LEFT JOIN invocations inv ON inv.id = er.invocation_id
              ORDER BY er.recorded_at DESC
              LIMIT ?1",
        )?;
        let rows = stmt
            .query_map([limit as i64], |row| {
                Ok(ExerciseRunRow {
                    run_id: row.get(0)?,
                    invocation_id: row.get(1)?,
                    tier: row.get(2)?,
                    total: row.get(3)?,
                    passed: row.get(4)?,
                    failed: row.get(5)?,
                    skipped: row.get(6)?,
                    duration_secs: row.get(7)?,
                    recorded_at: row.get(8)?,
                    invocation_status: row.get(9)?,
                    git_commit: row.get(10)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Fetch per-exercise results for a run.
    pub fn get_exercise_results_for_run(&self, run_id: i64) -> Result<Vec<ExerciseResultRow>> {
        let mut stmt = self.conn.prepare(
            r"SELECT exercise_id, exercise_tier, passed, duration_secs, error, step_count
              FROM exercise_results WHERE run_id = ?1 ORDER BY id",
        )?;
        let rows = stmt
            .query_map([run_id], |row| {
                Ok(ExerciseResultRow {
                    exercise_id: row.get(0)?,
                    exercise_tier: row.get(1)?,
                    passed: row.get::<_, i64>(2)? != 0,
                    duration_secs: row.get(3)?,
                    error: row.get(4)?,
                    step_count: row.get(5)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

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

    /// I4: Get cross-invocation chronological timeline with diagnostic counts.
    pub fn get_invocation_timeline(
        &self,
        command: Option<&str>,
        days: u32,
        limit: usize,
    ) -> Result<Vec<InvocationTimelineEntry>> {
        self.get_invocation_timeline_with_zombies(command, days, limit, false)
    }

    /// I4: Get cross-invocation chronological timeline, optionally including zombie cancellations.
    pub fn get_invocation_timeline_with_zombies(
        &self,
        command: Option<&str>,
        days: u32,
        limit: usize,
        include_zombies: bool,
    ) -> Result<Vec<InvocationTimelineEntry>> {
        let cutoff = format_history_timestamp(
            time::OffsetDateTime::now_utc() - time::Duration::days(i64::from(days)),
            "history timeline cutoff",
        )?;

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
        if !include_zombies {
            sql.push_str(" AND ");
            sql.push_str(&non_zombie_cancel_filter("i."));
        }

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
                let started_at_raw: String = row.get(3)?;
                let started_at = format_invocation_timestamp(
                    3,
                    "started_at",
                    parse_invocation_timestamp(3, "started_at", &started_at_raw)?,
                )?;
                Ok(InvocationTimelineEntry {
                    id: row.get(0)?,
                    command: row.get(1)?,
                    status: parse_stored_invocation_status(status_str)?,
                    started_at,
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
        self.get_working_sessions_with_zombies(limit, gap_minutes, false)
    }

    /// I6: Group invocations into working sessions, optionally including zombie cancellations.
    pub fn get_working_sessions_with_zombies(
        &self,
        limit: usize,
        gap_minutes: u32,
        include_zombies: bool,
    ) -> Result<Vec<WorkingSession>> {
        struct Row {
            command: String,
            started_at: String,
            started_at_ts: OffsetDateTime,
            finished_at: Option<String>,
            duration_secs: Option<f64>,
            status: String,
        }

        let mut sql = String::from(
            r"
            SELECT command, started_at, finished_at, duration_secs, status
            FROM invocations
            WHERE status IN ('success', 'failed', 'cancelled')
            ",
        );
        if !include_zombies {
            sql.push_str(" AND ");
            sql.push_str(&non_zombie_cancel_filter(""));
        }
        sql.push_str(" ORDER BY started_at ASC LIMIT 2000");

        let mut stmt = self.conn.prepare(&sql)?;

        let rows: Vec<Row> = stmt
            .query_map([], |row| {
                let started_at: String = row.get(1)?;
                let finished_at: Option<String> = row.get(2)?;
                let started_at_ts = parse_invocation_timestamp(1, "started_at", &started_at)?;
                if let Some(finished_at_value) = finished_at.as_deref() {
                    let _ = parse_invocation_timestamp(2, "finished_at", finished_at_value)?;
                }
                Ok(Row {
                    command: row.get(0)?,
                    started_at,
                    started_at_ts,
                    finished_at,
                    duration_secs: row.get(3)?,
                    status: row.get(4)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        let gap_secs = i64::from(gap_minutes) * 60;
        let mut sessions: Vec<WorkingSession> = Vec::new();
        let mut current: Option<WorkingSession> = None;
        let mut prev_started: Option<OffsetDateTime> = None;

        for row in &rows {
            let gap_exceeded = prev_started
                .is_none_or(|prev| (row.started_at_ts - prev).whole_seconds() > gap_secs);

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
            prev_started = Some(row.started_at_ts);
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

        let Some(inv) = inv else {
            return Ok(None);
        };

        let stages = self.get_stage_timings_for_invocation(id)?;

        let mut diag_stmt = self.conn.prepare(
            r"SELECT id, level, code, message, file_path, line, col, rendered, package,
                     fix_replacement, fix_applicability, fix_byte_start, fix_byte_end,
                     COALESCE(authority, 'proof') as authority, NULL as source_command,
                     NULL as source_time
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
                            .map_or(serde_json::Value::Null, serde_json::Value::Number),
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

    fn parse_invocation_selector(selector: &str) -> Result<InvocationSelector> {
        if selector == "latest" {
            return Ok(InvocationSelector::Latest);
        }
        if selector == "previous" {
            return Ok(InvocationSelector::Previous);
        }
        if selector == "current" {
            return Ok(InvocationSelector::Current);
        }

        let (kind, raw_id) = if let Some(value) = selector.strip_prefix("job:") {
            ("job", value)
        } else if let Some(value) = selector.strip_prefix("background-job:") {
            ("job", value)
        } else if let Some(value) = selector.strip_prefix("inv:") {
            ("invocation", value)
        } else if let Some(value) = selector.strip_prefix("invocation:") {
            ("invocation", value)
        } else {
            ("invocation", selector)
        };

        let id = raw_id.parse::<i64>().map_err(|_| {
            color_eyre::eyre::eyre!(
                "invalid invocation selector: '{selector}' (expected 'latest', 'previous', 'current', a numeric invocation ID, 'inv:<id>', or 'job:<id>')"
            )
        })?;

        Ok(match kind {
            "job" => InvocationSelector::BackgroundJobId(id),
            _ => InvocationSelector::InvocationId(id),
        })
    }

    fn resolve_completed_invocation_offset(
        &self,
        command: Option<&str>,
        offset: usize,
    ) -> Result<Option<i64>> {
        let offset = offset as i64;
        let id = if let Some(cmd) = command {
            self.conn
                .query_row(
                    r"SELECT id FROM invocations WHERE status IN ('success', 'failed')
                      AND command = ?1 ORDER BY id DESC LIMIT 1 OFFSET ?2",
                    params![cmd, offset],
                    |row| row.get(0),
                )
                .optional()?
        } else {
            self.conn
                .query_row(
                    r"SELECT id FROM invocations WHERE status IN ('success', 'failed')
                      ORDER BY id DESC LIMIT 1 OFFSET ?1",
                    params![offset],
                    |row| row.get(0),
                )
                .optional()?
        };
        Ok(id)
    }

    fn resolve_current_invocation(&self, command: Option<&str>) -> Result<Option<i64>> {
        let host = crate::config::config().hostname.clone();
        let cwd = capture_working_directory(std::env::current_dir());
        let id = if let Some(cmd) = command {
            self.conn
                .query_row(
                    r"
                    SELECT id
                    FROM invocations
                    WHERE host = ?1
                      AND cwd = ?2
                      AND command = ?3
                    ORDER BY CASE WHEN status = 'running' THEN 0 ELSE 1 END, id DESC
                    LIMIT 1
                    ",
                    params![host, cwd, cmd],
                    |row| row.get(0),
                )
                .optional()?
        } else {
            self.conn
                .query_row(
                    r"
                    SELECT id
                    FROM invocations
                    WHERE host = ?1
                      AND cwd = ?2
                    ORDER BY CASE WHEN status = 'running' THEN 0 ELSE 1 END, id DESC
                    LIMIT 1
                    ",
                    params![host, cwd],
                    |row| row.get(0),
                )
                .optional()?
        };
        Ok(id)
    }

    fn resolve_background_job_invocation(
        &self,
        job_id: i64,
        command: Option<&str>,
    ) -> Result<Option<i64>> {
        // `invocation_id` is nullable in background_jobs — use Option<i64> to
        // distinguish "row not found" (outer None) from "row found but NULL id"
        // (inner None), then flatten both into Option<i64>.
        let id: Option<Option<i64>> = if let Some(cmd) = command {
            self.conn
                .query_row(
                    r"
                    SELECT invocation_id
                    FROM background_jobs
                    WHERE id = ?1
                      AND command = ?2
                    LIMIT 1
                    ",
                    params![job_id, cmd],
                    |row| row.get::<_, Option<i64>>(0),
                )
                .optional()?
        } else {
            self.conn
                .query_row(
                    r"SELECT invocation_id FROM background_jobs WHERE id = ?1 LIMIT 1",
                    params![job_id],
                    |row| row.get::<_, Option<i64>>(0),
                )
                .optional()?
        };
        Ok(id.flatten())
    }

    /// Resolve an invocation selector to a concrete invocation ID.
    ///
    /// Supports:
    /// - `latest`: most recent completed invocation (`success` / `failed`)
    /// - `previous`: invocation immediately before `latest`
    /// - `current`: most recent invocation from the current checkout, preferring a running one
    /// - numeric ID / `inv:<id>`: explicit invocation
    /// - `job:<id>`: background job handle mapped back to its invocation
    pub fn resolve_invocation_id(
        &self,
        id_or_latest: &str,
        command: Option<&str>,
    ) -> Result<Option<i64>> {
        match Self::parse_invocation_selector(id_or_latest)? {
            InvocationSelector::Latest => self.resolve_completed_invocation_offset(command, 0),
            InvocationSelector::Previous => self.resolve_completed_invocation_offset(command, 1),
            InvocationSelector::Current => self.resolve_current_invocation(command),
            InvocationSelector::BackgroundJobId(job_id) => {
                self.resolve_background_job_invocation(job_id, command)
            }
            InvocationSelector::InvocationId(invocation_id) => {
                if id_or_latest.chars().all(|ch| ch.is_ascii_digit())
                    && self
                        .conn
                        .query_row(
                            r"SELECT 1 FROM invocations WHERE id = ?1 LIMIT 1",
                            params![invocation_id],
                            |_| Ok(()),
                        )
                        .optional()?
                        .is_none()
                {
                    return self.resolve_background_job_invocation(invocation_id, command);
                }
                Ok(Some(invocation_id))
            }
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
            .wrap_err_with(|| {
                format!(
                    "failed to compute transition probability from '{from_command}' to '{to_command}'"
                )
            })?;

        if total == 0 {
            return Ok(0.0);
        }

        Ok((followed as f64 / total as f64) * 100.0)
    }
}

#[cfg(test)]
mod tests;

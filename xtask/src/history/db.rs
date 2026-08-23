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

mod audit;
mod background;
mod diagnostics;
mod exercise;
mod git;
mod impact;
mod integrity;
mod invocations;
mod metrics;
mod prediction;
mod process;
mod rows;
mod sandbox_meta;
mod schema;
mod test_results;
mod types;
mod views;
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
}

impl HistoryDb {}

#[cfg(test)]
#[path = "db_test.rs"]
mod tests;

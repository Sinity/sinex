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
use serde::{Deserialize, Serialize};

mod diagnostics;
mod git;
use diagnostics::row_to_diagnostic_full;
pub use diagnostics::{
    DiagnosticCounts, DiagnosticDelta, DiagnosticLifecycle, DiagnosticTrendPoint, LifecycleStatus,
    StoredDiagnostic,
};
use git::current_git_snapshot;
use sinex_primitives::temporal::Timestamp;

/// A devshell wrapper rebuild event persisted into the `wrapper_events` table
/// from `xtask-wrapper-events.jsonl`. Mirrors the JSONL fields needed to make
/// checkout-local rebuild cost SQL-queryable and joinable with `invocations`.
#[derive(Debug, Clone)]
pub struct WrapperEventRow {
    pub event: String,
    pub status: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub duration_secs: Option<f64>,
    pub command: Option<String>,
    pub args: Option<String>,
    pub force_rebuild: bool,
    pub rebuild_reason: Option<String>,
    pub stage_durations_json: Option<String>,
}
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::NamedTempFile;
use time::OffsetDateTime;

const HISTORY_DB_SCHEMA_VERSION: i32 = 1;
const SQLITE_LOCK_RETRY_ATTEMPTS: usize = 6;
const SQLITE_LOCK_RETRY_BASE_DELAY: Duration = Duration::from_millis(50);
const SQLITE_LOCK_RETRY_MAX_DELAY: Duration = Duration::from_millis(500);
const SQLITE_PERSISTENT_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const SQLITE_EPHEMERAL_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const SQLITE_QUERY_BUSY_TIMEOUT: Duration = Duration::from_secs(1);
const SQLITE_STALE_CLEANUP_BUSY_TIMEOUT: Duration = Duration::from_millis(50);
const HISTORY_DB_INTEGRITY_CHECK_INTERVAL: Duration = Duration::from_hours(6);
const HISTORY_DB_INTEGRITY_STAMP_EXTENSION: &str = "db.integrity.json";
#[cfg(not(test))]
const ZOMBIE_REAPER_SIGTERM_GRACE: Duration = Duration::from_secs(2);
#[cfg(test)]
const ZOMBIE_REAPER_SIGTERM_GRACE: Duration = Duration::from_millis(25);

#[derive(Debug, Deserialize)]
struct TestDependencyEdgeArtifact {
    test_name: String,
    package: Option<String>,
    edge_kind: String,
    subject: String,
    fingerprint: Option<String>,
    origin: String,
}

#[derive(Debug, Deserialize)]
struct TestExecutionManifestArtifact {
    test_name: String,
    package: Option<String>,
    module_path: String,
    source_file: String,
    source_line: u32,
    binary_id: Option<String>,
    pid: u32,
    attempt_id: String,
    planner_version: String,
}

#[derive(Debug, Deserialize)]
struct TestCoverageRegionArtifact {
    test_name: String,
    package: Option<String>,
    file_path: String,
    function_name: Option<String>,
    line_start: Option<u32>,
    line_end: Option<u32>,
    region_hash: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "artifact_kind", rename_all = "snake_case")]
enum ImpactArtifactEnvelope {
    DependencyEdges {
        edges: Vec<TestDependencyEdgeArtifact>,
    },
    TestExecutionManifest {
        manifest: TestExecutionManifestArtifact,
    },
    CoverageRegions {
        regions: Vec<TestCoverageRegionArtifact>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HistoryDbOpenMode {
    Persistent,
    Ephemeral,
    Query,
}

impl HistoryDbOpenMode {
    fn pragmas(self) -> &'static str {
        match self {
            Self::Persistent => {
                "PRAGMA foreign_keys=ON;
                 PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=NORMAL;
                 PRAGMA busy_timeout=5000;"
            }
            Self::Ephemeral => {
                "PRAGMA foreign_keys=ON;
                 PRAGMA journal_mode=MEMORY;
                 PRAGMA synchronous=OFF;
                 PRAGMA temp_store=MEMORY;
                 PRAGMA busy_timeout=5000;"
            }
            Self::Query => {
                "PRAGMA foreign_keys=ON;
                 PRAGMA query_only=ON;
                 PRAGMA busy_timeout=1000;"
            }
        }
    }

    const fn busy_timeout(self) -> Duration {
        match self {
            Self::Persistent => SQLITE_PERSISTENT_BUSY_TIMEOUT,
            Self::Ephemeral => SQLITE_EPHEMERAL_BUSY_TIMEOUT,
            Self::Query => SQLITE_QUERY_BUSY_TIMEOUT,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HistoryIntegrityStamp {
    schema_version: i32,
    checked_at_unix: i64,
}

#[derive(Debug, Clone)]
struct StaleInvocationCandidate {
    invocation_id: i64,
    background_job_id: Option<i64>,
    command: String,
    pid: Option<i64>,
    /// Seconds since started_at, computed in SQL via julianday() arithmetic.
    /// `None` if started_at couldn't be parsed.
    age_secs: Option<f64>,
}

fn background_watchdog_timeout_secs(command: &str) -> f64 {
    if command == "test" { 3600.0 } else { 1800.0 }
}

fn background_watchdog_escape_threshold_secs(command: &str) -> f64 {
    background_watchdog_timeout_secs(command) * 2.0
}

/// Best-effort zombie reaper: SIGTERM, 2s grace, SIGKILL if still alive.
///
/// Used by the open-time sweep to clean up watchdog escapees. Returns Ok(())
/// on success or if the PID is already dead; returns Err only on system error
/// (rare — invalid PID, EPERM despite being alive).
fn try_reap_zombie_pid(pid: i64) {
    if !(1..=i64::from(i32::MAX)).contains(&pid) {
        return;
    }
    let nix_pid = nix::unistd::Pid::from_raw(pid as i32);

    // Send SIGTERM first
    let _ = nix::sys::signal::kill(nix_pid, nix::sys::signal::Signal::SIGTERM);

    // Grace period
    std::thread::sleep(ZOMBIE_REAPER_SIGTERM_GRACE);

    // SIGKILL if still alive
    if nix::sys::signal::kill(nix_pid, None).is_ok() {
        let _ = nix::sys::signal::kill(nix_pid, nix::sys::signal::Signal::SIGKILL);
    }
}

impl HistoryIntegrityStamp {
    fn new(now: OffsetDateTime) -> Self {
        Self {
            schema_version: HISTORY_DB_SCHEMA_VERSION,
            checked_at_unix: now.unix_timestamp(),
        }
    }

    fn is_fresh(&self, now: OffsetDateTime, interval: Duration) -> bool {
        if self.schema_version != HISTORY_DB_SCHEMA_VERSION {
            return false;
        }

        let age_secs = now.unix_timestamp().saturating_sub(self.checked_at_unix);
        age_secs <= interval.as_secs().min(i64::MAX as u64) as i64
    }
}

fn history_process_is_alive(pid: i64) -> bool {
    if !(1..=i64::from(i32::MAX)).contains(&pid) {
        return false;
    }

    let pid = nix::unistd::Pid::from_raw(pid as i32);
    matches!(
        nix::sys::signal::killpg(pid, None),
        Ok(()) | Err(nix::errno::Errno::EPERM)
    ) || matches!(
        nix::sys::signal::kill(pid, None),
        Ok(()) | Err(nix::errno::Errno::EPERM)
    )
}

fn history_integrity_stamp_path(path: &Path) -> PathBuf {
    path.with_extension(HISTORY_DB_INTEGRITY_STAMP_EXTENSION)
}

fn history_recreation_artifact_paths(path: &Path) -> [PathBuf; 4] {
    [
        path.to_path_buf(),
        path.with_extension("db-wal"),
        path.with_extension("db-shm"),
        history_integrity_stamp_path(path),
    ]
}

fn history_artifact_backup_dir(path: &Path, suffix: &str) -> Result<PathBuf> {
    let file_name = path.file_name().ok_or_else(|| {
        color_eyre::eyre::eyre!("history artifact path has no file name: {}", path.display())
    })?;
    let file_name = file_name.to_string_lossy();
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    for index in 0..1000 {
        let candidate_name = if index == 0 {
            format!("{file_name}.{suffix}.bak")
        } else {
            format!("{file_name}.{suffix}.{index}.bak")
        };
        let candidate = parent.join(candidate_name);
        match std::fs::create_dir(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to create history artifact backup directory: {}",
                        candidate.display()
                    )
                });
            }
        }
    }

    color_eyre::eyre::bail!(
        "failed to allocate unique backup directory for history artifact: {}",
        path.display()
    );
}

fn preserve_history_artifacts_for_recreation(
    path: &Path,
    reason: &str,
) -> Result<Vec<(PathBuf, PathBuf)>> {
    let suffix = format!(
        "{reason}-{}",
        OffsetDateTime::now_utc().unix_timestamp_nanos()
    );
    let artifacts = history_recreation_artifact_paths(path)
        .into_iter()
        .filter(|artifact| artifact.exists())
        .collect::<Vec<_>>();
    if artifacts.is_empty() {
        return Ok(Vec::new());
    }

    let backup_dir = history_artifact_backup_dir(path, &suffix)?;
    let mut preserved = Vec::new();
    for artifact in artifacts {
        let artifact_name = artifact.file_name().ok_or_else(|| {
            color_eyre::eyre::eyre!(
                "history artifact path has no file name: {}",
                artifact.display()
            )
        })?;
        let backup_path = backup_dir.join(artifact_name);
        match std::fs::rename(&artifact, &backup_path) {
            Ok(()) => preserved.push((artifact, backup_path)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to preserve history artifact {} before recreation",
                        artifact.display()
                    )
                });
            }
        }
    }
    Ok(preserved)
}

fn format_preserved_history_artifact_destinations(backups: &[(PathBuf, PathBuf)]) -> String {
    backups
        .iter()
        .map(|(_, backup)| backup.display().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn history_integrity_check_interval() -> Duration {
    match std::env::var("XTASK_HISTORY_INTEGRITY_INTERVAL_SECS") {
        Ok(raw) => raw
            .trim()
            .parse::<u64>()
            .map_or(HISTORY_DB_INTEGRITY_CHECK_INTERVAL, Duration::from_secs),
        Err(_) => HISTORY_DB_INTEGRITY_CHECK_INTERVAL,
    }
}

fn load_history_integrity_stamp(path: &Path) -> Option<HistoryIntegrityStamp> {
    let bytes = std::fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn should_run_history_integrity_check(path: &Path, now: OffsetDateTime) -> bool {
    let interval = history_integrity_check_interval();
    if interval.is_zero() {
        return true;
    }

    let stamp_path = history_integrity_stamp_path(path);
    !load_history_integrity_stamp(&stamp_path).is_some_and(|stamp| stamp.is_fresh(now, interval))
}

fn persist_history_integrity_stamp(path: &Path, now: OffsetDateTime) -> Result<()> {
    let stamp_path = history_integrity_stamp_path(path);
    let parent = stamp_path.parent().ok_or_else(|| {
        color_eyre::eyre::eyre!(
            "history integrity stamp path has no parent: {}",
            stamp_path.display()
        )
    })?;
    std::fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create history stamp directory: {}",
            parent.display()
        )
    })?;
    let payload = serde_json::to_vec_pretty(&HistoryIntegrityStamp::new(now))
        .context("failed to serialize history integrity stamp")?;
    let mut temp_file = NamedTempFile::new_in(parent).with_context(|| {
        format!(
            "failed to create temporary history integrity stamp in {}",
            parent.display()
        )
    })?;
    use std::io::Write as _;
    temp_file
        .write_all(&payload)
        .with_context(|| "failed to write temporary history integrity stamp")?;
    temp_file
        .persist(&stamp_path)
        .map_err(|error| error.error)
        .with_context(|| {
            format!(
                "failed to persist history integrity stamp: {}",
                stamp_path.display()
            )
        })?;
    Ok(())
}

fn refresh_history_integrity_stamp(path: &Path, now: OffsetDateTime) {
    if let Err(error) = persist_history_integrity_stamp(path, now) {
        eprintln!(
            "⚠️  Failed to refresh history DB integrity stamp at {}: {error:#}",
            history_integrity_stamp_path(path).display()
        );
    }
}

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

fn normalize_junit_classname_package(classname: &str) -> Option<&str> {
    classname
        .split("::")
        .next()
        .map(str::trim)
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
    Failed,
    Orphaned,
    Killed,
}

impl JobLifecycleStatus {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Orphaned => "orphaned",
            Self::Killed => "killed",
        }
    }

    pub(crate) fn try_from_str(s: &str) -> Result<Self> {
        match s {
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "orphaned" => Ok(Self::Orphaned),
            "killed" => Ok(Self::Killed),
            _ => Err(color_eyre::eyre::eyre!("invalid job lifecycle status: {s}")),
        }
    }

    pub(crate) fn is_terminal(&self) -> bool {
        !matches!(self, Self::Running)
    }

    #[must_use]
    pub(crate) fn from_invocation_status(status: InvocationStatus) -> Self {
        match status {
            InvocationStatus::Running => Self::Running,
            InvocationStatus::Success => Self::Completed,
            InvocationStatus::Failed => Self::Failed,
            InvocationStatus::Cancelled => Self::Killed,
        }
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "called from rusqlite with String"
)]
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

fn invalid_invocation_field(
    column_index: usize,
    field_name: &'static str,
    error: impl std::error::Error + Send + Sync + 'static,
) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        column_index,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid invocation {field_name}: {error}"),
        )),
    )
}

fn parse_invocation_timestamp(
    column_index: usize,
    field_name: &'static str,
    value: &str,
) -> rusqlite::Result<OffsetDateTime> {
    OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
        .map_err(|error| invalid_invocation_field(column_index, field_name, error))
}

fn format_invocation_timestamp(
    column_index: usize,
    field_name: &'static str,
    value: OffsetDateTime,
) -> rusqlite::Result<String> {
    value
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(|error| invalid_invocation_field(column_index, field_name, error))
}

fn format_history_timestamp(timestamp: OffsetDateTime, context: &'static str) -> Result<String> {
    timestamp
        .format(&time::format_description::well_known::Rfc3339)
        .wrap_err_with(|| format!("failed to format {context} as RFC3339"))
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

/// A recorded drift guard bypass event (#1565).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftGuardBypass {
    pub id: i64,
    pub recorded_at: String,
    pub git_branch: Option<String>,
    pub head_sha: Option<String>,
    pub push_succeeded: Option<bool>,
}

/// A recorded impact-plan audit run (skip-accuracy evidence). Surfaced via
/// `xtask history view impact-audit` so the table needs no raw `sqlite3`.
#[derive(Debug, Clone, Serialize)]
pub struct ImpactAuditRunRow {
    pub id: i64,
    pub invocation_id: Option<i64>,
    pub sample_size: i64,
    pub status: String,
    pub false_negative_count: i64,
    pub created_at: String,
}

/// A recorded internal trace event. Surfaced via `xtask history view traces`
/// so the table needs no raw `sqlite3`.
#[derive(Debug, Clone, Serialize)]
pub struct TraceEventRow {
    pub id: i64,
    pub invocation_id: Option<i64>,
    pub ts: String,
    pub level: String,
    pub target: String,
    pub message: String,
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

    fn configure_connection(conn: &Connection, mode: HistoryDbOpenMode) -> Result<()> {
        conn.execute_batch(mode.pragmas())
            .context("failed to configure history database connection")
    }

    fn with_busy_timeout<T, F>(
        &self,
        timeout: Duration,
        restore_mode: HistoryDbOpenMode,
        operation: F,
    ) -> Result<T>
    where
        F: FnOnce() -> Result<T>,
    {
        self.conn
            .busy_timeout(timeout)
            .context("failed to configure temporary history database busy timeout")?;

        let operation_result = operation();
        let restore_result = self
            .conn
            .busy_timeout(restore_mode.busy_timeout())
            .context("failed to restore history database busy timeout");

        match (operation_result, restore_result) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), Ok(())) => Err(error),
            (_, Err(error)) => Err(error),
        }
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

    /// Initialize the database schema from scratch.
    ///
    /// All tables are defined with their full canonical column sets. Existing
    /// history databases are preserved with compatibility `ALTER TABLE` additions;
    /// the history DB is an evidence ledger and must not be treated as cache.
    fn init_schema(&self) -> Result<()> {
        self.conn.execute_batch(
            r"
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
                cancel_reason TEXT,
                cancelled_by TEXT,
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
                process_cpu_usage_avg REAL,
                process_memory_usage_max_mb REAL,
                root_process_cpu_usage_avg REAL,
                root_process_memory_usage_max_mb REAL,
                shared_nix_daemon_cpu_usage_avg REAL,
                shared_nix_daemon_memory_usage_max_mb REAL,
                shared_nix_build_slice_cpu_usage_avg REAL,
                shared_nix_build_slice_memory_usage_max_mb REAL,
                shared_background_slice_cpu_usage_avg REAL,
                shared_background_slice_memory_usage_max_mb REAL,
                host_cpu_pressure_some_avg10_max REAL,
                host_io_pressure_some_avg10_max REAL,
                host_io_pressure_full_avg10_max REAL,
                host_memory_pressure_some_avg10_max REAL,
                host_memory_pressure_full_avg10_max REAL,
                host_block_read_mib_delta REAL,
                host_block_write_mib_delta REAL,
                host_block_read_iops_avg REAL,
                host_block_write_iops_avg REAL,
                host_block_busiest_device TEXT,
                host_block_busiest_device_total_mib_delta REAL,
                host_block_busiest_device_read_iops_avg REAL,
                host_block_busiest_device_write_iops_avg REAL,
                host_block_busiest_device_weighted_io_ms_per_s REAL,
                shm_free_min_mb REAL,
                shm_used_max_mb REAL,
                process_count_max INTEGER,
                resource_sample_count INTEGER,
                tree_fingerprint TEXT,
                scope_key TEXT,
                live_stage TEXT,
                pre_fix_errors INTEGER,
                pre_fix_warnings INTEGER,
                pre_fix_fixable INTEGER,
                launch_mode TEXT DEFAULT 'foreground'
            );

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
                test_mode TEXT DEFAULT 'nextest',
                nats_context TEXT,
                UNIQUE(invocation_id, test_name, attempt)
            );

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
                fix_byte_end INTEGER,
                authority TEXT NOT NULL DEFAULT 'proof'
            );

            CREATE TABLE IF NOT EXISTS invocation_packages (
                invocation_id INTEGER NOT NULL REFERENCES invocations(id) ON DELETE CASCADE,
                package TEXT NOT NULL,
                PRIMARY KEY (invocation_id, package)
            );

            CREATE TABLE IF NOT EXISTS stage_timings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER NOT NULL REFERENCES invocations(id) ON DELETE CASCADE,
                stage_name TEXT NOT NULL,
                started_at TEXT NOT NULL,
                duration_secs REAL NOT NULL,
                success INTEGER NOT NULL DEFAULT 1,
                -- End-of-stage PSI (pressure-stall) snapshot for per-stage causal
                -- attribution of dev-loop slowdowns. avg10 is a 10s decaying average:
                -- meaningful for long stages (compile/test/clippy), coarse for sub-10s
                -- stages. Nullable: /proc/pressure may be unavailable.
                io_full_avg10 REAL,
                cpu_some_avg10 REAL,
                memory_some_avg10 REAL,
                -- Delta of /proc/pressure `total=` stall microseconds over
                -- [stage_start, stage_end]: exact stall μs attributable to the
                -- stage, length-independent (unlike the tail-biased avg10).
                -- Nullable: /proc/pressure may be unavailable, or a start/end
                -- counter may be missing.
                io_full_stall_us INTEGER,
                cpu_some_stall_us INTEGER,
                memory_some_stall_us INTEGER
            );

            CREATE TABLE IF NOT EXISTS invocation_progress (
                invocation_id INTEGER PRIMARY KEY REFERENCES invocations(id) ON DELETE CASCADE,
                phase TEXT,
                step TEXT,
                pct_done REAL,
                items_done INTEGER,
                items_total INTEGER,
                updated_at TEXT NOT NULL,
                mode TEXT,
                unit_kind TEXT,
                rate_per_sec REAL,
                eta_confidence TEXT,
                terminal_summary TEXT
            );

            CREATE TABLE IF NOT EXISTS proof_evidence (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER NOT NULL REFERENCES invocations(id) ON DELETE CASCADE,
                command TEXT NOT NULL,
                proof_kind TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                input_fingerprint TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'running',
                started_at TEXT NOT NULL,
                finished_at TEXT,
                duration_secs REAL,
                scope_json TEXT,
                artifact_json TEXT,
                UNIQUE(invocation_id, proof_kind, scope_key, input_fingerprint)
            );

            CREATE TABLE IF NOT EXISTS test_proof_units (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER NOT NULL REFERENCES invocations(id) ON DELETE CASCADE,
                proof_kind TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                input_fingerprint TEXT NOT NULL,
                manifest_json TEXT NOT NULL,
                reusable INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'running',
                started_at TEXT NOT NULL,
                finished_at TEXT,
                duration_secs REAL,
                test_filter TEXT,
                UNIQUE(invocation_id, proof_kind, scope_key, input_fingerprint)
            );

            CREATE TABLE IF NOT EXISTS test_dependency_edges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE CASCADE,
                test_name TEXT NOT NULL,
                package TEXT,
                edge_kind TEXT NOT NULL,
                subject TEXT NOT NULL,
                fingerprint TEXT,
                origin TEXT NOT NULL,
                recorded_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(invocation_id, test_name, edge_kind, subject, origin)
            );

            CREATE TABLE IF NOT EXISTS coverage_regions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE CASCADE,
                test_name TEXT NOT NULL,
                package TEXT,
                file_path TEXT NOT NULL,
                function_name TEXT,
                line_start INTEGER,
                line_end INTEGER,
                region_hash TEXT,
                recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS test_execution_manifests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE CASCADE,
                test_name TEXT NOT NULL,
                package TEXT,
                module_path TEXT NOT NULL,
                source_file TEXT NOT NULL,
                source_line INTEGER NOT NULL,
                binary_id TEXT,
                pid INTEGER NOT NULL,
                attempt_id TEXT NOT NULL,
                planner_version TEXT NOT NULL,
                recorded_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(invocation_id, test_name, module_path, source_file, source_line)
            );

            CREATE TABLE IF NOT EXISTS impact_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE SET NULL,
                mode TEXT NOT NULL,
                changed_json TEXT NOT NULL,
                plan_json TEXT NOT NULL,
                accepted_risk_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS impact_decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                impact_run_id INTEGER NOT NULL REFERENCES impact_runs(id) ON DELETE CASCADE,
                action TEXT NOT NULL,
                subject TEXT,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS impact_audit_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE SET NULL,
                impact_run_id INTEGER REFERENCES impact_runs(id) ON DELETE SET NULL,
                sample_size INTEGER NOT NULL,
                sampled_json TEXT NOT NULL,
                command_json TEXT NOT NULL,
                status TEXT NOT NULL,
                false_negative_count INTEGER NOT NULL DEFAULT 0,
                output_json TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS invocation_eta_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER NOT NULL REFERENCES invocations(id) ON DELETE CASCADE,
                command TEXT NOT NULL,
                phase TEXT NOT NULL,
                duration_secs REAL NOT NULL,
                sampled_at TEXT NOT NULL
            );

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

            CREATE TABLE IF NOT EXISTS background_job_logs (
                job_id INTEGER PRIMARY KEY REFERENCES background_jobs(id) ON DELETE CASCADE,
                stdout_content TEXT,
                stderr_content TEXT
            );

            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            );

            -- Indices
            CREATE INDEX IF NOT EXISTS idx_invocations_command ON invocations(command);
            CREATE INDEX IF NOT EXISTS idx_invocations_started ON invocations(started_at);
            CREATE INDEX IF NOT EXISTS idx_invocations_status ON invocations(status);
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
            CREATE INDEX IF NOT EXISTS idx_proof_evidence_exact
                ON proof_evidence(command, proof_kind, scope_key, input_fingerprint, status, finished_at);
            CREATE INDEX IF NOT EXISTS idx_test_proof_units_exact
                ON test_proof_units(proof_kind, scope_key, input_fingerprint, reusable, status, finished_at);
            CREATE INDEX IF NOT EXISTS idx_test_dependency_edges_subject
                ON test_dependency_edges(edge_kind, subject, package, test_name);
            CREATE INDEX IF NOT EXISTS idx_coverage_regions_path
                ON coverage_regions(file_path, package, test_name);
            CREATE INDEX IF NOT EXISTS idx_test_execution_manifest_source
                ON test_execution_manifests(source_file, package, test_name);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_coverage_regions_identity
                ON coverage_regions(
                    invocation_id,
                    test_name,
                    file_path,
                    COALESCE(function_name, ''),
                    COALESCE(line_start, -1),
                    COALESCE(line_end, -1)
                );
            CREATE INDEX IF NOT EXISTS idx_impact_runs_invocation ON impact_runs(invocation_id);
            CREATE INDEX IF NOT EXISTS idx_impact_decisions_run ON impact_decisions(impact_run_id);
            CREATE INDEX IF NOT EXISTS idx_impact_audit_invocation ON impact_audit_runs(invocation_id);

            CREATE TABLE IF NOT EXISTS exercise_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE CASCADE,
                tier TEXT,
                total INTEGER NOT NULL,
                passed INTEGER NOT NULL,
                failed INTEGER NOT NULL,
                skipped INTEGER NOT NULL,
                duration_secs REAL NOT NULL,
                report_json TEXT,
                recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS exercise_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL REFERENCES exercise_runs(id) ON DELETE CASCADE,
                exercise_id TEXT NOT NULL,
                exercise_tier TEXT,
                passed INTEGER NOT NULL,
                duration_secs REAL NOT NULL,
                error TEXT,
                step_count INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS drift_guard_bypasses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recorded_at TEXT NOT NULL DEFAULT (datetime('now')),
                git_branch TEXT,
                head_sha TEXT,
                push_succeeded INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_exercise_runs_invocation ON exercise_runs(invocation_id);
            CREATE INDEX IF NOT EXISTS idx_exercise_runs_recorded ON exercise_runs(recorded_at);
            CREATE INDEX IF NOT EXISTS idx_exercise_results_run ON exercise_results(run_id);
            CREATE INDEX IF NOT EXISTS idx_exercise_results_id ON exercise_results(exercise_id);
            CREATE INDEX IF NOT EXISTS idx_drift_guard_bypasses_recorded ON drift_guard_bypasses(recorded_at);

            CREATE TABLE IF NOT EXISTS wrapper_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                duration_secs REAL,
                command TEXT,
                args TEXT,
                force_rebuild INTEGER NOT NULL DEFAULT 0,
                rebuild_reason TEXT,
                stage_durations_json TEXT,
                UNIQUE(event, started_at)
            );
            CREATE INDEX IF NOT EXISTS idx_wrapper_events_started ON wrapper_events(started_at);
            ",
        )?;
        Ok(())
    }

    /// Upsert devshell wrapper rebuild events (from `xtask-wrapper-events.jsonl`)
    /// into the `wrapper_events` table so checkout-local rebuild cost — the
    /// `xtask_build` stage plus any schema/initdb bootstrap — is queryable via
    /// `xtask history query` and joinable with `invocations` by time, instead of
    /// living only in the append-only JSONL. Idempotent via
    /// `UNIQUE(event, started_at)` + `INSERT OR IGNORE`; returns rows inserted.
    pub fn upsert_wrapper_events(&self, rows: &[WrapperEventRow]) -> Result<usize> {
        // Ensure the table on write: init_schema is schema-version-gated and does
        // not re-run on already-initialized databases, so a newly added table
        // must be ensured here (same approach as `ensure_proof_schema`).
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS wrapper_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                duration_secs REAL,
                command TEXT,
                args TEXT,
                force_rebuild INTEGER NOT NULL DEFAULT 0,
                rebuild_reason TEXT,
                stage_durations_json TEXT,
                UNIQUE(event, started_at)
            );
            CREATE INDEX IF NOT EXISTS idx_wrapper_events_started ON wrapper_events(started_at);",
        )?;
        let mut stmt = self.conn.prepare(
            "INSERT OR IGNORE INTO wrapper_events \
             (event, status, started_at, finished_at, duration_secs, command, args, \
              force_rebuild, rebuild_reason, stage_durations_json) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        )?;
        let mut inserted = 0usize;
        for row in rows {
            inserted += stmt.execute(params![
                row.event,
                row.status,
                row.started_at,
                row.finished_at,
                row.duration_secs,
                row.command,
                row.args,
                i64::from(row.force_rebuild),
                row.rebuild_reason,
                row.stage_durations_json,
            ])?;
        }
        Ok(inserted)
    }

    fn ensure_column_exists(&self, table: &str, column: &str, definition: &str) -> Result<()> {
        if self.column_exists(table, column)? {
            return Ok(());
        }

        let sql = format!("ALTER TABLE {table} ADD COLUMN {column} {definition}");
        with_sqlite_lock_retry(
            &format!("add {table}.{column} compatibility column"),
            || match self.conn.execute(&sql, []) {
                Ok(_) => Ok(()),
                Err(error) => {
                    if self.column_exists(table, column)? {
                        return Ok(());
                    }
                    Err(error).with_context(|| {
                        format!("failed to add {table}.{column} compatibility column")
                    })
                }
            },
        )?;
        Ok(())
    }

    fn column_exists(&self, table: &str, column: &str) -> Result<bool> {
        let pragma = format!("PRAGMA table_info({table})");
        let mut stmt = self
            .conn
            .prepare(&pragma)
            .with_context(|| format!("failed to inspect {table} columns"))?;
        let exists = stmt
            .query_map([], |row| row.get::<_, String>(1))?
            .flatten()
            .any(|name| name == column);

        Ok(exists)
    }

    fn table_exists(&self, table: &str) -> Result<bool> {
        self.conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
                params![table],
                |row| row.get::<_, i64>(0),
            )
            .map(|value| value != 0)
            .context("failed to inspect history DB tables")
    }

    fn ensure_proof_schema(&self) -> Result<()> {
        if self.table_exists("proof_evidence")? && self.table_exists("test_proof_units")? {
            return Ok(());
        }
        if self.conn.is_readonly(rusqlite::DatabaseName::Main)? {
            return Ok(());
        }
        self.conn.execute_batch(
            r"
            CREATE TABLE IF NOT EXISTS proof_evidence (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER NOT NULL REFERENCES invocations(id) ON DELETE CASCADE,
                command TEXT NOT NULL,
                proof_kind TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                input_fingerprint TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'running',
                started_at TEXT NOT NULL,
                finished_at TEXT,
                duration_secs REAL,
                scope_json TEXT,
                artifact_json TEXT,
                UNIQUE(invocation_id, proof_kind, scope_key, input_fingerprint)
            );

            CREATE TABLE IF NOT EXISTS test_proof_units (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER NOT NULL REFERENCES invocations(id) ON DELETE CASCADE,
                proof_kind TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                input_fingerprint TEXT NOT NULL,
                manifest_json TEXT NOT NULL,
                reusable INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'running',
                started_at TEXT NOT NULL,
                finished_at TEXT,
                duration_secs REAL,
                test_filter TEXT,
                UNIQUE(invocation_id, proof_kind, scope_key, input_fingerprint)
            );

            CREATE INDEX IF NOT EXISTS idx_proof_evidence_exact
                ON proof_evidence(command, proof_kind, scope_key, input_fingerprint, status, finished_at);
            CREATE INDEX IF NOT EXISTS idx_test_proof_units_exact
                ON test_proof_units(proof_kind, scope_key, input_fingerprint, reusable, status, finished_at);
            ",
        )?;
        // Add test_filter column for test-name granularity evidence (#1393 Phase 3).
        // The column is nullable — broad runs without a filter leave it NULL.
        let _ = self.conn.execute(
            "ALTER TABLE test_proof_units ADD COLUMN test_filter TEXT",
            [],
        );
        Ok(())
    }

    fn ensure_impact_schema(&self) -> Result<()> {
        if self.table_exists("test_dependency_edges")?
            && self.table_exists("coverage_regions")?
            && self.table_exists("test_execution_manifests")?
            && self.table_exists("impact_runs")?
            && self.table_exists("impact_decisions")?
            && self.table_exists("impact_audit_runs")?
        {
            return Ok(());
        }
        if self.conn.is_readonly(rusqlite::DatabaseName::Main)? {
            return Ok(());
        }
        self.conn.execute_batch(
            r"
            CREATE TABLE IF NOT EXISTS test_dependency_edges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE CASCADE,
                test_name TEXT NOT NULL,
                package TEXT,
                edge_kind TEXT NOT NULL,
                subject TEXT NOT NULL,
                fingerprint TEXT,
                origin TEXT NOT NULL,
                recorded_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(invocation_id, test_name, edge_kind, subject, origin)
            );

            CREATE TABLE IF NOT EXISTS coverage_regions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE CASCADE,
                test_name TEXT NOT NULL,
                package TEXT,
                file_path TEXT NOT NULL,
                function_name TEXT,
                line_start INTEGER,
                line_end INTEGER,
                region_hash TEXT,
                recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS test_execution_manifests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE CASCADE,
                test_name TEXT NOT NULL,
                package TEXT,
                module_path TEXT NOT NULL,
                source_file TEXT NOT NULL,
                source_line INTEGER NOT NULL,
                binary_id TEXT,
                pid INTEGER NOT NULL,
                attempt_id TEXT NOT NULL,
                planner_version TEXT NOT NULL,
                recorded_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(invocation_id, test_name, module_path, source_file, source_line)
            );

            CREATE TABLE IF NOT EXISTS impact_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE SET NULL,
                mode TEXT NOT NULL,
                changed_json TEXT NOT NULL,
                plan_json TEXT NOT NULL,
                accepted_risk_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS impact_decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                impact_run_id INTEGER NOT NULL REFERENCES impact_runs(id) ON DELETE CASCADE,
                action TEXT NOT NULL,
                subject TEXT,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS impact_audit_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE SET NULL,
                impact_run_id INTEGER REFERENCES impact_runs(id) ON DELETE SET NULL,
                sample_size INTEGER NOT NULL,
                sampled_json TEXT NOT NULL,
                command_json TEXT NOT NULL,
                status TEXT NOT NULL,
                false_negative_count INTEGER NOT NULL DEFAULT 0,
                output_json TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_test_dependency_edges_subject
                ON test_dependency_edges(edge_kind, subject, package, test_name);
            CREATE INDEX IF NOT EXISTS idx_coverage_regions_path
                ON coverage_regions(file_path, package, test_name);
            CREATE INDEX IF NOT EXISTS idx_test_execution_manifest_source
                ON test_execution_manifests(source_file, package, test_name);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_coverage_regions_identity
                ON coverage_regions(
                    invocation_id,
                    test_name,
                    file_path,
                    COALESCE(function_name, ''),
                    COALESCE(line_start, -1),
                    COALESCE(line_end, -1)
                );
            CREATE INDEX IF NOT EXISTS idx_impact_runs_invocation ON impact_runs(invocation_id);
            CREATE INDEX IF NOT EXISTS idx_impact_decisions_run ON impact_decisions(impact_run_id);
            CREATE INDEX IF NOT EXISTS idx_impact_audit_invocation ON impact_audit_runs(invocation_id);
            ",
        )?;
        Ok(())
    }

    fn ensure_compat_schema(&self) -> Result<()> {
        self.ensure_proof_schema()?;
        self.ensure_impact_schema()?;
        self.ensure_column_exists("invocations", "process_cpu_usage_avg", "REAL")?;
        self.ensure_column_exists("invocations", "process_memory_usage_max_mb", "REAL")?;
        self.ensure_column_exists("invocations", "root_process_cpu_usage_avg", "REAL")?;
        self.ensure_column_exists("invocations", "root_process_memory_usage_max_mb", "REAL")?;
        self.ensure_column_exists("invocations", "shared_nix_daemon_cpu_usage_avg", "REAL")?;
        self.ensure_column_exists(
            "invocations",
            "shared_nix_daemon_memory_usage_max_mb",
            "REAL",
        )?;
        self.ensure_column_exists(
            "invocations",
            "shared_nix_build_slice_cpu_usage_avg",
            "REAL",
        )?;
        self.ensure_column_exists(
            "invocations",
            "shared_nix_build_slice_memory_usage_max_mb",
            "REAL",
        )?;
        self.ensure_column_exists(
            "invocations",
            "shared_background_slice_cpu_usage_avg",
            "REAL",
        )?;
        self.ensure_column_exists(
            "invocations",
            "shared_background_slice_memory_usage_max_mb",
            "REAL",
        )?;
        self.ensure_column_exists("invocations", "host_cpu_pressure_some_avg10_max", "REAL")?;
        self.ensure_column_exists("invocations", "host_io_pressure_some_avg10_max", "REAL")?;
        self.ensure_column_exists("invocations", "host_io_pressure_full_avg10_max", "REAL")?;
        self.ensure_column_exists("invocations", "host_memory_pressure_some_avg10_max", "REAL")?;
        self.ensure_column_exists("invocations", "host_memory_pressure_full_avg10_max", "REAL")?;
        self.ensure_column_exists("invocations", "host_block_read_mib_delta", "REAL")?;
        self.ensure_column_exists("invocations", "host_block_write_mib_delta", "REAL")?;
        self.ensure_column_exists("invocations", "host_block_read_iops_avg", "REAL")?;
        self.ensure_column_exists("invocations", "host_block_write_iops_avg", "REAL")?;
        self.ensure_column_exists("invocations", "host_block_busiest_device", "TEXT")?;
        self.ensure_column_exists(
            "invocations",
            "host_block_busiest_device_total_mib_delta",
            "REAL",
        )?;
        self.ensure_column_exists(
            "invocations",
            "host_block_busiest_device_read_iops_avg",
            "REAL",
        )?;
        self.ensure_column_exists(
            "invocations",
            "host_block_busiest_device_write_iops_avg",
            "REAL",
        )?;
        self.ensure_column_exists(
            "invocations",
            "host_block_busiest_device_weighted_io_ms_per_s",
            "REAL",
        )?;
        self.ensure_column_exists("invocations", "cancel_reason", "TEXT")?;
        self.ensure_column_exists("invocations", "cancelled_by", "TEXT")?;
        self.ensure_column_exists("invocations", "shm_free_min_mb", "REAL")?;
        self.ensure_column_exists("invocations", "shm_used_max_mb", "REAL")?;
        self.ensure_column_exists("invocations", "process_count_max", "INTEGER")?;
        self.ensure_column_exists("invocations", "resource_sample_count", "INTEGER")?;
        self.ensure_column_exists(
            "build_diagnostics",
            "authority",
            "TEXT NOT NULL DEFAULT 'proof'",
        )?;
        // Per-stage end-of-stage PSI snapshot (added for per-stage causal attribution
        // of dev-loop slowdowns). Nullable REAL — /proc/pressure may be unavailable.
        self.ensure_column_exists("stage_timings", "io_full_avg10", "REAL")?;
        self.ensure_column_exists("stage_timings", "cpu_some_avg10", "REAL")?;
        self.ensure_column_exists("stage_timings", "memory_some_avg10", "REAL")?;
        self.ensure_column_exists("stage_timings", "io_full_stall_us", "INTEGER")?;
        self.ensure_column_exists("stage_timings", "cpu_some_stall_us", "INTEGER")?;
        self.ensure_column_exists("stage_timings", "memory_some_stall_us", "INTEGER")?;
        Ok(())
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

    /// Mark invocations stuck in 'running' for over 10 minutes as 'cancelled',
    /// and aggressively reap zombies (alive PIDs running past 2× watchdog timeout).
    ///
    /// Called on `open()` to prevent orphaned invocations from accumulating
    /// when a process crashes before calling `finish_invocation()`.
    ///
    /// Three branches per candidate:
    /// - **Dead PID**: just mark cancelled (the crash/orphan safety net)
    /// - **Alive PID past zombie threshold**: SIGTERM → 2s wait → SIGKILL, then
    ///   mark killed with exit_code=124. This catches per-job watchdogs that
    ///   fail to survive their launching cgroup.
    /// - **Alive PID within legitimate window**: leave alone (drop guard handles
    ///   normal completion)
    fn cleanup_stale_invocations(&self) -> Result<()> {
        let stale_candidates = if self.has_stale_invocations()? {
            self.stale_invocation_candidates()?
        } else {
            Vec::new()
        };
        let mut stale_invocation_ids = Vec::new();
        let mut zombie_invocation_ids = Vec::new();
        let mut orphaned_background_job_ids = self
            .finished_invocation_running_background_job_ids()?
            .into_iter()
            .collect::<HashSet<_>>();
        let mut killed_background_job_ids = HashSet::new();
        let mut reaped_zombies = 0usize;

        for candidate in stale_candidates {
            let pid_alive = candidate.pid.is_some_and(history_process_is_alive);

            if pid_alive {
                let escape_threshold =
                    background_watchdog_escape_threshold_secs(&candidate.command);
                if candidate.age_secs.unwrap_or(0.0) < escape_threshold {
                    continue; // legitimate long-running bg job, skip
                }

                // Zombie: alive but past the command-specific escape threshold.
                if let Some(pid) = candidate.pid {
                    try_reap_zombie_pid(pid);
                    reaped_zombies += 1;
                }
                zombie_invocation_ids.push(candidate.invocation_id);
                if let Some(background_job_id) = candidate.background_job_id {
                    killed_background_job_ids.insert(background_job_id);
                }
            } else {
                stale_invocation_ids.push(candidate.invocation_id);
                if let Some(background_job_id) = candidate.background_job_id {
                    orphaned_background_job_ids.insert(background_job_id);
                }
            }
        }

        if reaped_zombies > 0 {
            eprintln!(
                "ℹ️  Reaped {reaped_zombies} zombie invocation(s) (alive PID running past 2× watchdog timeout — see issue #1211)"
            );
        }

        let stale_cleaned = self.mark_stale_invocations_cancelled(
            &stale_invocation_ids,
            "stale_pid",
            "open_time_sweep",
            None,
            false,
        )?;
        let zombie_cleaned = self.mark_stale_invocations_cancelled(
            &zombie_invocation_ids,
            "zombie_reaped",
            "open_time_sweep",
            Some(124),
            true,
        )?;
        let cleaned = stale_cleaned + zombie_cleaned;
        if cleaned > 0 {
            eprintln!(
                "ℹ️  Cleaned up {cleaned} stale 'running' invocation(s) older than 10 minutes"
            );
        }

        self.mark_background_jobs_orphaned(
            &orphaned_background_job_ids.into_iter().collect::<Vec<_>>(),
        )?;
        self.mark_background_jobs_killed_by_watchdog(
            &killed_background_job_ids.into_iter().collect::<Vec<_>>(),
        )?;
        Ok(())
    }

    fn has_stale_invocations(&self) -> Result<bool> {
        let has_stale: i64 = self
            .conn
            .query_row(
                r"
                SELECT EXISTS(
                    SELECT 1
                    FROM invocations
                    WHERE status = 'running'
                      AND started_at < strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-10 minutes')
                )
                ",
                [],
                |row| row.get(0),
            )
            .context("failed to detect stale invocations before cleanup")?;

        Ok(has_stale != 0)
    }

    fn stale_invocation_candidates(&self) -> Result<Vec<StaleInvocationCandidate>> {
        let mut stmt = self
            .conn
            .prepare(
                r"
                SELECT
                    i.id,
                    bg.id,
                    COALESCE(bg.command, i.command),
                    COALESCE(i.pid, bg.pid),
                    (julianday('now') - julianday(i.started_at)) * 86400.0
                FROM invocations i
                LEFT JOIN background_jobs bg
                    ON bg.invocation_id = i.id
                   AND bg.job_status = 'running'
                WHERE i.status = 'running'
                  AND i.started_at < strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-10 minutes')
                ",
            )
            .context("failed to prepare stale invocation candidate query")?;
        let rows = stmt
            .query_map([], |row| {
                Ok(StaleInvocationCandidate {
                    invocation_id: row.get(0)?,
                    background_job_id: row.get(1)?,
                    command: row.get(2)?,
                    pid: row.get(3)?,
                    age_secs: row.get(4)?,
                })
            })
            .context("failed to execute stale invocation candidate query")?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .context("failed to collect stale invocation candidates")
    }

    fn finished_invocation_running_background_job_ids(&self) -> Result<Vec<i64>> {
        let mut stmt = self
            .conn
            .prepare(
                r"
                SELECT bg.id
                FROM background_jobs bg
                JOIN invocations i ON i.id = bg.invocation_id
                WHERE bg.job_status = 'running'
                  AND i.finished_at IS NOT NULL
                ",
            )
            .context("failed to prepare finished invocation background-job repair query")?;
        let rows = stmt
            .query_map([], |row| row.get(0))
            .context("failed to execute finished invocation background-job repair query")?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .context("failed to collect finished invocation background-job repair candidates")
    }

    fn mark_stale_invocations_cancelled(
        &self,
        invocation_ids: &[i64],
        cancel_reason: &str,
        cancelled_by: &str,
        exit_code: Option<i32>,
        duration_known: bool,
    ) -> Result<usize> {
        if invocation_ids.is_empty() {
            return Ok(0);
        }

        // SQLite bind-variable limit is ~999; chunk at 500 for safety.
        // Guard with status IN ('running', 'pending') to avoid double-cancelling
        // rows that another process already transitioned to a terminal state.
        const BATCH: usize = 500;
        let mut total_cancelled = 0usize;
        for chunk in invocation_ids.chunks(BATCH) {
            let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
            let sql = format!(
                r"
                UPDATE invocations
                SET status = 'cancelled',
                    finished_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                    exit_code = COALESCE(?3, exit_code),
                    duration_secs = CASE
                        WHEN ?4 THEN (julianday('now') - julianday(started_at)) * 86400
                        ELSE NULL
                    END,
                    cancel_reason = ?1,
                    cancelled_by = ?2
                WHERE id IN ({})
                  AND status IN ('running', 'pending')
                ",
                placeholders.join(",")
            );
            let params = std::iter::once(&cancel_reason as &dyn rusqlite::ToSql)
                .chain(std::iter::once(&cancelled_by as &dyn rusqlite::ToSql))
                .chain(std::iter::once(&exit_code as &dyn rusqlite::ToSql))
                .chain(std::iter::once(&duration_known as &dyn rusqlite::ToSql))
                .chain(chunk.iter().map(|id| id as &dyn rusqlite::ToSql));
            total_cancelled += self
                .conn
                .execute(&sql, rusqlite::params_from_iter(params))
                .context("failed to mark stale invocations as cancelled")?;
        }
        Ok(total_cancelled)
    }

    fn mark_background_jobs_orphaned(&self, background_job_ids: &[i64]) -> Result<()> {
        if background_job_ids.is_empty() {
            return Ok(());
        }

        // SQLite bind-variable limit is ~999; chunk at 500 for safety.
        // Guard with job_status = 'running' to avoid overwriting terminal states.
        const BATCH: usize = 500;
        for chunk in background_job_ids.chunks(BATCH) {
            let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
            let sql = format!(
                r"
                UPDATE background_jobs
                SET job_status = 'orphaned',
                    finished_at = COALESCE(
                        (
                            SELECT inv.finished_at
                            FROM invocations inv
                            WHERE inv.id = background_jobs.invocation_id
                        ),
                        strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                    )
                WHERE id IN ({})
                  AND job_status = 'running'
                ",
                placeholders.join(",")
            );
            self.conn
                .execute(&sql, rusqlite::params_from_iter(chunk.iter()))
                .context("failed to mark stale background jobs as orphaned")?;
        }
        Ok(())
    }

    fn mark_background_jobs_killed_by_watchdog(&self, background_job_ids: &[i64]) -> Result<()> {
        if background_job_ids.is_empty() {
            return Ok(());
        }

        const BATCH: usize = 500;
        for chunk in background_job_ids.chunks(BATCH) {
            let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
            let sql = format!(
                r"
                UPDATE background_jobs
                SET job_status = 'killed',
                    exit_code = 124,
                    finished_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                WHERE id IN ({})
                  AND job_status = 'running'
                ",
                placeholders.join(",")
            );
            self.conn
                .execute(&sql, rusqlite::params_from_iter(chunk.iter()))
                .context("failed to mark zombie background jobs as killed")?;
        }
        Ok(())
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

        let stdout_content = Self::read_background_job_log(stdout_path, "stdout")?;
        let stderr_content = Self::read_background_job_log(stderr_path, "stderr")?;

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

    fn read_background_job_log(
        path: Option<&std::path::Path>,
        stream_name: &str,
    ) -> Result<Option<String>> {
        let Some(path) = path else {
            return Ok(None);
        };
        let content = std::fs::read_to_string(path).with_context(|| {
            format!(
                "failed to read archived {stream_name} log from {}",
                path.display()
            )
        })?;
        Ok(Some(content))
    }

    /// Get log content for a completed job (reads from `background_job_logs`).
    pub fn get_job_logs(&self, job_id: i64) -> Result<(Option<String>, Option<String>)> {
        let result = self
            .conn
            .query_row(
                "SELECT stdout_content, stderr_content FROM background_job_logs WHERE job_id = ?1",
                params![job_id],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                    ))
                },
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

    pub fn record_impact_plan(
        &self,
        invocation_id: Option<i64>,
        mode: &str,
        plan: &crate::impact::ImpactPlan,
    ) -> Result<i64> {
        let changed_json = serde_json::to_string(&plan.changed)?;
        let plan_json = serde_json::to_string(plan)?;
        let accepted_risk_json = serde_json::to_string(&plan.accepted_risks)?;
        with_sqlite_lock_retry("record impact plan", || {
            self.conn.execute(
                r"
                INSERT INTO impact_runs (
                    invocation_id,
                    mode,
                    changed_json,
                    plan_json,
                    accepted_risk_json
                )
                VALUES (?1, ?2, ?3, ?4, ?5)
                ",
                params![
                    invocation_id,
                    mode,
                    changed_json,
                    plan_json,
                    accepted_risk_json
                ],
            )?;
            let run_id = self.conn.last_insert_rowid();
            for decision in &plan.decisions {
                self.conn.execute(
                    r"
                    INSERT INTO impact_decisions (
                        impact_run_id,
                        action,
                        subject,
                        reason
                    )
                    VALUES (?1, ?2, ?3, ?4)
                    ",
                    params![
                        run_id,
                        format!("{:?}", decision.action),
                        decision.subject.as_deref(),
                        decision.reason.as_str()
                    ],
                )?;
            }
            Ok(run_id)
        })
    }

    pub fn record_impact_audit_run(
        &self,
        invocation_id: Option<i64>,
        impact_run_id: Option<i64>,
        sample_size: usize,
        sampled_json: &str,
        command_json: &str,
        status: &str,
        false_negative_count: usize,
        output_json: Option<&str>,
    ) -> Result<i64> {
        with_sqlite_lock_retry("record impact audit run", || {
            self.conn.execute(
                r"
                INSERT INTO impact_audit_runs (
                    invocation_id,
                    impact_run_id,
                    sample_size,
                    sampled_json,
                    command_json,
                    status,
                    false_negative_count,
                    output_json
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                ",
                params![
                    invocation_id,
                    impact_run_id,
                    i64::try_from(sample_size).unwrap_or(i64::MAX),
                    sampled_json,
                    command_json,
                    status,
                    i64::try_from(false_negative_count).unwrap_or(i64::MAX),
                    output_json,
                ],
            )?;
            Ok(self.conn.last_insert_rowid())
        })
    }

    pub fn impacted_tests_for_changed_files(
        &self,
        changed_files: &[String],
    ) -> Result<Vec<crate::impact::ImpactedTest>> {
        self.impacted_tests_for_changed_files_and_hunks(changed_files, &[])
    }

    pub fn impacted_tests_for_changed_files_and_hunks(
        &self,
        changed_files: &[String],
        changed_hunks: &[crate::impact::FileChangedHunks],
    ) -> Result<Vec<crate::impact::ImpactedTest>> {
        if changed_files.is_empty()
            || !self.table_exists("coverage_regions")?
            || !self.table_exists("test_dependency_edges")?
        {
            return Ok(Vec::new());
        }

        let mut tests: BTreeMap<(Option<String>, String), Vec<crate::impact::ImpactEvidence>> =
            BTreeMap::new();
        for path in changed_files {
            let hunks = changed_hunks
                .iter()
                .find(|hunks| hunks.path == *path)
                .map_or(&[][..], |hunks| hunks.hunks.as_slice());
            self.collect_coverage_impacts(path, hunks, &mut tests)?;
            self.collect_dependency_edge_impacts(path, &mut tests)?;
            self.collect_manifest_impacts(path, hunks, &mut tests)?;
        }

        Ok(tests
            .into_iter()
            .map(
                |((package, test_name), evidence)| crate::impact::ImpactedTest {
                    package,
                    test_name,
                    evidence,
                },
            )
            .collect())
    }

    fn collect_coverage_impacts(
        &self,
        path: &str,
        hunks: &[crate::impact::ChangedHunk],
        tests: &mut BTreeMap<(Option<String>, String), Vec<crate::impact::ImpactEvidence>>,
    ) -> Result<()> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT DISTINCT
                test_name,
                package,
                COALESCE(function_name, ''),
                COALESCE(line_start, -1),
                COALESCE(line_end, -1)
            FROM coverage_regions
            WHERE file_path = ?1 OR file_path = ?2
            ORDER BY package, test_name
            ",
        )?;
        let dotted = format!("./{path}");
        let rows = stmt.query_map(params![path, dotted], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })?;
        for row in rows {
            let (test_name, package, function_name, line_start, line_end) = row?;
            let line_start_u32 = u32::try_from(line_start).ok();
            let line_end_u32 = u32::try_from(line_end).ok();
            if !hunks.is_empty() {
                let Some((region_start, region_end)) = line_start_u32.zip(line_end_u32) else {
                    continue;
                };
                if !hunks.iter().any(|hunk| {
                    crate::impact::ranges_overlap(
                        hunk.line_start,
                        hunk.line_end,
                        region_start,
                        region_end,
                    )
                }) {
                    continue;
                }
            }
            let reason = if function_name.is_empty() {
                "LLVM coverage touched this file".to_string()
            } else if line_start >= 0 && line_end >= 0 {
                format!("LLVM coverage touched {function_name}:{line_start}-{line_end}")
            } else {
                format!("LLVM coverage touched {function_name}")
            };
            tests
                .entry((package, test_name))
                .or_default()
                .push(crate::impact::ImpactEvidence {
                    source: crate::impact::ImpactEvidenceSource::CoverageRegion,
                    subject: path.to_string(),
                    reason,
                    line_start: line_start_u32,
                    line_end: line_end_u32,
                });
        }
        Ok(())
    }

    fn collect_dependency_edge_impacts(
        &self,
        path: &str,
        tests: &mut BTreeMap<(Option<String>, String), Vec<crate::impact::ImpactEvidence>>,
    ) -> Result<()> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT DISTINCT test_name, package, edge_kind, origin
            FROM test_dependency_edges
            WHERE subject = ?1
              AND edge_kind IN ('file', 'rust_item', 'rust_module', 'runtime_file')
            ORDER BY package, test_name
            ",
        )?;
        let rows = stmt.query_map(params![path], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        for row in rows {
            let (test_name, package, edge_kind, origin) = row?;
            tests
                .entry((package, test_name))
                .or_default()
                .push(crate::impact::ImpactEvidence {
                    source: crate::impact::ImpactEvidenceSource::DependencyEdge,
                    subject: path.to_string(),
                    reason: format!("test declared {edge_kind} dependency from {origin}"),
                    line_start: None,
                    line_end: None,
                });
        }
        Ok(())
    }

    fn collect_manifest_impacts(
        &self,
        path: &str,
        hunks: &[crate::impact::ChangedHunk],
        tests: &mut BTreeMap<(Option<String>, String), Vec<crate::impact::ImpactEvidence>>,
    ) -> Result<()> {
        if !self.table_exists("test_execution_manifests")? {
            return Ok(());
        }
        let mut stmt = self.conn.prepare(
            r"
            SELECT DISTINCT test_name, package, source_line, module_path
            FROM test_execution_manifests
            WHERE source_file = ?1 OR source_file = ?2
            ORDER BY package, test_name
            ",
        )?;
        let dotted = format!("./{path}");
        let rows = stmt.query_map(params![path, dotted], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        for row in rows {
            let (test_name, package, source_line, module_path) = row?;
            let Some(source_line) = u32::try_from(source_line).ok() else {
                continue;
            };
            if !hunks.is_empty()
                && !hunks.iter().any(|hunk| {
                    crate::impact::ranges_overlap(
                        hunk.line_start,
                        hunk.line_end,
                        source_line,
                        source_line,
                    )
                })
            {
                continue;
            }
            tests
                .entry((package, test_name))
                .or_default()
                .push(crate::impact::ImpactEvidence {
                    source: crate::impact::ImpactEvidenceSource::TestExecutionManifest,
                    subject: path.to_string(),
                    reason: format!(
                        "test entrypoint manifest recorded {module_path}:{source_line}"
                    ),
                    line_start: Some(source_line),
                    line_end: Some(source_line),
                });
        }
        Ok(())
    }

    pub fn import_test_dependency_artifacts(
        &self,
        invocation_id: i64,
        artifact_dir: &Path,
    ) -> Result<usize> {
        if !artifact_dir.exists() {
            return Ok(0);
        }
        let mut imported = 0;
        with_sqlite_lock_retry("import test dependency artifacts", || {
            for entry in fs::read_dir(artifact_dir).wrap_err_with(|| {
                format!(
                    "failed to read impact artifact directory {}",
                    artifact_dir.display()
                )
            })? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().and_then(std::ffi::OsStr::to_str) != Some("json") {
                    continue;
                }
                let rendered = fs::read_to_string(&path).wrap_err_with(|| {
                    format!("failed to read impact artifact {}", path.display())
                })?;
                if let Ok(envelope) = serde_json::from_str::<ImpactArtifactEnvelope>(&rendered) {
                    imported += self.import_impact_artifact_envelope(invocation_id, envelope)?;
                } else {
                    let edges: Vec<TestDependencyEdgeArtifact> = serde_json::from_str(&rendered)
                        .wrap_err_with(|| {
                            format!("failed to parse impact artifact {}", path.display())
                        })?;
                    for edge in edges {
                        imported += self.insert_test_dependency_edge(invocation_id, &edge)?;
                    }
                }
            }
            Ok(imported)
        })
    }

    fn import_impact_artifact_envelope(
        &self,
        invocation_id: i64,
        envelope: ImpactArtifactEnvelope,
    ) -> Result<usize> {
        match envelope {
            ImpactArtifactEnvelope::DependencyEdges { edges } => {
                let mut imported = 0;
                for edge in edges {
                    imported += self.insert_test_dependency_edge(invocation_id, &edge)?;
                }
                Ok(imported)
            }
            ImpactArtifactEnvelope::TestExecutionManifest { manifest } => {
                self.conn.execute(
                    r"
                    INSERT INTO test_execution_manifests (
                        invocation_id,
                        test_name,
                        package,
                        module_path,
                        source_file,
                        source_line,
                        binary_id,
                        pid,
                        attempt_id,
                        planner_version
                    )
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                    ON CONFLICT(invocation_id, test_name, module_path, source_file, source_line)
                    DO UPDATE SET
                        package = excluded.package,
                        binary_id = excluded.binary_id,
                        pid = excluded.pid,
                        attempt_id = excluded.attempt_id,
                        planner_version = excluded.planner_version
                    ",
                    params![
                        invocation_id,
                        manifest.test_name,
                        manifest.package,
                        manifest.module_path,
                        manifest.source_file,
                        i64::from(manifest.source_line),
                        manifest.binary_id,
                        i64::from(manifest.pid),
                        manifest.attempt_id,
                        manifest.planner_version,
                    ],
                )?;
                Ok(1)
            }
            ImpactArtifactEnvelope::CoverageRegions { regions } => {
                let mut imported = 0;
                for region in regions {
                    self.conn.execute(
                        r"
                        INSERT OR REPLACE INTO coverage_regions (
                            invocation_id,
                            test_name,
                            package,
                            file_path,
                            function_name,
                            line_start,
                            line_end,
                            region_hash
                        )
                        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                        ",
                        params![
                            invocation_id,
                            region.test_name,
                            region.package,
                            region.file_path,
                            region.function_name,
                            region.line_start.map(i64::from),
                            region.line_end.map(i64::from),
                            region.region_hash,
                        ],
                    )?;
                    imported += 1;
                }
                Ok(imported)
            }
        }
    }

    fn insert_test_dependency_edge(
        &self,
        invocation_id: i64,
        edge: &TestDependencyEdgeArtifact,
    ) -> Result<usize> {
        let changed = self.conn.execute(
            r"
            INSERT INTO test_dependency_edges (
                invocation_id,
                test_name,
                package,
                edge_kind,
                subject,
                fingerprint,
                origin
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(invocation_id, test_name, edge_kind, subject, origin)
            DO UPDATE SET
                package = excluded.package,
                fingerprint = excluded.fingerprint
            ",
            params![
                invocation_id,
                edge.test_name,
                edge.package,
                edge.edge_kind,
                edge.subject,
                edge.fingerprint,
                edge.origin
            ],
        )?;
        Ok(changed)
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

    /// Return the running background job PID for an invocation, if any.
    pub fn get_running_job_pid_for_invocation(&self, invocation_id: i64) -> Result<Option<u32>> {
        self.conn
            .query_row(
                r"
                SELECT pid
                FROM background_jobs
                WHERE invocation_id = ?1
                  AND job_status = 'running'
                  AND pid IS NOT NULL
                ORDER BY id DESC
                LIMIT 1
                ",
                params![invocation_id],
                |row| row.get(0),
            )
            .optional()
            .context("failed to get running background job pid for invocation")
    }

    // ============ Background Job Methods ============

    /// Start a background job. Creates both an invocation row and a background_jobs row.
    ///
    /// Returns `(invocation_id, job_id)`. The invocation is the durable execution record;
    /// the job is the process handle. The child process claims the invocation via
    /// `XTASK_BG_INVOCATION_ID`; the job_id is used for directory naming and coordinator tracking.
    pub fn start_background_job(
        &self,
        command: &str,
        args: &[String],
        pid: Option<u32>,
        stdout_path: &Path,
        stderr_path: &Path,
    ) -> Result<(i64, i64)> {
        let args_json = serde_json::to_string(args)?;
        let git_snapshot = current_git_snapshot();
        let git_commit = git_snapshot.commit.clone();
        let git_dirty = git_snapshot.dirty;
        let host = crate::config::config().hostname.clone();
        let cwd = capture_working_directory(std::env::current_dir());
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
        self.conn.execute(
            r"INSERT INTO background_jobs
                (invocation_id, command, args_json, pid, stdout_path, stderr_path, job_status, started_at)
              VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'running', ?7)",
            params![invocation_id, command, args_json, pid, stdout_str, stderr_str, started_at],
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
        let updated = with_sqlite_lock_retry("claim background invocation", || {
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
            Ok(updated)
        })?;
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

// ─── I: Semantic Query Intelligence types ─────────────────────────────────────

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
    /// "indeterminate" | "determinate"
    pub mode: Option<String>,
    /// "packages" | "files" | "bytes" | "tests"
    pub unit_kind: Option<String>,
    /// items/sec computed from recent deltas
    pub rate_per_sec: Option<f64>,
    /// "none" | "rough" | "calibrated"
    pub eta_confidence: Option<String>,
    /// One-line human display string
    pub terminal_summary: Option<String>,
}

/// Recorded timing for a single pipeline stage within an invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageTiming {
    pub invocation_id: i64,
    pub stage_name: String,
    pub started_at: String,
    pub duration_secs: f64,
    pub success: bool,
    /// End-of-stage PSI io.full avg10 snapshot (None if /proc/pressure unavailable).
    pub io_full_avg10: Option<f64>,
    /// End-of-stage PSI cpu.some avg10 snapshot.
    pub cpu_some_avg10: Option<f64>,
    /// End-of-stage PSI memory.some avg10 snapshot.
    pub memory_some_avg10: Option<f64>,
    /// Delta of /proc/pressure io.full `total=` stall μs over the stage.
    pub io_full_stall_us: Option<i64>,
    /// Delta of /proc/pressure cpu.some `total=` stall μs over the stage.
    pub cpu_some_stall_us: Option<i64>,
    /// Delta of /proc/pressure memory.some `total=` stall μs over the stage.
    pub memory_some_stall_us: Option<i64>,
}

/// Per-stage pressure-stall metrics recorded alongside a stage timing.
///
/// Bundles the tail-biased end-of-stage avg10 snapshot with the precise,
/// length-independent stall-microsecond delta over the stage window. Passed as
/// a single struct to `record_stage_timing` to keep its signature manageable.
#[derive(Debug, Clone, Copy, Default)]
pub struct StagePressure {
    /// End-of-stage PSI io.full avg10 snapshot.
    pub io_full_avg10: Option<f64>,
    /// End-of-stage PSI cpu.some avg10 snapshot.
    pub cpu_some_avg10: Option<f64>,
    /// End-of-stage PSI memory.some avg10 snapshot.
    pub memory_some_avg10: Option<f64>,
    /// Delta of /proc/pressure io.full `total=` stall μs over the stage.
    pub io_full_stall_us: Option<i64>,
    /// Delta of /proc/pressure cpu.some `total=` stall μs over the stage.
    pub cpu_some_stall_us: Option<i64>,
    /// Delta of /proc/pressure memory.some `total=` stall μs over the stage.
    pub memory_some_stall_us: Option<i64>,
}

/// Map a SQLite row to a `BackgroundJob`.
///
/// Expected column order (0-indexed):
///   0: id, 1: invocation_id, 2: command, 3: args_json, 4: started_at,
///   5: pid, 6: stdout_path, 7: stderr_path, 8: job_status, 9: exit_code
fn row_to_background_job(row: &rusqlite::Row<'_>) -> rusqlite::Result<BackgroundJob> {
    fn invalid_background_job_field(
        column_index: usize,
        field_name: &'static str,
        error: impl std::error::Error + Send + Sync + 'static,
    ) -> rusqlite::Error {
        rusqlite::Error::FromSqlConversionFailure(
            column_index,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid background job {field_name}: {error}"),
            )),
        )
    }

    let args_json: Option<String> = row.get(3)?;
    let started_at_str: String = row.get(4)?;
    let pid: Option<u32> = row.get(5)?;
    let job_status_str: String = row.get(8)?;
    Ok(BackgroundJob {
        id: row.get(0)?,
        invocation_id: row.get(1)?,
        command: row.get(2)?,
        args: match args_json {
            Some(args_json) => serde_json::from_str(&args_json)
                .map_err(|error| invalid_background_job_field(3, "args_json", error))?,
            None => Vec::new(),
        },
        started_at: OffsetDateTime::parse(
            &started_at_str,
            &time::format_description::well_known::Rfc3339,
        )
        .map_err(|error| invalid_background_job_field(4, "started_at", error))?,
        pid,
        stdout_path: row.get(6)?,
        stderr_path: row.get(7)?,
        job_status: JobLifecycleStatus::try_from_str(&job_status_str).map_err(|error| {
            invalid_background_job_field(
                8,
                "job_status",
                std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string()),
            )
        })?,
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
    pub pid: Option<u32>,
    pub stdout_path: Option<String>,
    pub stderr_path: Option<String>,
    /// Process lifecycle status (running/completed/failed/orphaned/killed).
    pub job_status: JobLifecycleStatus,
    pub exit_code: Option<i32>,
}

/// Resource usage snapshot for a single invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUsage {
    pub command: String,
    pub status: String,
    pub started_at: String,
    pub duration_secs: Option<f64>,
    pub process_cpu_usage_avg: Option<f64>,
    pub process_memory_usage_max_mb: Option<f64>,
    pub root_process_cpu_usage_avg: Option<f64>,
    pub root_process_memory_usage_max_mb: Option<f64>,
    pub shared_nix_daemon_cpu_usage_avg: Option<f64>,
    pub shared_nix_daemon_memory_usage_max_mb: Option<f64>,
    pub shared_nix_build_slice_cpu_usage_avg: Option<f64>,
    pub shared_nix_build_slice_memory_usage_max_mb: Option<f64>,
    pub shared_background_slice_cpu_usage_avg: Option<f64>,
    pub shared_background_slice_memory_usage_max_mb: Option<f64>,
    pub process_count_max: Option<u32>,
    pub sample_count: Option<u32>,
    pub host_cpu_usage_avg: Option<f64>,
    pub host_memory_usage_max_mb: Option<f64>,
    pub host_cpu_pressure_some_avg10_max: Option<f64>,
    pub host_io_pressure_some_avg10_max: Option<f64>,
    pub host_io_pressure_full_avg10_max: Option<f64>,
    pub host_memory_pressure_some_avg10_max: Option<f64>,
    pub host_memory_pressure_full_avg10_max: Option<f64>,
    pub host_block_read_mib_delta: Option<f64>,
    pub host_block_write_mib_delta: Option<f64>,
    pub host_block_read_iops_avg: Option<f64>,
    pub host_block_write_iops_avg: Option<f64>,
    pub host_block_busiest_device: Option<String>,
    pub host_block_busiest_device_total_mib_delta: Option<f64>,
    pub host_block_busiest_device_read_iops_avg: Option<f64>,
    pub host_block_busiest_device_write_iops_avg: Option<f64>,
    pub host_block_busiest_device_weighted_io_ms_per_s: Option<f64>,
    pub shm_free_min_mb: Option<f64>,
    pub shm_used_max_mb: Option<f64>,
}

impl ResourceUsage {
    #[must_use]
    pub fn has_samples(&self) -> bool {
        self.process_cpu_usage_avg.is_some()
            || self.process_memory_usage_max_mb.is_some()
            || self.root_process_cpu_usage_avg.is_some()
            || self.root_process_memory_usage_max_mb.is_some()
            || self.shared_nix_daemon_cpu_usage_avg.is_some()
            || self.shared_nix_daemon_memory_usage_max_mb.is_some()
            || self.shared_nix_build_slice_cpu_usage_avg.is_some()
            || self.shared_nix_build_slice_memory_usage_max_mb.is_some()
            || self.shared_background_slice_cpu_usage_avg.is_some()
            || self.shared_background_slice_memory_usage_max_mb.is_some()
            || self.process_count_max.is_some()
            || self.sample_count.is_some()
            || self.host_cpu_usage_avg.is_some()
            || self.host_memory_usage_max_mb.is_some()
            || self.host_cpu_pressure_some_avg10_max.is_some()
            || self.host_io_pressure_some_avg10_max.is_some()
            || self.host_io_pressure_full_avg10_max.is_some()
            || self.host_memory_pressure_some_avg10_max.is_some()
            || self.host_memory_pressure_full_avg10_max.is_some()
            || self.host_block_read_mib_delta.is_some()
            || self.host_block_write_mib_delta.is_some()
            || self.host_block_read_iops_avg.is_some()
            || self.host_block_write_iops_avg.is_some()
            || self.host_block_busiest_device.is_some()
            || self.host_block_busiest_device_total_mib_delta.is_some()
            || self.host_block_busiest_device_read_iops_avg.is_some()
            || self.host_block_busiest_device_write_iops_avg.is_some()
            || self
                .host_block_busiest_device_weighted_io_ms_per_s
                .is_some()
            || self.shm_free_min_mb.is_some()
            || self.shm_used_max_mb.is_some()
    }
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

/// A durable proof row produced by a successful xtask invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProofEvidence {
    pub id: i64,
    pub invocation_id: i64,
    pub command: String,
    pub proof_kind: String,
    pub scope_key: String,
    pub input_fingerprint: String,
    pub status: InvocationStatus,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub duration_secs: Option<f64>,
    pub scope_json: Option<String>,
    pub artifact_json: Option<String>,
}

fn row_to_proof_evidence(row: &rusqlite::Row<'_>) -> rusqlite::Result<ProofEvidence> {
    let status_str: String = row.get(6)?;
    Ok(ProofEvidence {
        id: row.get(0)?,
        invocation_id: row.get(1)?,
        command: row.get(2)?,
        proof_kind: row.get(3)?,
        scope_key: row.get(4)?,
        input_fingerprint: row.get(5)?,
        status: parse_stored_invocation_status(status_str)?,
        started_at: row.get(7)?,
        finished_at: row.get(8)?,
        duration_secs: row.get(9)?,
        scope_json: row.get(10)?,
        artifact_json: row.get(11)?,
    })
}

/// A resolved test execution plan that can be reused when its inputs match.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestProofUnit {
    pub id: i64,
    pub invocation_id: i64,
    pub proof_kind: String,
    pub scope_key: String,
    pub input_fingerprint: String,
    pub manifest_json: String,
    pub test_filter: Option<String>,
    pub reusable: bool,
    pub status: InvocationStatus,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub duration_secs: Option<f64>,
}

fn row_to_test_proof_unit(row: &rusqlite::Row<'_>) -> rusqlite::Result<TestProofUnit> {
    let status_str: String = row.get(7)?;
    Ok(TestProofUnit {
        id: row.get(0)?,
        invocation_id: row.get(1)?,
        proof_kind: row.get(2)?,
        scope_key: row.get(3)?,
        input_fingerprint: row.get(4)?,
        manifest_json: row.get(5)?,
        reusable: row.get::<_, i64>(6)? != 0,
        status: parse_stored_invocation_status(status_str)?,
        started_at: row.get(8)?,
        finished_at: row.get(9)?,
        duration_secs: row.get(10)?,
        test_filter: row.get(11)?,
    })
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

/// Statistics for a command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandStats {
    pub total: i64,
    pub successes: i64,
    pub failures: i64,
    pub avg_duration_secs: Option<f64>,
}

/// One row from `exercise_runs` joined to `invocations`.
pub struct ExerciseRunRow {
    pub run_id: i64,
    pub invocation_id: Option<i64>,
    pub tier: Option<String>,
    pub total: i64,
    pub passed: i64,
    pub failed: i64,
    pub skipped: i64,
    pub duration_secs: f64,
    pub recorded_at: String,
    pub invocation_status: Option<String>,
    pub git_commit: Option<String>,
}

/// One row from `exercise_results`.
pub struct ExerciseResultRow {
    pub exercise_id: String,
    pub exercise_tier: Option<String>,
    pub passed: bool,
    pub duration_secs: f64,
    pub error: Option<String>,
    pub step_count: i64,
}

pub(super) fn row_to_invocation(row: &rusqlite::Row) -> rusqlite::Result<Invocation> {
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
        started_at: parse_invocation_timestamp(7, "started_at", &started_at_str)?,
        finished_at: finished_at_str
            .as_deref()
            .map(|value| parse_invocation_timestamp(8, "finished_at", value))
            .transpose()?,
        duration_secs: row.get(9)?,
        exit_code: row.get(10)?,
        status: parse_stored_invocation_status(status_str)?,
        host: row.get(12)?,
        cwd: row.get(13)?,
        live_stage: row.get(14)?,
    })
}

/// Sandbox infrastructure metadata extracted from slog events in test output.
#[derive(Debug, Default)]
struct SandboxMeta {
    slot_name: Option<String>,
    slot_wait_ms: Option<i64>,
    cleanup_ms: Option<i64>,
}

fn parse_sandbox_metric(field: &str, value: &str) -> Result<i64> {
    value
        .parse()
        .wrap_err_with(|| format!("invalid sandbox metadata field {field}={value}"))
}

/// Parse sandbox slog events from test output to extract infrastructure metadata.
///
/// Looks for `[sandbox:*] event=slot_acquired` lines and extracts:
/// - `slot` → slot_name (e.g., "sinex_test_pool_13")
/// - `duration_ms` → slot_wait_ms (total acquisition time including cleanup)
/// - `clean_ms` → cleanup_ms (cleanup time for dirty slots, absent for clean slots)
fn parse_sandbox_meta(output: &str) -> Result<SandboxMeta> {
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
                meta.slot_wait_ms = Some(parse_sandbox_metric("duration_ms", val)?);
            } else if let Some(val) = part.strip_prefix("clean_ms=") {
                meta.cleanup_ms = Some(parse_sandbox_metric("clean_ms", val)?);
            }
        }

        // Take the first slot_acquired event (the test's primary database)
        break;
    }

    Ok(meta)
}

#[cfg(test)]
mod tests;

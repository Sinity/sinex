use camino::{Utf8Path, Utf8PathBuf};
use rusqlite::{Connection, DatabaseName, OpenFlags, Params, Row};
use serde::{Deserialize, Serialize};
use sinex_primitives::{SinexError, Uuid, temporal::Timestamp};
use std::path::Path;
use std::{error::Error, fmt};
use tempfile::NamedTempFile;

const SQLITE_ONLINE_BACKUP_METHOD: &str = "sqlite_online_backup";

fn open_read_only(path: &Utf8Path) -> Result<Connection, rusqlite::Error> {
    let conn =
        Connection::open_with_flags(Path::new(path.as_str()), OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    // Set a busy timeout so we retry on SQLITE_BUSY (source app holding a write lock)
    // instead of failing immediately. 5 seconds is generous enough for WAL checkpoints.
    conn.busy_timeout(std::time::Duration::from_secs(5))?;
    Ok(conn)
}

/// Boundaries that can justify capturing an immutable `SQLite` evidence snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqliteSnapshotTrigger {
    FirstObservation,
    ElapsedDuration,
    RowDelta,
    HistoricalBoundary,
    StaleCleanShutdown,
    Forced,
}

/// Policy controlling when a `SQLite` record source should capture snapshot evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteSnapshotPolicy {
    pub first_observation: bool,
    pub min_elapsed: Option<std::time::Duration>,
    pub min_row_delta: Option<u64>,
    pub historical_boundary: bool,
    pub stale_clean_shutdown_after: Option<std::time::Duration>,
}

impl SqliteSnapshotPolicy {
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            first_observation: false,
            min_elapsed: None,
            min_row_delta: None,
            historical_boundary: false,
            stale_clean_shutdown_after: None,
        }
    }

    #[must_use]
    pub const fn audit_default() -> Self {
        Self {
            first_observation: true,
            min_elapsed: Some(std::time::Duration::from_hours(1)),
            min_row_delta: Some(1_000),
            historical_boundary: true,
            stale_clean_shutdown_after: Some(std::time::Duration::from_hours(24)),
        }
    }

    #[must_use]
    pub fn with_first_observation(mut self, enabled: bool) -> Self {
        self.first_observation = enabled;
        self
    }

    #[must_use]
    pub fn with_min_elapsed(mut self, duration: Option<std::time::Duration>) -> Self {
        self.min_elapsed = duration;
        self
    }

    #[must_use]
    pub fn with_min_row_delta(mut self, rows: Option<u64>) -> Self {
        self.min_row_delta = rows;
        self
    }

    #[must_use]
    pub fn with_historical_boundary(mut self, enabled: bool) -> Self {
        self.historical_boundary = enabled;
        self
    }

    #[must_use]
    pub fn with_stale_clean_shutdown_after(
        mut self,
        duration: Option<std::time::Duration>,
    ) -> Self {
        self.stale_clean_shutdown_after = duration;
        self
    }

    #[must_use]
    pub fn decide(
        &self,
        state: &SqliteSnapshotState,
        checkpoint: i64,
        horizon_is_bounded: bool,
        now: Timestamp,
    ) -> Option<SqliteSnapshotTrigger> {
        if self.first_observation && !state.first_observation_captured {
            return Some(SqliteSnapshotTrigger::FirstObservation);
        }

        if self.historical_boundary && horizon_is_bounded {
            return Some(SqliteSnapshotTrigger::HistoricalBoundary);
        }

        if let Some(min_row_delta) = self.min_row_delta
            && let Some(last_row_id) = state.last_snapshot_row_id
        {
            let delta = checkpoint.saturating_sub(last_row_id);
            if u64::try_from(delta).is_ok_and(|delta| delta >= min_row_delta) {
                return Some(SqliteSnapshotTrigger::RowDelta);
            }
        }

        if let Some(min_elapsed) = self.min_elapsed
            && let Some(last_at) = state.last_snapshot_at
        {
            let elapsed = (now - last_at).whole_seconds().max(0);
            if u64::try_from(elapsed).is_ok_and(|elapsed| elapsed >= min_elapsed.as_secs()) {
                return Some(SqliteSnapshotTrigger::ElapsedDuration);
            }
        }

        if let Some(stale_after) = self.stale_clean_shutdown_after
            && let Some(clean_shutdown_at) = state.last_clean_shutdown_at
        {
            let elapsed = (now - clean_shutdown_at).whole_seconds().max(0);
            if u64::try_from(elapsed).is_ok_and(|elapsed| elapsed >= stale_after.as_secs()) {
                return Some(SqliteSnapshotTrigger::StaleCleanShutdown);
            }
        }

        None
    }
}

impl Default for SqliteSnapshotPolicy {
    fn default() -> Self {
        Self::disabled()
    }
}

/// Persistable state for applying a `SQLite` snapshot policy across scans.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteSnapshotState {
    #[serde(default)]
    pub first_observation_captured: bool,
    #[serde(default)]
    pub last_snapshot_at: Option<Timestamp>,
    #[serde(default)]
    pub last_snapshot_row_id: Option<i64>,
    #[serde(default)]
    pub last_clean_shutdown_at: Option<Timestamp>,
}

impl SqliteSnapshotState {
    pub fn record_success(&mut self, captured_at: Timestamp, row_id: i64) {
        self.first_observation_captured = true;
        self.last_snapshot_at = Some(captured_at);
        self.last_snapshot_row_id = Some(row_id);
        self.last_clean_shutdown_at = None;
    }

    pub fn record_clean_shutdown(&mut self, at: Timestamp) {
        self.last_clean_shutdown_at = Some(at);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteSnapshotMetadata {
    pub source_identifier: String,
    pub source_path: String,
    pub captured_at: Timestamp,
    pub capture_method: String,
    pub page_size: i64,
    pub page_count: i64,
    pub schema_version: i64,
    pub user_version: i64,
    pub schema_fingerprint: String,
    pub total_bytes: i64,
}

/// A consistent `SQLite` snapshot held in a temporary file until staged.
pub struct SqliteSnapshotCapture {
    _temp_file: NamedTempFile,
    snapshot_path: Utf8PathBuf,
    metadata: SqliteSnapshotMetadata,
}

impl SqliteSnapshotCapture {
    #[must_use]
    pub fn path(&self) -> &Utf8Path {
        &self.snapshot_path
    }

    #[must_use]
    pub fn metadata(&self) -> &SqliteSnapshotMetadata {
        &self.metadata
    }

    #[must_use]
    pub fn into_metadata(self) -> SqliteSnapshotMetadata {
        self.metadata
    }

    #[must_use]
    pub fn material_metadata(
        &self,
        trigger: SqliteSnapshotTrigger,
        start_row_id: i64,
        final_row_id: i64,
    ) -> serde_json::Value {
        serde_json::json!({
            "evidence_role": "sqlite_snapshot",
            "source_identifier": self.metadata.source_identifier.as_str(),
            "source_path": self.metadata.source_path.as_str(),
            "snapshot_captured_at": self.metadata.captured_at.format_rfc3339(),
            "capture_method": self.metadata.capture_method,
            "trigger": trigger,
            "row_range": {
                "start_row_id": start_row_id,
                "final_row_id": final_row_id,
            },
            "sqlite": {
                "page_size": self.metadata.page_size,
                "page_count": self.metadata.page_count,
                "schema_version": self.metadata.schema_version,
                "user_version": self.metadata.user_version,
                "schema_fingerprint": self.metadata.schema_fingerprint,
                "total_bytes": self.metadata.total_bytes,
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteSnapshotEvidenceReport {
    pub snapshot_material_id: Option<Uuid>,
    pub trigger: SqliteSnapshotTrigger,
    pub source_identifier: String,
    pub source_path: String,
    pub captured_at: Option<Timestamp>,
    pub total_bytes: Option<i64>,
    pub start_row_id: i64,
    pub final_row_id: i64,
    pub linked_material_count: usize,
    pub link_errors: Vec<String>,
    pub failure: Option<String>,
}

impl SqliteSnapshotEvidenceReport {
    #[must_use]
    pub fn failure(
        trigger: SqliteSnapshotTrigger,
        source_identifier: impl Into<String>,
        source_path: impl Into<String>,
        start_row_id: i64,
        final_row_id: i64,
        error: impl Into<String>,
    ) -> Self {
        Self {
            snapshot_material_id: None,
            trigger,
            source_identifier: source_identifier.into(),
            source_path: source_path.into(),
            captured_at: None,
            total_bytes: None,
            start_row_id,
            final_row_id,
            linked_material_count: 0,
            link_errors: Vec::new(),
            failure: Some(error.into()),
        }
    }
}

#[derive(Debug)]
pub enum SqliteSnapshotError {
    TempFile(std::io::Error),
    NonUtf8SnapshotPath,
    OpenFailed(rusqlite::Error),
    BackupFailed(rusqlite::Error),
    MetadataFailed(rusqlite::Error),
    StatFailed(std::io::Error),
}

impl fmt::Display for SqliteSnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TempFile(error) => {
                write!(f, "failed to create SQLite snapshot temp file: {error}")
            }
            Self::NonUtf8SnapshotPath => write!(f, "SQLite snapshot temp path is not valid UTF-8"),
            Self::OpenFailed(error) => {
                write!(f, "failed to open SQLite source for snapshot: {error}")
            }
            Self::BackupFailed(error) => write!(f, "failed to back up SQLite source: {error}"),
            Self::MetadataFailed(error) => {
                write!(f, "failed to inspect SQLite snapshot metadata: {error}")
            }
            Self::StatFailed(error) => write!(f, "failed to stat SQLite snapshot file: {error}"),
        }
    }
}

impl Error for SqliteSnapshotError {}

impl From<SqliteSnapshotError> for SinexError {
    fn from(error: SqliteSnapshotError) -> Self {
        SinexError::processing("SQLite snapshot capture failed").with_source(error)
    }
}

pub fn capture_sqlite_snapshot(
    path: &Utf8Path,
    source_identifier: &str,
) -> Result<SqliteSnapshotCapture, SqliteSnapshotError> {
    let temp_file = tempfile::Builder::new()
        .prefix("sinex_sqlite_snapshot_")
        .suffix(".sqlite")
        .tempfile()
        .map_err(SqliteSnapshotError::TempFile)?;
    let snapshot_path = Utf8PathBuf::from_path_buf(temp_file.path().to_path_buf())
        .map_err(|_| SqliteSnapshotError::NonUtf8SnapshotPath)?;

    let conn = open_read_only(path).map_err(SqliteSnapshotError::OpenFailed)?;
    conn.backup(DatabaseName::Main, temp_file.path(), None)
        .map_err(SqliteSnapshotError::BackupFailed)?;

    let page_size = sqlite_pragma_i64(&conn, "page_size")?;
    let page_count = sqlite_pragma_i64(&conn, "page_count")?;
    let schema_version = sqlite_pragma_i64(&conn, "schema_version")?;
    let user_version = sqlite_pragma_i64(&conn, "user_version")?;
    let schema_fingerprint = schema_fingerprint(&conn)?;
    let total_bytes = std::fs::metadata(temp_file.path())
        .map_err(SqliteSnapshotError::StatFailed)?
        .len()
        .try_into()
        .map_err(|error| SqliteSnapshotError::StatFailed(std::io::Error::other(error)))?;

    Ok(SqliteSnapshotCapture {
        _temp_file: temp_file,
        snapshot_path,
        metadata: SqliteSnapshotMetadata {
            source_identifier: source_identifier.to_string(),
            source_path: path.to_string(),
            captured_at: Timestamp::now(),
            capture_method: SQLITE_ONLINE_BACKUP_METHOD.to_string(),
            page_size,
            page_count,
            schema_version,
            user_version,
            schema_fingerprint,
            total_bytes,
        },
    })
}

fn sqlite_pragma_i64(conn: &Connection, pragma: &str) -> Result<i64, SqliteSnapshotError> {
    conn.pragma_query_value(None, pragma, |row| row.get::<_, i64>(0))
        .map_err(SqliteSnapshotError::MetadataFailed)
}

fn schema_fingerprint(conn: &Connection) -> Result<String, SqliteSnapshotError> {
    let mut stmt = conn
        .prepare(
            r"
            SELECT type, name, tbl_name, COALESCE(sql, '')
            FROM sqlite_master
            WHERE sql IS NOT NULL
            ORDER BY type, name, tbl_name
            ",
        )
        .map_err(SqliteSnapshotError::MetadataFailed)?;
    let mut rows = stmt
        .query([])
        .map_err(SqliteSnapshotError::MetadataFailed)?;
    let mut hasher = blake3::Hasher::new();
    while let Some(row) = rows.next().map_err(SqliteSnapshotError::MetadataFailed)? {
        let kind: String = row.get(0).map_err(SqliteSnapshotError::MetadataFailed)?;
        let name: String = row.get(1).map_err(SqliteSnapshotError::MetadataFailed)?;
        let table: String = row.get(2).map_err(SqliteSnapshotError::MetadataFailed)?;
        let sql: String = row.get(3).map_err(SqliteSnapshotError::MetadataFailed)?;
        hasher.update(kind.as_bytes());
        hasher.update(b"\0");
        hasher.update(name.as_bytes());
        hasher.update(b"\0");
        hasher.update(table.as_bytes());
        hasher.update(b"\0");
        hasher.update(sql.as_bytes());
        hasher.update(b"\n");
    }
    Ok(hasher.finalize().to_hex().to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqliteTableCheckError {
    MissingPath {
        path: Utf8PathBuf,
    },
    OpenFailed {
        path: Utf8PathBuf,
        error: String,
    },
    MetadataQueryFailed {
        path: Utf8PathBuf,
        table: String,
        error: String,
    },
    MissingTables {
        path: Utf8PathBuf,
        tables: Vec<String>,
    },
}

impl fmt::Display for SqliteTableCheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingPath { path } => {
                write!(f, "SQLite path does not exist: {path}")
            }
            Self::OpenFailed { path, error } => {
                write!(f, "failed to open SQLite database {path}: {error}")
            }
            Self::MetadataQueryFailed { path, table, error } => {
                write!(
                    f,
                    "failed to inspect SQLite schema for table {table} in {path}: {error}"
                )
            }
            Self::MissingTables { path, tables } => {
                write!(
                    f,
                    "SQLite database {path} is missing required tables: {}",
                    tables.join(", ")
                )
            }
        }
    }
}

impl Error for SqliteTableCheckError {}

pub fn ensure_sqlite_with_tables(
    path: &Utf8Path,
    tables: &[&str],
) -> Result<(), SqliteTableCheckError> {
    if !Path::new(path.as_str()).exists() {
        return Err(SqliteTableCheckError::MissingPath {
            path: path.to_path_buf(),
        });
    }

    let conn = open_read_only(path).map_err(|error| SqliteTableCheckError::OpenFailed {
        path: path.to_path_buf(),
        error: error.to_string(),
    })?;

    let mut missing_tables = Vec::new();
    for table in tables {
        let exists = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?1)",
                [table],
                |row| row.get::<_, bool>(0),
            )
            .map_err(|error| SqliteTableCheckError::MetadataQueryFailed {
                path: path.to_path_buf(),
                table: (*table).to_string(),
                error: error.to_string(),
            })?;
        if !exists {
            missing_tables.push((*table).to_string());
        }
    }

    if missing_tables.is_empty() {
        Ok(())
    } else {
        Err(SqliteTableCheckError::MissingTables {
            path: path.to_path_buf(),
            tables: missing_tables,
        })
    }
}

pub fn read_rows_after<T, F>(
    path: &Utf8Path,
    query: &str,
    from_row_id: i64,
    map: F,
) -> Result<(Vec<T>, i64), rusqlite::Error>
where
    F: FnMut(&Row<'_>) -> Result<T, rusqlite::Error>,
{
    read_rows_with_params(path, query, [from_row_id], from_row_id, map)
}

pub fn read_rows_with_params<T, P, F>(
    path: &Utf8Path,
    query: &str,
    params: P,
    initial_row_id: i64,
    mut map: F,
) -> Result<(Vec<T>, i64), rusqlite::Error>
where
    P: Params,
    F: FnMut(&Row<'_>) -> Result<T, rusqlite::Error>,
{
    let conn = open_read_only(path)?;
    let mut stmt = conn.prepare(query)?;
    let mut rows = stmt.query(params)?;
    let mut items = Vec::new();
    let mut last_row_id = initial_row_id;

    while let Some(row) = rows.next()? {
        let row_id = row.get::<_, i64>(0)?;
        match map(row) {
            Ok(item) => {
                last_row_id = last_row_id.max(row_id);
                items.push(item);
            }
            Err(error) => {
                return Err(rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Null,
                    Box::new(std::io::Error::other(format!(
                        "failed to map SQLite row {row_id} from {path}: {error}"
                    ))),
                ));
            }
        }
    }

    Ok((items, last_row_id))
}

pub fn max_row_id_for_query(path: &Utf8Path, query: &str) -> Result<i64, rusqlite::Error> {
    let conn = open_read_only(path)?;
    let max_id: Option<i64> = conn.query_row(query, [], |row| row.get::<_, Option<i64>>(0))?;
    Ok(max_id.unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn sqlite_snapshot_uses_online_backup_and_reports_shape() -> TestResult<()> {
        let temp = tempfile::NamedTempFile::new()?;
        let conn = rusqlite::Connection::open(temp.path())?;
        conn.execute(
            "CREATE TABLE history (id INTEGER PRIMARY KEY, value TEXT)",
            [],
        )?;
        conn.execute("INSERT INTO history (value) VALUES ('one'), ('two')", [])?;
        drop(conn);

        let path = Utf8PathBuf::from_path_buf(temp.path().to_path_buf())
            .map_err(|path| color_eyre::eyre::eyre!("non-utf8 temp path: {path:?}"))?;
        let capture = capture_sqlite_snapshot(&path, "test://sqlite")?;
        assert!(capture.metadata().total_bytes > 0);
        assert!(capture.metadata().page_size > 0);
        assert!(capture.metadata().page_count > 0);
        assert_eq!(
            capture.metadata().capture_method,
            SQLITE_ONLINE_BACKUP_METHOD
        );
        assert_eq!(capture.metadata().source_identifier, "test://sqlite");

        let snapshot_conn = rusqlite::Connection::open(capture.path().as_std_path())?;
        let count: i64 =
            snapshot_conn.query_row("SELECT COUNT(*) FROM history", [], |row| row.get(0))?;
        assert_eq!(count, 2);
        Ok(())
    }

    #[sinex_test]
    async fn sqlite_snapshot_policy_uses_explicit_boundaries() -> TestResult<()> {
        let now = Timestamp::now();
        let mut state = SqliteSnapshotState::default();
        let policy = SqliteSnapshotPolicy::disabled()
            .with_first_observation(true)
            .with_historical_boundary(true)
            .with_min_row_delta(Some(10))
            .with_min_elapsed(Some(std::time::Duration::from_mins(1)));

        assert_eq!(
            policy.decide(&state, 0, false, now),
            Some(SqliteSnapshotTrigger::FirstObservation)
        );

        state.record_success(now, 7);
        assert_eq!(
            policy.decide(&state, 8, true, now),
            Some(SqliteSnapshotTrigger::HistoricalBoundary)
        );
        assert_eq!(
            policy.decide(&state, 17, false, now),
            Some(SqliteSnapshotTrigger::RowDelta)
        );
        Ok(())
    }
}

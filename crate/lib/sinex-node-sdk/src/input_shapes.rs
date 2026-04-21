use crate::{
    AppendOnlyFilePollResult, AppendOnlyFileState, BatchImporterState, DiscoveredFile, ScanError,
    SqliteHistoryImportError, SqliteHistoryImportReport, SqliteHistoryRowOutcome,
    SqliteHistoryWarningDisposition, import_sqlite_history_lenient, import_sqlite_history_strict,
    poll_utf8_lines, scan_for_new_files,
};
use camino::Utf8Path;
use serde::{Deserialize, Serialize};
use sinex_primitives::Timestamp;
use std::{collections::BTreeMap, future::Future};

/// Shared checkpoint state for multiple SQLite-backed acquisition sources.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct SqliteSourceCheckpointState {
    #[serde(default)]
    row_ids: BTreeMap<String, i64>,
}

impl SqliteSourceCheckpointState {
    #[must_use]
    pub fn cursor(&self, key: &str) -> i64 {
        self.row_ids.get(key).copied().unwrap_or_default()
    }

    pub fn set_cursor(&mut self, key: impl Into<String>, row_id: i64) {
        self.row_ids.insert(key.into(), row_id);
    }

    pub fn advance_cursor(&mut self, key: impl Into<String>, row_id: i64) {
        let key = key.into();
        let next = self.cursor(&key).max(row_id);
        self.row_ids.insert(key, next);
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.row_ids.is_empty()
    }
}

/// Remember the import root before scanning it for changed files.
pub fn discover_importable_files_at_root(
    state: &mut BatchImporterState,
    scan_root: &Utf8Path,
    extensions: &[&str],
) -> Result<Vec<DiscoveredFile>, ScanError> {
    state.remember_scan_root(scan_root.to_owned());
    scan_for_new_files(state, scan_root, extensions)
}

/// Poll an append-only UTF-8 source and update the tracked file state in-place.
pub async fn poll_append_only_utf8_source(
    path: &Utf8Path,
    state: &mut AppendOnlyFileState,
) -> Result<AppendOnlyFilePollResult, crate::TailError> {
    let result = poll_utf8_lines(path, state.clone()).await?;
    *state = result.state.clone();
    Ok(result)
}

/// Import rows from a SQLite-backed source and advance the row-ID cursor on success.
pub async fn checkpointed_sqlite_history_lenient<
    Entry,
    Warning,
    Read,
    ReadError,
    RowId,
    Process,
    ProcessFuture,
    WarningDisposition,
>(
    cursor: &mut i64,
    end_time: Option<Timestamp>,
    read: Read,
    row_id: RowId,
    process: Process,
    warning_disposition: WarningDisposition,
) -> Result<SqliteHistoryImportReport<Warning>, ReadError>
where
    Read: FnOnce(i64, Option<Timestamp>) -> Result<(Vec<Entry>, i64), ReadError>,
    RowId: Fn(&Entry) -> i64,
    Process: FnMut(Entry) -> ProcessFuture,
    ProcessFuture: Future<Output = Result<SqliteHistoryRowOutcome, Warning>>,
    WarningDisposition: Fn(&Warning) -> SqliteHistoryWarningDisposition,
{
    let report = import_sqlite_history_lenient(
        *cursor,
        end_time,
        read,
        row_id,
        process,
        warning_disposition,
    )
    .await?;
    *cursor = (*cursor).max(report.last_row_id);
    Ok(report)
}

/// Strict `SQLite` import variant that also advances the caller-owned row-ID cursor.
pub async fn checkpointed_sqlite_history_strict<
    Entry,
    Read,
    ReadError,
    Process,
    ProcessFuture,
    ProcessError,
>(
    cursor: &mut i64,
    end_time: Option<Timestamp>,
    read: Read,
    process: Process,
) -> Result<SqliteHistoryImportReport<()>, SqliteHistoryImportError<ReadError, ProcessError>>
where
    Read: FnOnce(i64, Option<Timestamp>) -> Result<(Vec<Entry>, i64), ReadError>,
    Process: FnMut(Entry) -> ProcessFuture,
    ProcessFuture: Future<Output = Result<SqliteHistoryRowOutcome, ProcessError>>,
{
    let report = import_sqlite_history_strict(*cursor, end_time, read, process).await?;
    *cursor = (*cursor).max(report.last_row_id);
    Ok(report)
}

/// Lenient `SQLite` import variant backed by a keyed checkpoint store.
pub async fn checkpointed_sqlite_source_lenient<
    Entry,
    Warning,
    Read,
    ReadError,
    RowId,
    Process,
    ProcessFuture,
    WarningDisposition,
>(
    state: &mut SqliteSourceCheckpointState,
    key: &str,
    end_time: Option<Timestamp>,
    read: Read,
    row_id: RowId,
    process: Process,
    warning_disposition: WarningDisposition,
) -> Result<SqliteHistoryImportReport<Warning>, ReadError>
where
    Read: FnOnce(i64, Option<Timestamp>) -> Result<(Vec<Entry>, i64), ReadError>,
    RowId: Fn(&Entry) -> i64,
    Process: FnMut(Entry) -> ProcessFuture,
    ProcessFuture: Future<Output = Result<SqliteHistoryRowOutcome, Warning>>,
    WarningDisposition: Fn(&Warning) -> SqliteHistoryWarningDisposition,
{
    let mut cursor = state.cursor(key);
    let report = checkpointed_sqlite_history_lenient(
        &mut cursor,
        end_time,
        read,
        row_id,
        process,
        warning_disposition,
    )
    .await?;
    state.set_cursor(key.to_string(), cursor);
    Ok(report)
}

/// Strict `SQLite` import variant backed by a keyed checkpoint store.
pub async fn checkpointed_sqlite_source_strict<
    Entry,
    Read,
    ReadError,
    Process,
    ProcessFuture,
    ProcessError,
>(
    state: &mut SqliteSourceCheckpointState,
    key: &str,
    end_time: Option<Timestamp>,
    read: Read,
    process: Process,
) -> Result<SqliteHistoryImportReport<()>, SqliteHistoryImportError<ReadError, ProcessError>>
where
    Read: FnOnce(i64, Option<Timestamp>) -> Result<(Vec<Entry>, i64), ReadError>,
    Process: FnMut(Entry) -> ProcessFuture,
    ProcessFuture: Future<Output = Result<SqliteHistoryRowOutcome, ProcessError>>,
{
    let mut cursor = state.cursor(key);
    let report = checkpointed_sqlite_history_strict(&mut cursor, end_time, read, process).await?;
    state.set_cursor(key.to_string(), cursor);
    Ok(report)
}

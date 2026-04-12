//! `ActivityWatch` `SQLite` historical reader for the desktop ingestor.
//!
//! Kept crate-private because it is an implementation detail of the desktop node.

use camino::Utf8PathBuf;
use rusqlite::types::Type;
use serde_json::{Value as JsonValue, json};
use sinex_node_sdk::{
    SqliteTableCheckError, ensure_sqlite_with_tables, read_rows_after, read_rows_with_params,
};
use sinex_primitives::Timestamp;
use std::io::{Error as IoError, ErrorKind};
use tracing::warn;

const WINDOW_BUCKET_PREFIX: &str = "aw-watcher-window_";
const WEB_BUCKET_PREFIX: &str = "aw-watcher-web_";
const AFK_BUCKET_PREFIX: &str = "aw-watcher-afk_";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityWatchEntryKind {
    Window,
    Web,
    Afk,
}

impl ActivityWatchEntryKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Window => "window",
            Self::Web => "web",
            Self::Afk => "afk",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ActivityWatchHistoryEntry {
    pub row_id: i64,
    pub bucket_id: String,
    pub kind: ActivityWatchEntryKind,
    pub host: String,
    pub started_at: Timestamp,
    pub ended_at: Timestamp,
    pub duration_ms: u64,
    pub data: JsonValue,
}

impl ActivityWatchHistoryEntry {
    #[must_use]
    pub fn raw_material_payload(&self) -> JsonValue {
        json!({
            "row_id": self.row_id,
            "bucket_id": self.bucket_id,
            "kind": self.kind.as_str(),
            "host": self.host,
            "started_at": self.started_at.format_rfc3339(),
            "ended_at": self.ended_at.format_rfc3339(),
            "duration_ms": self.duration_ms,
            "data": self.data,
        })
    }
}

pub fn ensure_activitywatch_sqlite(path: &Utf8PathBuf) -> Result<(), SqliteTableCheckError> {
    ensure_sqlite_with_tables(path, &["events", "buckets"])
}

pub fn read_activitywatch_history(
    path: &Utf8PathBuf,
    from_row_id: i64,
    end_time: Option<Timestamp>,
) -> Result<(Vec<ActivityWatchHistoryEntry>, i64), rusqlite::Error> {
    fn skip_row(
        path: &Utf8PathBuf,
        row_id: i64,
        error: rusqlite::Error,
    ) -> Option<ActivityWatchHistoryEntry> {
        warn!(
            path = %path,
            row_id,
            error = %error,
            "Skipping malformed ActivityWatch history row"
        );
        None
    }

    fn map_activitywatch_row(
        path: &Utf8PathBuf,
        row: &rusqlite::Row<'_>,
    ) -> Result<Option<ActivityWatchHistoryEntry>, rusqlite::Error> {
        let row_id: i64 = row.get(0)?;
        let bucket_id: String = match row.get(1) {
            Ok(value) => value,
            Err(error) => return Ok(skip_row(path, row_id, error)),
        };
        let (kind, host) = classify_bucket(&bucket_id);
        let start_ns: i64 = match row.get(2) {
            Ok(value) => value,
            Err(error) => return Ok(skip_row(path, row_id, error)),
        };
        let end_ns: i64 = match row.get(3) {
            Ok(value) => value,
            Err(error) => return Ok(skip_row(path, row_id, error)),
        };
        let started_at = match parse_timestamp_ns(row_id, "starttime", 2, start_ns) {
            Ok(value) => value,
            Err(error) => return Ok(skip_row(path, row_id, error)),
        };
        let ended_at = match parse_timestamp_ns(row_id, "endtime", 3, end_ns) {
            Ok(value) => value,
            Err(error) => return Ok(skip_row(path, row_id, error)),
        };
        let duration_ns = (i128::from(end_ns) - i128::from(start_ns)).max(0);
        let duration_ms = (duration_ns / 1_000_000) as u64;

        let raw_payload = row.get::<_, Option<String>>(4)?;
        let payload = match raw_payload {
            Some(value) => match parse_activitywatch_payload(row_id, &value) {
                Ok(payload) => payload,
                Err(error) => return Ok(skip_row(path, row_id, error)),
            },
            None => JsonValue::Null,
        };

        Ok(Some(ActivityWatchHistoryEntry {
            row_id,
            bucket_id,
            kind,
            host,
            started_at,
            ended_at,
            duration_ms,
            data: payload,
        }))
    }

    if let Some(end_time) = end_time {
        let end_time_ns = encode_query_timestamp_ns(end_time)?;
        let (entries, last_row_id) = read_rows_with_params(
            path,
            "SELECT
                e.ROWID,
                b.name,
                e.starttime,
                e.endtime,
                e.data
             FROM events e
             JOIN buckets b ON b.id = e.bucketrow
             WHERE e.ROWID > ?1
               AND e.starttime <= ?2
               AND (
                 b.name LIKE 'aw-watcher-window_%'
                 OR b.name LIKE 'aw-watcher-web_%'
                 OR b.name LIKE 'aw-watcher-afk_%'
               )
             ORDER BY e.ROWID ASC",
            (from_row_id, end_time_ns),
            from_row_id,
            |row| map_activitywatch_row(path, row),
        )?;
        Ok((entries.into_iter().flatten().collect(), last_row_id))
    } else {
        let (entries, last_row_id) = read_rows_after(
            path,
            "SELECT
                e.ROWID,
                b.name,
                e.starttime,
                e.endtime,
                e.data
             FROM events e
             JOIN buckets b ON b.id = e.bucketrow
             WHERE e.ROWID > ?
               AND (
                 b.name LIKE 'aw-watcher-window_%'
                 OR b.name LIKE 'aw-watcher-web_%'
                 OR b.name LIKE 'aw-watcher-afk_%'
               )
             ORDER BY e.ROWID ASC",
            from_row_id,
            |row| map_activitywatch_row(path, row),
        )?;
        Ok((entries.into_iter().flatten().collect(), last_row_id))
    }
}

#[cfg(test)]
pub fn get_max_row_id(path: &Utf8PathBuf) -> Result<i64, rusqlite::Error> {
    use sinex_node_sdk::max_row_id_for_query;

    max_row_id_for_query(path, "SELECT MAX(ROWID) FROM events")
}

fn classify_bucket(bucket_id: &str) -> (ActivityWatchEntryKind, String) {
    if let Some(host) = bucket_id.strip_prefix(WINDOW_BUCKET_PREFIX) {
        return (ActivityWatchEntryKind::Window, host.to_string());
    }
    if let Some(host) = bucket_id.strip_prefix(WEB_BUCKET_PREFIX) {
        return (ActivityWatchEntryKind::Web, host.to_string());
    }
    if let Some(host) = bucket_id.strip_prefix(AFK_BUCKET_PREFIX) {
        return (ActivityWatchEntryKind::Afk, host.to_string());
    }
    (ActivityWatchEntryKind::Window, bucket_id.to_string())
}

fn parse_timestamp_ns(
    row_id: i64,
    field: &'static str,
    column_index: usize,
    raw_ns: i64,
) -> Result<Timestamp, rusqlite::Error> {
    Timestamp::from_unix_timestamp_nanos(i128::from(raw_ns)).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            column_index,
            Type::Integer,
            Box::new(IoError::new(
                ErrorKind::InvalidData,
                format!("ActivityWatch row {row_id} has invalid {field} nanoseconds: {raw_ns}"),
            )),
        )
    })
}

fn parse_activitywatch_payload(
    row_id: i64,
    raw_payload: &str,
) -> Result<JsonValue, rusqlite::Error> {
    serde_json::from_str(raw_payload).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            4,
            Type::Text,
            Box::new(IoError::new(
                ErrorKind::InvalidData,
                format!("ActivityWatch row {row_id} has invalid JSON payload: {error}"),
            )),
        )
    })
}

fn encode_query_timestamp_ns(end_time: Timestamp) -> Result<i64, rusqlite::Error> {
    i64::try_from(end_time.inner().unix_timestamp_nanos()).map_err(|error| {
        rusqlite::Error::ToSqlConversionFailure(Box::new(IoError::new(
            ErrorKind::InvalidData,
            format!(
                "ActivityWatch query end_time is outside SQLite i64 nanosecond range: {} ({error})",
                end_time.format_rfc3339()
            ),
        )))
    })
}

#[cfg(test)]
mod tests {
    use super::{
        ActivityWatchEntryKind, ensure_activitywatch_sqlite, get_max_row_id,
        read_activitywatch_history,
    };
    use camino::Utf8PathBuf;
    use color_eyre::eyre::eyre;
    use rusqlite::Connection;
    use tempfile::NamedTempFile;
    use xtask::sandbox::{TestResult, sinex_test};

    fn fixture_db() -> TestResult<Utf8PathBuf> {
        let temp = NamedTempFile::new()?;
        let path = Utf8PathBuf::from_path_buf(temp.into_temp_path().keep()?)
            .map_err(|path| eyre!("non-utf8 temp path: {path:?}"))?;

        let conn = Connection::open(path.as_str())?;
        conn.execute_batch(
            "
            CREATE TABLE buckets (
              id INTEGER PRIMARY KEY,
              name TEXT NOT NULL
            );
            CREATE TABLE events (
              bucketrow INTEGER NOT NULL,
              starttime INTEGER NOT NULL,
              endtime INTEGER NOT NULL,
              data TEXT,
              FOREIGN KEY(bucketrow) REFERENCES buckets(id)
            );
            INSERT INTO buckets (id, name) VALUES
              (1, 'aw-watcher-window_sinnix-prime'),
              (2, 'aw-watcher-web_sinnix-prime'),
              (3, 'aw-watcher-afk_sinnix-prime');
            INSERT INTO events (bucketrow, starttime, endtime, data) VALUES
              (1, 1000000000, 4000000000, '{\"app\":\"kitty\",\"title\":\"main.rs\"}'),
              (2, 5000000000, 9000000000, '{\"app\":\"Firefox\",\"title\":\"Docs\",\"url\":\"https://example.com\"}'),
              (3, 10000000000, 16000000000, '{\"status\":\"afk\"}');
            ",
        )?;

        Ok(path)
    }

    #[sinex_test]
    async fn activitywatch_sqlite_detection_requires_expected_schema() -> TestResult<()> {
        let path = fixture_db()?;
        ensure_activitywatch_sqlite(&path)?;
        Ok(())
    }

    #[sinex_test]
    async fn activitywatch_history_reader_parses_all_supported_bucket_types() -> TestResult<()> {
        let path = fixture_db()?;
        let (entries, last_row_id) = read_activitywatch_history(&path, 0, None)?;

        assert_eq!(entries.len(), 3);
        assert_eq!(last_row_id, 3);
        assert_eq!(entries[0].kind, ActivityWatchEntryKind::Window);
        assert_eq!(entries[0].host, "sinnix-prime");
        assert_eq!(entries[0].duration_ms, 3_000);
        assert_eq!(
            entries[0]
                .data
                .get("app")
                .and_then(serde_json::Value::as_str),
            Some("kitty")
        );
        assert_eq!(entries[1].kind, ActivityWatchEntryKind::Web);
        assert_eq!(
            entries[1]
                .data
                .get("url")
                .and_then(serde_json::Value::as_str),
            Some("https://example.com")
        );
        assert_eq!(entries[2].kind, ActivityWatchEntryKind::Afk);
        assert_eq!(
            entries[2]
                .data
                .get("status")
                .and_then(serde_json::Value::as_str),
            Some("afk")
        );

        Ok(())
    }

    #[sinex_test]
    async fn activitywatch_history_reader_respects_end_time_boundary() -> TestResult<()> {
        let path = fixture_db()?;
        let end_time = super::Timestamp::from_unix_timestamp(4)
            .ok_or_else(|| eyre!("valid ActivityWatch end time"))?;

        let (entries, last_row_id) = read_activitywatch_history(&path, 0, Some(end_time))?;

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].kind, ActivityWatchEntryKind::Window);
        assert_eq!(last_row_id, 1);
        Ok(())
    }

    #[sinex_test]
    async fn activitywatch_history_reader_rejects_unrepresentable_end_time_filter() -> TestResult<()>
    {
        let path = fixture_db()?;
        let end_time = super::Timestamp::from_unix_timestamp_nanos(i128::from(i64::MAX) + 1)
            .ok_or_else(|| eyre!("valid far-future timestamp"))?;

        let error = read_activitywatch_history(&path, 0, Some(end_time))
            .expect_err("far-future end_time filter should fail honestly");

        assert!(
            error
                .to_string()
                .contains("outside SQLite i64 nanosecond range")
        );
        Ok(())
    }

    #[sinex_test]
    async fn activitywatch_history_reader_respects_row_checkpoint() -> TestResult<()> {
        let path = fixture_db()?;
        let (entries, last_row_id) = read_activitywatch_history(&path, 1, None)?;

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].row_id, 2);
        assert_eq!(last_row_id, 3);
        assert_eq!(get_max_row_id(&path)?, 3);

        Ok(())
    }

    #[sinex_test]
    async fn activitywatch_history_reader_skips_invalid_json_rows() -> TestResult<()> {
        let path = fixture_db()?;
        let conn = Connection::open(path.as_std_path())?;
        conn.execute(
            "INSERT INTO events (bucketrow, starttime, endtime, data) VALUES (?, ?, ?, ?)",
            (1, 20_000_000_000_i64, 21_000_000_000_i64, "{\"app\":"),
        )?;
        conn.execute(
            "INSERT INTO events (bucketrow, starttime, endtime, data) VALUES (?, ?, ?, ?)",
            (
                1,
                22_000_000_000_i64,
                23_000_000_000_i64,
                "{\"app\":\"kitty\",\"title\":\"after malformed json\"}",
            ),
        )?;

        let (entries, last_row_id) = read_activitywatch_history(&path, 3, None)?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].row_id, 5);
        assert_eq!(
            entries[0]
                .data
                .get("title")
                .and_then(serde_json::Value::as_str),
            Some("after malformed json")
        );
        assert_eq!(last_row_id, 5);

        Ok(())
    }

    #[sinex_test]
    async fn activitywatch_history_reader_skips_invalid_timestamp_rows() -> TestResult<()> {
        let path = fixture_db()?;
        let conn = Connection::open(path.as_std_path())?;
        conn.execute(
            "INSERT INTO events (bucketrow, starttime, endtime, data) VALUES (?, ?, ?, ?)",
            (
                1,
                "not-a-timestamp",
                21_000_000_000_i64,
                "{\"app\":\"kitty\"}",
            ),
        )?;
        conn.execute(
            "INSERT INTO events (bucketrow, starttime, endtime, data) VALUES (?, ?, ?, ?)",
            (
                1,
                24_000_000_000_i64,
                25_000_000_000_i64,
                "{\"app\":\"kitty\",\"title\":\"after malformed timestamp\"}",
            ),
        )?;

        let (entries, last_row_id) = read_activitywatch_history(&path, 3, None)?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].row_id, 5);
        assert_eq!(
            entries[0]
                .data
                .get("title")
                .and_then(serde_json::Value::as_str),
            Some("after malformed timestamp")
        );
        assert_eq!(last_row_id, 5);

        Ok(())
    }
}

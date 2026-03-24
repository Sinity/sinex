//! ActivityWatch SQLite historical reader for the desktop ingestor.
//!
//! Kept crate-private because it is an implementation detail of the desktop node.

use camino::Utf8PathBuf;
use serde_json::{Value as JsonValue, json};
use sinex_node_sdk::{is_sqlite_with_tables, read_rows_after};
use sinex_primitives::Timestamp;

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
    pub fn string_field(&self, key: &str) -> Option<String> {
        self.data
            .get(key)
            .and_then(JsonValue::as_str)
            .map(ToOwned::to_owned)
    }

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

#[must_use]
pub fn is_activitywatch_sqlite(path: &Utf8PathBuf) -> bool {
    is_sqlite_with_tables(path, &["events", "buckets"])
}

pub fn read_activitywatch_history(
    path: &Utf8PathBuf,
    from_row_id: i64,
) -> Result<(Vec<ActivityWatchHistoryEntry>, i64), rusqlite::Error> {
    read_rows_after(
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
        |row| {
            let bucket_id: String = row.get(1)?;
            let (kind, host) = classify_bucket(&bucket_id);
            let start_ns: i64 = row.get(2)?;
            let end_ns: i64 = row.get(3)?;
            let started_at = Timestamp::from_unix_timestamp_nanos(i128::from(start_ns))
                .unwrap_or(Timestamp::UNIX_EPOCH);
            let ended_at =
                Timestamp::from_unix_timestamp_nanos(i128::from(end_ns)).unwrap_or(started_at);
            let duration_ns = (i128::from(end_ns) - i128::from(start_ns)).max(0);
            let duration_ms = u64::try_from(duration_ns / 1_000_000).unwrap_or(0);

            let payload = row
                .get::<_, Option<String>>(4)?
                .and_then(|value| serde_json::from_str(&value).ok())
                .unwrap_or(JsonValue::Object(Default::default()));

            Ok(ActivityWatchHistoryEntry {
                row_id: row.get(0)?,
                bucket_id,
                kind,
                host,
                started_at,
                ended_at,
                duration_ms,
                data: payload,
            })
        },
    )
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

#[cfg(test)]
mod tests {
    use super::{
        ActivityWatchEntryKind, get_max_row_id, is_activitywatch_sqlite, read_activitywatch_history,
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
        assert!(is_activitywatch_sqlite(&path));
        Ok(())
    }

    #[sinex_test]
    async fn activitywatch_history_reader_parses_all_supported_bucket_types() -> TestResult<()> {
        let path = fixture_db()?;
        let (entries, last_row_id) = read_activitywatch_history(&path, 0)?;

        assert_eq!(entries.len(), 3);
        assert_eq!(last_row_id, 3);
        assert_eq!(entries[0].kind, ActivityWatchEntryKind::Window);
        assert_eq!(entries[0].host, "sinnix-prime");
        assert_eq!(entries[0].string_field("app").as_deref(), Some("kitty"));
        assert_eq!(entries[1].kind, ActivityWatchEntryKind::Web);
        assert_eq!(
            entries[1].string_field("url").as_deref(),
            Some("https://example.com")
        );
        assert_eq!(entries[2].kind, ActivityWatchEntryKind::Afk);
        assert_eq!(entries[2].string_field("status").as_deref(), Some("afk"));

        Ok(())
    }

    #[sinex_test]
    async fn activitywatch_history_reader_respects_row_checkpoint() -> TestResult<()> {
        let path = fixture_db()?;
        let (entries, last_row_id) = read_activitywatch_history(&path, 1)?;

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].row_id, 2);
        assert_eq!(last_row_id, 3);
        assert_eq!(get_max_row_id(&path)?, 3);

        Ok(())
    }
}

use crate::visit::{BrowserVisitRecord, build_material_bytes, normalize_url, parse_numeric_timestamp_i64};
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sinex_node_sdk::{
    SqliteTableCheckError, ensure_sqlite_with_tables, read_rows_after, read_rows_with_params,
};
use sinex_primitives::Timestamp;
use std::io::Error as IoError;

const CHROMIUM_EPOCH_OFFSET_MICROS: i64 = 11_644_473_600_i64 * 1_000_000_i64;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum BrowserSqliteFormat {
    QutebrowserNative,
    ChromiumHistory,
}

impl BrowserSqliteFormat {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::QutebrowserNative => "qutebrowser-native",
            Self::ChromiumHistory => "chromium-history",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowserSqliteSourceConfig {
    pub path: Utf8PathBuf,
    pub browser: String,
    pub format: BrowserSqliteFormat,
}

impl BrowserSqliteSourceConfig {
    #[must_use]
    pub fn checkpoint_key(&self) -> String {
        format!("{}::{}", self.format.as_str(), self.path)
    }
}

pub fn ensure_browser_sqlite_source(
    source: &BrowserSqliteSourceConfig,
) -> Result<(), SqliteTableCheckError> {
    match source.format {
        BrowserSqliteFormat::QutebrowserNative => ensure_sqlite_with_tables(&source.path, &["History"]),
        BrowserSqliteFormat::ChromiumHistory => ensure_sqlite_with_tables(&source.path, &["urls", "visits"]),
    }
}

pub fn read_browser_sqlite_history(
    source: &BrowserSqliteSourceConfig,
    from_row_id: i64,
    end_time: Option<Timestamp>,
) -> Result<(Vec<BrowserVisitRecord>, i64), rusqlite::Error> {
    match source.format {
        BrowserSqliteFormat::QutebrowserNative => read_qutebrowser_history(source, from_row_id, end_time),
        BrowserSqliteFormat::ChromiumHistory => read_chromium_history(source, from_row_id, end_time),
    }
}

fn read_qutebrowser_history(
    source: &BrowserSqliteSourceConfig,
    from_row_id: i64,
    end_time: Option<Timestamp>,
) -> Result<(Vec<BrowserVisitRecord>, i64), rusqlite::Error> {
    fn map_row(
        source: &BrowserSqliteSourceConfig,
        row: &rusqlite::Row<'_>,
    ) -> Result<BrowserVisitRecord, rusqlite::Error> {
        let row_id: i64 = row.get(0)?;
        let url: String = row.get(1)?;
        let title: String = row.get(2)?;
        let atime: i64 = row.get(3)?;
        let redirect: i64 = row.get(4)?;
        let timestamp = parse_numeric_timestamp_i64(atime).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                3,
                rusqlite::types::Type::Integer,
                Box::new(IoError::other(format!(
                    "invalid qutebrowser atime {atime} for row {row_id}"
                ))),
            )
        })?;

        let payload = Map::from_iter([
            ("rowid".to_string(), Value::from(row_id)),
            ("url".to_string(), Value::from(url.clone())),
            ("title".to_string(), Value::from(title.clone())),
            ("atime".to_string(), Value::from(atime)),
            ("redirect".to_string(), Value::from(redirect)),
        ]);
        let material_bytes = build_material_bytes(&payload).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::new(IoError::other(error.to_string())),
            )
        })?;

        Ok(BrowserVisitRecord {
            browser: source.browser.clone(),
            title,
            url: url.clone(),
            normalized_url: normalize_url(&url),
            visit_time: timestamp,
            referrer: None,
            transition: (redirect != 0).then(|| "redirect".to_string()),
            visit_id: Some(row_id.to_string()),
            visit_duration_ms: None,
            source_file: source.path.to_string(),
            line_number: None,
            db_row_id: Some(row_id as u64),
            material_bytes,
        })
    }

    if let Some(end_time) = end_time {
        read_rows_with_params(
            &source.path,
            "SELECT ROWID, url, title, atime, redirect
             FROM History
             WHERE ROWID > ?1 AND atime <= ?2
             ORDER BY ROWID ASC",
            (from_row_id, end_time.inner().unix_timestamp()),
            from_row_id,
            |row| map_row(source, row),
        )
    } else {
        read_rows_after(
            &source.path,
            "SELECT ROWID, url, title, atime, redirect
             FROM History
             WHERE ROWID > ?
             ORDER BY ROWID ASC",
            from_row_id,
            |row| map_row(source, row),
        )
    }
}

fn read_chromium_history(
    source: &BrowserSqliteSourceConfig,
    from_row_id: i64,
    end_time: Option<Timestamp>,
) -> Result<(Vec<BrowserVisitRecord>, i64), rusqlite::Error> {
    fn map_row(
        source: &BrowserSqliteSourceConfig,
        row: &rusqlite::Row<'_>,
    ) -> Result<BrowserVisitRecord, rusqlite::Error> {
        let row_id: i64 = row.get(0)?;
        let url: String = row.get(1)?;
        let title: String = row.get(2)?;
        let visit_time_raw: i64 = row.get(3)?;
        let referrer: Option<String> = row.get(4)?;
        let transition_raw: i64 = row.get(5)?;
        let visit_duration: i64 = row.get(6)?;
        let timestamp = chromium_visit_timestamp(visit_time_raw).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                3,
                rusqlite::types::Type::Integer,
                Box::new(IoError::other(format!(
                    "invalid chromium visit_time {visit_time_raw} for row {row_id}"
                ))),
            )
        })?;

        let payload = Map::from_iter([
            ("rowid".to_string(), Value::from(row_id)),
            ("url".to_string(), Value::from(url.clone())),
            ("title".to_string(), Value::from(title.clone())),
            (
                "visit_time".to_string(),
                Value::from(visit_time_raw),
            ),
            (
                "external_referrer_url".to_string(),
                referrer
                    .as_ref()
                    .map_or(Value::Null, |value| Value::from(value.clone())),
            ),
            ("transition".to_string(), Value::from(transition_raw)),
            ("visit_duration".to_string(), Value::from(visit_duration)),
        ]);
        let material_bytes = build_material_bytes(&payload).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::new(IoError::other(error.to_string())),
            )
        })?;

        Ok(BrowserVisitRecord {
            browser: source.browser.clone(),
            title,
            url: url.clone(),
            normalized_url: normalize_url(&url),
            visit_time: timestamp,
            referrer,
            transition: Some(transition_raw.to_string()),
            visit_id: Some(row_id.to_string()),
            visit_duration_ms: (visit_duration >= 0).then_some((visit_duration as u64) / 1_000),
            source_file: source.path.to_string(),
            line_number: None,
            db_row_id: Some(row_id as u64),
            material_bytes,
        })
    }

    let end_time_bound = end_time.and_then(chromium_timestamp_bound);
    if let Some(end_time_bound) = end_time_bound {
        read_rows_with_params(
            &source.path,
            "SELECT
                visits.id,
                urls.url,
                urls.title,
                visits.visit_time,
                visits.external_referrer_url,
                visits.transition,
                visits.visit_duration
             FROM visits
             JOIN urls ON urls.id = visits.url
             WHERE visits.id > ?1 AND visits.visit_time <= ?2
             ORDER BY visits.id ASC",
            (from_row_id, end_time_bound),
            from_row_id,
            |row| map_row(source, row),
        )
    } else {
        read_rows_with_params(
            &source.path,
            "SELECT
                visits.id,
                urls.url,
                urls.title,
                visits.visit_time,
                visits.external_referrer_url,
                visits.transition,
                visits.visit_duration
             FROM visits
             JOIN urls ON urls.id = visits.url
             WHERE visits.id > ?1
             ORDER BY visits.id ASC",
            [from_row_id],
            from_row_id,
            |row| map_row(source, row),
        )
    }
}

fn chromium_visit_timestamp(raw: i64) -> Option<Timestamp> {
    let unix_micros = raw.checked_sub(CHROMIUM_EPOCH_OFFSET_MICROS)?;
    Timestamp::from_unix_timestamp_nanos(i128::from(unix_micros) * 1_000)
}

fn chromium_timestamp_bound(end_time: Timestamp) -> Option<i64> {
    let unix_micros = end_time.inner().unix_timestamp_nanos() / 1_000;
    i64::try_from(unix_micros)
        .ok()
        .and_then(|unix_micros| unix_micros.checked_add(CHROMIUM_EPOCH_OFFSET_MICROS))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chromium_visit_timestamp_converts_epoch() {
        let timestamp = chromium_visit_timestamp(133_869_418_254_638_00).unwrap();
        assert_eq!(timestamp.format_rfc3339(), "2025-03-20T10:57:05.4638Z");
    }
}

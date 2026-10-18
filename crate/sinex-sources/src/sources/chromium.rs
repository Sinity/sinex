//! Chromium-family browser history (`<profile>/History`), on `SqliteRows`:
//! `visits` joined with `urls` gives one event per visit, and `urls` alone
//! gives each page's running counts. Every profile directory with a `History`
//! file is its own binding, however many there are; the profile is named from
//! the material's path, so a replay reads it the same way.
//!
//! Chromium writes a visit when it starts and fills in `visit_duration` when
//! the page is left, and it updates a URL's counts and last visit in place, so
//! both are newer readings of the same key. Times are microseconds since
//! 1601-01-01; 0 means "none".
use super::fields;
use crate::drain::RecordContext;
use crate::drivers::sqlite_rows::SqliteRows;
use crate::registry::{Declared, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::Class;
use sinex_model::kinds::browser::{BrowserUrl, BrowserVisit};
use std::path::{Path, PathBuf};

pub const VISITS: SourceSpec = SourceSpec {
    name: "browser.chromium-visits",
    driver: Driver::SqliteRows,
    consumer: "browsing recall and the attention stream",
    class: Class::Open,
    parser: "chromium-visits",
    parser_version: 1,
    event_types: &["browser.visit"],
    framer: crate::drivers::append_file::lines,
};

pub const URLS: SourceSpec = SourceSpec {
    name: "browser.chromium-urls",
    driver: Driver::SqliteRows,
    consumer: "browsing recall and page titles",
    class: Class::Open,
    parser: "chromium-urls",
    parser_version: 1,
    event_types: &["browser.url"],
    framer: crate::drivers::append_file::lines,
};

/// The profile a `History` path belongs to: `<browser dir>/<profile dir>`.
pub fn profile_of(history: &str) -> String {
    let mut parts = Path::new(history).components().rev().skip(1);
    let profile = parts
        .next()
        .map(|c| c.as_os_str().to_string_lossy().into_owned());
    let browser = parts
        .next()
        .map(|c| c.as_os_str().to_string_lossy().into_owned());
    match (browser, profile) {
        (Some(b), Some(p)) => format!("{b}/{p}"),
        (None, Some(p)) => p,
        _ => history.to_owned(),
    }
}

fn profile(context: &RecordContext<'_>) -> Result<String, RecordError> {
    context
        .path()
        .map(profile_of)
        .ok_or_else(|| RecordError::new("a Chromium row needs its History path"))
}

const VISIT_KNOWN: &[&str] = &[
    "_rowid",
    "rowid",
    "id",
    "url",
    "visit_time",
    "from_visit",
    "transition",
    "visit_duration",
    "_url",
    "_title",
];

pub struct VisitParser;

impl Parser for VisitParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = BrowserVisit;

    fn parse(
        &self,
        _: &Map<String, Value>,
        _: &(),
    ) -> Result<(Vec<BrowserVisit>, ()), RecordError> {
        Err(RecordError::new("a Chromium row needs its History path"))
    }

    fn parse_record(
        &self,
        row: &Map<String, Value>,
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<BrowserVisit>, ()), RecordError> {
        let from_visit = fields::int(row, "from_visit").unwrap_or(0);
        let duration = row
            .get("visit_duration")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let payload = BrowserVisit {
            profile: profile(context)?,
            visit_id: fields::int(row, "id")?,
            url: fields::optional_text(row, "_url"),
            title: fields::optional_text(row, "_title"),
            visited_at: fields::from_webkit_us(fields::int(row, "visit_time")?)?,
            from_visit: (from_visit != 0).then_some(from_visit),
            transition: fields::int(row, "transition")?,
            duration_us: (duration > 0).then_some(duration),
            extra: fields::extra(row, VISIT_KNOWN),
        };
        Ok((vec![payload], ()))
    }
}

const URL_KNOWN: &[&str] = &[
    "_rowid",
    "id",
    "url",
    "title",
    "visit_count",
    "typed_count",
    "last_visit_time",
    "hidden",
];

pub struct UrlParser;

impl Parser for UrlParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = BrowserUrl;

    fn parse(&self, _: &Map<String, Value>, _: &()) -> Result<(Vec<BrowserUrl>, ()), RecordError> {
        Err(RecordError::new("a Chromium row needs its History path"))
    }

    fn parse_record(
        &self,
        row: &Map<String, Value>,
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<BrowserUrl>, ()), RecordError> {
        let last = fields::int(row, "last_visit_time")?;
        let payload = BrowserUrl {
            profile: profile(context)?,
            url_id: fields::int(row, "id")?,
            url: fields::text(row, "url")?,
            title: fields::optional_text(row, "title"),
            visit_count: fields::int(row, "visit_count")?,
            typed_count: fields::int(row, "typed_count")?,
            last_visited_at: if last == 0 {
                None
            } else {
                Some(fields::from_webkit_us(last)?)
            },
            hidden: fields::int(row, "hidden")? != 0,
            extra: fields::extra(row, URL_KNOWN),
        };
        Ok((vec![payload], ()))
    }
}

pub fn visits() -> Declared<VisitParser> {
    Declared {
        spec: VISITS,
        parser: VisitParser,
    }
}

pub fn urls() -> Declared<UrlParser> {
    Declared {
        spec: URLS,
        parser: UrlParser,
    }
}

// `?1` is the start of the mutable window in Unix nanoseconds; Chromium's
// times are microseconds since 1601 (`fields::WEBKIT_EPOCH_OFFSET_US` later).
const RECENT_VISIT: &str = "visit_duration = 0 OR visit_time > ?1 / 1000 + 11644473600000000";
const RECENT_URL: &str = "last_visit_time > ?1 / 1000 + 11644473600000000";

pub fn visits_driver(history: PathBuf, host: String) -> SqliteRows {
    SqliteRows {
        path: history,
        host,
        table: "visits",
        from: Some(
            "(SELECT visits.id AS rowid, visits.*, urls.url AS _url, urls.title AS _title FROM visits LEFT JOIN urls ON urls.id = visits.url)",
        ),
        required: &[
            "id",
            "url",
            "visit_time",
            "from_visit",
            "transition",
            "visit_duration",
        ],
        mutable: Some(RECENT_VISIT),
        full_pass_secs: 3600,
    }
}

pub fn urls_driver(history: PathBuf, host: String) -> SqliteRows {
    SqliteRows {
        path: history,
        host,
        table: "urls",
        from: None,
        required: &[
            "id",
            "url",
            "title",
            "visit_count",
            "typed_count",
            "last_visit_time",
            "hidden",
        ],
        mutable: Some(RECENT_URL),
        full_pass_secs: 3600,
    }
}

/// Every profile under a browser's user-data directory
/// (`~/.config/google-chrome`, `~/.config/chromium`): each directory holding
/// a `History` database, named `<browser dir>/<profile dir>`.
pub fn profiles(user_data: &Path) -> Vec<(String, PathBuf)> {
    let mut out: Vec<(String, PathBuf)> = std::fs::read_dir(user_data)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| {
            let history = entry.path().join("History");
            history
                .is_file()
                .then(|| (profile_of(&history.display().to_string()), history))
        })
        .collect();
    out.sort();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::drivers::sqlite_rows::RowMemory;
    use crate::sources::fields::WEBKIT_EPOCH_OFFSET_US;
    use rusqlite::Connection;
    use serde_json::json;

    const NOW_WEBKIT: i64 = 13_370_000_000_000_000;

    fn history() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("sinex-chromium-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(dir.join("Default")).unwrap();
        std::fs::create_dir_all(dir.join("Profile 7")).unwrap();
        std::fs::create_dir_all(dir.join("Crashpad")).unwrap();
        let path = dir.join("Default/History");
        let c = Connection::open(&path).unwrap();
        c.execute_batch(&format!(
            "CREATE TABLE urls (id INTEGER PRIMARY KEY, url TEXT, title TEXT, visit_count INTEGER, typed_count INTEGER, last_visit_time INTEGER, hidden INTEGER);
             CREATE TABLE visits (id INTEGER PRIMARY KEY, url INTEGER, visit_time INTEGER, from_visit INTEGER, transition INTEGER, segment_id INTEGER, visit_duration INTEGER, opener_visit INTEGER);
             INSERT INTO urls VALUES (1, 'https://a.example/', 'A', 2, 1, {NOW_WEBKIT}, 0), (2, 'https://b.example/', '', 0, 0, 0, 1);
             INSERT INTO visits VALUES (10, 1, {NOW_WEBKIT}, 0, 805306369, 0, 0, 0), (11, 1, {}, 10, 0, 0, 1500000, 3);",
            NOW_WEBKIT + 1
        ))
        .unwrap();
        std::fs::copy(&path, dir.join("Profile 7/History")).unwrap();
        path
    }

    fn rows(d: &SqliteRows) -> (Vec<Map<String, Value>>, Value) {
        let a = d.poll(&Value::Null, &mut RowMemory::default()).unwrap();
        let m = a.material.unwrap();
        let rows = m
            .records
            .iter()
            .map(|(s, e)| serde_json::from_slice(&m.bytes[*s..*e]).unwrap())
            .collect();
        (rows, m.manifest)
    }

    fn context(manifest: &Value) -> RecordContext<'_> {
        RecordContext {
            manifest,
            member: None,
        }
    }

    #[test]
    fn visits_carry_their_url_and_unknown_times_stay_unknown() {
        let path = history();
        let (rows, manifest) = rows(&visits_driver(path, "h".into()));
        let cx = context(&manifest);
        let (open, ()) = VisitParser.parse_record(&rows[0], &(), &cx).unwrap();
        assert!(open[0].profile.ends_with("/Default"));
        assert_eq!(open[0].url.as_deref(), Some("https://a.example/"));
        assert_eq!(open[0].title.as_deref(), Some("A"));
        assert_eq!((open[0].from_visit, open[0].duration_us), (None, None));
        assert_eq!(
            open[0].visited_at.unix_ns(),
            i128::from(NOW_WEBKIT - WEBKIT_EPOCH_OFFSET_US) * 1000
        );
        let (closed, ()) = VisitParser.parse_record(&rows[1], &(), &cx).unwrap();
        assert_eq!(
            (closed[0].from_visit, closed[0].duration_us),
            (Some(10), Some(1_500_000))
        );
        assert_eq!(closed[0].extra.get("opener_visit"), Some(&json!(3)));
        assert!(!closed[0].extra.contains_key("rowid"));
    }

    #[test]
    fn urls_read_counts_and_never_visited_is_unknown() {
        let path = history();
        let (rows, manifest) = rows(&urls_driver(path, "h".into()));
        let (b, ()) = UrlParser
            .parse_record(&rows[1], &(), &context(&manifest))
            .unwrap();
        assert_eq!(b[0].last_visited_at, None);
        assert_eq!(b[0].title, None);
        assert!(b[0].hidden);
    }

    #[test]
    fn profiles_are_named_from_their_path() {
        assert_eq!(
            profile_of("/home/u/.config/google-chrome/Profile 7/History"),
            "google-chrome/Profile 7"
        );
        let path = history();
        let root = path.parent().unwrap().parent().unwrap();
        let names: Vec<String> = profiles(root).into_iter().map(|(n, _)| n).collect();
        let browser = root.file_name().unwrap().to_string_lossy();
        assert_eq!(
            names,
            vec![format!("{browser}/Default"), format!("{browser}/Profile 7")]
        );
    }
}

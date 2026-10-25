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
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::Class;
use sinex_model::event_types::browser::{BrowserUrl, BrowserVisit};
use std::path::{Path, PathBuf};

pub const VISITS: SourceSpec = SourceSpec {
    name: "browser.chromium-visits",
    driver: Driver::SqliteRows,
    consumer: "browsing recall and the attention stream",
    class: Class::Open,
    parser: "chromium-visits",
    parser_version: 1,
    event_types: &["browser.visit"],
    framer: crate::Framer::incremental(crate::drivers::append_file::lines),
};

pub const URLS: SourceSpec = SourceSpec {
    name: "browser.chromium-urls",
    driver: Driver::SqliteRows,
    consumer: "browsing recall and page titles",
    class: Class::Open,
    parser: "chromium-urls",
    parser_version: 1,
    event_types: &["browser.url"],
    framer: crate::Framer::incremental(crate::drivers::append_file::lines),
};

/// The profile a `History` path belongs to: `<browser dir>/<profile dir>`.
fn profile_of(history: &str) -> String {
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

    fn parse(
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

pub fn visits() -> DeclaredSource<VisitParser> {
    DeclaredSource {
        spec: VISITS,
        parser: VisitParser,
    }
}

pub fn urls() -> DeclaredSource<UrlParser> {
    DeclaredSource {
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
fn profiles(user_data: &Path) -> Vec<(String, PathBuf)> {
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

/// `--source chromium=PATH`: every profile under a browser's user-data
/// directory, its visits and its URLs each a binding.
pub const KIND: crate::binding::InputKind = crate::binding::InputKind {
    name: "chromium",
    specs: || vec![&VISITS, &URLS],
    plan: Some(|input, settings| {
        Ok(profiles(input)
            .into_iter()
            .flat_map(|(profile, history)| {
                [&VISITS, &URLS].map(|spec| {
                    crate::binding::Binding::of(spec, &history, settings.clone()).named(&profile)
                })
            })
            .collect())
    }),
    source: |b| {
        if b.source == VISITS.name {
            Ok(Box::new(visits()))
        } else {
            Ok(Box::new(urls()))
        }
    },
    reader: Some(|b| {
        let (input, host) = (b.input.clone(), b.settings.host.clone());
        Ok(crate::binding::rows(if b.source == VISITS.name {
            visits_driver(input, host)
        } else {
            urls_driver(input, host)
        }))
    }),
    capture: None,
};

#[cfg(test)]
mod tests;

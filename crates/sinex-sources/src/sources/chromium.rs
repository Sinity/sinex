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
use crate::sources::input_format;
use serde_json::{Map, Value};
use sinex_model::event_types::browser::{BrowserUrl, BrowserVisit};
use sinex_model::json::JsonExt;
use sinex_source_core::discovery::{DiscoverySpec, Location};
use sinex_source_core::drain::RecordContext;
use sinex_source_core::drivers::sqlite_rows::{Changes, SqliteRows};
use sinex_source_core::registry::{DeclaredSource, Expectation, Origin, Shape, SourceSpec};
use sinex_source_core::{RecordError, RecordResult, Stateless, StatelessParser, binding};
use std::path::{Path, PathBuf};

pub const VISITS: SourceSpec = SourceSpec {
    name: "browser.chromium-visits",
    origin: Origin::Input(Shape::SqliteRows),
    expect: Expectation::continuous_days(2),
    expires_after: Some(sinex_source_core::registry::days(90)),
    purpose: "browsing recall and the attention stream",
    parser: "chromium-visits",
    parser_version: 2,
    event_types: &["browser.visit"],
    framer: sinex_source_core::Framer::LINES,
    ..SourceSpec::DEFAULT
};

pub const URLS: SourceSpec = SourceSpec {
    name: "browser.chromium-urls",
    origin: Origin::Input(Shape::SqliteRows),
    expect: Expectation::continuous_days(2),
    expires_after: Some(sinex_source_core::registry::days(90)),
    purpose: "browsing recall and page titles",
    parser: "chromium-urls",
    parser_version: 2,
    event_types: &["browser.url"],
    framer: sinex_source_core::Framer::LINES,
    ..SourceSpec::DEFAULT
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
        .record("a Chromium row needs its History path")
}

fn database_generation(context: &RecordContext<'_>) -> Result<String, RecordError> {
    context.manifest.detail("database_generation").as_str()
        .map(str::to_owned)
        .record("Chromium database generation is unknown; keep this material and establish its original generation before replay")
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

impl StatelessParser for VisitParser {
    type Record<'a> = Map<String, Value>;
    type Payload = BrowserVisit;

    fn parse(
        &self,
        row: &Map<String, Value>,
        context: &RecordContext<'_>,
    ) -> Result<Vec<BrowserVisit>, RecordError> {
        let from_visit = fields::int(row, "from_visit").unwrap_or(0);
        let duration = row.i64("visit_duration").unwrap_or(0);
        let payload = BrowserVisit {
            database_generation: database_generation(context)?,
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
        Ok(vec![payload])
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

impl StatelessParser for UrlParser {
    type Record<'a> = Map<String, Value>;
    type Payload = BrowserUrl;

    fn parse(
        &self,
        row: &Map<String, Value>,
        context: &RecordContext<'_>,
    ) -> Result<Vec<BrowserUrl>, RecordError> {
        let last = fields::int(row, "last_visit_time")?;
        let payload = BrowserUrl {
            database_generation: database_generation(context)?,
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
        Ok(vec![payload])
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
        changes: Changes::InPlace {
            mutable: RECENT_VISIT,
            full_pass_secs: 3600,
        },
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
        changes: Changes::InPlace {
            mutable: RECENT_URL,
            full_pass_secs: 3600,
        },
    }
}

/// Every profile under a browser's user-data directory
/// (`~/.config/google-chrome`, `~/.config/chromium`): each directory holding
/// a `History` database, named `<browser dir>/<profile dir>`.
fn profiles(user_data: &Path) -> Result<Vec<(String, PathBuf)>, String> {
    if user_data.is_file() {
        return Ok(vec![(
            profile_of(&user_data.display().to_string()),
            user_data.to_owned(),
        )]);
    }
    let entries = std::fs::read_dir(user_data)
        .map_err(|error| format!("{}: {error}", user_data.display()))?;
    let mut out = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| format!("{}: {error}", user_data.display()))?;
        let history = entry.path().join("History");
        match std::fs::metadata(&history) {
            Ok(metadata) if metadata.is_file() => {
                out.push((profile_of(&history.display().to_string()), history));
            }
            Ok(_) => {
                return Err(format!(
                    "{}: History is not a regular database",
                    history.display()
                ));
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    || error.kind() == std::io::ErrorKind::NotADirectory => {}
            Err(error) => return Err(format!("{}: {error}", history.display())),
        }
    }
    out.sort();
    Ok(out)
}

/// `stage --parser chromium PATH`: every profile under a browser's user-data
/// directory, its visits and its URLs each a binding.
input_format! { pub FORMAT = "chromium" {
    specs: [VISITS, URLS],
    source: |b| {
        if b.source == VISITS.name {
            Ok(Box::new(DeclaredSource { spec: VISITS, parser: Stateless(VisitParser) }))
        } else {
            Ok(Box::new(DeclaredSource { spec: URLS, parser: Stateless(UrlParser) }))
        }
    },
    discovery: DiscoverySpec::system(&[
        Location::new("config", "chromium"),
        Location::new("config", "google-chrome"),
        Location::new("config", "google-chrome-beta"),
        Location::new("config", "BraveSoftware/Brave-Browser"),
        Location::new("input", "chromium"),
    ]),
    plan: |input, settings| {
        Ok(profiles(input)?
            .into_iter()
            .flat_map(|(profile, history)| {
                [&VISITS, &URLS].map(|spec| {
                    binding::Binding::of(spec, &history, settings.clone()).named(&profile)
                })
            })
            .collect())
    },
    reader: |b| {
        let (input, host) = (b.input.clone(), b.settings.host.clone());
        Ok(binding::rows(if b.source == VISITS.name {
            visits_driver(input, host)
        } else {
            urls_driver(input, host)
        }))
    },
} }

#[cfg(test)]
mod tests;

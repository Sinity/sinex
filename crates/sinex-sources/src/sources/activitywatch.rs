//! ActivityWatch (`~/.local/share/activitywatch/aw-server-rust/sqlite.db`), on
//! `SqliteRows`. Each watcher writes events into its bucket; ActivityWatch
//! extends the latest event of a bucket in place while nothing changes
//! (heartbeat merging), so rows change after they are first read.
//!
//! Two capture defects are corrected here, while reading, and not in a later
//! layer. The stored rows keep the bytes exactly as ActivityWatch wrote them;
//! only the reading is corrected, and a replay re-derives it.
//!
//! - **Spinner titles.** Coding agents in kitty animate the window title with
//!   a spinner glyph, and every tick defeated ActivityWatch's merging: one
//!   agent session became thousands of near-zero events. The glyph is
//!   stripped and adjacent identical events become one span.
//! - **False not-AFK nights** (missing idle transitions) need evidence from
//!   other sources (keyboard, shell); that is a derivation over presence and
//!   those sources, not something a single row can show.
//!
//! The correction parameters live in [`CORRECTIONS`], one declaration.
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sinex_model::event_types::desktop::{
    ActivityWatchOther, BrowserTabActive, PresenceSpan, WindowSpan,
};
use sinex_model::{Class, Timestamp, json::JsonExt};
use sinex_source_core::discovery::{DiscoverySpec, Location};
use sinex_source_core::drivers::sqlite_rows::{Changes, SqliteRows};
use sinex_source_core::registry::{Expectation, Origin, Shape, SourceSpec};
use sinex_source_core::{Parser, RecordError, RecordResult};
use std::collections::BTreeMap;
use std::path::PathBuf;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub const SPEC: SourceSpec = SourceSpec {
    name: "desktop.activitywatch",
    origin: Origin::Input(Shape::SqliteRows),
    expect: Expectation::continuous_days(2),
    purpose: "attention, presence and recall",
    class: Class::Guarded,
    parser: "activitywatch-events",
    parser_version: 3,
    event_types: &[
        "window.span",
        "presence.span",
        "browser.tab_active",
        "activitywatch.other",
    ],
    framer: sinex_source_core::Framer::LINES,
    ..SourceSpec::DEFAULT
};

/// The corrections applied while reading.
pub struct Corrections {
    /// Applications whose titles carry a spinner
    /// ([`without_progress`](super::titles::without_progress)).
    pub spinner_apps: &'static [&'static str],
    /// Adjacent identical window events whose gap is at most this many
    /// nanoseconds join one span (the watcher polls every 2-3 s).
    pub merge_gap_ns: i64,
}

pub const CORRECTIONS: Corrections = Corrections {
    spinner_apps: &["kitty"],
    merge_gap_ns: 3_000_000_000,
};

/// The title without a leading spinner frame.
fn corrected_title<'a>(app: &str, title: &'a str) -> &'a str {
    if !CORRECTIONS
        .spinner_apps
        .iter()
        .any(|a| a.eq_ignore_ascii_case(app))
    {
        return title;
    }
    super::titles::without_progress(title)
}

sinex_model::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum Activity {
        Window(WindowSpan),
        Presence(PresenceSpan),
        Tab(BrowserTabActive),
        Other(ActivityWatchOther),
    }
}

/// The window span still growing in each window bucket, carried between rows.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenSpans {
    pub spans: BTreeMap<String, OpenSpan>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenSpan {
    pub span_id: i64,
    pub last_row: i64,
    pub started_ns: i128,
    pub ended_ns: i128,
    pub app: String,
    pub title: String,
    pub merged: u32,
}

pub struct ActivityWatchParser;

fn timestamp_ns(value: Option<&Value>) -> Option<i128> {
    match value? {
        Value::Number(n) => n.as_i64().map(i128::from),
        Value::String(s) => s.parse::<i128>().ok().or_else(|| {
            OffsetDateTime::parse(s, &Rfc3339)
                .ok()
                .map(|t| t.unix_timestamp_nanos())
        }),
        _ => None,
    }
}

fn ts(ns: i128) -> Result<Timestamp, RecordError> {
    Timestamp::from_unix_ns(ns).record("ActivityWatch time out of range")
}

impl Parser for ActivityWatchParser {
    type Record<'a> = Map<String, Value>;
    type Carry = OpenSpans;
    type Payload = Activity;

    fn parse(
        &self,
        row: &Map<String, Value>,
        open: &OpenSpans,
        _context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<(Vec<Activity>, OpenSpans), RecordError> {
        let id = row.i64("rowid").record("row has no rowid")?;
        let bucket = row.str("bucket").record("row has no bucket")?.to_owned();
        let bucket_type = row.str("bucket_type").unwrap_or("");
        let started_ns = timestamp_ns(row.get("starttime")).record("starttime is not a time")?;
        let ended_ns = timestamp_ns(row.get("endtime")).record("endtime is not a time")?;
        let data: Map<String, Value> = match row.get("data") {
            Some(Value::String(raw)) => {
                serde_json::from_str(raw).record("data is not a JSON object")?
            }
            Some(Value::Object(o)) => o.clone(),
            _ => return Err(RecordError::new("row has no data")),
        };
        let field = |name: &str| data.str(name).unwrap_or("").to_owned();

        let is = |type_name: &str, prefix: &str| {
            bucket_type == type_name || (bucket_type.is_empty() && bucket.starts_with(prefix))
        };
        let mut next = open.clone();
        let event = if is("currentwindow", "aw-watcher-window") {
            let app = field("app");
            let title = corrected_title(&app, &field("title")).to_owned();
            let span = match next.spans.get(&bucket) {
                // The row the span ends with, extended in place.
                Some(s) if s.last_row == id => OpenSpan {
                    ended_ns: s.ended_ns.max(ended_ns),
                    app,
                    title,
                    ..s.clone()
                },
                // The next event, same window, no real gap: one span.
                Some(s)
                    if id > s.last_row
                        && s.app == app
                        && s.title == title
                        && started_ns - s.ended_ns <= i128::from(CORRECTIONS.merge_gap_ns) =>
                {
                    OpenSpan {
                        last_row: id,
                        ended_ns: s.ended_ns.max(ended_ns),
                        merged: s.merged + 1,
                        ..s.clone()
                    }
                }
                // A new span; also where reading history again starts over.
                _ => OpenSpan {
                    span_id: id,
                    last_row: id,
                    started_ns,
                    ended_ns,
                    app,
                    title,
                    merged: 1,
                },
            };
            let event = Activity::Window(WindowSpan {
                host: _context.manifest.detail("host").as_str().map(str::to_owned),
                scope: _context
                    .manifest
                    .detail("host")
                    .as_str()
                    .map(|host| serde_json::json!([host, null]).to_string()),
                bucket: bucket.clone(),
                span_id: span.span_id,
                started_at: ts(span.started_ns)?,
                ended_at: ts(span.ended_ns)?,
                app: span.app.clone(),
                title: span.title.clone(),
                merged: span.merged,
            });
            next.spans.insert(bucket, span);
            event
        } else if is("afkstatus", "aw-watcher-afk") {
            Activity::Presence(PresenceSpan {
                host: _context.manifest.detail("host").as_str().map(str::to_owned),
                scope: _context
                    .manifest
                    .detail("host")
                    .as_str()
                    .map(|host| serde_json::json!([host, null]).to_string()),
                bucket,
                row: id,
                started_at: ts(started_ns)?,
                ended_at: ts(ended_ns)?,
                status: field("status"),
            })
        } else if is("web.tab.current", "aw-watcher-web") {
            let browser = bucket
                .strip_prefix("aw-watcher-web-")
                .and_then(|b| b.split('_').next())
                .unwrap_or("")
                .to_owned();
            Activity::Tab(BrowserTabActive {
                bucket,
                row: id,
                started_at: ts(started_ns)?,
                ended_at: ts(ended_ns)?,
                browser,
                url: field("url"),
                title: field("title"),
                incognito: data.bool("incognito"),
                audible: data.bool("audible"),
                tab_count: data.i64("tabCount"),
            })
        } else {
            Activity::Other(ActivityWatchOther {
                bucket,
                row: id,
                started_at: ts(started_ns)?,
                ended_at: ts(ended_ns)?,
                data: Value::Object(data),
            })
        };
        Ok((vec![event], next))
    }
}

pub fn driver(path: PathBuf, host: String) -> SqliteRows {
    SqliteRows {
        path,
        host,
        table: "events",
        from: Some(
            "(SELECT e.id AS rowid, b.name AS bucket, b.type AS bucket_type, e.starttime AS starttime, e.endtime AS endtime, e.data AS data FROM events e JOIN buckets b ON e.bucketrow = b.id)",
        ),
        required: &["id", "bucketrow", "starttime", "endtime", "data"],
        // A bucket's latest event grows while the watcher heartbeats.
        changes: Changes::InPlace {
            mutable: "endtime >= ?1",
            full_pass_secs: 86_400,
        },
    }
}

input_format! {
    /// `stage --parser activitywatch PATH`: `aw-server-rust`'s database.
    pub FORMAT = "activitywatch" {
    specs: [SPEC],
    parser: |_b| ActivityWatchParser,
    discovery: DiscoverySpec::system(&[Location::new("data", "activitywatch/aw-server-rust/sqlite.db")]),
    plan: single,
    reader: rows(driver),
} }

#[cfg(test)]
mod tests;

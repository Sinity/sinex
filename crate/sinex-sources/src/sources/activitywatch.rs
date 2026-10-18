//! ActivityWatch (`~/.local/share/activitywatch/aw-server-rust/sqlite.db`), on
//! `SqliteRows`. Each watcher writes events into its bucket; ActivityWatch
//! extends the latest event of a bucket in place while nothing changes
//! (heartbeat merging), so rows change after they are first read.
//!
//! Two capture defects are corrected here, while reading, and not in a later
//! layer (operator ruling 2026-08-23, `y0o3.13`). The stored rows keep the
//! bytes exactly as ActivityWatch wrote them; only the interpretation is
//! corrected, and a replay re-derives it.
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
use crate::drivers::sqlite_rows::SqliteRows;
use crate::registry::{Declared, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sinex_model::kinds::desktop::{ActivityWatchOther, BrowserTabActive, PresenceSpan, WindowSpan};
use sinex_model::{Class, Timestamp};
use std::collections::BTreeMap;
use std::path::PathBuf;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub const SPEC: SourceSpec = SourceSpec {
    name: "desktop.activitywatch",
    driver: Driver::SqliteRows,
    consumer: "attention, presence and recall",
    class: Class::Guarded,
    parser: "activitywatch-events",
    parser_version: 1,
    event_types: &[
        "window.span",
        "presence.span",
        "browser.tab_active",
        "activitywatch.other",
    ],
    framer: crate::drivers::append_file::lines,
};

/// The corrections applied while reading (`y0o3.13` AC4: one declared spec).
pub struct Corrections {
    /// Leading glyphs that are a spinner frame, not part of the title
    /// (corpus-verified class, measured 2026-08-09..23).
    pub spinner_glyphs: &'static [char],
    /// Applications whose titles carry the spinner.
    pub spinner_apps: &'static [&'static str],
    /// Adjacent identical window events whose gap is at most this many
    /// nanoseconds join one span (the watcher polls every 2-3 s).
    pub merge_gap_ns: i64,
}

pub const CORRECTIONS: Corrections = Corrections {
    spinner_glyphs: &[
        '◐', '◑', '⠂', '⠐', '⠏', '⠸', '⠦', '⠴', '⠇', '⠧', '⠋', '⠼', '⠹', '⠙', '✳',
    ],
    spinner_apps: &["kitty"],
    merge_gap_ns: 3_000_000_000,
};

/// The title without a leading spinner frame.
pub fn corrected_title<'a>(app: &str, title: &'a str) -> &'a str {
    if !CORRECTIONS
        .spinner_apps
        .iter()
        .any(|a| a.eq_ignore_ascii_case(app))
    {
        return title;
    }
    let mut chars = title.chars();
    match (chars.next(), chars.next()) {
        (Some(glyph), Some(' ')) if CORRECTIONS.spinner_glyphs.contains(&glyph) => chars.as_str(),
        _ => title,
    }
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
    Timestamp::from_unix_ns(ns).map_err(|_| RecordError::new("ActivityWatch time out of range"))
}

impl Parser for ActivityWatchParser {
    type Record<'a> = Map<String, Value>;
    type Carry = OpenSpans;
    type Payload = Activity;

    fn parse(
        &self,
        row: &Map<String, Value>,
        open: &OpenSpans,
    ) -> Result<(Vec<Activity>, OpenSpans), RecordError> {
        let id = row
            .get("rowid")
            .and_then(Value::as_i64)
            .ok_or_else(|| RecordError::new("row has no rowid"))?;
        let bucket = row
            .get("bucket")
            .and_then(Value::as_str)
            .ok_or_else(|| RecordError::new("row has no bucket"))?
            .to_owned();
        let bucket_type = row.get("bucket_type").and_then(Value::as_str).unwrap_or("");
        let started_ns = timestamp_ns(row.get("starttime"))
            .ok_or_else(|| RecordError::new("starttime is not a time"))?;
        let ended_ns = timestamp_ns(row.get("endtime"))
            .ok_or_else(|| RecordError::new("endtime is not a time"))?;
        let data: Map<String, Value> = match row.get("data") {
            Some(Value::String(raw)) => serde_json::from_str(raw)
                .map_err(|e| RecordError::new(format!("data is not a JSON object: {e}")))?,
            Some(Value::Object(o)) => o.clone(),
            _ => return Err(RecordError::new("row has no data")),
        };
        let field = |name: &str| {
            data.get(name)
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned()
        };

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
                incognito: data.get("incognito").and_then(Value::as_bool),
                audible: data.get("audible").and_then(Value::as_bool),
                tab_count: data.get("tabCount").and_then(Value::as_i64),
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

pub fn source() -> Declared<ActivityWatchParser> {
    Declared {
        spec: SPEC,
        parser: ActivityWatchParser,
    }
}

pub fn default_path() -> Option<PathBuf> {
    let data = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/share")))?;
    Some(data.join("activitywatch/aw-server-rust/sqlite.db"))
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
        mutable: Some("endtime >= ?1"),
        full_pass_secs: 86_400,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::drivers::sqlite_rows::RowMemory;
    use serde_json::json;

    fn row(
        id: i64,
        bucket: &str,
        kind: &str,
        start_s: i64,
        end_s: i64,
        data: Value,
    ) -> Map<String, Value> {
        json!({
            "rowid": id, "bucket": bucket, "bucket_type": kind,
            "starttime": start_s * 1_000_000_000, "endtime": end_s * 1_000_000_000,
            "data": data.to_string(),
        })
        .as_object()
        .unwrap()
        .clone()
    }

    fn win(id: i64, start: i64, end: i64, app: &str, title: &str) -> Map<String, Value> {
        row(
            id,
            "aw-watcher-window_h",
            "currentwindow",
            start,
            end,
            json!({"app": app, "title": title}),
        )
    }

    fn run(rows: &[Map<String, Value>]) -> (Vec<Activity>, OpenSpans) {
        let mut carry = OpenSpans::default();
        let mut out = Vec::new();
        for r in rows {
            let (e, next) = ActivityWatchParser.parse(r, &carry).unwrap();
            out.extend(e);
            carry = next;
        }
        (out, carry)
    }

    fn spans(out: &[Activity]) -> Vec<(i64, String, i64, u32)> {
        out.iter()
            .filter_map(|a| match a {
                Activity::Window(w) => Some((
                    w.span_id,
                    w.title.clone(),
                    (w.ended_at.unix_ns() / 1_000_000_000) as i64,
                    w.merged,
                )),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn spinner_glyphs_are_stripped_only_from_the_declared_apps() {
        assert_eq!(corrected_title("kitty", "⠋ claude"), "claude");
        assert_eq!(corrected_title("Kitty", "✳ claude"), "claude");
        assert_eq!(corrected_title("kitty", "⠋claude"), "⠋claude");
        assert_eq!(corrected_title("firefox", "⠋ claude"), "⠋ claude");
        assert_eq!(corrected_title("kitty", "plain"), "plain");
    }

    #[test]
    fn spinner_ticks_become_one_span() {
        let (out, _) = run(&[
            win(1, 0, 2, "kitty", "⠋ claude"),
            win(2, 2, 4, "kitty", "⠙ claude"),
            win(3, 4, 7, "kitty", "⠹ claude"),
            win(4, 20, 25, "kitty", "⠸ claude"), // 13 s gap: a new span
            win(5, 25, 26, "firefox", "docs"),
        ]);
        let s = spans(&out);
        // Every row emits its span as it stands; the last emission of each
        // key is what stays live.
        assert_eq!(s[2], (1, "claude".into(), 7, 3));
        assert_eq!(s[3], (4, "claude".into(), 25, 1));
        assert_eq!(s[4], (5, "docs".into(), 26, 1));
    }

    #[test]
    fn a_heartbeat_extending_the_last_row_extends_its_span() {
        let (_, carry) = run(&[win(1, 0, 2, "kitty", "a"), win(2, 2, 4, "kitty", "a")]);
        let (out, _) = ActivityWatchParser
            .parse(&win(2, 2, 9, "kitty", "a"), &carry)
            .unwrap();
        assert_eq!(spans(&out), vec![(1, "a".into(), 9, 2)]);
    }

    #[test]
    fn reading_history_again_reproduces_the_same_spans() {
        let rows = [
            win(1, 0, 2, "kitty", "⠋ x"),
            win(2, 2, 4, "kitty", "⠙ x"),
            win(3, 9, 10, "kitty", "y"),
        ];
        let (first, carry) = run(&rows);
        // A full pass starts from the first row again with the carry from the end.
        let mut c = carry;
        let mut again = Vec::new();
        for r in &rows {
            let (e, next) = ActivityWatchParser.parse(r, &c).unwrap();
            again.extend(e);
            c = next;
        }
        assert_eq!(spans(&first), spans(&again));
    }

    #[test]
    fn presence_tabs_and_unknown_watchers_are_kept() {
        let (out, _) = run(&[
            row(
                7,
                "aw-watcher-afk_h",
                "afkstatus",
                0,
                60,
                json!({"status": "not-afk"}),
            ),
            row(
                8,
                "aw-watcher-web-chrome_desktop",
                "web.tab.current",
                0,
                5,
                json!({"url": "https://a", "title": "A", "incognito": false, "tabCount": 3}),
            ),
            row(
                9,
                "aw-stopwatch",
                "general.stopwatch",
                0,
                1,
                json!({"label": "x"}),
            ),
        ]);
        assert!(matches!(&out[0], Activity::Presence(p) if p.status == "not-afk" && p.row == 7));
        assert!(
            matches!(&out[1], Activity::Tab(t) if t.browser == "chrome" && t.tab_count == Some(3))
        );
        assert!(matches!(&out[2], Activity::Other(o) if o.data["label"] == "x"));
    }

    #[test]
    fn rfc3339_times_from_the_python_server_are_read() {
        let mut r = win(1, 0, 1, "a", "b");
        r.insert("starttime".into(), json!("2026-01-01T00:00:00Z"));
        r.insert("endtime".into(), json!("2026-01-01T00:00:05.5Z"));
        let (out, _) = ActivityWatchParser
            .parse(&r, &OpenSpans::default())
            .unwrap();
        assert!(
            matches!(&out[0], Activity::Window(w) if w.ended_at.unix_ns() - w.started_at.unix_ns() == 5_500_000_000)
        );
    }

    #[test]
    fn the_driver_reads_the_join_and_sees_heartbeat_growth() {
        let dir = std::env::temp_dir().join(format!("sinex-aw-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("sqlite.db");
        let c = rusqlite::Connection::open(&path).unwrap();
        let now = sinex_model::Timestamp::now().unix_ns() as i64;
        c.execute_batch(&format!(
            "CREATE TABLE buckets (id INTEGER PRIMARY KEY, name TEXT, type TEXT, client TEXT, hostname TEXT, created TEXT, data TEXT);
             CREATE TABLE events (id INTEGER PRIMARY KEY, bucketrow INTEGER, starttime INTEGER, endtime INTEGER, data TEXT);
             INSERT INTO buckets VALUES (1, 'aw-watcher-window_h', 'currentwindow', 'c', 'h', 'x', '{{}}');
             INSERT INTO events VALUES (1, 1, {now}, {now}, '{{\"app\":\"kitty\",\"title\":\"⠋ a\"}}');"
        ))
        .unwrap();
        let d = driver(path.clone(), "h".into());
        let mut memory = RowMemory::default();
        let first = d.poll(&Value::Null, &mut memory).unwrap();
        let m = first.material.as_ref().unwrap();
        let cursor = (m.cursor_at)(m.bytes.len());
        let r: Map<String, Value> =
            serde_json::from_slice(&m.bytes[m.records[0].0..m.records[0].1]).unwrap();
        assert_eq!(r["bucket"], json!("aw-watcher-window_h"));
        c.execute_batch(&format!(
            "UPDATE events SET endtime = {} WHERE id = 1",
            now + 5_000_000_000
        ))
        .unwrap();
        let next = d.poll(&cursor, &mut memory).unwrap();
        assert_eq!(
            next.material.unwrap().records.len(),
            1,
            "the grown row is read again"
        );
    }
}

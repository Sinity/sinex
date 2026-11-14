//! Occurrences several sources record that no fusion family joins
//! (docs/derivations.md#fusion): window focus, page visits, media plays and
//! rest. Each source's records stay separate records. A consumer reads them
//! through a [`Family`] here, which says which records belong to it and reads
//! each into the family's common payload: `started_at`, `ended_at` (null when
//! unknown), the family's fields, `record` (the record's stable identity) and
//! `source`.
//!
//! Counts across sources are per source: an occurrence two sources saw counts
//! once for each. Time measured across sources is the union of their
//! intervals, so overlapping records never count a second twice.
use crate::Kind;
use serde_json::{Map, Value, json};
use sinex_model::clock::IntervalEnd;
use sinex_model::record::identity as record_identity;
use sinex_model::{Event, RecordState, Timestamp};

/// One source kind of a family.
pub struct Observed {
    pub event_type: &'static str,
    /// Payload fields a record must have these values in to belong.
    pub only: &'static [(&'static str, &'static str)],
    /// Where the stored payload records the end, for overlap reads; `None`
    /// when the record is read as an instant.
    pub end: Option<IntervalEnd>,
    /// The family's fields and `ended_at`; `None` when the record does not
    /// describe an occurrence of the family.
    pub read: fn(&Event) -> Option<Map<String, Value>>,
}

impl Observed {
    fn admits(&self, event_type: &str, payload: &Value) -> bool {
        self.event_type == event_type
            && self
                .only
                .iter()
                .all(|(f, v)| payload.get(*f).and_then(Value::as_str) == Some(*v))
    }
}

/// A kind of occurrence and the source kinds that record it.
pub struct Family {
    pub sources: &'static [Observed],
}

impl Family {
    /// The kinds a consumer reads for this family.
    pub fn kinds(&self) -> Vec<Kind> {
        self.sources
            .iter()
            .map(|o| Kind::only(o.event_type, o.only))
            .collect()
    }

    /// The overlap reads of this family's interval kinds.
    pub fn intervals(&self) -> Vec<(&'static str, IntervalEnd)> {
        let mut out = Vec::new();
        for source in self.sources {
            if let Some(end) = source.end
                && !out.iter().any(|(kind, _)| *kind == source.event_type)
            {
                out.push((source.event_type, end));
            }
        }
        out
    }

    /// Whether a record of this type and payload belongs to the family.
    pub fn admits(&self, event_type: &str, payload: &Value) -> bool {
        self.sources.iter().any(|o| o.admits(event_type, payload))
    }

    /// The record in the family's common payload; `None` when it does not
    /// belong, has an unknown time or is removed.
    pub fn read(&self, event: &Event) -> Option<Value> {
        if event.record_state == RecordState::Removed {
            return None;
        }
        let source = self
            .sources
            .iter()
            .find(|o| o.admits(&event.event_type, &event.payload))?;
        let start = event.ts_orig.at?;
        let mut payload = (source.read)(event)?;
        payload.insert("started_at".into(), json!(start));
        payload.entry("ended_at").or_insert(Value::Null);
        payload.insert("record".into(), json!(record_identity(event)));
        payload.insert("source".into(), json!(event.source));
        Some(Value::Object(payload))
    }
}

fn text<'a>(payload: &'a Value, pointer: &str) -> Option<&'a str> {
    payload
        .pointer(pointer)
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
}

fn time(payload: &Value, pointer: &str) -> Option<Timestamp> {
    serde_json::from_value(payload.pointer(pointer)?.clone()).ok()
}

/// Fields from `(name, value)` pairs, leaving out absent values.
fn fields<const N: usize>(pairs: [(&str, Option<Value>); N]) -> Map<String, Value> {
    pairs
        .into_iter()
        .filter_map(|(k, v)| Some((k.to_owned(), v?)))
        .collect()
}

fn focus(app: Option<&str>, title: Option<&str>, end: Option<Timestamp>) -> Map<String, Value> {
    fields([
        ("app", app.map(|a| json!(a))),
        ("title", title.map(|t| json!(t))),
        ("ended_at", end.map(|e| json!(e))),
    ])
}

/// Window focus: the compositor's focus intervals (cut by going away) and
/// ActivityWatch's window spans. Fields: `app`, `title`; `ended_at` is null
/// while a focus holds.
pub static FOCUS: Family = Family {
    sources: &[
        Observed {
            event_type: "state.interval",
            only: &[("state_kind", "desktop.focus")],
            end: Some(IntervalEnd::At("ended_at")),
            read: |e| {
                let p = &e.payload;
                Some(focus(
                    text(p, "/attributes/class"),
                    text(p, "/attributes/title"),
                    time(p, "/ended_at"),
                ))
            },
        },
        Observed {
            event_type: "window.span",
            only: &[],
            end: Some(IntervalEnd::At("ended_at")),
            read: |e| {
                let p = &e.payload;
                Some(focus(
                    text(p, "/app"),
                    text(p, "/title"),
                    time(p, "/ended_at"),
                ))
            },
        },
    ],
};

fn visit(url: &str, title: Option<&str>) -> Map<String, Value> {
    fields([
        ("url", Some(json!(url))),
        ("title", title.map(|t| json!(t))),
    ])
}

/// Page visits: the browser's own history, a history export and the
/// browser-navigation capture. A tab attention span is not a visit. Fields:
/// `url`, `title`.
pub static VISITS: Family = Family {
    sources: &[
        Observed {
            event_type: "browser.visit",
            only: &[],
            end: None,
            read: |e| Some(visit(text(&e.payload, "/url")?, text(&e.payload, "/title"))),
        },
        Observed {
            event_type: "browser.history_entry",
            only: &[],
            end: None,
            read: |e| Some(visit(text(&e.payload, "/url")?, text(&e.payload, "/title"))),
        },
        Observed {
            event_type: "link.followed",
            only: &[],
            end: None,
            read: |e| {
                Some(visit(
                    text(&e.payload, "/to_url")?,
                    text(&e.payload, "/to_title"),
                ))
            },
        },
    ],
};

/// The end of a player's report from its start and the track's length.
fn after(event: &Event, length_ns: Option<f64>) -> Option<Timestamp> {
    let start = event.ts_orig.at?.unix_ns();
    // MPRIS players report a stream of unknown length (a live stream) as the
    // largest integer, which no real track reaches in nanoseconds; such a
    // play's end is unknown, not centuries away.
    let length = length_ns.filter(|l| l.is_finite() && *l >= 0.0 && *l < i64::MAX as f64)?;
    Timestamp::from_unix_ns(start + i128::from(length as i64)).ok()
}

/// Media plays: a streaming history export's plays and the media player's
/// reports of a track starting or playing (a status other than paused or
/// stopped). A report's end is its track's length after it. Fields: `title`,
/// `artist`, `album`, `player`.
pub static PLAYS: Family = Family {
    sources: &[
        Observed {
            event_type: "music.played",
            only: &[],
            end: Some(IntervalEnd::At("ended_at")),
            read: |e| {
                let p = &e.payload;
                let title = text(p, "/track_name").or_else(|| text(p, "/episode_name"))?;
                let artist = text(p, "/artist_name").or_else(|| text(p, "/show_name"));
                Some(fields([
                    ("title", Some(json!(title))),
                    ("artist", artist.map(|a| json!(a))),
                    ("album", p.get("album_name").cloned()),
                    ("ended_at", time(p, "/ended_at").map(|t| json!(t))),
                ]))
            },
        },
        Observed {
            event_type: "player.changed",
            only: &[],
            end: None,
            read: |e| {
                let p = &e.payload;
                if matches!(text(p, "/status"), Some("Paused" | "Stopped")) {
                    return None;
                }
                let length = p.get("length_us").and_then(Value::as_f64);
                Some(fields([
                    ("title", Some(json!(text(p, "/title")?))),
                    ("artist", text(p, "/artists/0").map(|a| json!(a))),
                    ("album", p.get("album").cloned()),
                    ("player", p.get("player").cloned()),
                    (
                        "ended_at",
                        after(e, length.map(|us| us * 1e3)).map(|t| json!(t)),
                    ),
                ]))
            },
        },
    ],
};

/// The category the health providers give a nap; every other rest category
/// is sleep.
const NAP_CATEGORY: &str = "Shealth Vitality Nap Data";

const fn rest(only: &'static [(&'static str, &'static str)]) -> Observed {
    Observed {
        event_type: "health.record",
        only,
        end: Some(IntervalEnd::At("end")),
        read: |e| {
            let nap = e.payload.get("category").and_then(Value::as_str) == Some(NAP_CATEGORY);
            Some(fields([
                ("kind", Some(json!(if nap { "nap" } else { "sleep" }))),
                ("ended_at", time(&e.payload, "/end").map(|t| json!(t))),
            ]))
        },
    }
}

/// Sleep and naps as the health providers recorded them. Field: `kind`,
/// `sleep` or `nap`.
pub static REST: Family = Family {
    sources: &[
        rest(&[("provider", "sleep-as-android"), ("category", "sleep")]),
        rest(&[("provider", "samsung-health"), ("category", "Sleep")]),
        rest(&[("provider", "samsung-health-cloud"), ("category", "Sleep")]),
        rest(&[
            ("provider", "samsung-health"),
            ("category", "Sleep Combined"),
        ]),
        rest(&[
            ("provider", "samsung-health-cloud"),
            ("category", "sleep_combined"),
        ]),
        rest(&[("provider", "samsung-health"), ("category", NAP_CATEGORY)]),
        rest(&[
            ("provider", "samsung-health-cloud"),
            ("category", NAP_CATEGORY),
        ]),
    ],
};

/// Whether a record is a rest record that is sleep rather than a nap.
pub fn is_sleep(event_type: &str, payload: &Value) -> bool {
    REST.admits(event_type, payload)
        && payload.get("category").and_then(Value::as_str) != Some(NAP_CATEGORY)
}

/// The kinds of rest records that are sleep.
pub fn sleep_kinds() -> Vec<Kind> {
    REST.kinds()
        .into_iter()
        .filter(|k| !k.equal.iter().any(|(_, v)| *v == NAP_CATEGORY))
        .collect()
}

#[cfg(test)]
pub(crate) mod tests;

//! The fused kinds: each rule's fusion inputs, in preference order, and how they
//! match. Adding a source that sees one of these things is a fusion input here;
//! adding a fused kind is a rule, its payload type and a test with synthetic
//! overlaps.
//!
//! Not fused: ActivityWatch's browser-tab spans (they record which tab held
//! attention, not that a page was visited, so they would add visits nobody
//! made), window samples from arbtt (one record every few seconds, not one
//! per focus), and the terminal's command reports (they carry no host, which
//! a command's identity needs).
use super::{FusionInput, FusionRecord, FusionRule};
use serde_json::{Map, Value, json};
use sinex_model::Event;
use sinex_model::Timestamp;

const fn secs(n: i128) -> i128 {
    n * 1_000_000_000
}

/// Text compared case- and spacing-blind: players and exports disagree on both.
fn norm(s: &str) -> String {
    s.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
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

/// Output fields from `(name, value)` pairs, leaving out absent values.
fn fields<const N: usize>(pairs: [(&str, Option<Value>); N]) -> Map<String, Value> {
    pairs
        .into_iter()
        .filter_map(|(k, v)| Some((k.to_owned(), v?)))
        .collect()
}

fn fusion_record(
    event: &Event,
    identity: String,
    end: Option<Timestamp>,
    fields: Map<String, Value>,
) -> Option<FusionRecord> {
    Some(FusionRecord {
        identity,
        start: event.ts_orig.at?,
        end,
        fields,
    })
}

// Commands.

/// The host part of an Atuin `host:user` hostname.
fn host_of(hostname: &str) -> &str {
    hostname.split(':').next().unwrap_or(hostname)
}

fn command(event: &Event, extra: Map<String, Value>) -> Option<FusionRecord> {
    let p = &event.payload;
    let command = p.get("command")?.as_str()?.trim();
    let host = host_of(p.get("hostname")?.as_str()?);
    let mut fields = fields([
        ("command", Some(json!(command))),
        ("host", Some(json!(host))),
    ]);
    fields.extend(extra);
    fusion_record(event, format!("{host}\0{command}"), None, fields)
}

pub static COMMANDS: FusionRule = FusionRule {
    derivation: "command.fusion",
    version: 2,
    output: "command.fused",
    fusion_inputs: &[
        FusionInput {
            event_type: "command.executed",
            only: &[],
            source: None,
            spans: false,
            read: |e| {
                let p = &e.payload;
                command(
                    e,
                    fields([
                        ("cwd", p.get("cwd").cloned()),
                        ("session", p.get("session").cloned()),
                        ("duration_ns", p.get("duration_ns").cloned()),
                        ("exit_code", p.get("exit_code").cloned()),
                    ]),
                )
            },
        },
        FusionInput {
            event_type: "command.imported",
            only: &[],
            source: None,
            spans: false,
            read: |e| command(e, Map::new()),
        },
    ],
    inputs: &["command.executed", "command.imported"],
    window_ns: secs(2),
    fallback_ns: secs(60),
    absorb_ns: None,
    start_field: "started_at",
    end_field: None,
};

// Window focus. Focus is exclusive, so every focus fusion_record has one identity:
// what one source calls `kitty` and another `Kitty` is the same focus when it
// began at the same moment.

fn focus(
    event: &Event,
    end: Option<Timestamp>,
    app: Option<&str>,
    title: Option<&str>,
) -> Option<FusionRecord> {
    fusion_record(
        event,
        String::new(),
        end,
        fields([
            ("app", app.map(|a| json!(a))),
            ("title", title.map(|t| json!(t))),
        ]),
    )
}

pub static FOCUS: FusionRule = FusionRule {
    derivation: "focus.fusion",
    version: 1,
    output: "focus.fused",
    fusion_inputs: &[
        // The compositor's focus changes, as intervals cut by going AFK.
        FusionInput {
            event_type: "state.interval",
            only: &[("state_kind", "desktop.focus")],
            source: None,
            spans: true,
            read: |e| {
                let p = &e.payload;
                focus(
                    e,
                    time(p, "/ended_at"),
                    text(p, "/attributes/class"),
                    text(p, "/attributes/title"),
                )
            },
        },
        FusionInput {
            event_type: "window.span",
            only: &[],
            source: None,
            spans: true,
            read: |e| {
                let p = &e.payload;
                focus(e, time(p, "/ended_at"), text(p, "/app"), text(p, "/title"))
            },
        },
    ],
    inputs: &["state.interval", "window.span"],
    window_ns: secs(2),
    fallback_ns: secs(30),
    absorb_ns: Some(secs(4 * 3600)),
    start_field: "started_at",
    end_field: Some("ended_at"),
};

// Page visits.

fn visit(event: &Event, url: &str, title: Option<&str>) -> Option<FusionRecord> {
    fusion_record(
        event,
        url.to_owned(),
        None,
        fields([
            ("url", Some(json!(url))),
            ("title", title.map(|t| json!(t))),
        ]),
    )
}

pub static VISITS: FusionRule = FusionRule {
    derivation: "visit.fusion",
    version: 2,
    output: "visit.fused",
    fusion_inputs: &[
        FusionInput {
            event_type: "browser.visit",
            only: &[],
            source: None,
            spans: false,
            read: |e| visit(e, text(&e.payload, "/url")?, text(&e.payload, "/title")),
        },
        FusionInput {
            event_type: "browser.history_entry",
            only: &[],
            source: None,
            spans: false,
            read: |e| visit(e, text(&e.payload, "/url")?, text(&e.payload, "/title")),
        },
        FusionInput {
            event_type: "link.followed",
            only: &[],
            source: None,
            spans: false,
            read: |e| {
                visit(
                    e,
                    text(&e.payload, "/to_url")?,
                    text(&e.payload, "/to_title"),
                )
            },
        },
    ],
    inputs: &["browser.visit", "browser.history_entry", "link.followed"],
    window_ns: secs(3),
    fallback_ns: secs(30),
    absorb_ns: None,
    start_field: "visited_at",
    end_field: None,
};

// Plays. A player reports the track it starts, and again when its metadata
// is refreshed; the track's length gives the report an end, so a repeat
// inside it joins the same play.

fn play(
    event: &Event,
    title: &str,
    artist: Option<&str>,
    end: Option<Timestamp>,
    extra: [(&str, Option<Value>); 2],
) -> Option<FusionRecord> {
    let mut fields = fields([
        ("title", Some(json!(title))),
        ("artist", artist.map(|a| json!(a))),
    ]);
    fields.extend(self::fields(extra));
    fusion_record(
        event,
        format!("{}\0{}", norm(title), artist.map(norm).unwrap_or_default()),
        end,
        fields,
    )
}

/// A player's report of a track starting or playing: one with a title and a
/// status other than paused or stopped.
fn playing(status: Option<&str>) -> bool {
    !matches!(status, Some("Paused" | "Stopped"))
}

/// The end of a report from its start and the track's length.
fn after(event: &Event, length_ns: Option<f64>) -> Option<Timestamp> {
    let start = event.ts_orig.at?.unix_ns();
    // MPRIS players report a stream of unknown length (a live stream) as the
    // largest integer, which no real track reaches in nanoseconds; such a
    // play's end is unknown, not centuries away.
    let length = length_ns.filter(|l| l.is_finite() && *l >= 0.0 && *l < i64::MAX as f64)?;
    Timestamp::from_unix_ns(start + i128::from(length as i64)).ok()
}

fn player(e: &Event) -> Option<FusionRecord> {
    let p = &e.payload;
    if !playing(text(p, "/status")) {
        return None;
    }
    let length = p.get("length_us").and_then(Value::as_f64);
    play(
        e,
        text(p, "/title")?,
        text(p, "/artists/0"),
        after(e, length.map(|us| us * 1e3)),
        [
            ("album", p.get("album").cloned()),
            ("player", p.get("player").cloned()),
        ],
    )
}

pub static PLAYS: FusionRule = FusionRule {
    derivation: "play.fusion",
    version: 3,
    output: "play.fused",
    fusion_inputs: &[
        FusionInput {
            event_type: "music.played",
            only: &[],
            source: None,
            spans: true,
            read: |e| {
                let p = &e.payload;
                let title = text(p, "/track_name").or_else(|| text(p, "/episode_name"))?;
                let artist = text(p, "/artist_name").or_else(|| text(p, "/show_name"));
                play(
                    e,
                    title,
                    artist,
                    time(p, "/ended_at"),
                    [("album", p.get("album_name").cloned()), ("player", None)],
                )
            },
        },
        FusionInput {
            event_type: "player.changed",
            only: &[("captured_by", "sinex")],
            source: None,
            spans: false,
            read: player,
        },
        FusionInput {
            event_type: "player.changed",
            only: &[("captured_by", "sinnix")],
            source: None,
            spans: false,
            read: player,
        },
    ],
    inputs: &["music.played", "player.changed"],
    window_ns: secs(10),
    fallback_ns: secs(60),
    absorb_ns: Some(secs(2 * 3600)),
    start_field: "started_at",
    end_field: Some("ended_at"),
};

// Clipboard and notifications: the same change seen by Sinex's live capture
// and the Sinnix capture lake while both run. The capture program lets fusion
// pair the captures without joining repeated records from one program.

fn copy(
    event: &Event,
    selection: &str,
    text: Option<&str>,
    mime_type: Option<&str>,
    bytes: Option<u64>,
) -> Option<FusionRecord> {
    let content = match (text, mime_type, bytes) {
        (Some(t), _, _) => format!("text\0{t}"),
        (None, Some(m), Some(b)) => format!("{m}\0{b}"),
        _ => return None,
    };
    fusion_record(
        event,
        format!("{selection}\0{content}"),
        None,
        fields([
            ("selection", Some(json!(selection))),
            ("text", text.map(|t| json!(t))),
            ("mime_type", mime_type.map(|m| json!(m))),
        ]),
    )
}

fn clipboard(e: &Event) -> Option<FusionRecord> {
    let p = &e.payload;
    if p.get("secret").and_then(Value::as_bool) == Some(true) {
        return None;
    }
    copy(
        e,
        text(p, "/selection")?,
        text(p, "/text"),
        text(p, "/mime_type"),
        p.get("bytes").and_then(Value::as_u64),
    )
}

pub static CLIPBOARD: FusionRule = FusionRule {
    derivation: "clipboard.fusion",
    version: 2,
    output: "clipboard.fused",
    fusion_inputs: &[
        FusionInput {
            event_type: "clipboard.copied",
            only: &[("captured_by", "sinex")],
            source: None,
            spans: false,
            read: clipboard,
        },
        FusionInput {
            event_type: "clipboard.copied",
            only: &[("captured_by", "sinnix")],
            source: None,
            spans: false,
            read: clipboard,
        },
    ],
    inputs: &["clipboard.copied"],
    window_ns: secs(2),
    fallback_ns: secs(10),
    absorb_ns: None,
    start_field: "copied_at",
    end_field: None,
};

fn notification(
    event: &Event,
    app: &str,
    summary: &str,
    body: Option<&str>,
) -> Option<FusionRecord> {
    fusion_record(
        event,
        format!("{app}\0{summary}\0{}", body.unwrap_or_default()),
        None,
        fields([
            ("app", Some(json!(app))),
            ("summary", Some(json!(summary))),
            ("body", body.map(|b| json!(b))),
        ]),
    )
}

fn shown_notification(e: &Event) -> Option<FusionRecord> {
    let p = &e.payload;
    notification(e, text(p, "/app")?, text(p, "/summary")?, text(p, "/body"))
}

pub static NOTIFICATIONS: FusionRule = FusionRule {
    derivation: "notification.fusion",
    version: 2,
    output: "notification.fused",
    fusion_inputs: &[
        FusionInput {
            event_type: "notification.shown",
            only: &[("captured_by", "sinex")],
            source: None,
            spans: false,
            read: shown_notification,
        },
        FusionInput {
            event_type: "notification.shown",
            only: &[("captured_by", "sinnix")],
            source: None,
            spans: false,
            read: shown_notification,
        },
    ],
    inputs: &["notification.shown"],
    window_ns: secs(2),
    fallback_ns: secs(10),
    absorb_ns: None,
    start_field: "shown_at",
    end_field: None,
};

// Mail: one message stored by several mail sources.

fn mail(event: &Event) -> Option<FusionRecord> {
    let p = &event.payload;
    let message_id = text(p, "/message_id")?;
    fusion_record(
        event,
        message_id.to_owned(),
        None,
        fields([
            ("message_id", Some(json!(message_id))),
            ("subject", p.get("subject").cloned()),
            ("from", p.get("from").cloned()),
            ("thread_id", p.get("thread_id").cloned()),
        ]),
    )
}

const fn mail_from(source: &'static str) -> FusionInput {
    FusionInput {
        event_type: "mail.message",
        only: &[],
        source: Some(source),
        spans: false,
        read: mail,
    }
}

pub static MAIL: FusionRule = FusionRule {
    derivation: "mail.fusion",
    version: 1,
    output: "mail.fused",
    fusion_inputs: &[
        mail_from("communication.imap"),
        mail_from("communication.maildir"),
        mail_from("communication.mbox"),
        mail_from("communication.outlook"),
        mail_from("communication.outlook-csv"),
    ],
    inputs: &["mail.message"],
    // The same `Date` header wherever the message is stored.
    window_ns: secs(1),
    fallback_ns: secs(1),
    absorb_ns: None,
    start_field: "sent_at",
    end_field: None,
};

// Posts: the operator's GDPR export and a public archive's copy carry the
// service's own IDs and creation times.

fn post(event: &Event) -> Option<FusionRecord> {
    let p = &event.payload;
    let service = text(p, "/service")?;
    let kind = text(p, "/kind")?;
    let native_id = text(p, "/native_id")?;
    fusion_record(
        event,
        format!("{service}\0{kind}\0{native_id}"),
        None,
        fields([
            ("service", Some(json!(service))),
            ("kind", Some(json!(kind))),
            ("native_id", Some(json!(native_id))),
            ("title", p.get("title").cloned()),
            ("body", p.get("body").cloned()),
            ("url", p.get("url").cloned()),
            ("community", p.get("community").cloned()),
        ]),
    )
}

const fn post_from(source: &'static str) -> FusionInput {
    FusionInput {
        event_type: "post.published",
        only: &[],
        source: Some(source),
        spans: false,
        read: post,
    }
}

pub static POSTS: FusionRule = FusionRule {
    derivation: "post.fusion",
    version: 1,
    output: "post.fused",
    fusion_inputs: &[
        post_from("social.reddit"),
        post_from("social.reddit-archive"),
    ],
    inputs: &["post.published"],
    // Both give the service's creation time, to the second.
    window_ns: secs(1),
    fallback_ns: secs(1),
    absorb_ns: None,
    start_field: "created_at",
    end_field: None,
};

// Conversation messages: a GDPR export names people, a tool that saved the
// same messages names accounts by ID, so the text and the millisecond it was
// sent are what both share.

fn message(event: &Event) -> Option<FusionRecord> {
    let p = &event.payload;
    let content = text(p, "/text").map(norm).unwrap_or_default();
    fusion_record(
        event,
        format!("{}\0{content}", text(p, "/service")?),
        None,
        fields([
            ("service", p.get("service").cloned()),
            ("thread", p.get("thread").cloned()),
            ("thread_name", p.get("thread_name").cloned()),
            ("sender", p.get("sender").cloned()),
            ("text", p.get("text").cloned()),
            ("message_id", p.get("message_id").cloned()),
        ]),
    )
}

const fn message_from(source: &'static str) -> FusionInput {
    FusionInput {
        event_type: "conversation.message",
        only: &[],
        source: Some(source),
        spans: false,
        read: message,
    }
}

pub static CONVERSATIONS: FusionRule = FusionRule {
    derivation: "conversation.fusion",
    version: 1,
    output: "conversation.fused",
    fusion_inputs: &[
        message_from("communication.messenger"),
        message_from("communication.messenger-sqlite"),
    ],
    inputs: &["conversation.message"],
    // Both give Messenger's send time in milliseconds.
    window_ns: 1_000_000,
    fallback_ns: 1_000_000,
    absorb_ns: None,
    start_field: "sent_at",
    end_field: None,
};

/// Every fusion rule.
pub static RULES: &[&FusionRule] = &[
    &COMMANDS,
    &FOCUS,
    &VISITS,
    &PLAYS,
    &CLIPBOARD,
    &NOTIFICATIONS,
    &MAIL,
    &POSTS,
    &CONVERSATIONS,
];

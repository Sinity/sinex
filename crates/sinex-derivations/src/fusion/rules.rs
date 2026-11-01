//! The fused kinds: each rule's fusion inputs, in preference order, and how
//! they match. Two families are defined: one shell command seen through Atuin
//! and the shell's own history, and one mail message stored by several mail
//! sources of one account. Adding a source of one of these occurrences is a
//! fusion input here; adding a fused kind is a rule written against its
//! actual sources, its payload type and a test with synthetic overlaps.
//!
//! The terminal's command reports are not a command input: they carry no
//! host, which a command's identity needs.
use super::{FusionInput, FusionRecord, FusionRule};
use serde_json::{Map, Value, json};
use sinex_model::Event;

const fn secs(n: i128) -> i128 {
    n * 1_000_000_000
}

fn text<'a>(payload: &'a Value, pointer: &str) -> Option<&'a str> {
    payload
        .pointer(pointer)
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
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
    identity: Option<String>,
    fields: Map<String, Value>,
) -> Option<FusionRecord> {
    Some(FusionRecord {
        identity,
        start: event.ts_orig.at?,
        end: None,
        fields,
    })
}

// Commands.

/// The host part of an Atuin `host:user` hostname.
fn host_of(hostname: &str) -> &str {
    hostname.split(':').next().unwrap_or(hostname)
}

/// The host of a command record: a fused command's `host`, or the host part
/// of a source record's `hostname`.
pub fn command_host(payload: &Value) -> Option<&str> {
    payload
        .get("host")
        .and_then(Value::as_str)
        .or_else(|| payload.get("hostname").and_then(Value::as_str).map(host_of))
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
    fusion_record(event, Some(format!("{host}\0{command}")), fields)
}

pub static COMMANDS: FusionRule = FusionRule {
    derivation: "command.fusion",
    version: 3,
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

// Mail: one message stored by several mail sources.

/// A message's identity is its Message-ID within its account; one without
/// an account or a Message-ID never joins another.
fn mail(event: &Event) -> Option<FusionRecord> {
    let p = &event.payload;
    let message_id = text(p, "/message_id");
    let identity = text(p, "/account")
        .zip(message_id)
        .map(|(account, id)| json!([account, id]).to_string());
    fusion_record(
        event,
        identity,
        fields([
            ("account", p.get("account").cloned()),
            ("message_id", Some(json!(message_id))),
            ("subject", p.get("subject").cloned()),
            ("from", p.get("from").cloned()),
            ("thread_id", p.get("thread_id").cloned()),
            (
                "labels",
                Some(p.get("labels").cloned().unwrap_or_else(|| json!([]))),
            ),
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
    version: 3,
    output: "mail.fused",
    fusion_inputs: &[
        mail_from("communication.gmail"),
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

/// Every fusion rule.
pub static RULES: &[&FusionRule] = &[&COMMANDS, &MAIL];

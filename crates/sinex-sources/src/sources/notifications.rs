//! Desktop notifications, read off the session bus: every `Notify` call to the
//! notification server, and its `NotificationClosed` and `ActionInvoked`
//! signals. Nothing keeps notifications after they close, so capture spools
//! each message as it passes (`dbus`).
//!
//! The id a server assigns comes back in its reply to the call, which names
//! the call by the caller and the call's serial number. Capture also
//! monitors the server's replies and errors, and the parser holds each
//! `Notify` call until its reply arrives, then records the shown notification
//! with its id at the reply's record. A call answered with an error, or with
//! no answer within D-Bus's default reply timeout (25 seconds) of a later
//! message, is recorded without an id. Closing and actions are recorded by
//! id.
use super::dbus::{self, Message};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sinex_model::Class;
use sinex_model::event_types::session::{
    NotificationAction, NotificationClosed, NotificationShown,
};
#[cfg(test)]
use sinex_source_core::DeclaredSource;
use sinex_source_core::RecordResult;
use sinex_source_core::discovery::{CaptureStream, CapturedBy, DiscoverySpec};
use sinex_source_core::drivers::live_stream;
use sinex_source_core::registry::{Expectation, Origin, Shape, SourceSpec};
use sinex_source_core::{Parser, RecordError};
use std::path::Path;

pub const SPEC: SourceSpec = SourceSpec {
    name: "desktop.notifications",
    origin: Origin::Input(Shape::LiveStream),
    expect: Expectation::continuous_days(2),
    purpose: "recall and the day's interruptions",
    // Message bodies are anything any program chose to show.
    class: Class::Guarded,
    parser: "dbus-notifications",
    parser_version: 3,
    event_types: &[
        "notification.shown",
        "notification.closed",
        "notification.action",
    ],
    framer: sinex_source_core::Framer::LINES,
    ..SourceSpec::DEFAULT
};

/// The match rules capture monitors.
pub const MATCHES: &[&str] = &[
    "type=method_call,interface=org.freedesktop.Notifications,member=Notify",
    "type=signal,interface=org.freedesktop.Notifications,member=NotificationClosed",
    "type=signal,interface=org.freedesktop.Notifications,member=ActionInvoked",
    "type=method_return,sender=org.freedesktop.Notifications",
    "type=error,sender=org.freedesktop.Notifications",
];

/// D-Bus's default reply timeout: a call unanswered this long after it was
/// made has failed for its caller.
const REPLY_TIMEOUT_NS: i128 = 25_000_000_000;

/// A `Notify` call waiting for its reply.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Waiting {
    /// The caller's unique bus name and the call's serial number.
    pub caller: String,
    pub serial: u64,
    pub shown: NotificationShown,
}

sinex_model::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum Notification {
        Shown(NotificationShown),
        Closed(NotificationClosed),
        Action(NotificationAction),
    }
}

pub struct NotificationsParser;

fn closed_reason(code: u64) -> &'static str {
    match code {
        1 => "expired",
        2 => "dismissed",
        3 => "closed",
        _ => "undefined",
    }
}

fn text(v: Option<&Value>) -> String {
    v.and_then(Value::as_str).unwrap_or_default().to_owned()
}

fn id(v: Option<&Value>) -> Result<u32, RecordError> {
    v.and_then(Value::as_u64)
        .and_then(|n| u32::try_from(n).ok())
        .record("notification id is not a u32")
}

impl Parser for NotificationsParser {
    type Record<'a> = Map<String, Value>;
    /// The `Notify` calls waiting for their replies, oldest first.
    type Carry = Vec<Waiting>;
    type Payload = Notification;

    fn parse(
        &self,
        record: &Map<String, Value>,
        waiting: &Vec<Waiting>,
        _context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<(Vec<Notification>, Vec<Waiting>), RecordError> {
        let (received_at, bytes) = live_stream::received(record).map_err(RecordError::new)?;
        let m = Message::parse(&bytes).map_err(RecordError::new)?;
        // Calls whose reply is overdue are recorded without an id.
        let (overdue, mut waiting): (Vec<Waiting>, Vec<Waiting>) =
            waiting.iter().cloned().partition(|w| {
                received_at.unix_ns() - w.shown.received_at.unix_ns() > REPLY_TIMEOUT_NS
            });
        let mut out: Vec<Notification> = overdue
            .into_iter()
            .map(|w| Notification::Shown(w.shown))
            .collect();
        if matches!(m.kind.as_str(), "method_return" | "error") {
            let answered = waiting
                .iter()
                .position(|w| Some(w.serial) == m.reply_serial && w.caller == m.destination);
            if let Some(i) = answered {
                let mut shown = waiting.remove(i).shown;
                if m.kind == "method_return" {
                    shown.notification_id = m
                        .args
                        .first()
                        .and_then(Value::as_u64)
                        .and_then(|n| u32::try_from(n).ok());
                }
                out.push(Notification::Shown(shown));
            }
            // A reply to another call (capabilities, server information).
            return Ok((out, waiting));
        }
        let read = match (m.kind.as_str(), m.member.as_str()) {
            ("method_call", "Notify") => {
                let a = &m.args;
                if a.len() != 8 {
                    return Err(RecordError::new(format!(
                        "Notify with {} arguments, expected 8",
                        a.len()
                    )));
                }
                let replaces = a[1]
                    .as_u64()
                    .and_then(|n| u32::try_from(n).ok())
                    .filter(|n| *n != 0);
                let icon = text(a.get(2));
                let hints = a[6].as_object().cloned().unwrap_or_default();
                let mut extra = Map::new();
                if let Some((product, delivery)) = signals::notifications::signal_origin(
                    hints
                        .iter()
                        .filter_map(|(key, value)| Some((key.as_str(), value.as_str()?))),
                ) {
                    extra.insert(
                        "signal_origin".into(),
                        serde_json::json!({"product": product, "delivery": delivery}),
                    );
                }
                let shown = NotificationShown {
                    captured_by: sinex_model::event_types::session::CaptureProgram::Sinex,
                    received_at,
                    app: text(a.first()),
                    notification_id: None,
                    replaces_id: replaces,
                    icon: (!icon.is_empty()).then_some(icon),
                    summary: text(a.get(3)),
                    body: text(a.get(4)),
                    actions: a[5]
                        .as_array()
                        .map(|xs| {
                            xs.iter()
                                .filter_map(Value::as_str)
                                .map(str::to_owned)
                                .collect()
                        })
                        .unwrap_or_default(),
                    hints,
                    expire_timeout_ms: a[7]
                        .as_i64()
                        .and_then(|n| i32::try_from(n).ok())
                        .unwrap_or(-1),
                    sender: m.sender.clone(),
                    extra,
                };
                match m.serial {
                    Some(serial) => {
                        waiting.push(Waiting {
                            caller: m.sender,
                            serial,
                            shown,
                        });
                        Vec::new()
                    }
                    // Without a serial no reply can name it.
                    None => vec![Notification::Shown(shown)],
                }
            }
            ("signal", "NotificationClosed") => vec![Notification::Closed(NotificationClosed {
                received_at,
                notification_id: id(m.args.first())?,
                reason: closed_reason(m.args.get(1).and_then(Value::as_u64).unwrap_or(4)).into(),
            })],
            ("signal", "ActionInvoked") => vec![Notification::Action(NotificationAction {
                received_at,
                notification_id: id(m.args.first())?,
                action: text(m.args.get(1)),
            })],
            // Other traffic the rules let through: read, nothing to record.
            _ => Vec::new(),
        };
        out.extend(read);
        Ok((out, waiting))
    }
}

pub fn capture(
    spool: &Path,
    stop: &sinex_source_core::capture::CaptureStop,
) -> std::io::Result<u64> {
    dbus::capture(MATCHES, &[], spool, stop)
}

/// `stage --parser notifications SPOOL`: desktop notifications on the session
/// bus, captured into SPOOL.
super::input_format! { pub FORMAT = "notifications" {
    specs: [SPEC],
    parser: |_b| NotificationsParser,
    discovery: DiscoverySpec {
        captured_by: CapturedBy::Session,
        capabilities: &["session_bus", "busctl"],
        streams: &[CaptureStream { spool: "notifications", options: &[], capabilities: &[] }],
        ..DiscoverySpec::DEFAULT
    },
    plan: single,
    reader: |b| {
        Ok(sinex_source_core::binding::polled(dbus::driver(
            b.input.clone(),
            b.settings.host.clone(),
            "dbus:session:org.freedesktop.Notifications",
        )))
    },
    capture: |b, _context, stop| capture(&b.input, stop).map_err(|e| e.to_string()),
} }

#[cfg(test)]
mod tests;

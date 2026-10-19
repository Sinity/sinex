//! Desktop notifications, read off the session bus: every `Notify` call to the
//! notification server, and its `NotificationClosed` and `ActionInvoked`
//! signals. Nothing keeps notifications after they close, so capture spools
//! each message as it passes (`dbus`).
//!
//! The id a server assigns comes back in the call's reply, which carries no
//! interface to match on, so a shown notification records the id it replaces
//! but not its own; closing and actions are recorded by id.
use super::dbus::{self, Message};
use crate::drivers::live_stream::{self, LiveStream};
use crate::registry::{Declared, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::Class;
use sinex_model::kinds::session::{NotificationAction, NotificationClosed, NotificationShown};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const SPEC: SourceSpec = SourceSpec {
    name: "desktop.notifications",
    driver: Driver::LiveStream,
    consumer: "recall and the day's interruptions",
    // Message bodies are anything any program chose to show.
    class: Class::Guarded,
    parser: "dbus-notifications",
    parser_version: 1,
    event_types: &[
        "notification.shown",
        "notification.closed",
        "notification.action",
    ],
    framer: crate::drivers::append_file::lines,
};

/// The match rules capture monitors.
pub const MATCHES: &[&str] = &[
    "type=method_call,interface=org.freedesktop.Notifications,member=Notify",
    "type=signal,interface=org.freedesktop.Notifications,member=NotificationClosed",
    "type=signal,interface=org.freedesktop.Notifications,member=ActionInvoked",
];

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
        .ok_or_else(|| RecordError::new("notification id is not a u32"))
}

impl Parser for NotificationsParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = Notification;

    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<Notification>, ()), RecordError> {
        let (received_at, bytes) = live_stream::received(record).map_err(RecordError::new)?;
        let m = Message::parse(&bytes).map_err(RecordError::new)?;
        let out = match (m.kind.as_str(), m.member.as_str()) {
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
                vec![Notification::Shown(NotificationShown {
                    received_at,
                    app: text(a.first()),
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
                    hints: a[6].as_object().cloned().unwrap_or_default(),
                    expire_timeout_ms: a[7]
                        .as_i64()
                        .and_then(|n| i32::try_from(n).ok())
                        .unwrap_or(-1),
                    sender: m.sender,
                })]
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
        Ok((out, ()))
    }
}

pub fn source() -> Declared<NotificationsParser> {
    Declared {
        spec: SPEC,
        parser: NotificationsParser,
    }
}

pub fn driver(spool: PathBuf, host: String) -> LiveStream {
    dbus::driver(spool, host, "dbus:session:org.freedesktop.Notifications")
}

pub fn capture(spool: &Path, seconds: Option<Duration>) -> std::io::Result<u64> {
    dbus::capture(MATCHES, &[], spool, seconds)
}

/// `--source notifications=SPOOL`: desktop notifications on the session
/// bus, captured into SPOOL.
pub const KIND: crate::binding::Kind = crate::binding::Kind {
    name: "notifications",
    specs: || vec![&SPEC],
    plan: Some(|spool, settings| Ok(crate::binding::single(&SPEC, spool, settings))),
    source: |_| Ok(Box::new(source())),
    reader: Some(|b| {
        Ok(crate::binding::plain(driver(
            b.input.clone(),
            b.settings.host.clone(),
        )))
    }),
    capture: Some(|b, stop| capture(&b.input, stop).map_err(|e| e.to_string())),
};

#[cfg(test)]
mod tests;

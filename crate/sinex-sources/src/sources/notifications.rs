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
    event_types: &["notification.shown", "notification.closed", "notification.action"],
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

    fn parse(&self, record: &Map<String, Value>, _: &()) -> Result<(Vec<Notification>, ()), RecordError> {
        let (received_at, bytes) = live_stream::received(record).map_err(RecordError::new)?;
        let m = Message::parse(&bytes).map_err(RecordError::new)?;
        let out = match (m.kind.as_str(), m.member.as_str()) {
            ("method_call", "Notify") => {
                let a = &m.args;
                if a.len() != 8 {
                    return Err(RecordError::new(format!("Notify with {} arguments, expected 8", a.len())));
                }
                let replaces = a[1].as_u64().and_then(|n| u32::try_from(n).ok()).filter(|n| *n != 0);
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
                        .map(|xs| xs.iter().filter_map(Value::as_str).map(str::to_owned).collect())
                        .unwrap_or_default(),
                    hints: a[6].as_object().cloned().unwrap_or_default(),
                    expire_timeout_ms: a[7].as_i64().and_then(|n| i32::try_from(n).ok()).unwrap_or(-1),
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

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_model::Timestamp;

    fn rec(line: &str) -> Map<String, Value> {
        serde_json::from_slice(&live_stream::frame(Timestamp::from_unix_ns(1_000).unwrap(), line.as_bytes())).unwrap()
    }

    fn parse(line: &str) -> Vec<Notification> {
        NotificationsParser.parse(&rec(line), &()).unwrap().0
    }

    #[test]
    fn notify_becomes_shown() {
        let out = parse(
            r#"{"type":"method_call","sender":":1.9","interface":"org.freedesktop.Notifications","member":"Notify","payload":{"type":"susssasa{sv}i","data":["mail",7,"","New mail","from a",["default","Open"],{"urgency":{"type":"y","data":1}},5000]}}"#,
        );
        let Notification::Shown(s) = &out[0] else { panic!() };
        assert_eq!((s.app.as_str(), s.summary.as_str(), s.body.as_str()), ("mail", "New mail", "from a"));
        assert_eq!(s.replaces_id, Some(7));
        assert_eq!(s.actions, vec!["default", "Open"]);
        assert_eq!(s.hints["urgency"], 1);
        assert_eq!(s.expire_timeout_ms, 5000);
    }

    #[test]
    fn signals_become_closed_and_action() {
        let closed = parse(r#"{"type":"signal","member":"NotificationClosed","payload":{"type":"uu","data":[12,2]}}"#);
        assert!(matches!(&closed[0], Notification::Closed(c) if c.notification_id == 12 && c.reason == "dismissed"));
        let action = parse(r#"{"type":"signal","member":"ActionInvoked","payload":{"type":"us","data":[12,"default"]}}"#);
        assert!(matches!(&action[0], Notification::Action(a) if a.notification_id == 12 && a.action == "default"));
    }

    #[test]
    fn other_traffic_is_read_without_events() {
        assert!(parse(r#"{"type":"method_call","member":"GetCapabilities","payload":{"type":"","data":[]}}"#).is_empty());
    }
}

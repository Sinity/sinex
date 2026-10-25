use super::*;
use sinex_model::Timestamp;

fn rec(line: &str) -> Map<String, Value> {
    serde_json::from_slice(&live_stream::frame(
        Timestamp::from_unix_ns(1_000).unwrap(),
        line.as_bytes(),
    ))
    .unwrap()
}

fn parse(line: &str) -> Vec<Notification> {
    NotificationsParser
        .parse(&rec(line), &(), &crate::RecordContext::none())
        .unwrap()
        .0
}

#[devkit::test]
fn notify_becomes_shown() {
    let out = parse(
        r#"{"type":"method_call","sender":":1.9","interface":"org.freedesktop.Notifications","member":"Notify","payload":{"type":"susssasa{sv}i","data":["mail",7,"","New mail","from a",["default","Open"],{"urgency":{"type":"y","data":1}},5000]}}"#,
    );
    let Notification::Shown(s) = &out[0] else {
        panic!()
    };
    assert_eq!(
        (s.app.as_str(), s.summary.as_str(), s.body.as_str()),
        ("mail", "New mail", "from a")
    );
    assert_eq!(s.replaces_id, Some(7));
    assert_eq!(s.actions, vec!["default", "Open"]);
    assert_eq!(s.hints["urgency"], 1);
    assert_eq!(s.expire_timeout_ms, 5000);
}

#[devkit::test]
fn signals_become_closed_and_action() {
    let closed = parse(
        r#"{"type":"signal","member":"NotificationClosed","payload":{"type":"uu","data":[12,2]}}"#,
    );
    assert!(
        matches!(&closed[0], Notification::Closed(c) if c.notification_id == 12 && c.reason == "dismissed")
    );
    let action = parse(
        r#"{"type":"signal","member":"ActionInvoked","payload":{"type":"us","data":[12,"default"]}}"#,
    );
    assert!(
        matches!(&action[0], Notification::Action(a) if a.notification_id == 12 && a.action == "default")
    );
}

#[devkit::test]
fn other_traffic_is_read_without_events() {
    assert!(
        parse(
            r#"{"type":"method_call","member":"GetCapabilities","payload":{"type":"","data":[]}}"#
        )
        .is_empty()
    );
}

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
        .parse(
            &rec(line),
            &Vec::new(),
            &sinex_source_core::RecordContext::none(),
        )
        .unwrap()
        .0
}

/// Lines read in order, each received `secs` seconds in, carrying the
/// waiting calls from one to the next.
fn read(lines: &[(f64, &str)]) -> (Vec<Notification>, Vec<Waiting>) {
    let mut waiting = Vec::new();
    let mut out = Vec::new();
    for (secs, line) in lines {
        let record = serde_json::from_slice(&live_stream::frame(
            Timestamp::from_unix_ns((*secs * 1e9) as i128).unwrap(),
            line.as_bytes(),
        ))
        .unwrap();
        let (events, next) = NotificationsParser
            .parse(&record, &waiting, &sinex_source_core::RecordContext::none())
            .unwrap();
        out.extend(events);
        waiting = next;
    }
    (out, waiting)
}

const NOTIFY: &str = r#"{"type":"method_call","cookie":41,"sender":":1.9","interface":"org.freedesktop.Notifications","member":"Notify","payload":{"type":"susssasa{sv}i","data":["mail",0,"","New mail","from a",[],{},-1]}}"#;

fn reply(serial: u64, caller: &str, id: u32) -> String {
    format!(
        r#"{{"type":"method_return","cookie":900,"reply_cookie":{serial},"sender":":1.3","destination":"{caller}","payload":{{"type":"u","data":[{id}]}}}}"#
    )
}

fn shown(n: &Notification) -> &NotificationShown {
    let Notification::Shown(s) = n else {
        panic!("{n:?}")
    };
    s
}

#[devkit::test]
fn a_shown_notification_records_the_id_its_reply_gives() {
    let other = reply(41, ":1.10", 99);
    let (out, waiting) = read(&[
        (1.0, NOTIFY),
        (1.001, &other),
        (1.002, &reply(41, ":1.9", 57)),
    ]);
    assert!(waiting.is_empty());
    assert_eq!(
        out.len(),
        1,
        "another caller's reply with the same serial is not this one's"
    );
    let s = shown(&out[0]);
    assert_eq!(s.notification_id, Some(57));
    assert_eq!(
        s.received_at,
        Timestamp::from_unix_ns(1_000_000_000).unwrap(),
        "the call's time"
    );
}

#[devkit::test]
fn an_unanswered_call_is_recorded_without_an_id() {
    let error = r#"{"type":"error","cookie":5,"reply_cookie":41,"sender":":1.3","destination":":1.9","payload":{"type":"s","data":["refused"]}}"#;
    let (out, _) = read(&[(1.0, NOTIFY), (1.5, error)]);
    assert_eq!(shown(&out[0]).notification_id, None);
    let closed =
        r#"{"type":"signal","member":"NotificationClosed","payload":{"type":"uu","data":[3,1]}}"#;
    let (out, waiting) = read(&[(1.0, NOTIFY), (20.0, closed)]);
    assert_eq!(out.len(), 1, "still waiting inside the timeout");
    assert_eq!(waiting.len(), 1);
    let (out, waiting) = read(&[(1.0, NOTIFY), (30.0, closed)]);
    assert!(waiting.is_empty());
    assert_eq!(shown(&out[0]).notification_id, None);
    assert!(matches!(out[1], Notification::Closed(_)));
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

#[devkit::test]
fn dbus_notification_with_delivery_hint_records_signal_origin() {
    for product in ["sinex", "polylogue"] {
        let line = serde_json::json!({"type":"method_call", "member":"Notify", "payload":{
            "type":"susssasa{sv}i", "data":[product,0,"","Notice","Details",[],
                {format!("x-{product}-delivery"): {"type":"s", "data":"delivery-1"}},-1]}});
        let events = parse(&line.to_string());
        assert_eq!(
            shown(&events[0]).extra["signal_origin"],
            serde_json::json!({"product":product,"delivery":"delivery-1"})
        );
        assert_eq!(
            shown(&events[0]).hints[&format!("x-{product}-delivery")],
            "delivery-1"
        );
    }
    let (events, _) = read(&[(1.0, NOTIFY), (1.1, &reply(41, ":1.9", 7))]);
    assert!(shown(&events[0]).extra.is_empty());
}

fn recorded(lines: &[&str]) -> Vec<sinex_model::Event> {
    use sinex_model::{Manifest, MaterialId, ReadingOrder, RecordOutcome};
    use sinex_source_core::{DrainInput, Limits, Source};
    let mut bytes = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        bytes.extend(live_stream::frame(
            Timestamp::from_unix_ns(i as i128 + 1).unwrap(),
            line.as_bytes(),
        ));
        bytes.push(b'\n');
    }
    let records = SPEC.framer.frame(&bytes).0;
    DeclaredSource {
        spec: SPEC,
        parser: NotificationsParser,
    }
    .drain(DrainInput {
        source: SPEC.name,
        class: SPEC.class,
        material: MaterialId::new(),
        bytes: bytes.as_slice().into(),
        records: &records,
        carry: Value::Null,
        reading_order: &|offset| ReadingOrder(offset as i64),
        limits: Limits::default(),
        manifest: &Manifest::from_json(serde_json::json!({})),
    })
    .unwrap()
    .into_iter()
    .flat_map(|chunk| chunk.outcomes)
    .flat_map(|outcome| match outcome {
        RecordOutcome::Events { events, .. } => events,
        RecordOutcome::Failure { reason, .. } => panic!("{reason}"),
    })
    .collect()
}

#[devkit::test]
fn same_summary_different_body_are_two_events() {
    let notification = |body| {
        serde_json::json!({
        "type": "method_call", "member": "Notify",
        "payload": {"type": "susssasa{sv}i", "data": ["mail", 0, "", "New mail", body, [], {}, -1]}
    }).to_string()
    };
    let first = notification("first body");
    let second = notification("second body");
    let events = recorded(&[&first, &second]);
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].payload["summary"], "New mail");
    assert_eq!(events[1].payload["summary"], "New mail");
    assert_eq!(events[0].payload["body"], "first body");
    assert_eq!(events[1].payload["body"], "second body");
    assert_eq!(events[0].equivalence_key, None);
    assert_eq!(events[1].equivalence_key, None);
    assert_ne!(events[0].location(), events[1].location());
}

#[devkit::test]
fn reused_id_closes_are_two_events() {
    let closed =
        r#"{"type":"signal","member":"NotificationClosed","payload":{"type":"uu","data":[12,2]}}"#;
    let events = recorded(&[closed, closed]);
    assert_eq!(events.len(), 2);
    for event in &events {
        assert_eq!(event.event_type, "notification.closed");
        assert_eq!(event.payload["notification_id"], 12);
        assert_eq!(event.payload["reason"], "dismissed");
        assert_eq!(event.equivalence_key, None);
    }
    assert_ne!(events[0].location(), events[1].location());
}

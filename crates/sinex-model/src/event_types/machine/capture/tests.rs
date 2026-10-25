use super::*;
use serde_json::json;

fn row(lane: &str, payload: Value) -> Value {
    json!({"observed_at":"2026-01-01T00:00:00Z","lane":lane,"host":"synthetic-host","seq":7,"payload":payload,"raw_ref":null})
}
fn known_samples() -> Vec<(&'static str, Value)> {
    vec![
        (
            "clipboard",
            json!({"category":"text","mime":"text/plain","text":"synthetic","size":9,"source_window":{"class":null,"title":"synthetic"}}),
        ),
        (
            "primary",
            json!({"category":"binary","mime":"image/png","sha256":"synthetic","size":42,"source_window":{"class":"synthetic","title":null}}),
        ),
        (
            "notifications",
            json!({"app_name":"synthetic","app_icon":"","replaces_id":1,"summary":"synthetic","body":"body","actions":[{"id":"default","label":"Open"}],"urgency":null,"hints":{},"expire_timeout":-1,"sender":null,"timestamp":1.5}),
        ),
        (
            "mpris",
            json!({"event":"heartbeat","player":"synthetic","status":"Playing","title":null,"artist":"","album":null,"position_seconds":1.5,"duration_seconds":null}),
        ),
        (
            "a11y",
            json!({"kind":"focus","app":"synthetic","role":"text","name":"synthetic","window":null}),
        ),
        (
            "a11y",
            json!({"kind":"text-changed","app":"synthetic","role":"text","name":"synthetic","change_type":"insert","detail":null}),
        ),
        (
            "a11y",
            json!({"kind":"subtree","app":"synthetic","window":null,"tree":{"role":"frame","name":"synthetic","children":[{"role":"text","name":"","text":"value","truncated":"max_nodes"}]}}),
        ),
        (
            "idle-inhibitors",
            json!({"event":"heartbeat","provenance":{"hyprland_clients":{"status":"available","inhibitors":[{"address":"synthetic","pid":1,"class":null,"title":"synthetic","workspace":{"id":1,"name":"main"}}]},"systemd_logind":{"status":"available","inhibitors":[{"who":"synthetic","what":"sleep","why":"synthetic","mode":"block","pid":1,"uid":1000}]},"active_window":{"status":"unavailable","reason":"command-failed","exit_code":1},"other_session_dbus":{"status":"unsupported","reason":"no-enumerable-provenance-interface"}}}),
        ),
        (
            "screen-frames",
            json!({"ts":1.5,"window_class":null,"window_title":"synthetic","workspace":null,"geometry":{"x":null,"y":0,"width":800,"height":600},"monitor":"synthetic","sha256":"synthetic","trigger":"heartbeat"}),
        ),
        (
            "audio-devices",
            json!({"kind":"devices-state","reason":"change","asr_source":null,"recording":[{"node_name":"synthetic","device_kind":"source","channel":"synthetic","description":"synthetic","since":1.5}],"excluded":[{"key":"synthetic","pattern":"synthetic"}],"draining":[],"failed":[{"key":"synthetic","error":"synthetic","at":1.5}]}),
        ),
        (
            "audio-topology",
            json!({"kind":"node","action":"added","id":1,"node.name":"synthetic","node.description":"synthetic","media.class":"Audio/Source"}),
        ),
        (
            "audio-topology",
            json!({"kind":"port","action":"added","id":2,"port.name":"synthetic","port.direction":"out","node.id":"1"}),
        ),
        (
            "audio-topology",
            json!({"kind":"link","action":"added","id":3,"output-node-id":"1","output-port-id":"2","input-node-id":"4","input-port-id":"5"}),
        ),
        (
            "audio-index",
            json!({"kind":"speech-index","channel":"synthetic","segment":"synthetic.flac","segment_start":1.5,"speech_spans":[{"start":1.5,"end":2.5}],"decoded_seconds":1.0,"span_seconds":1.0,"coverage":1.0}),
        ),
        (
            "browser-nav-edges",
            json!({"source_url":null,"source_title":null,"target_url":"https://example.test/","anchor_text":"synthetic","ts":1.5,"trigger":"click"}),
        ),
        (
            "reading-stack",
            json!({"event":"push","url":"https://example.test/","title":"synthetic","source_url":null,"source_title":null,"anchor_text":null,"note":null,"pushed_at":1.5}),
        ),
        (
            "monitor",
            json!({"brightness":50,"contrast":null,"power_sl":1,"input_sl":17}),
        ),
        (
            "bt-battery",
            json!([{"device_address":null,"device_alias":"synthetic","percentage":50,"source":null}]),
        ),
        (
            "bt-audio",
            json!([{"device_address":null,"device_alias":null,"uuid":"synthetic","state":"active","codec":0,"volume":null}]),
        ),
        (
            "logitech",
            json!({"devices":[{"name":"synthetic","unit_id":null,"battery_percent":50,"battery_mv":3000,"battery_status":"full"}]}),
        ),
        ("awair", json!({"score":83,"co2":600})),
        ("environment", json!({"score":83,"co2":600})),
    ]
}

#[devkit::test]
fn every_declared_event_lane_uses_concrete_fields_and_exact_wire_values() {
    for (lane, payload) in known_samples() {
        let original = row(lane, payload);
        let decoded: CaptureObserved = serde_json::from_value(original.clone()).unwrap();
        assert!(
            !matches!(decoded.payload(), CapturePayload::Unknown(_)),
            "{lane}"
        );
        assert_eq!(serde_json::to_value(decoded).unwrap(), original, "{lane}");
    }
}

#[devkit::test]
fn missing_null_large_numbers_and_unicode_remain_distinct() {
    let original = row(
        "clipboard",
        json!({"text":"synthetic\r\nł","size":9007199254740993_i64,"source_window":null}),
    );
    let decoded: CaptureObserved = serde_json::from_value(original.clone()).unwrap();
    let CapturePayload::Selection(selection) = decoded.payload() else {
        panic!("not selection")
    };
    assert_eq!(selection.mime, None);
    assert_eq!(selection.source_window, Some(None));
    assert_eq!(serde_json::to_value(decoded).unwrap(), original);
}

#[devkit::test]
fn malformed_known_fields_do_not_fall_through_to_unknown() {
    for (lane, payload) in [
        ("clipboard", json!({"text":4})),
        ("notifications", json!({"actions":[{"id":4}]})),
        ("mpris", json!({"position_seconds":"1"})),
        ("a11y", json!({"kind":"focus","name":[]})),
        (
            "a11y",
            json!({"kind":"subtree","tree":{"children":[false]}}),
        ),
        (
            "idle-inhibitors",
            json!({"provenance":{"active_window":{"window":{"pid":"1"}}}}),
        ),
        ("screen-frames", json!({"geometry":{"width":"1"}})),
        (
            "audio-devices",
            json!({"kind":"devices-state","recording":[{"since":"1"}]}),
        ),
        ("audio-topology", json!({"kind":"node","id":"1"})),
        (
            "audio-index",
            json!({"kind":"speech-index","speech_spans":[{"start":"1"}]}),
        ),
        ("browser-nav-edges", json!({"target_url":false})),
        ("reading-stack", json!({"pushed_at":"1"})),
        ("monitor", json!({"brightness":"1"})),
        ("bt-battery", json!([{"percentage":"1"}])),
        ("bt-audio", json!([{"volume":"1"}])),
        ("logitech", json!({"devices":[{"battery_mv":"1"}]})),
        ("environment", json!({"score":"83"})),
        ("a11y", json!({"kind":4})),
    ] {
        assert!(
            serde_json::from_value::<CaptureObserved>(row(lane, payload.clone())).is_err(),
            "{lane}: {payload}"
        );
    }
}

#[devkit::test]
fn unknown_lanes_and_representation_kinds_remain_explicit() {
    for (lane, payload) in [
        ("future-lane", json!([null, 4, {}])),
        ("a11y", json!({"kind":"future-kind","name":false})),
        ("audio-topology", json!({"kind":"future-kind","id":[]})),
    ] {
        let original = row(lane, payload);
        let decoded: CaptureObserved = serde_json::from_value(original.clone()).unwrap();
        assert!(matches!(decoded.payload(), CapturePayload::Unknown(_)));
        let output = serde_json::to_value(decoded).unwrap();
        assert_eq!(output["payload"], original["payload"]);
        assert_eq!(output["untyped"], true);
        assert_eq!(
            serde_json::to_value(
                serde_json::from_value::<CaptureObserved>(output.clone()).unwrap()
            )
            .unwrap(),
            output
        );
    }
}

#[devkit::test]
fn unknown_fields_and_open_hint_values_have_exact_output_pointers() {
    for (lane, payload, paths) in [
        (
            "clipboard",
            json!({"source_window":{"new/field":false}}),
            json!(["/payload/source_window/new~1field"]),
        ),
        (
            "notifications",
            json!({"hints":{"future":{"raw":true}}}),
            json!(["/payload/hints/future"]),
        ),
        (
            "a11y",
            json!({"kind":"subtree","tree":{"children":[{"extra":null}]}}),
            json!(["/payload/tree/children/0/extra"]),
        ),
        (
            "audio-devices",
            json!({"kind":"devices-state","failed":[{"extra":false}]}),
            json!(["/payload/failed/0/extra"]),
        ),
        (
            "bt-audio",
            json!([{"extra":false}]),
            json!(["/payload/0/extra"]),
        ),
        (
            "environment",
            json!({"score":83,"future":{"raw":true}}),
            json!(["/payload/future"]),
        ),
    ] {
        let original = row(lane, payload);
        let output = serde_json::to_value(
            serde_json::from_value::<CaptureObserved>(original.clone()).unwrap(),
        )
        .unwrap();
        assert_eq!(output["payload"], original["payload"]);
        assert_eq!(output["unknown_fields"], paths);
    }
}

#[devkit::test]
fn markers_in_the_input_do_not_override_source_values() {
    let mut original = row("monitor", json!({"brightness":50}));
    original["untyped"] = json!(true);
    original["unknown_fields"] = json!(["/payload/brightness"]);
    let output =
        serde_json::to_value(serde_json::from_value::<CaptureObserved>(original).unwrap()).unwrap();
    assert!(output.get("untyped").is_none());
    assert!(output.get("unknown_fields").is_none());
}

#[devkit::test]
fn registration_keeps_location_identity_repeat_rule_and_capture_time() {
    use crate::{EventPayload, KeyKind, ReplaceRule, TimeQuality};
    assert_eq!(CaptureObserved::KEY, KeyKind::Location);
    assert_eq!(CaptureObserved::REPLACE, ReplaceRule::SuppressRepeat);
    let original = row("monitor", json!({"brightness":50}));
    let record: CaptureObserved = serde_json::from_value(original.clone()).unwrap();
    assert_eq!(record.ts_orig().at, Some(record.observed_at));
    assert_eq!(record.ts_orig().quality, TimeQuality::Captured);
    let kind = crate::event_types::find("capture.observed").unwrap();
    let schema = (kind.json_schema)();
    assert!(
        schema["$defs"]["Monitor"]["properties"]
            .get("brightness")
            .is_some()
    );
    assert!(schema["properties"].get("unknown_fields").is_some());
    let origin = crate::Origin::Record {
        location: crate::RecordLocation {
            material: crate::MaterialId::new(),
            anchor: 7,
        },
        ordinal: 0,
    };
    let event = (kind.build)(
        &original,
        "producer:synthetic",
        origin.clone(),
        crate::ReadingOrder(1),
        [0; 32],
        crate::Class::Open,
    )
    .unwrap();
    assert_eq!(event.payload, original);
    let mut bad = original;
    bad["payload"]["brightness"] = json!([]);
    assert!(
        (kind.build)(
            &bad,
            "producer:synthetic",
            origin,
            crate::ReadingOrder(1),
            [0; 32],
            crate::Class::Open
        )
        .is_err()
    );
}

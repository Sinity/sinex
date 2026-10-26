use super::lanes::LakeEvent;
use super::*;
use serde_json::json;

fn envelope_line(lane: &str, seq: i64, payload: Value) -> Map<String, Value> {
    json!({"schema": "sinnix-capture-v1", "schema_version": 1, "lane": lane,
        "ts": 1_760_000_000.25, "host": "synthetic-host", "seq": seq,
        "payload": payload, "raw_ref": null})
    .as_object()
    .unwrap()
    .clone()
}

fn text_copy() -> Value {
    json!({"category":"text","mime":"text/plain","text":"synthetic","size":9,
           "source_window":{"class":"synthetic-app","title":"synthetic"}})
}

fn parse(source: &str, record: &Map<String, Value>) -> Result<Vec<LakeEvent>, RecordError> {
    let parser = EnvelopeParser {
        lake_source: lake_source(source).unwrap(),
    };
    parser
        .parse(record, &(), &crate::RecordContext::none())
        .map(|(events, ())| events)
}

fn one(source: &str, lane: &str, payload: Value) -> LakeEvent {
    let mut events = parse(source, &envelope_line(lane, 1, payload)).unwrap();
    assert_eq!(events.len(), 1, "{lane}: {events:?}");
    events.remove(0)
}

fn envelope_time() -> Timestamp {
    Timestamp::from_unix_ns(1_760_000_000_250_000_000).unwrap()
}

#[devkit::test]
fn a_selection_is_the_live_clipboard_event() {
    let LakeEvent::Copied(copy) = one("desktop.clipboard", "primary", text_copy()) else {
        panic!("not a copy")
    };
    assert_eq!(copy.selection, "primary");
    assert_eq!(copy.text.as_deref(), Some("synthetic"));
    assert_eq!(copy.mime_type.as_deref(), Some("text/plain"));
    assert_eq!(copy.bytes, Some(9));
    assert_eq!(
        copy.mime_types, None,
        "the lane does not record offered types"
    );
    assert_eq!(copy.window.unwrap().class.as_deref(), Some("synthetic-app"));
    assert_eq!(copy.received_at, envelope_time());
    assert!(copy.extra.is_empty(), "the category is the MIME type's");

    let LakeEvent::Copied(imported) = one(
        "desktop.clipboard",
        "clipboard",
        json!({"category":"binary","mime":"image/png","sha256":"ab","size":4,
               "source":"clipse-import","pinned":false}),
    ) else {
        panic!("not a copy")
    };
    assert_eq!(imported.sha256.as_deref(), Some("ab"));
    assert_eq!(imported.imported_from.as_deref(), Some("clipse-import"));
    assert_eq!(
        imported.extra["pinned"],
        json!(false),
        "unknown fields kept"
    );
    assert!(imported.text.is_none());
}

#[devkit::test]
fn a_notification_takes_the_monitor_time_and_flat_actions() {
    let LakeEvent::Notification(n) = one(
        "desktop.notifications",
        "notifications",
        json!({"app_name":"synthetic","app_icon":"","replaces_id":0,"summary":"Hello",
               "body":"body","actions":[{"id":"default","label":"Open"}],"urgency":2,
               "hints":{"sender-pid":4},"expire_timeout":-1,"sender":":1.7",
               "timestamp":1_760_000_001.5}),
    ) else {
        panic!("not a notification")
    };
    assert_eq!(
        n.received_at,
        Timestamp::from_unix_ns(1_760_000_001_500_000_000).unwrap()
    );
    assert_eq!(n.app, "synthetic");
    assert_eq!(n.icon, None);
    assert_eq!(n.replaces_id, None, "0 replaces nothing");
    assert_eq!(n.actions, ["default", "Open"]);
    assert_eq!(n.hints["urgency"], json!(2));
    assert_eq!(n.expire_timeout_ms, -1);
    let missing = envelope_line(
        "notifications",
        1,
        json!({"app_name":"synthetic","summary":"Hello"}),
    );
    assert!(parse("desktop.notifications", &missing).is_err());
}

#[devkit::test]
fn a_player_record_says_whether_it_is_a_heartbeat() {
    let LakeEvent::Player(p) = one(
        "desktop.media",
        "mpris",
        json!({"event":"heartbeat","player":"chromium","status":"Playing","title":"Song",
               "artist":"Artist","album":null,"duration_seconds":92.5,"position_seconds":1.25}),
    ) else {
        panic!("not a player change")
    };
    assert!(p.heartbeat && !p.snapshot);
    assert_eq!(p.artists, ["Artist"]);
    assert_eq!(p.length_us, Some(92_500_000));
    assert_eq!(p.position_us, Some(1_250_000));
    let LakeEvent::Player(first) = one(
        "desktop.media",
        "mpris",
        json!({"event":"initial","player":"chromium","status":"Paused","artist":""}),
    ) else {
        panic!("not a player change")
    };
    assert!(first.snapshot && !first.heartbeat);
    assert!(first.artists.is_empty());
}

#[devkit::test]
fn an_accessibility_tree_keeps_its_text_once_in_tree_order() {
    let LakeEvent::Tree(tree) = one(
        "desktop.a11y",
        "a11y",
        json!({"kind":"subtree","app":"synthetic","window":"Window","tree":{
        "role":"frame","name":"Window","children":[
            {"role":"button","name":"Back","text":"Back"},
            {"role":"text","name":"","text":"value","truncated":"max_nodes"},
            {"role":"panel","name":"Window","children":[{"role":"label","name":"Last"}]}
        ]}}),
    ) else {
        panic!("not a tree")
    };
    assert_eq!(tree.controls, 5);
    assert_eq!(tree.truncated, 1);
    assert_eq!(tree.text, "Window\nBack\nvalue\nLast");
    assert!(tree.extra.is_empty());

    let LakeEvent::Focused(focus) = one(
        "desktop.a11y",
        "a11y",
        json!({"kind":"focus","app":"synthetic","role":"entry","name":"Search","window":null}),
    ) else {
        panic!("not a focus")
    };
    assert_eq!(focus.role.as_deref(), Some("entry"));
    assert!(matches!(
        one(
            "desktop.a11y",
            "a11y",
            json!({"kind":"text-changed","app":"a","role":"text","name":"n",
                   "change_type":"insert","detail":"typed"}),
        ),
        LakeEvent::TextChanged(_)
    ));
    let unknown = envelope_line("a11y", 1, json!({"kind":"future"}));
    assert!(parse("desktop.a11y", &unknown).is_err());
}

#[devkit::test]
fn idle_inhibitors_keep_what_each_probe_answered() {
    let LakeEvent::IdleInhibitors(report) = one(
        "desktop.idle-inhibitor",
        "idle-inhibitors",
        json!({"event":"change","provenance":{
            "hyprland_clients":{"status":"available","inhibitors":[{"address":"0x1","class":"player",
                "title":"synthetic","pid":7,"workspace":{"id":1,"name":"1"}}]},
            "systemd_logind":{"status":"available","inhibitors":[{"who":"synthetic","what":"sleep",
                "why":"synthetic","mode":"delay","pid":1,"uid":0}]},
            "active_window":{"status":"unavailable","reason":"command-failed","exit_code":1},
            "other_session_dbus":{"status":"unsupported","reason":"no-enumerable-provenance-interface"}}}),
    ) else {
        panic!("not an idle report")
    };
    assert!(!report.heartbeat);
    let windows = report.windows.unwrap();
    assert_eq!(windows[0].class.as_deref(), Some("player"));
    assert_eq!(windows[0].workspace_id, Some(1));
    assert_eq!(report.system.unwrap()[0].mode.as_deref(), Some("delay"));
    assert!(report.focused_window.is_none());
    assert_eq!(report.unavailable["active_window"], "command-failed");
    assert_eq!(
        report.unavailable["other_session_dbus"],
        "no-enumerable-provenance-interface"
    );
}

#[devkit::test]
fn a_frame_names_its_image_and_capture_instant() {
    let mut line = envelope_line(
        "screen-frames",
        1,
        json!({"ts":1_760_000_000.125,"window_class":"synthetic","window_title":"t","workspace":"1",
               "geometry":{"x":1,"y":2,"width":800,"height":600},"monitor":"DP-1","sha256":"ab",
               "trigger":"idle-pause"}),
    );
    line.insert("raw_ref".into(), json!("/lake/frames/1.webp"));
    let LakeEvent::Frame(frame) = parse("media.screen", &line).unwrap().remove(0) else {
        panic!("not a frame")
    };
    assert_eq!(
        frame.captured_at,
        Timestamp::from_unix_ns(1_760_000_000_125_000_000).unwrap()
    );
    assert_eq!(frame.image.as_deref(), Some("/lake/frames/1.webp"));
    assert_eq!((frame.width, frame.height), (Some(800), Some(600)));
}

#[devkit::test]
fn audio_lanes_become_recording_graph_and_speech_events() {
    let LakeEvent::Recording(state) = one(
        "media.audio",
        "audio-devices",
        json!({"kind":"devices-state","reason":"heartbeat","asr_source":"mic",
               "recording":[{"node_name":"mic","device_kind":"source","channel":"c",
                             "description":"Mic","since":1_760_000_000.0}],
               "excluded":[{"key":"k","pattern":"p"}],"draining":[],
               "failed":[{"key":"k","error":"e","at":1_760_000_000.5}]}),
    ) else {
        panic!("not a recording state")
    };
    assert_eq!(state.speech_input.as_deref(), Some("mic"));
    assert_eq!(
        state.recording[0].since,
        Some(Timestamp::from_unix_secs(1_760_000_000).unwrap())
    );
    assert!(state.extra.is_empty(), "the kind is the event type's");

    let LakeEvent::Graph(port) = one(
        "media.audio",
        "audio-topology",
        json!({"kind":"port","action":"added","id":2,"port.name":"input_FL",
               "port.direction":"in","node.id":"1"}),
    ) else {
        panic!("not a graph change")
    };
    assert_eq!(port.object, "port");
    assert_eq!(port.port_name.as_deref(), Some("input_FL"));
    assert_eq!(port.node_id.as_deref(), Some("1"));
    assert!(port.extra.is_empty());

    let LakeEvent::Speech(speech) = one(
        "media.audio",
        "audio-index",
        json!({"kind":"speech-index","channel":"c","segment":"s.opus","segment_start":1_760_000_000.0,
               "decoded_seconds":3600.0,"span_seconds":3600.0,"coverage":1.0,
               "speech_spans":[{"start":1_760_000_010.0,"end":1_760_000_011.5}]}),
    ) else {
        panic!("not a speech index")
    };
    assert_eq!(speech.speech.len(), 1);
    assert_eq!(
        speech.segment_start,
        Timestamp::from_unix_secs(1_760_000_000).unwrap()
    );
}

#[devkit::test]
fn browsing_lanes_become_links_and_reading_changes() {
    let LakeEvent::Link(link) = one(
        "browser.navigation",
        "browser-nav-edges",
        json!({"source_url":"https://example.test/a","source_title":"A",
               "target_url":"https://example.test/b","anchor_text":"B","trigger":"click",
               "ts":1_760_000_002.0}),
    ) else {
        panic!("not a link")
    };
    assert_eq!(link.to_url.as_deref(), Some("https://example.test/b"));
    assert_eq!(
        link.followed_at,
        Timestamp::from_unix_secs(1_760_000_002).unwrap()
    );
    let LakeEvent::Reading(item) = one(
        "reading.stack",
        "reading-stack",
        json!({"event":"push","url":"https://example.test/b","title":"B","note":null,
               "pushed_at":1_760_000_003.0}),
    ) else {
        panic!("not a reading change")
    };
    assert_eq!(item.action, "push");
}

#[devkit::test]
fn a_device_sample_is_one_event_per_device() {
    let batteries = parse(
        "machine.peripheral",
        &envelope_line(
            "bt-battery",
            1,
            json!([{"device_address":"00:00","device_alias":"a","percentage":50,"source":"bluez"},
                   {"device_address":"00:01","device_alias":"b","percentage":75.5}]),
        ),
    )
    .unwrap();
    assert_eq!(batteries.len(), 2);
    let LakeEvent::Battery(second) = &batteries[1] else {
        panic!("not a battery")
    };
    assert_eq!(second.percent, Some(75.5));
    assert_eq!(second.reported_by, "bluez");
    assert!(
        parse(
            "machine.peripheral",
            &envelope_line("bt-battery", 2, json!([]))
        )
        .unwrap()
        .is_empty(),
        "an empty sample has no device to report"
    );
    let LakeEvent::Battery(keyboard) = one(
        "machine.peripheral",
        "logitech",
        json!({"devices":[{"name":"Keyboard","unit_id":"1","battery_percent":100,
                           "battery_mv":4200,"battery_status":"FULL"}]}),
    ) else {
        panic!("not a battery")
    };
    assert_eq!(keyboard.reported_by, "solaar");
    assert_eq!(keyboard.millivolts, Some(4200));
    assert!(matches!(
        one(
            "machine.peripheral",
            "bt-audio",
            json!([{"device_address":"00:00","device_alias":"s","uuid":"u","state":"active",
                    "codec":255,"volume":42}]),
        ),
        LakeEvent::BluetoothAudio(_)
    ));
    let LakeEvent::Display(display) = one(
        "desktop.display",
        "monitor",
        json!({"brightness":50,"contrast":null,"power_sl":4,"input_sl":16}),
    ) else {
        panic!("not a display sample")
    };
    assert_eq!(display.power_mode, Some(4));
    assert_eq!(display.contrast, None);
}

#[devkit::test]
fn a_network_flow_is_one_connection_event() {
    let flow = json!({"bytes":1367,"dport":5600,"dst":"192.0.2.1","packets":10,"proto":"tcp",
                      "sport":58550,"src":"192.0.2.2"});
    let LakeEvent::Connection(c) = one("network.netflow", "netflow", flow.clone()) else {
        panic!("not a connection")
    };
    assert_eq!(c.destination_port, Some(5600));
    assert_eq!(c.bytes, Some(1367));
    assert_eq!(c.ended_at, envelope_time());
    let mut wrong = flow;
    wrong["dport"] = json!("http");
    assert!(parse("network.netflow", &envelope_line("netflow", 1, wrong)).is_err());
}

#[devkit::test]
fn an_air_sample_is_keyed_by_the_sensor_clock_under_either_lane_name() {
    for lane in ["awair", "environment"] {
        let LakeEvent::Air(air) = one(
            "environment.awair",
            lane,
            json!({"timestamp":"2025-10-09T08:53:20.5Z","score":90,"temp":21.5,"humid":40.0,
                   "co2":600,"voc":250,"pm25":1,"dew_point":7.5}),
        ) else {
            panic!("not an air sample")
        };
        assert_eq!(
            air.sampled_at,
            Timestamp::from_unix_ns(1_760_000_000_500_000_000).unwrap()
        );
        assert_eq!(air.co2_ppm, Some(600.0));
        assert_eq!(air.extra["dew_point"], json!(7.5));
    }
}

#[devkit::test]
fn unbound_lanes_and_bad_envelopes_fail() {
    assert!(parse("desktop.clipboard", &envelope_line("mpris", 1, text_copy())).is_err());
    let mut bad = envelope_line("clipboard", 1, text_copy());
    bad.insert("schema".into(), json!("other"));
    assert!(parse("desktop.clipboard", &bad).is_err());
    let mut bad = envelope_line("clipboard", 1, text_copy());
    bad.remove("seq");
    assert!(parse("desktop.clipboard", &bad).is_err());
    let wrong_type = envelope_line("clipboard", 1, json!({"text": 4}));
    assert!(parse("desktop.clipboard", &wrong_type).is_err());
}

#[devkit::test]
fn every_lane_maps_to_its_sources_declared_event_types() {
    use sinex_model::Payloads;
    let samples = [
        ("desktop.clipboard", "clipboard", text_copy()),
        ("desktop.media", "mpris", json!({"player":"p"})),
        ("desktop.display", "monitor", json!({})),
        ("network.netflow", "netflow", json!({})),
        ("environment.awair", "environment", json!({})),
    ];
    for (source, lane, payload) in samples {
        let spec = &lake_source(source).unwrap().spec;
        for event in parse(source, &envelope_line(lane, 1, payload)).unwrap() {
            let built = event
                .build(
                    source,
                    sinex_model::Origin::Record {
                        location: sinex_model::RecordLocation {
                            material: sinex_model::MaterialId::new(),
                            anchor: 0,
                        },
                        output_index: 0,
                    },
                    sinex_model::ReadingOrder(0),
                    [0; 32],
                    Class::Open,
                )
                .unwrap();
            assert!(
                spec.event_types.contains(&built.event_type),
                "{source} emits {} it does not declare",
                built.event_type
            );
        }
    }
}

#[devkit::test]
fn day_files_skip_index_and_sidecars() {
    let root = std::env::temp_dir().join(format!("sinex-lake-{}", uuid::Uuid::now_v7()));
    let dir = root.join("activity/desktop/notification");
    std::fs::create_dir_all(&dir).unwrap();
    for f in [
        "notifications-20261001.jsonl",
        "notifications-index.jsonl",
        "notifications-20261001.jsonl.hw",
        "notifications.seq",
    ] {
        std::fs::write(dir.join(f), b"").unwrap();
    }
    let files = day_files(&root, lake_source("desktop.notifications").unwrap());
    assert_eq!(files.len(), 1);
    assert!(files[0].ends_with("notifications-20261001.jsonl"));
    std::fs::remove_dir_all(&root).unwrap();
}

#[devkit::test]
fn source_names_and_parsers_are_unique() {
    let mut names: Vec<_> = LAKE_SOURCES.iter().map(|l| l.spec.name).collect();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), LAKE_SOURCES.len());
    assert!(
        LAKE_SOURCES
            .iter()
            .all(|l| l.spec.parser.ends_with(l.spec.name))
    );
}

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
        .parse(
            record,
            &sequence::Carry::default(),
            &sinex_source_core::RecordContext::none(),
        )
        .map(|(events, _)| events)
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
fn stream_owned_lanes_keep_the_observed_record_for_derivations() {
    for (source, lanes) in [
        ("desktop.clipboard", vec!["clipboard", "primary"]),
        ("desktop.notifications", vec!["notifications"]),
        ("desktop.media", vec!["mpris"]),
        ("desktop.a11y", vec!["a11y"]),
        ("desktop.idle-inhibitor", vec!["idle-inhibitors"]),
        ("media.screen", vec!["screen-frames"]),
        (
            "media.audio",
            vec!["audio-devices", "audio-topology", "audio-index"],
        ),
        ("network.netflow", vec!["netflow"]),
    ] {
        for lane in lanes {
            let payload = json!({"kind":"provider-kind", "value":[1,null,"original"]});
            let mut record = envelope_line(lane, u32::MAX as i64 + 1, payload.clone());
            record.insert("raw_ref".into(), json!("capture.bin"));
            let events = parse(source, &record).unwrap();
            let [LakeEvent::Observation(observed)] = events.as_slice() else {
                panic!("{source}/{lane} did not keep its observation")
            };
            assert_eq!(observed.payload, payload);
            assert_eq!(observed.seq, u32::MAX as u64 + 1);
            assert_eq!(observed.host, "synthetic-host");
            assert_eq!(observed.lane, lane);
            assert_eq!(observed.raw_ref.as_deref(), Some("capture.bin"));
            assert_eq!(observed.observed_at, envelope_time());
            assert_eq!(
                lake_source(source).unwrap().spec.event_types,
                &["lake.observed", "capture.gap"]
            );
        }
    }
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
    assert!(matches!(
        parse("desktop.clipboard", &wrong_type).unwrap()[0],
        LakeEvent::Observation(_)
    ));
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
                spec.event_types.contains(&built.event_type.as_str()),
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
    let files = day_files(&root, lake_source("desktop.notifications").unwrap()).unwrap();
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

#[devkit::test]
fn malformed_envelope_coordinates_and_out_of_range_times_fail() {
    let source = "desktop.clipboard";
    let valid = envelope_line("clipboard", 1, text_copy());
    for (field, value) in [
        ("host", json!(null)),
        ("host", json!("")),
        ("seq", json!(-1)),
        ("seq", json!("1")),
        ("raw_ref", json!({})),
        ("ts", json!(1e300)),
        ("ts", json!(-1e300)),
    ] {
        let mut record = valid.clone();
        record.insert(field.into(), value);
        assert!(parse(source, &record).is_err(), "{field}: {record:?}");
    }
}

#[devkit::test]
fn an_unreadable_lane_directory_is_not_an_empty_lake() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("activity/desktop");
    std::fs::create_dir_all(&path).unwrap();
    std::fs::write(path.join("clipboard"), "not a directory").unwrap();
    let error =
        (FORMAT.plan.unwrap())(dir.path(), &sinex_source_core::binding::Settings::default())
            .unwrap_err();
    assert!(error.contains("clipboard"), "{error}");
}

#[devkit::test]
fn a_missing_lake_root_is_not_an_empty_lake() {
    let directory = tempfile::tempdir().unwrap();
    assert!(
        (FORMAT.plan.unwrap())(
            &directory.path().join("missing"),
            &sinex_source_core::binding::Settings::default()
        )
        .is_err()
    );
    assert!(
        (FORMAT.plan.unwrap())(
            directory.path(),
            &sinex_source_core::binding::Settings::default()
        )
        .unwrap()
        .is_empty()
    );
}

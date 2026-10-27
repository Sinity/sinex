use super::*;
use serde_json::json;

fn rec(entry: Value) -> Map<String, Value> {
    serde_json::from_slice(&live_stream::frame(
        Timestamp::now(),
        entry.to_string().as_bytes(),
    ))
    .unwrap()
}

#[devkit::test]
fn known_fields_are_typed_and_the_rest_kept() {
    let (out, ()) = JournaldParser
        .parse(
            &rec(json!({
                "__CURSOR": "s=1;i=2",
                "__REALTIME_TIMESTAMP": "1700000000123456",
                "MESSAGE": "Started foo.",
                "PRIORITY": "6",
                "_SYSTEMD_UNIT": "init.scope",
                "_PID": "1",
                "_UID": "0",
                "_COMM": "systemd",
                "_TRANSPORT": "journal",
                "CODE_FILE": "src/core/job.c",
            })),
            &(),
            &sinex_source_core::RecordContext::none(),
        )
        .unwrap();
    let e = &out[0];
    assert_eq!(e.cursor, "s=1;i=2");
    assert_eq!(e.logged_at.unwrap().unix_ns(), 1_700_000_000_123_456_000);
    assert_eq!(e.message.as_deref(), Some("Started foo."));
    assert_eq!(e.priority, Some(6));
    assert_eq!((e.pid, e.uid), (Some(1), Some(0)));
    assert_eq!(e.fields.get("CODE_FILE"), Some(&json!("src/core/job.c")));
    assert!(!e.fields.contains_key("MESSAGE"));
}

#[devkit::test]
fn byte_array_messages_decode_when_utf8_and_stay_raw_otherwise() {
    // "\x1b[1mhi" (colour codes make journalctl print bytes).
    let (out, ()) = JournaldParser
        .parse(
            &rec(json!({"__CURSOR": "c", "MESSAGE": [27, 91, 49, 109, 104, 105]})),
            &(),
            &sinex_source_core::RecordContext::none(),
        )
        .unwrap();
    assert_eq!(out[0].message.as_deref(), Some("\u{1b}[1mhi"));
    let (out, ()) = JournaldParser
        .parse(
            &rec(json!({"__CURSOR": "c", "MESSAGE": [255, 0]})),
            &(),
            &sinex_source_core::RecordContext::none(),
        )
        .unwrap();
    assert_eq!(out[0].message, None);
    assert_eq!(out[0].fields.get("MESSAGE"), Some(&json!([255, 0])));
}

#[devkit::test]
fn a_missing_or_malformed_time_stays_unknown() {
    let (out, ()) = JournaldParser
        .parse(
            &rec(json!({"__CURSOR": "c", "__REALTIME_TIMESTAMP": "soon"})),
            &(),
            &sinex_source_core::RecordContext::none(),
        )
        .unwrap();
    assert_eq!(out[0].logged_at, None);
    assert_eq!(
        sinex_model::EventPayload::ts_orig(&out[0]).quality,
        sinex_model::TimeQuality::Unknown
    );
}

#[devkit::test]
fn an_entry_without_a_cursor_is_a_failure_not_a_guess() {
    assert!(
        JournaldParser
            .parse(
                &rec(json!({"MESSAGE": "x"})),
                &(),
                &sinex_source_core::RecordContext::none()
            )
            .is_err()
    );
}

#[devkit::test]
fn capture_resumes_after_the_last_spooled_cursor() {
    let spool = std::env::temp_dir().join(format!("sinex-jd-{}", uuid::Uuid::now_v7()));
    assert_eq!(last_spooled_cursor(&std::env::temp_dir()).unwrap(), None);
    let mut w = SpoolWriter::open(&spool, Rotation::default()).unwrap();
    for i in 0..3 {
        let line = json!({"__CURSOR": format!("c{i}"), "MESSAGE": "x".repeat(70_000)});
        w.append(Timestamp::now(), line.to_string().as_bytes())
            .unwrap();
    }
    w.sync().unwrap();
    assert_eq!(last_spooled_cursor(&spool).unwrap().as_deref(), Some("c2"));
}

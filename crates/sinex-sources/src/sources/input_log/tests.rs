use super::*;
use sinex_model::{EventPayload, MaterialId, ReadingOrder, RecordOutcome, TimeQuality};
use sinex_source_core::{DrainInput, Limits, drain};

fn parse_all(lines: &[&str]) -> Vec<Input> {
    let mut carry = Numbering::default();
    let mut out = Vec::new();
    for line in lines {
        let row: Map<String, Value> = serde_json::from_str(line).unwrap();
        let (events, next) = InputLogParser
            .parse(&row, &carry, &sinex_source_core::RecordContext::none())
            .unwrap();
        out.extend(events);
        carry = next;
    }
    out
}

fn parse_error(line: &str) -> String {
    let row: Map<String, Value> = serde_json::from_str(line).unwrap();
    InputLogParser
        .parse(
            &row,
            &Numbering::default(),
            &sinex_source_core::RecordContext::none(),
        )
        .unwrap_err()
        .to_string()
}

#[devkit::test]
fn key_records_keep_the_kernel_time_of_the_keystroke() {
    let out = parse_all(&[
        r#"{"ts":"2026-01-02T10:00:00.120Z","event":"press","session":"kb","keycode":"KEY_C","code":46,"value":1,"input_sec":1767348000,"input_usec":101234,"changed":true}"#,
        r#"{"ts":"2026-01-02T10:00:00.230Z","event":"release","session":"kb","keycode":"KEY_C","code":46,"value":0,"input_sec":1767348000,"input_usec":204000,"changed":false}"#,
        r#"{"ts":"2026-01-02T10:00:01.000Z","event":"press","session":"kb","keycode":"KEY_V","code":47,"value":1,"input_sec":1767348001,"input_usec":0,"changed":true,"clipboard":"pasted text"}"#,
    ]);
    let Input::Pressed(press) = &out[0] else {
        panic!("not a press")
    };
    assert_eq!(
        (press.key.as_str(), press.code, press.value),
        ("KEY_C", Some(46), Some(1))
    );
    assert_eq!(press.changed, Some(true));
    let kernel = Timestamp::from_unix_ns(1_767_348_000_101_234_000).unwrap();
    assert_eq!(press.kernel_at, Some(kernel));
    assert_eq!(press.ts_orig().at, Some(kernel));
    assert_eq!(press.ts_orig().quality, TimeQuality::Captured);
    assert!(press.extra.is_empty());
    let Input::Released(release) = &out[1] else {
        panic!("not a release")
    };
    assert_eq!(
        release.kernel_at,
        Some(Timestamp::from_unix_ns(1_767_348_000_204_000_000).unwrap())
    );
    let Input::Pressed(paste) = &out[2] else {
        panic!("not a press")
    };
    assert_eq!(paste.clipboard.as_deref(), Some("pasted text"));
}

#[devkit::test]
fn older_presses_without_kernel_time_use_the_logger_time() {
    let out = parse_all(&[
        r#"{"ts":"2025-10-06T08:17:39.318Z","event":"press","session":"s","window":"unknown","keycode":"KEY_106","changed":false}"#,
    ]);
    let Input::Pressed(press) = &out[0] else {
        panic!("not a press")
    };
    assert_eq!(
        (press.code, press.value, press.kernel_at),
        (None, None, None)
    );
    assert_eq!(press.window.as_deref(), Some("unknown"));
    assert_eq!(press.ts_orig().at, Some(press.logged_at));
}

#[devkit::test]
fn pointer_records_are_buttons_motion_and_wheel_steps() {
    let out = parse_all(&[
        r#"{"ts":"2026-01-02T10:00:00.017Z","event":"pointer_rel","session":"m","code":"REL_X","value":1}"#,
        r#"{"ts":"2026-01-02T10:00:00.017Z","event":"pointer_rel","session":"m","code":"REL_Y","value":1}"#,
        r#"{"ts":"2026-01-02T10:00:00.017Z","event":"pointer_rel","session":"m","code":"REL_X","value":1}"#,
        r#"{"ts":"2026-01-02T10:00:00.017Z","event":"pointer_rel","session":"other","code":"REL_X","value":1}"#,
        r#"{"ts":"2026-01-02T10:00:00.018Z","event":"pointer_rel","session":"m","code":"REL_X","value":1}"#,
        r#"{"ts":"2026-01-02T10:00:00.020Z","event":"pointer_rel","session":"m","code":"REL_WHEEL_HI_RES","value":-120}"#,
        r#"{"ts":"2026-01-02T10:00:00.021Z","event":"pointer_button_press","session":"m","code":"BTN_LEFT","value":1}"#,
        r#"{"ts":"2026-01-02T10:00:00.060Z","event":"pointer_button_release","session":"m","code":"BTN_LEFT","value":0}"#,
        r#"{"ts":"2026-01-02T10:00:00.070Z","event":"pointer_abs","session":"t","code":"ABS_X","value":5000}"#,
    ]);
    let moved: Vec<(String, String, u32)> = out
        .iter()
        .filter_map(|e| match e {
            Input::Moved(m) if !m.absolute => Some((m.session.clone(), m.axis.clone(), m.n)),
            _ => None,
        })
        .collect();
    assert_eq!(
        moved,
        [
            ("m".to_owned(), "REL_X".to_owned(), 0),
            ("m".to_owned(), "REL_Y".to_owned(), 0),
            ("m".to_owned(), "REL_X".to_owned(), 1),
            ("other".to_owned(), "REL_X".to_owned(), 0),
            ("m".to_owned(), "REL_X".to_owned(), 0),
        ]
    );
    let Input::Scrolled(wheel) = &out[5] else {
        panic!("not a wheel step")
    };
    assert_eq!(
        (wheel.axis.as_str(), wheel.value),
        ("REL_WHEEL_HI_RES", -120)
    );
    let Input::Button(down) = &out[6] else {
        panic!("not a button")
    };
    assert!(down.pressed);
    let Input::Button(up) = &out[7] else {
        panic!("not a button")
    };
    assert!(!up.pressed);
    let Input::Moved(tablet) = &out[8] else {
        panic!("not motion")
    };
    assert!(tablet.absolute);
}

#[devkit::test]
fn the_logger_records_keep_their_fields() {
    let out = parse_all(&[
        r#"{"ts":"2026-01-15T11:32:01.729Z","event":"start","session":"s","changed":false}"#,
        r#"{"ts":"2026-01-15T11:35:11.659Z","event":"focus","session":"s","window":"unknown","changed":false}"#,
    ]);
    let Input::Logger(start) = &out[0] else {
        panic!("not a logger record")
    };
    assert_eq!(start.event, "start");
    assert_eq!(start.fields.get("changed"), Some(&Value::Bool(false)));
    let Input::Logger(focus) = &out[1] else {
        panic!("not a logger record")
    };
    assert_eq!(focus.fields.get("window"), Some(&Value::from("unknown")));
}

#[devkit::test]
fn malformed_records_fail_by_field() {
    for (line, field) in [
        (r#"{"event":"press","session":"s","keycode":"KEY_A"}"#, "ts"),
        (
            r#"{"ts":"yesterday","event":"press","session":"s","keycode":"KEY_A"}"#,
            "yesterday",
        ),
        (r#"{"ts":"2026-01-02T10:00:00Z","session":"s"}"#, "event"),
        (
            r#"{"ts":"2026-01-02T10:00:00Z","event":"press"}"#,
            "session",
        ),
        (
            r#"{"ts":"2026-01-02T10:00:00Z","event":"press","session":"s"}"#,
            "keycode",
        ),
        (
            r#"{"ts":"2026-01-02T10:00:00Z","event":"pointer_rel","session":"s","code":"REL_X"}"#,
            "value",
        ),
        (
            r#"{"ts":"2026-01-02T10:00:00Z","event":"pointer_button_press","session":"s","value":1}"#,
            "code",
        ),
        (
            r#"{"ts":"2026-01-02T10:00:00Z","event":"press","session":"s","keycode":"KEY_A","input_sec":1.5,"input_usec":2}"#,
            "kernel",
        ),
    ] {
        assert!(parse_error(line).contains(field), "{line}");
    }
}

fn observations(
    bytes: &[u8],
    base: usize,
    carry: serde_json::Value,
) -> (Vec<serde_json::Value>, serde_json::Value) {
    let (records, _) = lines(bytes);
    let chunks = drain(
        &InputLogParser,
        DrainInput {
            manifest: sinex_source_core::RecordContext::none().manifest,
            source: SPEC.name,
            class: Class::Guarded,
            material: MaterialId(uuid::Uuid::nil()),
            bytes: bytes.into(),
            records: &records,
            carry: carry.clone(),
            reading_order: &|a| ReadingOrder((base + a) as i64),
            limits: Limits::default(),
        },
    )
    .unwrap();
    let mut out = Vec::new();
    for outcome in chunks.iter().flat_map(|c| &c.outcomes) {
        match outcome {
            RecordOutcome::Events { location, events } => {
                for event in events {
                    out.push(serde_json::json!([
                        base + location.unwrap().anchor as usize,
                        event.event_type,
                        event.payload,
                        event.equivalence_key,
                        event.class,
                    ]));
                }
            }
            RecordOutcome::Failure { location, reason } => {
                out.push(serde_json::json!([base + location.anchor as usize, reason]))
            }
        }
    }
    let carry = chunks.last().map(|c| c.carry.clone()).unwrap_or(carry);
    (out, carry)
}

const LOG: &str = concat!(
    r#"{"ts":"2026-01-02T10:00:00.017Z","event":"pointer_rel","session":"m","code":"REL_X","value":1}"#,
    "\n",
    r#"{"ts":"2026-01-02T10:00:00.017Z","event":"pointer_rel","session":"m","code":"REL_X","value":1}"#,
    "\n",
    r#"{"ts":"2026-01-02T10:00:00.018Z","event":"press","session":"k","keycode":"KEY_A","code":30,"value":1,"input_sec":1767348000,"input_usec":17900,"changed":true}"#,
    "\n",
    "not json\n",
    r#"{"ts":"2026-01-02T10:00:00.018Z","event":"pointer_rel","session":"m","code":"REL_X","value":1}"#,
    "\n",
    r#"{"ts":"2026-01-02T10:00:00.018Z","event":"pointer_rel","session":"m","code":"REL_X","value":1}"#,
    "\n",
);

#[devkit::test]
fn equal_records_get_distinct_keys_and_reading_in_pieces_equals_whole() {
    let file = LOG.as_bytes();
    let (whole, state) = observations(file, 0, serde_json::Value::Null);
    assert_eq!(whole.len(), 6);
    assert_eq!(whole[0][1], "pointer.moved");
    assert_ne!(whole[0][3], whole[1][3]);
    assert_eq!(whole[1][2]["n"], 1);
    assert_eq!(whole[2][1], "key.pressed");
    assert_eq!(whole[2][4], "guarded");
    assert_eq!(whole[4][2]["n"], 0);
    assert_eq!(whole[5][2]["n"], 1);
    for cut in 0..=file.len() {
        let (_, consumed) = lines(&file[..cut]);
        let (mut parts, carry) = observations(&file[..consumed], 0, serde_json::Value::Null);
        let (rest, final_carry) = observations(&file[consumed..], consumed, carry);
        parts.extend(rest);
        assert_eq!(parts, whole, "cut {cut}");
        assert_eq!(final_carry, state, "carry cut {cut}");
    }
}

#[devkit::test]
fn only_day_files_are_the_log() {
    assert!(is_day_file(Path::new("/logs/2026-10-26.jsonl")));
    for other in [
        "/logs/2026-08-12.torn-original.jsonl",
        "/logs/tmpoit7h06q.tmp",
        "/logs/2026-1-26.jsonl",
        "/logs/notes.jsonl",
    ] {
        assert!(!is_day_file(Path::new(other)), "{other}");
    }
}

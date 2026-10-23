use super::*;
use crate::{DrainInput, Limits, drain};
use proptest::prelude::*;
use sinex_model::{MaterialId, ReadingOrder, RecordOutcome};

fn parse(bytes: &[u8]) -> Result<String, RecordError> {
    ZshParser { host: "h".into() }
        .parse(&bytes, &Seen::default(), &crate::RecordContext::none())
        .map(|(p, _)| p[0].command.clone())
}

#[test]
fn identical_runs_in_one_second_are_numbered_in_file_order() {
    let parser = ZshParser { host: "h".into() };
    let lines: [&[u8]; 4] = [
        b": 10:0;ls\n",
        b": 10:0;pwd\n",
        b": 10:0;ls\n",
        b": 11:0;ls\n",
    ];
    let mut seen = Seen::default();
    let mut ns = Vec::new();
    for line in lines {
        let (out, next) = parser
            .parse(&line, &seen, &crate::RecordContext::none())
            .unwrap();
        ns.push(out[0].n);
        seen = next;
    }
    assert_eq!(ns, [0, 0, 1, 0]);
    // A re-read from the start numbers the same records the same way.
    let mut again = Seen::default();
    let mut ns2 = Vec::new();
    for line in lines {
        let (out, next) = parser
            .parse(&line, &again, &crate::RecordContext::none())
            .unwrap();
        ns2.push(out[0].n);
        again = next;
    }
    assert_eq!(ns, ns2);
}

#[test]
fn keeps_original_backslashes_spaces_and_tabs() {
    let records: &[(&[u8], &str)] = &[
        (b": 1:0;echo slash\\ \n", "echo slash\\"),
        (b": 1:0;echo slashspace\\  \n", "echo slashspace\\ "),
        (b": 1:0;echo slashtab\\\t\n", "echo slashtab\\\t"),
        (
            b": 1:0;echo slashnewline\\\\\nnext\n",
            "echo slashnewline\\\nnext",
        ),
        (b": 1:0;echo plainspace  \n", "echo plainspace  "),
    ];
    for (record, expected) in records {
        assert_eq!(frame(record), (vec![(0, record.len())], record.len()));
        assert_eq!(parse(record).unwrap(), *expected);
    }
}

#[test]
fn metafied_bytes_and_continuations() {
    let bytes = b": 3600:0;echo \xc4\x83\x85\n: 3601:0;printf a\\\nb\n: 3602:0;partial";
    let (r, consumed) = frame(bytes);
    assert_eq!(r.len(), 2);
    assert_eq!(consumed, bytes.len() - b": 3602:0;partial".len());
    assert_eq!(parse(&bytes[r[0].0..r[0].1]).unwrap(), "echo ĥ");
    assert_eq!(parse(&bytes[r[1].0..r[1].1]).unwrap(), "printf a\nb");
    assert_eq!(parse(b"plain\n").unwrap(), "plain");
    assert!(unmetafy(b"\x83").is_err());
}

fn commands(bytes: &[u8], base: usize) -> Vec<(usize, String)> {
    let (records, _) = frame(bytes);
    let out = drain(
        &ZshParser { host: "h".into() },
        DrainInput {
            manifest: &serde_json::Value::Null,
            source: SPEC.name,
            class: Class::Open,
            material: MaterialId(uuid::Uuid::nil()),
            bytes,
            records: &records,
            carry: serde_json::Value::Null,
            reading_order: &|a| ReadingOrder(a as i64),
            limits: Limits::default(),
        },
    )
    .unwrap();
    out.iter()
        .flat_map(|c| &c.outcomes)
        .map(|o| match o {
            RecordOutcome::Events { location, events } => (
                base + location.unwrap().anchor as usize,
                events[0].payload["command"].as_str().unwrap().to_owned(),
            ),
            RecordOutcome::Failure { location, reason } => {
                (base + location.anchor as usize, format!("FAIL {reason}"))
            }
        })
        .collect()
}

proptest! {
    /// Source law: reading the file in pieces gives the same records, at the
    /// same file offsets, as reading it at once.
    #[test]
    fn pieces_equal_whole(cmds in prop::collection::vec("[a-z \\\\]{0,6}", 1..12), cut in 0usize..400) {
        let mut file = Vec::new();
        for (i, c) in cmds.iter().enumerate() {
            file.extend_from_slice(format!(": {}:0;{}\n", 100 + i, c.replace('\\', "\\\\")).as_bytes());
        }
        let whole = commands(&file, 0);
        let cut = cut.min(file.len());
        let (_, consumed) = frame(&file[..cut]);
        let mut pieces = commands(&file[..consumed], 0);
        pieces.extend(commands(&file[consumed..], consumed));
        prop_assert_eq!(whole, pieces);
    }

    /// Source law: an anchor reads back exactly its record.
    #[test]
    fn anchors_read_back_records(cmds in prop::collection::vec("[a-z]{1,5}", 1..8)) {
        let mut file = Vec::new();
        for (i, c) in cmds.iter().enumerate() {
            file.extend_from_slice(format!(": {}:0;{}\n", i, c).as_bytes());
        }
        for (anchor, command) in commands(&file, 0) {
            let (r, _) = frame(&file[anchor..]);
            prop_assert_eq!(parse(&file[anchor..anchor + r[0].1]).unwrap(), command);
        }
    }
}

#[test]
fn plain_commands_preserve_text_and_do_not_invent_times() {
    let parser = ZshParser { host: "h".into() };
    let cases: &[(&[u8], &str)] = &[
        (b"  echo\t spaced  \n", "  echo\t spaced  "),
        (b"echo slash\\ \n", "echo slash\\"),
        (b"printf a\\\nb\n", "printf a\nb"),
        (b"echo \xc4\x83\x85\n", "echo ĥ"),
        (b"\\: echo colon\n", ": echo colon"),
        (b"\\: 10:0;literal\n", ": 10:0;literal"),
    ];
    for (raw, expected) in cases {
        let (events, state) = parser
            .parse(raw, &Seen::default(), &crate::RecordContext::none())
            .unwrap();
        assert_eq!(events[0].command, *expected);
        assert_eq!(events[0].started_at, None);
        assert_eq!(events[0].header_elapsed, None);
        assert_eq!(state, Seen::default());
    }
    assert_eq!(parse(b": 10:0;\\: literal\n").unwrap(), "\\: literal");
    assert!(
        parser
            .parse(
                &b"\n".as_slice(),
                &Seen::default(),
                &crate::RecordContext::none()
            )
            .unwrap()
            .0
            .is_empty()
    );
    for raw in [
        b": bad:0;cmd\n".as_slice(),
        b": 1:bad;cmd\n",
        b": 1:0bad\n",
        b"\xff\n",
        b"\x83",
        b"partial",
    ] {
        assert!(
            parser
                .parse(&raw, &Seen::default(), &crate::RecordContext::none())
                .is_err(),
            "{raw:?}"
        );
    }
}

// Normalize random event IDs away, retaining the payload, byte coordinates,
// source clock, natural key and record bytes hash.
fn observations(
    bytes: &[u8],
    base: usize,
    carry: serde_json::Value,
) -> (Vec<serde_json::Value>, serde_json::Value) {
    let (records, _) = frame(bytes);
    let chunks = drain(
        &ZshParser { host: "h".into() },
        DrainInput {
            manifest: &serde_json::Value::Null,
            source: SPEC.name,
            class: Class::Open,
            material: MaterialId(uuid::Uuid::nil()),
            bytes,
            records: &records,
            carry: carry.clone(),
            reading_order: &|a| ReadingOrder((base + a) as i64),
            limits: Limits::default(),
        },
    )
    .unwrap();
    let mut out = Vec::new();
    for chunk in &chunks {
        for outcome in &chunk.outcomes {
            match outcome {
                RecordOutcome::Events { location, events } => {
                    let anchor = location.unwrap().anchor as usize;
                    let end = records.iter().find(|r| r.0 == anchor).unwrap().1;
                    for event in events {
                        assert_eq!(
                            event.record_hash,
                            *blake3::hash(&bytes[anchor..end]).as_bytes()
                        );
                        out.push(serde_json::json!([
                            base + anchor,
                            event.payload,
                            event.ts_orig.at,
                            event.ts_orig.quality,
                            event.equivalence_key,
                            event.reading_order.0
                        ]));
                    }
                    if events.is_empty() {
                        out.push(serde_json::json!([base + anchor, "empty"]));
                    }
                }
                RecordOutcome::Failure { location, reason } => {
                    out.push(serde_json::json!([base + location.anchor as usize, reason]))
                }
            }
        }
    }
    let carry = chunks.last().map(|c| c.carry.clone()).unwrap_or(carry);
    (out, carry)
}

#[test]
fn mixed_history_append_cuts_keep_carry_coordinates_and_failures() {
    let file = b"pwd\n: 10:0;pwd\npwd\n\n: invalid\n: 10:2;pwd\nprintf a\\\nb\n\\: literal\npwd\n: 10:0;pwd\n";
    let (whole, state) = observations(file, 0, serde_json::Value::Null);
    assert_eq!(whole.len(), 10);
    assert_eq!(whole[0][3], "unknown");
    assert_eq!(whole[1][1]["n"], 0);
    assert_eq!(whole[5][1]["n"], 1);
    assert_eq!(whole[9][1]["n"], 2);
    for cut in 0..=file.len() {
        let (_, consumed) = frame(&file[..cut]);
        let (mut parts, carry) = observations(&file[..consumed], 0, serde_json::Value::Null);
        let (rest, final_carry) = observations(&file[consumed..], consumed, carry);
        parts.extend(rest);
        assert_eq!(parts, whole, "cut {cut}");
        assert_eq!(final_carry, state, "carry cut {cut}");
    }
}

#[test]
fn appended_plain_records_keep_distinct_material_locations_on_replay() {
    use crate::drivers::append_file::AppendState;
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("history");
    std::fs::write(&path, b"pwd\n: 10:0;pwd\nprintf a\\\n").unwrap();
    let driver = driver(path.clone(), "h".into());
    let mut state = AppendState::default();
    let first = driver
        .poll(&serde_json::Value::Null, &mut state)
        .unwrap()
        .material
        .unwrap();
    assert_eq!(first.bytes, b"pwd\n: 10:0;pwd\n");
    let (_, carry) = observations(&first.bytes, 0, serde_json::Value::Null);
    let cursor = (first.cursor_at)(first.bytes.len());
    let mut restarted = AppendState::rebuild([(first.id, first.bytes.as_slice())], frame);
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"b\npwd\n: 10:0;pwd\n")
        .unwrap();
    let second = driver
        .poll(&cursor, &mut restarted)
        .unwrap()
        .material
        .unwrap();
    assert_eq!(second.previous_part, Some(first.id));
    assert_ne!(second.id, first.id);
    assert_eq!(second.bytes, b"printf a\\\nb\npwd\n: 10:0;pwd\n");
    assert_eq!(second.records, frame(&second.bytes).0);
    let (actual, _) = observations(&second.bytes, first.bytes.len(), carry.clone());
    assert_eq!(actual[0][1]["command"], "printf a\nb");
    assert_eq!(actual[1][3], "unknown");
    assert_eq!(actual[2][1]["n"], 1);
    assert_eq!(
        actual,
        observations(&second.bytes, first.bytes.len(), carry).0
    );
}

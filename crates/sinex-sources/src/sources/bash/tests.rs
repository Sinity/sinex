use super::*;
use sinex_model::{MaterialId, ReadingOrder, RecordOutcome};
use sinex_source_core::{DrainInput, Limits, drain};

fn parser() -> BashParser {
    BashParser { host: "h".into() }
}

fn parse(record: &[u8]) -> Result<Vec<CommandImported>, RecordError> {
    parser()
        .parse(
            &record,
            &Seen::default(),
            &sinex_source_core::RecordContext::none(),
        )
        .map(|(events, _)| events)
}

/// Each record's anchor and payload (or failure), read from `bytes` placed at
/// `base` with `carry`, and the carry after them.
fn observations(
    bytes: &[u8],
    base: usize,
    carry: serde_json::Value,
) -> (Vec<serde_json::Value>, serde_json::Value) {
    let (records, _) = frame(bytes);
    let chunks = drain(
        &parser(),
        DrainInput {
            manifest: sinex_source_core::RecordContext::none().manifest,
            source: SPEC.name,
            class: Class::Open,
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
                        event.payload,
                        event.ts_orig.quality,
                        event.equivalence_key,
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

#[devkit::test]
fn timestamp_lines_start_records_that_keep_their_lines() {
    let file = b"#1700000000\nls -la\n#1700000005\nfor f in *; do\n  echo $f\ndone\n#1700000009\n";
    let (records, consumed) = frame(file);
    // The last timestamp waits for its command.
    assert_eq!(records.len(), 2);
    assert_eq!(consumed, file.len() - b"#1700000009\n".len());
    let (with_command, _) = frame(&[file.as_slice(), b"pwd\npartial"].concat());
    assert_eq!(with_command.len(), 3);
    let first = parse(&file[records[0].0..records[0].1]).unwrap();
    assert_eq!(first[0].command, "ls -la");
    assert_eq!(
        first[0].started_at,
        Some(Timestamp::from_unix_secs(1_700_000_000).unwrap())
    );
    assert_eq!(first[0].header_elapsed, None);
    let second = parse(&file[records[1].0..records[1].1]).unwrap();
    assert_eq!(second[0].command, "for f in *; do\n  echo $f\ndone");
    // A timestamp line with an empty command reads as nothing.
    assert!(parse(b"#1700000009\n\n").unwrap().is_empty());
}

#[devkit::test]
fn plain_lines_are_records_alone_with_unknown_start() {
    let file = b"ls\n  echo spaced  \n#not a timestamp\n";
    let (records, consumed) = frame(file);
    assert_eq!(consumed, file.len());
    let commands: Vec<String> = records
        .iter()
        .map(|r| parse(&file[r.0..r.1]).unwrap()[0].command.clone())
        .collect();
    assert_eq!(commands, ["ls", "  echo spaced  ", "#not a timestamp"]);
    assert_eq!(parse(b"ls\n").unwrap()[0].started_at, None);
    assert!(parse(b"\n").unwrap().is_empty());
}

#[devkit::test]
fn malformed_records_fail_with_their_bytes() {
    for raw in [
        b"#99999999999999999999\nls\n".as_slice(),
        b"#1x\nls\n",
        b"\xff\n",
        b"partial",
    ] {
        assert!(parse(raw).is_err(), "{raw:?}");
    }
}

#[devkit::test]
fn identical_timed_commands_are_numbered_and_untimed_ones_use_locations() {
    let file = b"pwd\n#10\nls\n#10\nls\n#11\nls\n";
    let (events, _) = observations(file, 0, serde_json::Value::Null);
    assert_eq!(events[0][2], "unknown");
    assert_eq!(events[0][3], serde_json::Value::Null);
    let ns: Vec<_> = events[1..].iter().map(|e| e[1]["n"].clone()).collect();
    assert_eq!(ns, [0, 1, 0]);
    assert_ne!(events[1][3], events[2][3]);
}

#[devkit::test]
fn reading_in_pieces_equals_reading_whole() {
    let file = b"pwd\n#10\nls\n#10\nls\n#12\nprintf a\nb\n#13\nls\n";
    let (whole, state) = observations(file, 0, serde_json::Value::Null);
    for cut in 0..=file.len() {
        let (_, consumed) = frame(&file[..cut]);
        // A cut inside a multi-line command splits it; bash writes a command
        // whole, so a poll only cuts between commands.
        if consumed > 0 && file[consumed..].starts_with(b"b\n") {
            continue;
        }
        let (mut parts, carry) = observations(&file[..consumed], 0, serde_json::Value::Null);
        let (rest, final_carry) = observations(&file[consumed..], consumed, carry);
        parts.extend(rest);
        assert_eq!(parts, whole, "cut {cut}");
        assert_eq!(final_carry, state, "carry cut {cut}");
    }
}

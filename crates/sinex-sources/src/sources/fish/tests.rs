use super::*;
use crate::{DrainInput, Limits, drain};
use sinex_model::{MaterialId, ReadingOrder, RecordOutcome};

fn parser() -> FishParser {
    FishParser { host: "h".into() }
}

fn parse(record: &[u8]) -> Result<CommandImported, RecordError> {
    parser()
        .parse(&record, &Seen::default(), &crate::RecordContext::none())
        .map(|(mut events, _)| events.remove(0))
}

fn observations(
    bytes: &[u8],
    base: usize,
    carry: serde_json::Value,
) -> (Vec<serde_json::Value>, serde_json::Value) {
    let (records, _) = frame(bytes);
    let chunks = drain(
        &parser(),
        DrainInput {
            manifest: crate::RecordContext::none().manifest,
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

const FILE: &[u8] = b"- cmd: cp notes.txt /tmp\n  when: 1700000000\n  paths:\n    - notes.txt\n- cmd: echo a\\\\nb\\nc\n  when: 1700000001\n- cmd: ls\n  when: 1700000002\n- cmd: ls\n  when: 1700000002\n- cmd: partial\n  wh";

#[devkit::test]
fn blocks_are_records_with_times_and_paths() {
    let (records, consumed) = frame(FILE);
    assert_eq!(records.len(), 5);
    assert_eq!(consumed, FILE.len() - b"  wh".len());
    let first = parse(&FILE[records[0].0..records[0].1]).unwrap();
    assert_eq!(first.command, "cp notes.txt /tmp");
    assert_eq!(first.paths, ["notes.txt"]);
    assert_eq!(
        first.started_at,
        Some(Timestamp::from_unix_secs(1_700_000_000).unwrap())
    );
    let second = parse(&FILE[records[1].0..records[1].1]).unwrap();
    assert_eq!(second.command, "echo a\\nb\nc");
    assert!(second.paths.is_empty());
}

#[devkit::test]
fn unescape_keeps_unknown_backslashes() {
    assert_eq!(unescape("a\\\\b"), "a\\b");
    assert_eq!(unescape("a\\nb"), "a\nb");
    assert_eq!(unescape("a\\tb\\"), "a\\tb\\");
}

#[devkit::test]
fn identical_commands_in_one_second_are_numbered() {
    let (events, _) = observations(FILE, 0, serde_json::Value::Null);
    assert_eq!(events[2][1]["n"], 0);
    assert_eq!(events[3][1]["n"], 1);
    assert_ne!(events[2][2], events[3][2]);
    // The block without `when` keeps an unknown start and its location.
    assert_eq!(events[4][1]["started_at"], serde_json::Value::Null);
    assert_eq!(events[4][2], serde_json::Value::Null);
}

#[devkit::test]
fn malformed_blocks_fail() {
    for raw in [
        b"  when: 1\n".as_slice(),
        b"- cmd: ls\n  when: soon\n",
        b"- cmd: ls\n  colour: red\n",
        b"- cmd: \xff\n",
    ] {
        assert!(parse(raw).is_err(), "{raw:?}");
    }
}

#[devkit::test]
fn reading_in_pieces_equals_reading_whole() {
    let file = &FILE[..FILE.len() - b"- cmd: partial\n  wh".len()];
    let (whole, state) = observations(file, 0, serde_json::Value::Null);
    let starts: Vec<usize> = frame(file).0.iter().map(|r| r.0).collect();
    for cut in 0..=file.len() {
        let (_, consumed) = frame(&file[..cut]);
        // fish writes a block whole, so a poll only cuts between blocks.
        if consumed < file.len() && !starts.contains(&consumed) {
            continue;
        }
        let (mut parts, carry) = observations(&file[..consumed], 0, serde_json::Value::Null);
        let (rest, final_carry) = observations(&file[consumed..], consumed, carry);
        parts.extend(rest);
        assert_eq!(parts, whole, "cut {cut}");
        assert_eq!(final_carry, state, "carry cut {cut}");
    }
}

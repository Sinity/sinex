use super::*;
use sinex_model::{Timestamp, event_types::CommandImported};

/// Lines `n:command`; carry counts records; "panic" panics; "bad" fails.
struct Lines;
impl Parser for Lines {
    type Record<'a> = &'a [u8];
    type Carry = u32;
    type Payload = CommandImported;
    fn parse(
        &self,
        record: &&[u8],
        carry: &u32,
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<CommandImported>, u32), RecordError> {
        let text = std::str::from_utf8(record)
            .map_err(|_| RecordError::new("utf8"))?
            .trim_end();
        if text == "panic" {
            panic!("boom");
        }
        if text == "bad" {
            return Err(RecordError::new("bad record"));
        }
        let payload = CommandImported {
            command: text.into(),
            hostname: "h".into(),
            started_at: Some(Timestamp::from_unix_secs(i64::from(*carry)).unwrap()),
            header_elapsed: Some(0),
            n: 0,
        };
        Ok((vec![payload], carry + 1))
    }
}

fn lines(bytes: &[u8]) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut start = 0;
    for (i, b) in bytes.iter().enumerate() {
        if *b == b'\n' {
            out.push((start, i + 1));
            start = i + 1;
        }
    }
    out
}

fn run(bytes: &[u8], carry: Value, limits: Limits) -> Vec<DrainOutput> {
    let records = lines(bytes);
    drain(
        &Lines,
        DrainInput {
            source: "t",
            class: Class::Open,
            material: MaterialId(uuid::Uuid::nil()),
            bytes: bytes.into(),
            records: &records,
            carry,
            reading_order: &|a| ReadingOrder(a as i64),
            limits,
            manifest: &Value::Null,
        },
    )
    .unwrap()
}

fn commands(chunks: &[DrainOutput]) -> Vec<(u64, String)> {
    chunks
        .iter()
        .flat_map(|c| &c.outcomes)
        .filter_map(|o| match o {
            RecordOutcome::Events { location, events } => Some((
                location.unwrap().anchor,
                events[0].payload["command"].as_str().unwrap().to_owned(),
            )),
            RecordOutcome::Failure { .. } => None,
        })
        .collect()
}

#[test]
fn panics_and_errors_fail_only_their_record() {
    let out = run(b"a\npanic\nbad\nb\n", Value::Null, Limits::default());
    assert_eq!(commands(&out), vec![(0, "a".into()), (12, "b".into())]);
    let failures: Vec<_> = out[0]
        .outcomes
        .iter()
        .filter_map(|o| match o {
            RecordOutcome::Failure { location, reason } => Some((location.anchor, reason.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(
        failures,
        vec![
            (2, "parser panicked: boom".into()),
            (8, "bad record".into())
        ]
    );
}

#[test]
fn chunks_respect_limits_and_carry_continues() {
    let bytes = b"a\nb\nc\nd\ne\n";
    let out = run(
        bytes,
        Value::Null,
        Limits {
            max_events: 2,
            max_bytes: 1 << 20,
        },
    );
    assert_eq!(out.len(), 3);
    assert_eq!(out[0].end, 4);
    assert_eq!(out[0].carry, serde_json::json!(2));
    assert_eq!(out[2].carry, serde_json::json!(5));
    // Resuming from a chunk's carry gives the same events as one pass.
    let tail = run(&bytes[4..], out[0].carry.clone(), Limits::default());
    let times: Vec<_> = tail[0]
        .outcomes
        .iter()
        .map(|o| match o {
            RecordOutcome::Events { events, .. } => {
                events[0].ts_orig.at.unwrap().0.unix_timestamp()
            }
            _ => -1,
        })
        .collect();
    assert_eq!(times, vec![2, 3, 4]);
}

#[test]
fn oversized_record_goes_alone() {
    let big = format!("{}\n", "x".repeat(100));
    let bytes = format!("a\n{big}b\n");
    let out = run(
        bytes.as_bytes(),
        Value::Null,
        Limits {
            max_events: 100,
            max_bytes: 10,
        },
    );
    assert_eq!(
        out.iter().map(|c| c.outcomes.len()).collect::<Vec<_>>(),
        vec![1, 1, 1]
    );
}

#[test]
fn blanked_records_are_skipped_not_failed() {
    let out = run(b"a\n     \nb\n", Value::Null, Limits::default());
    assert_eq!(commands(&out), vec![(0, "a".into()), (8, "b".into())]);
    assert_eq!(out[0].failures, 0);
    assert_eq!(out[0].end, 10);
}

#[test]
fn records_find_their_member() {
    let manifest = serde_json::json!({"members": [
        {"path": "a", "stream_offset": 0, "length": 4},
        {"path": "b", "stream_offset": 4, "length": 6},
    ]});
    let path = |anchor| member_at(&manifest, anchor).and_then(|m| m["path"].as_str());
    assert_eq!(path(0), Some("a"));
    assert_eq!(path(3), Some("a"));
    assert_eq!(path(4), Some("b"));
    assert_eq!(path(9), Some("b"));
    assert_eq!(path(10), None);
    assert_eq!(member_at(&Value::Null, 0), None);
}

#[test]
fn nul_payloads_become_failures() {
    let out = run(b"a\0b\n", Value::Null, Limits::default());
    assert!(matches!(out[0].outcomes[0], RecordOutcome::Failure { .. }));
}

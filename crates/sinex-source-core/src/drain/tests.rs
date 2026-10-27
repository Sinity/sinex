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
            paths: vec![],
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
            manifest: crate::RecordContext::none().manifest,
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

#[devkit::test]
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

#[devkit::test]
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

#[devkit::test]
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

#[devkit::test]
fn blank_records_are_skipped_not_failed() {
    let out = run(b"a\n     \nb\n", Value::Null, Limits::default());
    assert_eq!(commands(&out), vec![(0, "a".into()), (8, "b".into())]);
    assert_eq!(out[0].failures, 0);
    assert_eq!(out[0].end, 10);
}

#[devkit::test]
fn nul_payloads_become_failures() {
    let out = run(b"a\0b\n", Value::Null, Limits::default());
    assert!(matches!(out[0].outcomes[0], RecordOutcome::Failure { .. }));
}

#[devkit::test]
fn temporary_file_records_match_slice_hashes_locations_carry_and_limits() {
    use std::io::Write;
    let bytes = b"alpha\nbad\n \nthird\npanic\nlast\n";
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(bytes).unwrap();
    let records = lines(bytes);
    for max_events in [1, 2, 100] {
        for max_bytes in [1, 10, 1000] {
            let limits = Limits {
                max_events,
                max_bytes,
            };
            let expected = run(bytes, Value::Null, limits);
            let actual = drain(
                &Lines,
                DrainInput {
                    source: "t",
                    class: Class::Open,
                    material: MaterialId(uuid::Uuid::nil()),
                    bytes: RecordBytes::File(&file),
                    records: &records,
                    carry: Value::Null,
                    reading_order: &|a| ReadingOrder(a as i64),
                    limits,
                    manifest: crate::RecordContext::none().manifest,
                },
            )
            .unwrap();
            assert_eq!(actual.len(), expected.len());
            for (actual, expected) in actual.iter().zip(&expected) {
                assert_eq!(actual.end, expected.end);
                assert_eq!(actual.carry, expected.carry);
                assert_eq!(actual.events, expected.events);
                assert_eq!(actual.failures, expected.failures);
                assert_eq!(actual.outcomes.len(), expected.outcomes.len());
                for (actual, expected) in actual.outcomes.iter().zip(&expected.outcomes) {
                    match (actual, expected) {
                        (
                            RecordOutcome::Failure {
                                location: a,
                                reason: ar,
                            },
                            RecordOutcome::Failure {
                                location: b,
                                reason: br,
                            },
                        ) => {
                            assert_eq!(a, b);
                            assert_eq!(ar, br);
                        }
                        (
                            RecordOutcome::Events {
                                location: a,
                                events: ae,
                            },
                            RecordOutcome::Events {
                                location: b,
                                events: be,
                            },
                        ) => {
                            assert_eq!(a, b);
                            assert_eq!(ae.len(), be.len());
                            for (a, b) in ae.iter().zip(be) {
                                let mut a = a.clone();
                                a.id = b.id;
                                assert_eq!(&a, b);
                            }
                        }
                        _ => panic!("record outcome differs"),
                    }
                }
            }
        }
    }
}

#[devkit::test]
fn temporary_file_read_failure_stops_before_a_false_progress_chunk() {
    use std::io::Write;
    let bytes = b"alpha\nbeta\ngamma\ndelta\n";
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(bytes).unwrap();
    let records = lines(bytes);
    let mut chunks = Drainer::new(
        &Lines,
        DrainInput {
            source: "t",
            class: Class::Open,
            material: MaterialId::new(),
            bytes: RecordBytes::File(&file),
            records: &records,
            carry: Value::Null,
            reading_order: &|a| ReadingOrder(a as i64),
            limits: Limits {
                max_events: 1,
                max_bytes: 1000,
            },
            manifest: crate::RecordContext::none().manifest,
        },
    )
    .unwrap();
    let first = chunks.next().unwrap().unwrap();
    assert_eq!(first.end, 6);
    file.set_len(11).unwrap();
    assert!(chunks.next().unwrap().is_err());
    assert!(chunks.next().is_none());
}

#[devkit::test]
fn invalid_staged_ranges_are_refused_before_record_allocation() {
    let file = tempfile::tempfile().unwrap();
    assert!(
        Drainer::new(
            &Lines,
            DrainInput {
                source: "t",
                class: Class::Open,
                material: MaterialId::new(),
                bytes: RecordBytes::File(&file),
                records: &[(0, usize::MAX)],
                carry: Value::Null,
                reading_order: &|_| ReadingOrder(0),
                limits: Limits::default(),
                manifest: crate::RecordContext::none().manifest,
            }
        )
        .is_err()
    );
}

#[devkit::test]
fn directory_bundles_keep_empty_members_at_distinct_anchors() {
    use crate::test_parser::NotesParser;
    let manifest = sinex_model::Manifest::from_json(serde_json::json!({
        "path":"synthetic-vault", "media_type":"application/x-sinex-file-bundle",
        "members":[{"path":"a.md", "stream_offset":1, "length":5}, {"path":"b.md", "stream_offset":7, "length":0}],
    }));
    let run = |manifest: &sinex_model::Manifest, records: &[(usize, usize)]| {
        drain(
            &NotesParser,
            DrainInput {
                source: "synthetic",
                class: Class::Open,
                material: MaterialId::new(),
                bytes: b"\0alpha\0".as_slice().into(),
                records,
                carry: Value::Null,
                reading_order: &|_| ReadingOrder(0),
                limits: Limits {
                    max_events: 1,
                    max_bytes: 1,
                },
                manifest,
            },
        )
    };
    let chunks = run(&manifest, &[(1, 6), (7, 7)]).unwrap();
    let outcomes: Vec<_> = chunks.into_iter().flat_map(|c| c.outcomes).collect();
    for (index, (path, text, anchor)) in [("a.md", "alpha", 1), ("b.md", "", 7)]
        .into_iter()
        .enumerate()
    {
        let RecordOutcome::Events { location, events } = &outcomes[index] else {
            panic!()
        };
        assert_eq!(location.unwrap().anchor, anchor);
        assert_eq!(events[0].payload["path"], path);
        assert_eq!(events[0].payload["text"], text);
    }
    let mut overlap = manifest.clone();
    overlap.members[1] = sinex_model::Member {
        path: "b.md".into(),
        stream_offset: 3,
        length: 3,
        details: Default::default(),
    };
    assert!(run(&overlap, &[(1, 6), (3, 6)]).is_err());
}

#[devkit::test]
fn stored_reader_holds_one_record_and_stops_at_a_read_fault() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Stored {
        peak: AtomicUsize,
        calls: AtomicUsize,
    }
    impl RecordRead for Stored {
        fn length(&self) -> u64 {
            1 << 40
        }
        fn read_exact_at(&self, start: u64, out: &mut [u8]) -> std::io::Result<()> {
            self.peak.fetch_max(out.len(), Ordering::Relaxed);
            self.calls.fetch_add(1, Ordering::Relaxed);
            if start >= 12 {
                return Err(std::io::Error::other("synthetic stored read failure"));
            }
            assert_eq!(out.len(), 6);
            out.copy_from_slice(if start == 0 { b"first\n" } else { b"other\n" });
            Ok(())
        }
    }
    let stored = Stored {
        peak: AtomicUsize::new(0),
        calls: AtomicUsize::new(0),
    };
    let mut chunks = Drainer::new(
        &Lines,
        DrainInput {
            source: "synthetic",
            class: Class::Open,
            material: MaterialId::new(),
            bytes: RecordBytes::Reader(&stored),
            records: &[(0, 6), (6, 12), (12, 18)],
            carry: Value::Null,
            reading_order: &|a| ReadingOrder(a as i64),
            limits: Limits {
                max_events: 1,
                max_bytes: 6,
            },
            manifest: crate::RecordContext::none().manifest,
        },
    )
    .unwrap();
    let first = chunks.next().unwrap().unwrap();
    assert_eq!(first.end, 6);
    assert_eq!(first.carry, serde_json::json!(1));
    assert_eq!(commands(&[first]), vec![(0, "first".into())]);
    let error = chunks.next().unwrap().unwrap_err();
    assert!(error.0.contains("synthetic stored read failure"));
    assert!(chunks.next().is_none());
    assert_eq!(stored.peak.load(Ordering::Relaxed), 6);
    assert_eq!(stored.calls.load(Ordering::Relaxed), 3);
}

#[devkit::test]
fn a_blank_record_is_no_record_and_leaves_the_carry_alone() {
    let bytes = b"  \nkept\n";
    let records = lines(bytes);
    let chunks = drain(
        &Lines,
        DrainInput {
            source: "t",
            class: Class::Open,
            material: MaterialId::new(),
            bytes: bytes.as_slice().into(),
            records: &records,
            carry: Value::Null,
            reading_order: &|a| ReadingOrder(a as i64),
            limits: Limits::default(),
            manifest: crate::RecordContext::none().manifest,
        },
    )
    .unwrap();
    assert_eq!(commands(&chunks), vec![(3, "kept".into())]);
    assert_eq!(chunks.last().unwrap().carry, serde_json::json!(1));
}

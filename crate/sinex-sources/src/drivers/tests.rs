use super::*;
use crate::{Declared, Driver, Parser, RecordError, SourceSpec};
use sinex_model::kinds::CommandImported;
use sinex_model::{Class, Timestamp};
use std::sync::atomic::{AtomicUsize, Ordering};

static PARSED: AtomicUsize = AtomicUsize::new(0);

struct Counting;
impl Parser for Counting {
    type Record<'a> = &'a [u8];
    type Carry = ();
    type Payload = CommandImported;
    fn parse(
        &self,
        record: &&[u8],
        _: &(),
        _: &crate::RecordContext<'_>,
    ) -> Result<(Vec<CommandImported>, ()), RecordError> {
        PARSED.fetch_add(1, Ordering::SeqCst);
        Ok((
            vec![CommandImported {
                command: String::from_utf8_lossy(record).trim_end().into(),
                hostname: "h".into(),
                started_at: Timestamp::from_unix_secs(1).unwrap(),
                header_elapsed: 0,
                n: 0,
            }],
            (),
        ))
    }
}

struct Discard;
impl MaterialSink for Discard {
    fn put(&self, bytes: &[u8]) -> Result<String, String> {
        Ok(format!("blake3:{}", blake3::hash(bytes).to_hex()))
    }
}

fn lines(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    let mut out = Vec::new();
    let mut start = 0;
    for (i, b) in bytes.iter().enumerate() {
        if *b == b'\n' {
            out.push((start, i + 1));
            start = i + 1;
        }
    }
    (out, start)
}

/// Batches are produced as they are taken: taking the first parses about
/// two chunks' records, not the material's, so memory holds a bounded
/// number of batches whatever the material's size.
#[test]
fn batches_are_drained_lazily() {
    let source = Declared {
        spec: SourceSpec {
            name: "test.counting",
            driver: Driver::AppendFile,
            consumer: "test",
            class: Class::Open,
            parser: "counting",
            parser_version: 1,
            event_types: &["command.imported"],
            framer: lines,
        },
        parser: Counting,
    };
    let records = 100_000;
    let bytes: Vec<u8> = (0..records)
        .flat_map(|i| format!("cmd {i}\n").into_bytes())
        .collect();
    let (framed, _) = lines(&bytes);
    let total = bytes.len();
    let acquired = Acquired {
        input: InputRef {
            kind: "file".into(),
            locator: "/t".into(),
            host: "h".into(),
        },
        observation: ObservationFacts::default(),
        revision: RevisionChange::Appended,
        material: Some(NewMaterial {
            id: MaterialId::new(),
            bytes,
            stored_file: None,
            records: framed,
            part_index: 0,
            previous_part: None,
            manifest: serde_json::json!({}),
            reading_order: Box::new(|a| ReadingOrder(a as i64)),
            cursor_at: Box::new(move |end| serde_json::json!({"end": end})),
            aligned: false,
            vanished: vec![],
        }),
        cursor_unchanged: Value::Null,
    };
    let limits = Limits {
        max_events: 1_000,
        max_bytes: usize::MAX,
    };
    PARSED.store(0, Ordering::SeqCst);
    let prepared = prepare(
        &source,
        "b",
        RunId::new(),
        acquired,
        Value::Null,
        &Discard,
        limits,
    )
    .unwrap();
    let mut batches = prepared.batches().unwrap();
    let first = batches.next().unwrap();
    assert_eq!(first.event_count(), 1_000);
    assert!(
        PARSED.load(Ordering::SeqCst) <= 2_001,
        "taking one batch parsed {} records",
        PARSED.load(Ordering::SeqCst)
    );
    let rest: Vec<Batch> = batches.collect();
    assert_eq!(1 + rest.len(), records / 1_000);
    let BatchKind::Source(last) = &rest.last().unwrap().kind else {
        panic!()
    };
    assert_eq!(last.cursor, serde_json::json!({"end": total}));
}

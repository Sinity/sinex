use super::*;
use crate::{DeclaredSource, Driver, Parser, RecordError, SourceSpec};
use sinex_model::event_types::CommandImported;
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
    let source = DeclaredSource {
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

/// The process's peak resident memory, from `/proc/self/status`.
fn peak_rss() -> usize {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let line = status.lines().find(|l| l.starts_with("VmHWM:")).unwrap();
    let kb: usize = line.split_whitespace().nth(1).unwrap().parse().unwrap();
    kb << 10
}

/// A 64 MiB file read in 4 MiB parts and drained end to end peaks well under
/// its size, and barely higher after the last part than after the first: memory
/// holds a part (and its window source), the driver's bounded state and one
/// batch, not the file and not a part's events (about 95k here, some 250 MiB
/// if collected). nextest runs each test in its own process, so the peak is
/// this test's.
#[test]
fn backfill_of_a_large_file_peaks_well_under_its_size() {
    use super::append_file::{AppendFile, AppendState};
    use std::io::Write;
    let dir = std::env::temp_dir().join(format!("sinex-rss-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("big");
    let size = 64usize << 20;
    {
        let mut f = std::io::BufWriter::new(std::fs::File::create(&path).unwrap());
        let mut written = 0;
        let mut i = 0u64;
        while written < size {
            let line = format!("command number {i:012} with some arguments\n");
            f.write_all(line.as_bytes()).unwrap();
            written += line.len();
            i += 1;
        }
    }
    let source = DeclaredSource {
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
    let driver = AppendFile {
        path: path.clone(),
        host: "h".into(),
        framer: lines,
    };
    let before = peak_rss();
    let mut state = AppendState::default();
    let mut cursor = Value::Null;
    let mut events = 0;
    let mut after_first = None;
    loop {
        let acquired = driver.poll_in_parts(&cursor, &mut state, 4 << 20).unwrap();
        if acquired.material.is_none() {
            break;
        }
        let prepared = prepare(
            &source,
            "b",
            RunId::new(),
            acquired,
            Value::Null,
            &Discard,
            Limits::default(),
        )
        .unwrap();
        for batch in prepared.batches().unwrap() {
            events += batch.event_count();
            let BatchKind::Source(p) = &batch.kind else {
                panic!()
            };
            cursor = p.cursor.clone();
        }
        after_first.get_or_insert_with(peak_rss);
    }
    std::fs::remove_dir_all(&dir).unwrap();
    assert!(events > 1_000_000, "{events} events");
    let peak = peak_rss();
    let grew = peak.saturating_sub(before);
    assert!(
        grew < 40 << 20,
        "peak RSS grew {} MiB reading a {} MiB file",
        grew >> 20,
        size >> 20
    );
    // Allocator reuse settles a few MiB above the first part's peak; a
    // 192 MiB input measured the same as this one (34 MiB grown).
    let later = peak.saturating_sub(after_first.unwrap());
    assert!(
        later < 8 << 20,
        "peak RSS grew {} MiB more after the first part",
        later >> 20
    );
}

use super::*;
use crate::test_support::body;
use sinex_source_core::{DrainInput, Limits};
use sinex_model::{EventPayload, MaterialId, ReadingOrder, RecordOutcome};

#[devkit::test]
fn only_line_terminators_are_removed_and_no_time_is_invented() {
    let parser = TextHistoryParser {
        host: "example-host".into(),
    };
    for (raw, expected) in [
        ("  echo\t Café  \r\n", "  echo\t Café  "),
        ("#123\n", "#123"),
        ("   \n", "   "),
        ("echo a\\\n", "echo a\\"),
    ] {
        let payload = parser
            .parse(&raw.as_bytes(), &(), &RecordContext::none())
            .unwrap()
            .0
            .remove(0);
        assert_eq!(payload.command, expected);
        assert_eq!(payload.hostname, "example-host");
        assert_eq!(payload.started_at, None);
        assert_eq!(payload.header_elapsed, None);
        assert!(payload.paths.is_empty());
        assert_eq!(payload.n, 0);
        assert_eq!(payload.ts_orig().at, None);
    }
    assert!(
        parser
            .parse(&b"\n".as_slice(), &(), &RecordContext::none())
            .unwrap()
            .0
            .is_empty()
    );
    for raw in [b"\xff\n".as_slice(), b"unfinished"] {
        assert!(parser.parse(&raw, &(), &RecordContext::none()).is_err());
    }
}

#[devkit::test]
fn every_append_split_keeps_complete_utf8_lines_and_duplicate_locations() {
    let raw = "pwd\npwd\n\n#123\nCafé\r\npartial".as_bytes();
    let expected = frame(raw);
    assert_eq!(expected.1, raw.len() - b"partial".len());
    for cut in 0..=raw.len() {
        let (mut first, through) = frame(&raw[..cut]);
        let (second, covered) = frame(&raw[through..]);
        first.extend(second.into_iter().map(|(a, b)| (through + a, through + b)));
        assert_eq!((first, through + covered), expected);
    }
    let records = frame(raw).0;
    let events = sinex_source_core::drain(
        &TextHistoryParser {
            host: "example-host".into(),
        },
        DrainInput {
            source: SPEC.name,
            class: Class::Guarded,
            material: MaterialId::new(),
            bytes: raw.into(),
            records: &records,
            carry: serde_json::Value::Null,
            reading_order: &|a| ReadingOrder(a as i64),
            limits: Limits::default(),
            manifest: RecordContext::none().manifest,
        },
    )
    .unwrap()
    .into_iter()
    .flat_map(|c| c.outcomes)
    .flat_map(|r| match r {
        RecordOutcome::Events { events, .. } => events,
        _ => panic!("valid lines"),
    })
    .collect::<Vec<_>>();
    assert_eq!(events.len(), 4);
    assert_eq!(events[0].payload["command"], "pwd");
    assert_eq!(events[1].payload["command"], "pwd");
    assert_ne!(events[0].location(), events[1].location());
    assert!(
        events
            .iter()
            .all(|e| e.equivalence_key.is_none() && e.ts_orig.at.is_none())
    );
}

#[devkit::test]
fn append_restart_replacement_and_missing_source_use_shared_reader() {
    use sinex_source_core::drivers::append_file::AppendState;
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("history");
    std::fs::write(&path, b"pwd\npwd\npartial").unwrap();
    let driver = driver(path.clone(), "example-host".into());
    let mut state = AppendState::default();
    let first = driver
        .poll(&serde_json::Value::Null, &mut state)
        .unwrap()
        .material
        .unwrap();
    assert_eq!(body(&first), b"pwd\npwd\n");
    let cursor = (first.cursor_at)(body(&first).len()).unwrap();
    let mut restarted = AppendState::rebuild([(first.id, body(&first))], SPEC.framer);
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b" line\npwd\n")
        .unwrap();
    let second = driver
        .poll(&cursor, &mut restarted)
        .unwrap()
        .material
        .unwrap();
    assert_eq!(body(&second), b"partial line\npwd\n");
    let cursor = (second.cursor_at)(body(&second).len()).unwrap();
    std::fs::write(&path, b"id\nid\n").unwrap();
    let replacement = driver.poll(&cursor, &mut restarted).unwrap();
    assert_eq!(body(&replacement.material.unwrap()), b"id\nid\n");
    std::fs::remove_file(&path).unwrap();
    assert!(driver.poll(&cursor, &mut restarted).is_err());
    for (start, end) in &first.records {
        assert_eq!(
            TextHistoryParser {
                host: "example-host".into()
            }
            .parse(&&body(&first)[*start..*end], &(), &RecordContext::none())
            .unwrap()
            .0[0]
                .command,
            "pwd"
        );
    }
}

#[devkit::test]
fn binding_reader_retries_held_bytes_before_reading_an_append() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("history");
    std::fs::write(&path, "pwd\npwd\n").unwrap();
    let binding = (FORMAT.plan.unwrap())(&path, &binding::Settings::new("example-host"))
        .unwrap()
        .remove(0);
    let mut reader = (FORMAT.reader.unwrap())(&binding)
        .unwrap()
        .into_polled()
        .unwrap();
    let first = reader
        .poll(&serde_json::Value::Null)
        .unwrap()
        .material
        .unwrap();
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"pwd\n")
        .unwrap();
    let retry = reader
        .poll(&serde_json::Value::Null)
        .unwrap()
        .material
        .unwrap();
    assert_eq!(retry.id, first.id);
    assert_eq!(body(&retry), body(&first));
    let cursor = (first.cursor_at)(body(&first).len()).unwrap();
    let next = reader.poll(&cursor).unwrap().material.unwrap();
    assert_eq!(body(&next), b"pwd\n");
    assert_ne!(next.id, first.id);
}

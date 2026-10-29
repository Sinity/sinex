//! Golden cases: real input shapes and their expected readings, as data.
//!
//! Each case is a directory `tests/sources/<parser>/<case>/` at the repository
//! root holding:
//! - `input`: the material's bytes, exactly as the source stores them;
//! - `manifest.json` (optional): the material's manifest, for parsers that
//!   read a path or an archive member;
//! - `expected.json`: one entry per framed record, its events or its failure.
//!
//! The input is framed with the source's own framer and read through the
//! drain, the same path a backfill or a replay takes. `GOLDEN_BLESS=1`
//! rewrites `expected.json` from the current output, for review in the diff.
use serde_json::{Value, json};
use sinex_model::{MaterialId, ReadingOrder, RecordOutcome};
use sinex_source_core::{DrainInput, Limits};
use std::path::{Path, PathBuf};

const HOST: &str = "golden-host";

fn cases_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/sources")
}

fn render(outcome: &RecordOutcome) -> Value {
    match outcome {
        RecordOutcome::Events { location, events } => json!({
            "anchor": location.map(|l| l.anchor),
            "events": events.iter().map(|e| json!({
                "type": e.event_type,
                "ts": e.ts_orig.at,
                "time": format!("{:?}", e.ts_orig.quality),
                "key": e.equivalence_key.as_ref().map(|k| k.0.clone()),
                "state": format!("{:?}", e.record_state),
                "payload": e.payload,
            })).collect::<Vec<_>>(),
        }),
        RecordOutcome::Failure { location, reason } => json!({
            "anchor": location.anchor,
            "failure": reason,
        }),
    }
}

fn manifest_of(dir: &Path) -> sinex_model::Manifest {
    match std::fs::read(dir.join("manifest.json")) {
        Ok(raw) => serde_json::from_slice(&raw).expect("manifest.json is JSON"),
        Err(_) => sinex_model::Manifest::from_json(json!({ "path": "input" })),
    }
}

fn run_case(parser: &str, dir: &Path) -> Value {
    let binding = sinex_sources::sources::binding_for_parser(parser, HOST)
        .unwrap_or_else(|| panic!("no source reads with parser {parser}"));
    let source = sinex_sources::sources::source_for(&binding).expect("source");
    let bytes = std::fs::read(dir.join("input")).expect("case input");
    let manifest = manifest_of(dir);
    let (records, _covered) = source.spec().framer.frame(&bytes);
    struct Fragmented<R>(R);
    impl<R: std::io::Read> std::io::Read for Fragmented<R> {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            let n = out.len().min(7);
            self.0.read(&mut out[..n])
        }
    }
    let streamed = source
        .spec()
        .framer
        .ranges(&mut Fragmented(std::io::Cursor::new(&bytes)))
        .expect("reader framing");
    assert_eq!(
        streamed,
        records,
        "reader framing: {parser}, {}",
        dir.display()
    );
    let order = |anchor: usize| ReadingOrder(anchor as i64);
    let chunks = source
        .drain(DrainInput {
            source: source.spec().name,
            class: source.spec().class,
            material: MaterialId::new(),
            bytes: bytes.as_slice().into(),
            records: &records,
            carry: Value::Null,
            reading_order: &order,
            limits: Limits::default(),
            manifest: &manifest,
        })
        .expect("drain");
    Value::Array(
        chunks
            .iter()
            .flat_map(|c| c.outcomes.iter().map(render))
            .collect(),
    )
}

#[devkit::test]
fn golden_cases() {
    let root = cases_root();
    let bless = std::env::var_os("GOLDEN_BLESS").is_some();
    let mut failures = Vec::new();
    let mut ran = 0;
    let mut parsers = entries(&root).expect("golden case directory is readable");
    parsers.sort_by_key(|e| e.file_name());
    for parser in parsers {
        let name = parser.file_name().to_string_lossy().into_owned();
        let mut cases = entries(&parser.path()).expect("parser cases are readable");
        cases.sort_by_key(|e| e.file_name());
        for case in cases {
            ran += 1;
            let dir = case.path();
            for check in [anchors_read_back, a_bad_record_fails_alone] {
                if let Err(e) = check(&name, &dir) {
                    failures.push(format!(
                        "{name}/{}: {e}",
                        case.file_name().to_string_lossy()
                    ));
                }
            }
            let got = run_case(&name, &dir);
            let expected_path = dir.join("expected.json");
            if bless {
                let text = serde_json::to_string_pretty(&got).unwrap() + "\n";
                std::fs::write(&expected_path, text).unwrap();
                continue;
            }
            let expected: Value = serde_json::from_slice(
                &std::fs::read(&expected_path)
                    .unwrap_or_else(|_| panic!("{} has no expected.json", dir.display())),
            )
            .unwrap();
            if got != expected {
                failures.push(format!(
                    "{name}/{}: expected\n{}\ngot\n{}",
                    case.file_name().to_string_lossy(),
                    serde_json::to_string_pretty(&expected).unwrap(),
                    serde_json::to_string_pretty(&got).unwrap()
                ));
            }
        }
    }
    assert!(ran > 0, "no golden cases found");
    assert!(
        failures.is_empty(),
        "{} of {ran} golden cases differ:\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}

fn entries(root: &Path) -> std::io::Result<Vec<std::fs::DirEntry>> {
    let entries = std::fs::read_dir(root)?.collect::<std::io::Result<Vec<_>>>()?;
    if entries.is_empty() {
        return Err(std::io::Error::other(format!(
            "no golden cases in {}",
            root.display()
        )));
    }
    Ok(entries)
}

#[devkit::test]
fn missing_or_empty_case_directories_fail() {
    let root = std::env::temp_dir().join(format!("golden-{}", uuid::Uuid::now_v7()));
    assert!(entries(&root).is_err());
    std::fs::create_dir(&root).unwrap();
    assert!(entries(&root).is_err());
    std::fs::remove_dir(root).unwrap();
}

/// The case input as stored and as the same text written differently: with
/// CRLF line ends, with a blank line after every line, and without its last
/// line end.
fn variants(bytes: &[u8]) -> Vec<(&'static str, Vec<u8>)> {
    let mut out = vec![("as stored", bytes.to_vec())];
    let lines: Vec<&[u8]> = bytes
        .split_inclusive(|b| *b == b'\n')
        .map(|l| l.strip_suffix(b"\n").unwrap_or(l))
        .map(|l| l.strip_suffix(b"\r").unwrap_or(l))
        .collect();
    let joined = |end: &[u8]| -> Vec<u8> {
        let mut text = Vec::new();
        for line in &lines {
            text.extend_from_slice(line);
            text.extend_from_slice(end);
        }
        text
    };
    out.push(("with CRLF line ends", joined(b"\r\n")));
    out.push(("with blank lines", joined(b"\n\n")));
    let mut unterminated = joined(b"\n");
    unterminated.pop();
    out.push(("without a last line end", unterminated));
    out
}

/// Anchors read back, for every variant of the case input: every event's
/// anchor is where a framed record starts, its bytes frame alone as that
/// record, and read alone with the state the parser carried into it (a CSV
/// header, a span still being merged) they give the same events.
fn anchors_read_back(parser: &str, dir: &Path) -> Result<(), String> {
    let binding = sinex_sources::sources::binding_for_parser(parser, HOST)
        .ok_or_else(|| format!("no source reads with parser {parser}"))?;
    let source = sinex_sources::sources::source_for(&binding).expect("source");
    let manifest = manifest_of(dir);
    let stored = std::fs::read(dir.join("input")).expect("case input");
    let read = |bytes: &[u8], records: &[(usize, usize)], carry: Value| {
        let order = |anchor: usize| ReadingOrder(anchor as i64);
        source
            .drain(DrainInput {
                source: source.spec().name,
                class: source.spec().class,
                material: MaterialId(uuid::Uuid::nil()),
                bytes: bytes.into(),
                records,
                carry,
                reading_order: &order,
                limits: Limits::default(),
                manifest: &manifest,
            })
            .expect("drain")
    };
    let events = |outcome: &RecordOutcome| match outcome {
        RecordOutcome::Events { events, .. } => events
            .iter()
            .map(|e| (e.event_type.clone(), e.payload.clone()))
            .collect(),
        RecordOutcome::Failure { .. } => Vec::new(),
    };
    for (variant, bytes) in variants(&stored) {
        let (records, _) = source.spec().framer.frame(&bytes);
        for outcome in read(&bytes, &records, Value::Null)
            .into_iter()
            .flat_map(|c| c.outcomes)
        {
            let RecordOutcome::Events {
                location: Some(location),
                ..
            } = &outcome
            else {
                continue;
            };
            let anchor = usize::try_from(location.anchor).unwrap();
            let Some(&(start, end)) = records.iter().find(|(start, _)| *start == anchor) else {
                return Err(format!(
                    "{variant}: anchor {anchor} is not where a record starts"
                ));
            };
            let alone = &bytes[start..end];
            let (again, _) = source.spec().framer.frame(alone);
            if again != [(0, alone.len())] {
                return Err(format!(
                    "{variant}: the record at {anchor} frames as {again:?} on its own"
                ));
            }
            // The state the parser carried into this record.
            let before: Vec<(usize, usize)> = records
                .iter()
                .copied()
                .filter(|(_, e)| *e <= start)
                .collect();
            let carry = read(&bytes, &before, Value::Null)
                .last()
                .map_or(Value::Null, |c| c.carry.clone());
            let reread: Vec<_> = read(alone, &again, carry)
                .iter()
                .flat_map(|c| &c.outcomes)
                .flat_map(events)
                .collect();
            if reread != events(&outcome) {
                return Err(format!(
                    "{variant}: the record at {anchor} reads back as {reread:?}, not {:?}",
                    events(&outcome)
                ));
            }
        }
    }
    Ok(())
}

/// A bad record fails alone: a line of bytes no parser accepts, put between
/// the case's first and second records, is a record failure, and every other
/// record reads as before. Formats that do not frame such a line as a record
/// of its own (mail, where it joins the message before) are left out.
fn a_bad_record_fails_alone(parser: &str, dir: &Path) -> Result<(), String> {
    let binding = sinex_sources::sources::binding_for_parser(parser, HOST)
        .ok_or_else(|| format!("no source reads with parser {parser}"))?;
    let source = sinex_sources::sources::source_for(&binding).expect("source");
    let manifest = manifest_of(dir);
    let stored = std::fs::read(dir.join("input")).expect("case input");
    let (records, _) = source.spec().framer.frame(&stored);
    let Some(&(second, _)) = records.get(1) else {
        return Ok(());
    };
    let bad: &[u8] = b"\xff\xfe{ not a record\n";
    let mut bytes = stored[..second].to_vec();
    bytes.extend_from_slice(bad);
    bytes.extend_from_slice(&stored[second..]);
    let (with_bad, _) = source.spec().framer.frame(&bytes);
    if with_bad.len() != records.len() + 1 || !with_bad.contains(&(second, second + bad.len())) {
        return Ok(());
    }
    let read = |bytes: &[u8], records: &[(usize, usize)]| {
        let order = |anchor: usize| ReadingOrder(anchor as i64);
        source
            .drain(DrainInput {
                source: source.spec().name,
                class: source.spec().class,
                material: MaterialId(uuid::Uuid::nil()),
                bytes: bytes.into(),
                records,
                carry: Value::Null,
                reading_order: &order,
                limits: Limits::default(),
                manifest: &manifest,
            })
            .expect("drain")
            .into_iter()
            .flat_map(|c| c.outcomes)
            .map(|o| match o {
                RecordOutcome::Events { location, events } => (
                    location.map(|l| l.anchor),
                    Ok(events
                        .into_iter()
                        .map(|e| (e.event_type, e.payload))
                        .collect::<Vec<_>>()),
                ),
                RecordOutcome::Failure { location, reason } => (Some(location.anchor), Err(reason)),
            })
            .collect::<Vec<_>>()
    };
    let before = read(&stored, &records);
    let after = read(&bytes, &with_bad);
    let shift = |anchor: Option<u64>| {
        anchor.map(|a| {
            if a >= second as u64 {
                a + bad.len() as u64
            } else {
                a
            }
        })
    };
    let expected: Vec<_> = before.into_iter().map(|(a, o)| (shift(a), o)).collect();
    let (failed, rest): (Vec<_>, Vec<_>) = after
        .into_iter()
        .partition(|(a, _)| *a == Some(second as u64));
    if !matches!(failed.as_slice(), [(_, Err(_))]) {
        return Err(format!("the bad record read as {failed:?}"));
    }
    if rest != expected {
        return Err(format!(
            "a bad record changed its neighbours: {rest:?} instead of {expected:?}"
        ));
    }
    Ok(())
}

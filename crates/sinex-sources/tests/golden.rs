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
use sinex_sources::{DrainInput, Limits};
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

fn run_case(parser: &str, dir: &Path) -> Value {
    let binding = sinex_sources::sources::binding_for_parser(parser, HOST)
        .unwrap_or_else(|| panic!("no source reads with parser {parser}"));
    let source = sinex_sources::sources::source_for(&binding).expect("source");
    let bytes = std::fs::read(dir.join("input")).expect("case input");
    let manifest: Value = match std::fs::read(dir.join("manifest.json")) {
        Ok(raw) => serde_json::from_slice(&raw).expect("manifest.json is JSON"),
        Err(_) => json!({ "path": "input" }),
    };
    let (records, _covered) = (source.spec().framer)(&bytes);
    let order = |anchor: usize| ReadingOrder(anchor as i64);
    let chunks = source
        .drain(DrainInput {
            source: source.spec().name,
            class: source.spec().class,
            material: MaterialId::new(),
            bytes: &bytes,
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

#[test]
fn golden_cases() {
    let root = cases_root();
    let bless = std::env::var_os("GOLDEN_BLESS").is_some();
    let mut failures = Vec::new();
    let mut ran = 0;
    let Ok(parsers) = std::fs::read_dir(&root) else {
        return;
    };
    let mut parsers: Vec<_> = parsers.flatten().collect();
    parsers.sort_by_key(|e| e.file_name());
    for parser in parsers {
        let name = parser.file_name().to_string_lossy().into_owned();
        let mut cases: Vec<_> = std::fs::read_dir(parser.path())
            .unwrap()
            .flatten()
            .collect();
        cases.sort_by_key(|e| e.file_name());
        for case in cases {
            let dir = case.path();
            let got = run_case(&name, &dir);
            let expected_path = dir.join("expected.json");
            if bless {
                let text = serde_json::to_string_pretty(&got).unwrap() + "\n";
                std::fs::write(&expected_path, text).unwrap();
                continue;
            }
            ran += 1;
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
    assert!(
        failures.is_empty(),
        "{} of {ran} golden cases differ:\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}

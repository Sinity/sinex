use super::*;
use serde_json::json;

fn record(v: Value) -> Map<String, Value> {
    v.as_object().unwrap().clone()
}

fn line(lane: &str, seq: i64) -> Map<String, Value> {
    record(
        json!({"schema": "sinnix-capture-v1", "schema_version": 1, "lane": lane,
        "ts": 1_760_000_000.25, "host": "sinnix-prime", "seq": seq,
        "payload": {"category":"text","mime":"text/plain","text":"synthetic","size":9}, "raw_ref": null}),
    )
}

#[devkit::test]
fn reads_the_envelope_under_the_catalog_source() {
    let p = EnvelopeParser {
        lane: lane("desktop.clipboard").unwrap(),
    };
    let (out, ()) = p
        .parse(&line("primary", 7), &(), &crate::RecordContext::none())
        .unwrap();
    assert_eq!(out[0].lane(), "primary");
    assert_eq!(out[0].seq, 7);
    assert_eq!(
        serde_json::to_value(out[0].payload()).unwrap(),
        json!({"category":"text","mime":"text/plain","text":"synthetic","size":9})
    );
    assert_eq!(
        out[0].observed_at,
        Timestamp::from_unix_ns(1_760_000_000_250_000_000).unwrap()
    );
}

#[devkit::test]
fn unbound_lanes_and_bad_envelopes_fail() {
    let p = EnvelopeParser {
        lane: lane("desktop.clipboard").unwrap(),
    };
    assert!(
        p.parse(&line("mpris", 1), &(), &crate::RecordContext::none())
            .is_err()
    );
    let mut bad = line("clipboard", 1);
    bad.insert("schema".into(), json!("other"));
    assert!(p.parse(&bad, &(), &crate::RecordContext::none()).is_err());
    bad = line("clipboard", 1);
    bad.remove("seq");
    assert!(p.parse(&bad, &(), &crate::RecordContext::none()).is_err());
}

#[devkit::test]
fn firehoses_emit_nothing_but_still_validate() {
    let p = EnvelopeParser {
        lane: lane("network.netflow").unwrap(),
    };
    assert!(
        p.parse(&line("netflow", 1), &(), &crate::RecordContext::none())
            .unwrap()
            .0
            .is_empty()
    );
    assert!(
        p.parse(&line("mpris", 1), &(), &crate::RecordContext::none())
            .is_err()
    );
}

#[devkit::test]
fn day_files_skip_index_and_sidecars() {
    let root = std::env::temp_dir().join(format!("sinex-lake-{}", std::process::id()));
    let dir = root.join("activity/desktop/notification");
    std::fs::create_dir_all(&dir).unwrap();
    for f in [
        "notifications-20261001.jsonl",
        "notifications-index.jsonl",
        "notifications-20261001.jsonl.hw",
        "notifications.seq",
    ] {
        std::fs::write(dir.join(f), b"").unwrap();
    }
    let files = day_files(&root, lane("desktop.notification").unwrap());
    assert_eq!(files.len(), 1);
    assert!(files[0].ends_with("notifications-20261001.jsonl"));
}

#[devkit::test]
fn source_names_and_parsers_are_unique() {
    let mut names: Vec<_> = LANES.iter().map(|l| l.spec.name).collect();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), LANES.len());
    assert!(LANES.iter().all(|l| l.spec.parser.ends_with(l.spec.name)));
}

#[devkit::test]
fn known_payload_failures_are_not_untyped_successes() {
    let parser = EnvelopeParser {
        lane: lane("desktop.clipboard").unwrap(),
    };
    let mut bad = line("clipboard", 1);
    bad.insert("payload".into(), json!({"text":4}));
    assert!(
        parser
            .parse(&bad, &(), &crate::RecordContext::none())
            .is_err()
    );
    let mut extended = line("clipboard", 1);
    extended["payload"]["future"] = json!({"raw":false});
    let (out, ()) = parser
        .parse(&extended, &(), &crate::RecordContext::none())
        .unwrap();
    let wire = serde_json::to_value(&out[0]).unwrap();
    assert_eq!(wire["payload"], extended["payload"]);
    assert_eq!(wire["unknown_fields"], json!(["/payload/future"]));
}

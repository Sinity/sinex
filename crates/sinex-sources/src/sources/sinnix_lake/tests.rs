use super::*;
use serde_json::json;

fn record(v: Value) -> Map<String, Value> {
    v.as_object().unwrap().clone()
}

fn line(lane: &str, seq: i64) -> Map<String, Value> {
    record(
        json!({"schema": "sinnix-capture-v1", "schema_version": 1, "lane": lane,
        "ts": 1_760_000_000.25, "host": "sinnix-prime", "seq": seq,
        "payload": {"app": "kitty"}, "raw_ref": null}),
    )
}

#[test]
fn reads_the_envelope_under_the_catalog_source() {
    let p = EnvelopeParser {
        lane: lane("desktop.clipboard").unwrap(),
    };
    let (out, ()) = p
        .parse(&line("primary", 7), &(), &crate::RecordContext::none())
        .unwrap();
    assert_eq!(out[0].lane, "primary");
    assert_eq!(out[0].seq, 7);
    assert_eq!(out[0].payload, json!({"app": "kitty"}));
    assert_eq!(
        out[0].observed_at,
        Timestamp::from_unix_ns(1_760_000_000_250_000_000).unwrap()
    );
}

#[test]
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

#[test]
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

#[test]
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

#[test]
fn source_names_and_parsers_are_unique() {
    let mut names: Vec<_> = LANES.iter().map(|l| l.spec.name).collect();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), LANES.len());
    assert!(LANES.iter().all(|l| l.spec.parser.ends_with(l.spec.name)));
}

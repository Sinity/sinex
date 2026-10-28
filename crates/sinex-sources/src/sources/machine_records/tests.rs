use super::*;
use crate::sources::fields;
use serde_json::json;
use sinex_model::{EventPayload, KeyKind, MaterialId, ReadingOrder, RecordOutcome, TimeQuality};
use sinex_source_core::drivers::dir_snapshot::DirState;
use sinex_source_core::{DrainInput, Limits, drain};

fn reader(value: Value) -> FieldReader {
    FieldReader::new(value.as_object().unwrap().clone())
}

fn at(rfc3339: &str) -> sinex_model::Timestamp {
    fields::rfc3339(rfc3339).unwrap()
}

#[devkit::test]
fn a_reactivated_generation_is_its_own_record() {
    let line = |activated_at: &str, revision: &str| {
        json!({"generation": "453", "activated_at": activated_at,
            "store_path": "/nix/store/aaaa-nixos-system-host-1", "sinnix_revision": revision,
            "nixos_label": "26.11.1.abc", "host": "host-a"})
    };
    let first = generation(reader(line("2026-07-05T23:41:04+00:00", "f6de85-dirty"))).unwrap();
    let again = generation(reader(line("2026-07-06T08:31:47+00:00", "a30b46"))).unwrap();
    let unknown = generation(reader(line("2026-07-06T08:33:04+00:00", "unknown"))).unwrap();
    assert_eq!(first[0].generation, 453);
    assert_eq!(first[0].activated_at, at("2026-07-05T23:41:04Z"));
    assert_eq!(
        (first[0].revision.as_deref(), first[0].dirty),
        (Some("f6de85"), Some(true))
    );
    assert_eq!(
        (again[0].revision.as_deref(), again[0].dirty),
        (Some("a30b46"), Some(false))
    );
    assert_eq!(
        (unknown[0].revision.as_deref(), unknown[0].dirty),
        (None, None)
    );
    assert!(first[0].extra.is_empty());
    assert_eq!(
        GenerationActivated::KEY,
        KeyKind::Fields(&["host", "generation", "activated_at"])
    );
    let key = |g: &GenerationActivated| {
        sinex_model::EquivalenceKey::of(
            GenerationActivated::EVENT_TYPE,
            &["host", "generation", "activated_at"],
            &serde_json::to_value(g).unwrap(),
        )
    };
    assert_ne!(key(&first[0]), key(&again[0]), "a re-activation is new");
    let reread = generation(reader(line("2026-07-05T23:41:04+00:00", "f6de85-dirty"))).unwrap();
    assert_eq!(key(&first[0]), key(&reread[0]), "a re-read is a repeat");
    assert!(
        generation(reader(
            json!({"generation": "x", "activated_at": "2026-07-05T23:41:04+00:00", "host": "h"})
        ))
        .is_err()
    );
    assert!(generation(reader(json!({"generation": "1", "host": "h"}))).is_err());
}

#[devkit::test]
fn backup_checks_of_every_shape() {
    let fresh = backup_check(reader(json!({"ts": "2026-06-22T23:29:29+02:00",
        "type": "archive_freshness", "label": "realm", "ok": true, "age_sec": 120,
        "max_age_sec": 21600, "message": "archive fresh"})))
    .unwrap();
    assert_eq!(fresh[0].checked_at, at("2026-06-22T21:29:29Z"));
    assert_eq!(fresh[0].check, "archive_freshness");
    assert_eq!(fresh[0].age_sec, Some(120));
    let unreadable = backup_check(reader(json!({"ts": "2026-06-22T23:29:22+02:00",
        "type": "archive_freshness", "label": "persist", "ok": false, "age_sec": -1,
        "max_age_sec": 21600, "message": "no archives found"})))
    .unwrap();
    assert_eq!(unreadable[0].age_sec, None, "-1 is the check's unknown age");
    let transition = backup_check(reader(json!({"ts": "2026-08-07T15:11:20+02:00",
        "type": "notification_transition", "label": "persist", "state": "red",
        "run_id": "", "deadline_epoch": 0})))
    .unwrap();
    assert_eq!(
        (transition[0].run_id.as_deref(), transition[0].deadline),
        (None, None)
    );
    let planned = backup_check(reader(json!({"ts": "2026-08-16T10:07:35+02:00",
        "type": "archive_freshness", "state": "maintenance", "run_id": "r1",
        "deadline_epoch": 1_786_878_168, "count": "6", "new_field": [1]})))
    .unwrap();
    assert_eq!(
        planned[0].deadline,
        sinex_model::Timestamp::from_unix_secs(1_786_878_168).ok()
    );
    assert_eq!(planned[0].count, Some(6), "a number written as text");
    assert_eq!(planned[0].extra["new_field"], json!([1]));
    let failed = backup_check(reader(json!({"ts": "2026-08-02T21:38:01+02:00",
        "type": "service_failure", "unit": "backup.service", "result": "exit-code",
        "ok": false, "message": "unit entered failed state"})))
    .unwrap();
    assert_eq!(failed[0].unit.as_deref(), Some("backup.service"));
    assert!(failed[0].extra.is_empty());
    assert!(
        backup_check(reader(
            json!({"ts": "2026-08-02T21:38:01+02:00", "type": "x", "ok": "yes"})
        ))
        .is_err()
    );
}

#[devkit::test]
fn restore_drills_of_both_stores() {
    let drill = restore_drill(reader(json!({"repo": "file:///backup/repo-a",
        "archive": "host-a-2026-05-20", "started_at": "2026-05-20T02:27:25+00:00",
        "ended_at": "2026-05-20T04:47:44+00:00", "duration_s": 8419, "exit_code": 0,
        "status": "ok", "stderr_tail": "", "within_days": 30})))
    .unwrap();
    assert_eq!(drill[0].stderr_tail, None);
    assert_eq!(drill[0].mode, None, "drills before modes were recorded");
    assert_eq!(drill[0].duration_s, Some(8419));
    let tasks = task_store_drill(reader(json!({"ts": "2026-10-01T05:08:00+02:00",
        "type": "task_store_restore_drill", "archive": "realm.20261001",
        "authority_path": "state/tasks/.beads", "dolt_commit": "abc",
        "issue": {"id": "t-1", "title": "a task"}, "ok": true})))
    .unwrap();
    assert_eq!(tasks[0].drilled_at, at("2026-10-01T03:08:00Z"));
    assert_eq!(
        tasks[0].issue,
        Some(json!({"id": "t-1", "title": "a task"}))
    );
    assert_eq!(tasks[0].extra["type"], "task_store_restore_drill");
}

#[devkit::test]
fn sandbox_findings_without_a_manager() {
    let found = sandbox_finding(reader(json!({"timestamp": "2026-08-16T12:20:03Z",
        "unit": "-", "manager": "-", "kind": "lane-abandoned", "path": "/data/lane",
        "detail": "no writes for 30 days"})))
    .unwrap();
    assert_eq!(found[0].manager, None);
    assert_eq!(found[0].kind, "lane-abandoned");
}

#[devkit::test]
fn a_census_line_is_one_item_or_a_whole_census() {
    let item = census(reader(json!({"ts": 1_786_882_577, "window_days": 90,
        "class": "scripts", "name": "tool-a", "declared_in": "scripts/tool-a",
        "evidence": {"history": {"n": 18}}, "static_refs": ["modules/a.nix"],
        "used_via_edges": false, "verdict": "active"})))
    .unwrap();
    let [Census::Assessed(item)] = item.as_slice() else {
        panic!("one item: {item:?}");
    };
    assert_eq!(
        item.assessed_at,
        sinex_model::Timestamp::from_unix_secs(1_786_882_577).unwrap()
    );
    assert_eq!(
        (item.snapshot_id.as_deref(), item.window_days),
        (None, Some(90))
    );
    let whole = census(reader(json!({"schema": "usage-census-v2", "snapshot_id": "s-1",
        "ts": 1_791_856_675, "collected_at": 1_791_856_684, "since": 1_784_080_675,
        "until": 1_791_856_675.5, "window_days": 90, "complete": true, "row_count": 2,
        "classes": ["scripts"], "coverage_gaps": [], "index_digest": "d",
        "rows": [
            {"class": "scripts", "name": "a", "verdict": "active", "static_refs": [], "enabled": null},
            {"class": "skills", "name": "b", "verdict": "unused", "static_refs": ["x"], "never_prune": true}
        ]})))
    .unwrap();
    assert_eq!(whole.len(), 3);
    let Census::Taken(taken) = &whole[0] else {
        panic!("the census first");
    };
    assert_eq!(taken.until.unwrap().unix_ns(), 1_791_856_675_500_000_000);
    assert_eq!(taken.extra["index_digest"], "d");
    assert!(!taken.extra.contains_key("rows"));
    let Census::Assessed(b) = &whole[2] else {
        panic!("then its rows");
    };
    assert_eq!(b.snapshot_id.as_deref(), Some("s-1"));
    assert_eq!(b.assessed_at, taken.taken_at);
    assert_eq!(b.window_days, Some(90));
    assert_eq!(b.extra["never_prune"], true);
    assert!(
        census(reader(
            json!({"snapshot_id": "s", "ts": 1, "rows": [{"name": "no class"}]})
        ))
        .is_err()
    );
}

#[devkit::test]
fn drift_checks_have_no_time() {
    let check = config_drift(reader(json!({"check": "iocost", "evidence": "",
        "intended": {"model": "linear"}, "live": {"model": "linear"}, "match": true,
        "reboot_required": false, "schema": "config-drift-v1", "severity": "info",
        "status": "matched"})))
    .unwrap();
    assert_eq!(check[0].matches, Some(true));
    assert_eq!(check[0].evidence, None);
    assert_eq!(check[0].intended, Some(json!({"model": "linear"})));
    assert_eq!(check[0].ts_orig().at, None);
    assert_eq!(check[0].ts_orig().quality, TimeQuality::Unknown);
}

fn drain_payloads(
    parser: &ExperimentParser,
    acquired: &sinex_source_core::drivers::Acquired,
) -> Vec<(String, Value)> {
    let m = acquired.material.as_ref().unwrap();
    drain(
        parser,
        DrainInput {
            source: "machine.experiments",
            class: Class::Open,
            material: MaterialId(uuid::Uuid::nil()),
            bytes: m.record_bytes(),
            records: &m.records,
            carry: Value::Null,
            reading_order: &|_| ReadingOrder(0),
            limits: Limits::default(),
            manifest: &m.manifest,
        },
    )
    .unwrap()
    .into_iter()
    .flat_map(|c| c.outcomes)
    .flat_map(|o| match o {
        RecordOutcome::Events { events, .. } => events
            .into_iter()
            .map(|e| (e.event_type.to_owned(), e.payload))
            .collect::<Vec<_>>(),
        RecordOutcome::Failure { reason, .. } => panic!("{reason}"),
    })
    .collect()
}

#[devkit::test]
fn experiment_runs_start_and_finish() {
    let owned = tempfile::tempdir().unwrap();
    let root = owned.path().to_path_buf();
    let run = root.join("20260512T142820Z-smoke");
    let nested = root.join("bench-1/runs/bench-1-cold-1");
    std::fs::create_dir_all(&run).unwrap();
    std::fs::create_dir_all(&nested).unwrap();
    let started = json!({"run_id": "smoke-1", "host": "host-a", "command": ["true"],
        "cwd": "/work", "git": {"dirty": true}, "notes": [], "pre_state": {"kernel": "6"},
        "started_at": "2026-05-12T14:28:20+00:00", "workload": "smoke",
        "schema": "machine-experiment-run-v1"});
    std::fs::write(run.join("started.json"), started.to_string()).unwrap();
    let mut manifest = started.clone();
    manifest["ended_at"] = json!("2026-05-12T14:28:21+00:00");
    manifest["exit_status"] = json!(0);
    manifest["post_state"] = json!({"kernel": "6"});
    std::fs::write(run.join("manifest.json"), manifest.to_string()).unwrap();
    std::fs::write(
        nested.join("manifest.json"),
        json!({"run_id": "bench-1-cold-1", "host": "host-a", "command": ["nix", "build"],
            "started_at": "2026-06-29T19:40:00,963299500+00:00",
            "ended_at": "2026-06-29T19:41:32,963299500+00:00", "exit_status": 0,
            "execution_outcome": {"status": "success"}})
        .to_string(),
    )
    .unwrap();
    std::fs::write(nested.join("manifest.template.json"), "{}").unwrap();
    std::fs::write(root.join("bench-1/plan.json"), "{}").unwrap();

    let d = experiment_driver(root.clone(), "host-a".into());
    let mut state = DirState::default();
    let acquired = d.poll(&Value::Null, &mut state).unwrap();
    let events = drain_payloads(&ExperimentParser, &acquired);
    let mut types: Vec<&str> = events.iter().map(|(t, _)| t.as_str()).collect();
    types.sort_unstable();
    assert_eq!(
        types,
        ["experiment.run", "experiment.run", "experiment.started"],
        "templates and plans are not runs"
    );
    let bench = events
        .iter()
        .find(|(_, p)| p["run_id"] == "bench-1-cold-1")
        .unwrap();
    assert_eq!(
        serde_json::from_value::<sinex_model::Timestamp>(bench.1["ended_at"].clone()).unwrap(),
        at("2026-06-29T19:41:32.9632995Z"),
        "a comma before the fraction"
    );
    assert_eq!(bench.1["extra"]["execution_outcome"]["status"], "success");
    let smoke = events
        .iter()
        .find(|(t, p)| t == "experiment.run" && p["run_id"] == "smoke-1")
        .unwrap();
    assert_eq!(smoke.1["exit_status"], 0);
    assert_eq!(smoke.1["post_state"]["kernel"], "6");

    let m = acquired.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(m.records.last().unwrap().1).unwrap();
    std::fs::remove_file(run.join("started.json")).unwrap();
    let after = d.poll(&cursor, &mut state).unwrap();
    assert!(
        drain_payloads(&ExperimentParser, &after).is_empty(),
        "a removed file says nothing about its run"
    );
}

#[devkit::test]
fn a_backfilled_run_has_no_end() {
    let run = experiment(
        reader(
            json!({"run_id": "launch-1", "host": "host-a", "command": ["capture"],
            "started_at": "2026-03-12T17:28:50+01:00", "ended_at": null, "exit_status": null,
            "git": {}, "notes": ["backfilled"]}),
        ),
        false,
    )
    .unwrap();
    let Experiment::Run(run) = run else {
        panic!("a manifest is a run");
    };
    assert_eq!((run.ended_at, run.exit_status), (None, None));
    assert_eq!(run.started_at, Some(at("2026-03-12T16:28:50Z")));
    assert!(experiment(reader(json!({"host": "h"})), true).is_err());
}

#[devkit::test]
fn the_machine_root_plans_one_binding_per_file_present() {
    let owned = tempfile::tempdir().unwrap();
    let root = owned.path();
    for name in [
        "generations.jsonl",
        "config-drift.jsonl",
        "borg_status.jsonl.nested",
    ] {
        std::fs::write(root.join(name), "").unwrap();
    }
    std::fs::create_dir(root.join("experiment")).unwrap();
    let settings = sinex_source_core::Settings::new("host-a");
    let plan = FORMAT.plan.unwrap();
    let mut sources: Vec<String> = plan(root, &settings)
        .unwrap()
        .into_iter()
        .map(|b| b.source)
        .collect();
    sources.sort();
    assert_eq!(
        sources,
        [
            "machine.config-drift",
            "machine.experiments",
            "machine.generations"
        ]
    );
    let one = plan(&root.join("generations.jsonl"), &settings).unwrap();
    assert_eq!(one.len(), 1);
    assert_eq!(one[0].parser, "machine-generations");
    for b in plan(root, &settings).unwrap() {
        assert_eq!((FORMAT.source)(&b).unwrap().spec().name, b.source);
        assert!((FORMAT.reader.unwrap())(&b).is_ok());
    }
}

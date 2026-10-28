use super::*;
use serde_json::json;
use sinex_model::{EquivalenceKey, EventPayload, TimeQuality};

fn row(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

fn parse(table: &'static str, row: &Map<String, Value>) -> Result<Telemetry, RecordError> {
    let (mut payloads, ()) =
        TelemetryParser { table }.parse(row, &(), &sinex_source_core::RecordContext::none())?;
    assert_eq!(payloads.len(), 1, "one event per row");
    Ok(payloads.remove(0))
}

fn sample() -> Map<String, Value> {
    row(json!({
        "_rowid": 7, "id": 7, "observed_at": "2026-03-01T10:00:00+00:00", "host": "box",
        "boot_id": "b-1", "schema_version": 6, "interval_s": 5.0, "cpu_package_w": 31.5,
        "gpu_temp_c": null, "mem_used_mb": 0, "gap_codes_json": "[\"gpu\"]",
        "swaps_json": "not json"
    }))
}

#[devkit::test]
fn a_sample_row_is_one_event_with_its_columns() {
    let Telemetry::Sampled(p) = parse("metric_sample", &sample()).unwrap() else {
        panic!("a sample table gives resource.sampled");
    };
    assert_eq!((p.table.as_str(), p.row_id), ("metric_sample", 7));
    assert_eq!(
        (p.host.as_deref(), p.boot_id.as_deref()),
        (Some("box"), Some("b-1"))
    );
    let ts = p.ts_orig();
    assert_eq!(ts.quality, TimeQuality::Intrinsic);
    assert_eq!(
        ts.at.unwrap(),
        crate::sources::fields::rfc3339("2026-03-01T10:00:00Z").unwrap()
    );
    assert_eq!(p.values["cpu_package_w"], json!(31.5));
    assert_eq!(p.values["mem_used_mb"], json!(0), "a zero stays a zero");
    assert!(
        !p.values.contains_key("gpu_temp_c"),
        "a null column is unknown, never zero"
    );
    assert_eq!(p.values["gap_codes_json"], json!(["gpu"]));
    assert_eq!(
        p.values["swaps_json"],
        json!("not json"),
        "JSON text that does not parse stays text"
    );
    for gone in ["_rowid", "id", "observed_at", "host", "boot_id"] {
        assert!(
            !p.values.contains_key(gone),
            "{gone} is typed, not repeated"
        );
    }
}

#[devkit::test]
fn an_unreadable_time_is_unknown() {
    let mut r = sample();
    r.insert("observed_at".into(), json!("yesterday"));
    let Telemetry::Sampled(p) = parse("metric_sample", &r).unwrap() else {
        panic!()
    };
    assert_eq!(p.observed_at, None);
    assert_eq!(p.ts_orig().at, None);
}

#[devkit::test]
fn a_column_of_the_wrong_type_fails_the_row() {
    let mut r = sample();
    r.insert("host".into(), json!(3));
    assert!(parse("metric_sample", &r).is_err());
    let mut r = sample();
    r.remove("id");
    assert!(parse("metric_sample", &r).is_err(), "a row has an id");
}

#[devkit::test]
fn a_kill_row_is_a_killed_process() {
    let r = row(json!({
        "_rowid": 3, "id": 3, "observed_at": "2026-03-01T10:00:00+01:00", "host": "box",
        "boot_id": null, "schema_version": 6, "killer": "earlyoom", "victim_comm": "worker",
        "victim_pid": 4242, "victim_rss_mib": null, "cgroup_path": "/system.slice/x.service",
        "oom_score": null, "raw_line": "earlyoom: killing process 4242 worker",
        "journal_cursor": "s=1;i=2"
    }));
    let Telemetry::Killed(p) = parse("kill_event", &r).unwrap() else {
        panic!("kill_event gives process.killed");
    };
    assert_eq!(p.row_id, 3);
    assert_eq!(p.killer, "earlyoom");
    assert_eq!(p.victim_pid, Some(4242));
    assert_eq!(
        (p.victim_rss_mib, p.oom_score, p.boot_id),
        (None, None, None),
        "missing values are unknown"
    );
    assert_eq!(p.values, row(json!({"schema_version": 6})));
    assert!(p.observed_at.is_some());
    let mut r = r;
    r.remove("raw_line");
    assert!(parse("kill_event", &r).is_err(), "the journal line is kept");
}

#[devkit::test]
fn a_hardware_row_keeps_its_captured_state() {
    let r = row(json!({
        "_rowid": 9, "id": 9, "captured_at": "2026-03-01T10:00:00+00:00", "host": "box",
        "boot_id": "b-1", "schema_version": 6,
        "payload_json": "{\"kernel\": \"6.1\", \"cpu_governor\": \"powersave\"}"
    }));
    let Telemetry::Hardware(p) = parse("hardware_state", &r).unwrap() else {
        panic!("hardware_state gives hardware.state");
    };
    assert_eq!(p.state["kernel"], json!("6.1"));
    assert!(p.captured_at.is_some());
    let mut r = r;
    r.insert("payload_json".into(), json!("{broken"));
    assert!(parse("hardware_state", &r).is_err());
}

/// The same row read from the active file and from the sealed file it was
/// renamed to is one record; another table's row with the same ID is not.
#[devkit::test]
fn a_row_has_one_key_in_every_file() {
    let key = |p: &Telemetry| match p {
        Telemetry::Sampled(p) => EquivalenceKey::of(
            ResourceSampled::EVENT_TYPE,
            match ResourceSampled::KEY {
                sinex_model::KeyKind::Fields(fields) => fields,
                sinex_model::KeyKind::Location => panic!("keyed"),
            },
            &serde_json::to_value(p).unwrap(),
        ),
        _ => panic!(),
    };
    let dir = std::env::temp_dir().join(format!("sinex-telemetry-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let active = database(&dir.join("telemetry.sqlite"), true);
    let sealed = dir.join("telemetry.20260301T000000Z.sqlite");
    std::fs::copy(&active, &sealed).unwrap();
    let read = |path: &Path, table: &'static str| {
        let acquired = driver(path.to_path_buf(), "box".into(), table)
            .poll(&Value::Null, &mut Default::default())
            .unwrap();
        drained(table, &acquired)
    };
    let from_active = read(&active, "metric_sample");
    let from_sealed = read(&sealed, "metric_sample");
    assert_eq!(from_active.len(), 2);
    assert_eq!(
        from_active.iter().map(key).collect::<Vec<_>>(),
        from_sealed.iter().map(key).collect::<Vec<_>>()
    );
    let other = read(&active, "gpu_sample");
    assert_ne!(key(&from_active[0]), key(&other[0]));
}

/// A synthetic collector database: two metric rows and one GPU row, with
/// `filesystem_capacity_sample` only when `newer`.
fn database(path: &Path, newer: bool) -> PathBuf {
    let c = rusqlite::Connection::open(path).unwrap();
    c.execute_batch(
        "CREATE TABLE metric_sample (id INTEGER PRIMARY KEY AUTOINCREMENT, observed_at TEXT NOT NULL,
           host TEXT NOT NULL, boot_id TEXT, cpu_package_w REAL);
         CREATE TABLE gpu_sample (id INTEGER PRIMARY KEY AUTOINCREMENT, observed_at TEXT NOT NULL,
           host TEXT NOT NULL, boot_id TEXT, gpu_power_w REAL);
         CREATE TABLE source_status (source TEXT PRIMARY KEY, checked_at TEXT NOT NULL, status TEXT NOT NULL);
         INSERT INTO metric_sample VALUES (1, '2026-03-01T10:00:00+00:00', 'box', 'b-1', 30.0),
           (2, '2026-03-01T10:00:05+00:00', 'box', 'b-1', NULL);
         INSERT INTO gpu_sample VALUES (1, '2026-03-01T10:00:00+00:00', 'box', 'b-1', 80.0);",
    )
    .unwrap();
    if newer {
        c.execute_batch(
            "CREATE TABLE filesystem_capacity_sample (id INTEGER PRIMARY KEY AUTOINCREMENT,
               observed_at TEXT NOT NULL, host TEXT NOT NULL, boot_id TEXT, path TEXT NOT NULL);",
        )
        .unwrap();
    }
    path.to_path_buf()
}

fn drained(table: &'static str, acquired: &sinex_source_core::Acquired) -> Vec<Telemetry> {
    use std::os::unix::fs::FileExt;
    let material = acquired.material.as_ref().unwrap();
    let Some(sinex_source_core::drivers::StoredBytes::FramedFile(file)) = &material.stored else {
        panic!("rows are staged in a file");
    };
    let mut bytes = vec![0; file.metadata().unwrap().len() as usize];
    file.read_exact_at(&mut bytes, 0).unwrap();
    material
        .records
        .iter()
        .map(|(start, end)| {
            let row: Map<String, Value> = serde_json::from_slice(&bytes[*start..*end]).unwrap();
            parse(table, &row).unwrap()
        })
        .collect()
}

#[devkit::test]
fn a_directory_plans_each_database_and_table_it_has() {
    let dir = std::env::temp_dir().join(format!("sinex-telemetry-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    database(&dir.join("telemetry.sqlite"), true);
    database(&dir.join("telemetry.legacy.sqlite"), false);
    // The collector's other files are not databases to read.
    for other in [
        "telemetry.sqlite-wal",
        "telemetry.sqlite.next",
        "telemetry.sqlite.segments.json",
    ] {
        std::fs::write(dir.join(other), b"").unwrap();
    }
    let bindings = plan(&dir, &Settings::new("box")).unwrap();
    let mut names: Vec<String> = bindings
        .iter()
        .map(|b| {
            let file = b.input.file_name().unwrap().to_string_lossy().into_owned();
            format!("{file} {}", table_of(&b.parser).unwrap())
        })
        .collect();
    names.sort();
    assert_eq!(
        names,
        [
            "telemetry.legacy.sqlite gpu_sample",
            "telemetry.legacy.sqlite metric_sample",
            "telemetry.sqlite filesystem_capacity_sample",
            "telemetry.sqlite gpu_sample",
            "telemetry.sqlite metric_sample",
        ]
    );
    assert!(
        bindings
            .iter()
            .all(|b| b.source == "machine.telemetry" && b.name.contains('#')),
        "each table its own binding and cursor"
    );
    for b in &bindings {
        let source = crate::sources::source_for(b).unwrap();
        assert_eq!(source.spec().parser, b.parser);
    }
}

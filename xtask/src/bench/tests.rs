use super::*;

#[devkit::test]
fn missing_or_invalid_source_times_never_become_zero() {
    assert_eq!(
        source_wall_seconds(&json!({"sources": {"a": {"wall_s": 2.0}, "b": {"wall_s": 3.0}}})),
        Some(5.0)
    );
    for value in [
        json!({}),
        json!({"sources": {}}),
        json!({"sources": {"a": {}}}),
        json!({"sources": {"a": {"wall_s": 2.0}, "b": {}}}),
        json!({"sources": {"a": {"wall_s": -1.0}}}),
    ] {
        assert_eq!(source_wall_seconds(&value), None);
    }
}

#[devkit::test]
fn an_unavailable_component_leaves_the_total_unknown() {
    let known = bench::linear_estimate(Some(2.0), &std::collections::BTreeMap::new(), Some(10.0));
    let unknown = bench::linear_estimate(None, &std::collections::BTreeMap::new(), Some(10.0));
    let result = combined_estimate(&[known.clone(), known.clone()]);
    assert_eq!(result["estimated_seconds"], 40.0);
    assert_eq!(result["calibrated"], false);
    assert!(combined_estimate(&[known, unknown])["estimated_seconds"].is_null());
    assert!(combined_estimate(&[])["estimated_seconds"].is_null());
}

#[devkit::test]
fn failed_or_absent_work_never_counts_as_a_completed_benchmark() {
    let completed = json!({"notes": {"ok": true}, "shell": {"ok": true}});
    let inputs = completed.as_object().unwrap();
    assert!(benchmark_complete(inputs, true));
    assert!(!benchmark_complete(inputs, false));
    for incomplete in [
        json!({}),
        json!({"notes": {"ok": true}, "shell": {"ok": false, "error": "child failed"}}),
        json!({"notes": {"wall_s": 2.0}}),
        json!({"notes": {"ok": "true"}}),
    ] {
        assert!(!benchmark_complete(incomplete.as_object().unwrap(), true));
    }
}

#[devkit::test]
fn statements_use_offered_batches_even_when_a_statement_runs_more_than_once() {
    let statements = json!([{"calls": 12, "total_exec_time": 600.0, "mean_exec_time": 50.0}]);
    let normalized = measurements::normalize_statements(&statements, Some(3));
    assert_eq!(normalized[0]["exec_ms_per_offered_batch"], 200.0);
    assert_eq!(normalized[0]["mean_exec_time"], 50.0);
    for batches in [None, Some(0)] {
        assert!(measurements::normalize_statements(&statements, batches)[0]["exec_ms_per_offered_batch"].is_null());
    }
}

#[devkit::test]
fn mixed_source_counts_keep_integers_and_missing_times_stay_unknown() {
    let result = json!({"sources": {
        "a": {"admitted": 20, "batches": 2, "events_offered": 30, "admission_s": 1.0},
        "b": {"admitted": 40, "batches": 3, "events_offered": 50, "admission_s": 2.0}
    }});
    let totals = measurements::source_totals(&result);
    assert_eq!(totals["admitted"].as_u64(), Some(60));
    assert_eq!(totals["batches"].as_u64(), Some(5));
    assert_eq!(totals["events_offered"].as_u64(), Some(80));
    assert_eq!(totals["admission_s"], 3.0);
    assert!(totals["wall_s"].is_null());
    assert!(measurements::source_totals(&json!({})).is_null());
}

#[devkit::test]
fn benchmark_settings_share_module_defaults_and_preserve_disk_durability() {
    let configuration = measurements::postgres_configuration(true, &[]).unwrap();
    for setting in [
        "shared_buffers = '4GB'",
        "work_mem = '64MB'",
        "checkpoint_timeout = '30min'",
        "fsync = 'on'",
        "full_page_writes = 'on'",
        "synchronous_commit = 'on'",
        "shared_preload_libraries = 'timescaledb,pg_stat_statements'",
        "track_io_timing = 'on'",
    ] {
        assert!(configuration.contains(setting), "{setting}");
    }
    for setting in [
        "fsync=off",
        "full_page_writes=off",
        "synchronous_commit=off",
        "work_mem=64MB\nfsync=off",
    ] {
        assert!(
            measurements::postgres_configuration(true, &[setting.into()]).is_err(),
            "{setting}"
        );
    }
    assert!(
        measurements::postgres_configuration(false, &[])
            .unwrap()
            .contains("fsync = 'off'")
    );
    assert!(
        measurements::postgres_configuration(true, &["work_mem=128MB".into()])
            .unwrap()
            .contains("work_mem = '128MB'")
    );
}

#[devkit::test]
fn wal_reset_never_looks_like_a_negative_or_valid_delta() {
    let before = json!({"wal": {"stats_reset": "a", "wal_bytes": 100}});
    assert_eq!(
        measurements::wal_delta(
            &before,
            &json!({"wal": {"stats_reset": "a", "wal_bytes": 160}})
        ),
        Some(60)
    );
    assert_eq!(
        measurements::wal_delta(
            &before,
            &json!({"wal": {"stats_reset": "b", "wal_bytes": 160}})
        ),
        None
    );
    assert_eq!(
        measurements::wal_delta(
            &before,
            &json!({"wal": {"stats_reset": "a", "wal_bytes": 80}})
        ),
        None
    );
}

#[devkit::test]
fn stage_percentiles_compare_equal_batch_sizes_and_count_suppressed_batches() {
    let batches = json!([
        {"events": 10000, "admitted": 10000, "timings_ms": {"copy": 10.0}},
        {"events": 10000, "admitted": 0, "timings_ms": {"copy": 30.0}},
        {"events": 10000, "admitted": 10000, "timings_ms": {"copy": 20.0}},
        {"events": 287, "admitted": 287, "timings_ms": {"copy": 2.0}}
    ]);
    let summary = measurements::summarize_batches(batches.as_array().unwrap());
    assert_eq!(summary["totals"]["batches"], 4);
    assert_eq!(summary["totals"]["events_offered"], 30287);
    assert_eq!(summary["totals"]["admitted"], 20287);
    assert_eq!(summary["totals"]["copy_s"], 0.062);
    assert!(summary["totals"]["parse_s"].is_null());
    let groups = &summary["by_batch_size"];
    assert_eq!(groups[0]["events_per_batch"], 287);
    assert_eq!(groups[1]["events_per_batch"], 10000);
    assert_eq!(groups[1]["stages"]["copy"]["p50_ms"], 20.0);
    assert_eq!(groups[1]["stages"]["copy"]["p95_ms"], 30.0);
    assert!(measurements::summarize_batches(&[])["totals"]["batches"].is_null());
}

#[devkit::test]
fn socket_paths_remain_one_connection_parameter() {
    assert_eq!(
        measurements::socket_url(Path::new("/realm/bench one/pg&user=other")),
        "postgresql:///sinex?host=/realm/bench%20one/pg%26user%3Dother&user=sinex"
    );
}

#[devkit::test]
fn disposable_postgres_stops_after_measurement_and_startup_errors() {
    let root = env::temp_dir().join(format!("bc-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let settings = ["shared_buffers=16MB".into()];
    let measured = (|| -> Result<(), String> {
        let _cluster = Cluster::start(&root.join("measurement"), true, &settings)?;
        Err("measurement failed".into())
    })();
    assert_eq!(measured.unwrap_err(), "measurement failed");
    let started = Cluster::start(
        &root.join("startup"),
        true,
        &["unknown_benchmark_setting=invalid".into()],
    );
    assert!(started.is_err());
    for name in ["measurement", "startup"] {
        let status = Command::new("pg_ctl")
            .arg("-D")
            .arg(root.join(name).join("data"))
            .arg("status")
            .stdout(std::process::Stdio::null())
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(3), "{name} server still runs");
    }
    fs::remove_dir_all(root).unwrap();
}

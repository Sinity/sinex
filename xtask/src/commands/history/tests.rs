use super::*;
use crate::cargo_diagnostics::CompilerDiagnostic;
use crate::history::{HistoryDb, TestResult as StoredTestResult, TestStatus};
use crate::output::{OutputFormat, OutputWriter};
use crate::sandbox::sinex_test;
use color_eyre::eyre::eyre;
use std::collections::HashSet;
use tempfile::tempdir;

fn silent_ctx() -> CommandContext {
    CommandContext::new(
        OutputWriter::new(OutputFormat::Silent),
        false,
        None,
        "history",
    )
}

#[sinex_test]
async fn test_resolve_history_day_accepts_relative_labels() -> ::xtask::sandbox::TestResult<()> {
    let today = time::Date::from_calendar_date(2026, time::Month::June, 6)?;

    assert_eq!(resolve_history_day(None, today, "--day")?, "2026-06-06");
    assert_eq!(
        resolve_history_day(Some("2026-06-04"), today, "--day")?,
        "2026-06-04"
    );
    assert_eq!(
        resolve_history_day(Some("today"), today, "--day")?,
        time::OffsetDateTime::now_utc().date().to_string()
    );
    assert_eq!(
        resolve_history_day(Some(" yesterday "), today, "--against")?,
        (time::OffsetDateTime::now_utc().date() - time::Duration::days(1)).to_string()
    );
    Ok(())
}

#[sinex_test]
async fn test_resolve_history_day_rejects_unknown_relative_label()
-> ::xtask::sandbox::TestResult<()> {
    let today = time::Date::from_calendar_date(2026, time::Month::June, 6)?;
    let error = resolve_history_day(Some("tomorrow"), today, "--day")
        .expect_err("unsupported label should be rejected");

    assert!(error.to_string().contains("YYYY-MM-DD"));
    assert!(error.to_string().contains("tomorrow"));
    Ok(())
}

#[sinex_test]
async fn test_infra_timing_probe_from_result_reports_errors() -> ::xtask::sandbox::TestResult<()> {
    let probe = infra_timing_probe_from_result::<()>(Err(eyre!("infra exploded")));
    assert!(probe.value.is_none());
    assert!(probe.issue.unwrap_or_default().contains("infra exploded"));
    Ok(())
}

#[sinex_test]
async fn test_exercise_results_probe_from_result_reports_errors() -> ::xtask::sandbox::TestResult<()>
{
    let probe = exercise_results_probe_from_result(42, Err(eyre!("results exploded")));
    assert!(probe.results.is_empty());
    assert!(probe.issue.unwrap_or_default().contains("results exploded"));
    Ok(())
}

#[sinex_test]
async fn test_diagnostic_summary_probe_from_result_reports_errors()
-> ::xtask::sandbox::TestResult<()> {
    let probe = diagnostic_summary_probe_from_result(7, Err(eyre!("diag exploded")));
    assert_eq!(probe.fragment, "diag:ERR");
    assert!(probe.issue.unwrap_or_default().contains("diag exploded"));
    Ok(())
}

#[sinex_test]
async fn test_stage_summary_probe_from_result_reports_errors() -> ::xtask::sandbox::TestResult<()> {
    let probe = stage_summary_probe_from_result(7, Err(eyre!("stages exploded")));
    assert_eq!(probe.fragment, "stages:ERR");
    assert!(probe.issue.unwrap_or_default().contains("stages exploded"));
    Ok(())
}

#[sinex_test]
async fn test_test_summary_probe_from_result_reports_errors() -> ::xtask::sandbox::TestResult<()> {
    let probe = test_summary_probe_from_result(7, Err(eyre!("tests exploded")));
    assert_eq!(probe.fragment, "tests:ERR");
    assert!(probe.issue.unwrap_or_default().contains("tests exploded"));
    Ok(())
}

#[sinex_test]
async fn test_resolve_default_diagnostics_delta_target_reports_build_lookup_errors()
-> ::xtask::sandbox::TestResult<()> {
    let error = resolve_default_diagnostics_delta_target(None, Err(eyre!("build exploded")))
        .expect_err("build lookup failure should surface");
    let message = format!("{error:#}");
    assert!(message.contains("build exploded"));
    assert!(message.contains("diagnostics delta target"));
    Ok(())
}

#[sinex_test]
async fn test_ensure_sqlite3_available_reports_probe_failures() -> ::xtask::sandbox::TestResult<()>
{
    let error = ensure_sqlite3_available(Err(std::io::Error::other("probe exploded")))
        .expect_err("probe failure should surface");
    assert!(
        error
            .to_string()
            .contains("failed to probe sqlite3 availability")
    );
    assert!(error.to_string().contains("probe exploded"));
    Ok(())
}

#[sinex_test]
async fn test_ensure_sqlite3_available_reports_missing_sqlite3_honestly()
-> ::xtask::sandbox::TestResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;

        let output = std::process::Output {
            status: std::process::ExitStatus::from_raw(256),
            stdout: Vec::new(),
            stderr: b"which: no sqlite3 in PATH".to_vec(),
        };
        let error = ensure_sqlite3_available(Ok(output)).expect_err("missing sqlite3 should fail");
        let message = error.to_string();
        assert!(message.contains("sqlite3 is not available on PATH"));
        assert!(message.contains("which: no sqlite3 in PATH"));
    }
    Ok(())
}

fn sample_diagnostic(
    level: &str,
    file_path: Option<&str>,
    package: Option<&str>,
    code: Option<&str>,
    fixable: bool,
    command: Option<&str>,
) -> crate::history::StoredDiagnostic {
    crate::history::StoredDiagnostic {
        id: 1,
        level: level.to_string(),
        code: code.map(str::to_string),
        message: "sample".to_string(),
        file_path: file_path.map(str::to_string),
        line: Some(1),
        col: Some(1),
        rendered: None,
        package: package.map(str::to_string),
        fix_replacement: None,
        fix_applicability: fixable.then(|| "MachineApplicable".to_string()),
        fix_byte_start: None,
        fix_byte_end: None,
        authority: "proof".to_string(),
        source_command: command.map(str::to_string),
        source_time: None,
    }
}

fn seeded_history_db(name: &str) -> Result<HistoryDb> {
    let dir = tempdir()?;
    let db_path = dir.path().join(name);
    HistoryDb::open(&db_path)
}

fn store_test_result(
    db: &HistoryDb,
    invocation_id: i64,
    test_name: &str,
    package: &str,
    status: TestStatus,
) -> Result<()> {
    db.store_test_results(
        invocation_id,
        &[StoredTestResult {
            test_name: test_name.to_string(),
            package: package.to_string(),
            status,
            duration_secs: Some(0.1),
            attempt: 1,
            output: None,
        }],
    )
    .map(|_| ())
}

fn cost_row(
    id: i64,
    command: &str,
    started_after_secs: i64,
    duration_secs: f64,
    args_json: Option<&str>,
) -> CostInvocationRow {
    let started_at = time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(started_after_secs);
    let finished_at = started_at + time::Duration::seconds_f64(duration_secs);

    CostInvocationRow {
        id,
        command: command.to_string(),
        args_json: args_json.map(ToOwned::to_owned),
        started_at,
        finished_at,
        duration_secs: Some(duration_secs),
        status: "success".to_string(),
        cancel_reason: None,
        is_background: false,
        tree_fingerprint: None,
        scope_key: None,
        is_stale_cleanup: false,
    }
}

#[sinex_test]
async fn test_history_cost_summary_separates_wrappers_overlap_and_stages()
-> ::xtask::sandbox::TestResult<()> {
    let rows = vec![
        cost_row(1, "test", 0, 120.0, None),
        cost_row(
            2,
            "test",
            10,
            100.0,
            Some(r#"["--scope=packages:sinex-db"]"#),
        ),
        cost_row(3, "check", 60, 120.0, Some(r#"["-p","sinex-db"]"#)),
    ];

    let summary = build_history_cost_summary(7, vec!["check".into(), "test".into()], &rows, 90.0);

    assert_eq!(summary.invocation_count, 3);
    assert_eq!(summary.raw_invocation_hours, 340.0 / 3600.0);
    assert_eq!(summary.wrapper_invocation_hours, 120.0 / 3600.0);
    assert_eq!(summary.wrapper_wait_hours, 120.0 / 3600.0);
    assert_eq!(summary.non_wrapper_invocation_hours, 220.0 / 3600.0);
    assert_eq!(summary.unique_wall_hours, 180.0 / 3600.0);
    assert_eq!(
        summary.overlap_after_wrapper_adjustment_hours,
        40.0 / 3600.0
    );
    assert_eq!(summary.stage_hours, 90.0 / 3600.0);
    assert_eq!(summary.stage_unaccounted_hours, 130.0 / 3600.0);
    Ok(())
}

#[sinex_test]
async fn test_history_cost_summary_reports_cancelled_and_stale_pid_buckets()
-> ::xtask::sandbox::TestResult<()> {
    let mut cancelled = cost_row(1, "test", 0, 30.0, Some(r#"["-p","sinex-db"]"#));
    cancelled.status = "cancelled".to_string();
    cancelled.cancel_reason = Some("user_cancel".to_string());

    let mut background_cancelled = cost_row(2, "test", 40, 40.0, Some(r#"["-p","sinex-db"]"#));
    background_cancelled.status = "cancelled".to_string();
    background_cancelled.is_background = true;

    let mut stale = cost_row(3, "test", 90, 50.0, None);
    stale.status = "cancelled".to_string();
    stale.cancel_reason = Some("stale_pid".to_string());
    stale.is_stale_cleanup = true;

    let rows = vec![cancelled, background_cancelled, stale];
    let summary = build_history_cost_summary(7, vec!["test".into()], &rows, 0.0);

    assert_eq!(summary.invocation_count, 2);
    assert_eq!(summary.stale_cleanup_rows_excluded, 1);
    assert_eq!(summary.cancelled_foreground_hours, 30.0 / 3600.0);
    assert_eq!(summary.stale_pid_rows, 1);
    assert_eq!(summary.stale_pid_hours, 50.0 / 3600.0);
    Ok(())
}

#[sinex_test]
async fn test_history_cost_summary_ranks_repeated_successful_proofs()
-> ::xtask::sandbox::TestResult<()> {
    let mut first = cost_row(1, "check", 0, 10.0, Some(r#"["-p","sinex-db"]"#));
    first.tree_fingerprint = Some("fingerprint-a".to_string());
    first.scope_key = Some("scope-a".to_string());

    let mut second = cost_row(2, "check", 20, 20.0, Some(r#"["-p","sinex-db"]"#));
    second.tree_fingerprint = Some("fingerprint-a".to_string());
    second.scope_key = Some("scope-a".to_string());

    let mut third = cost_row(3, "check", 50, 30.0, Some(r#"["-p","sinex-db"]"#));
    third.tree_fingerprint = Some("fingerprint-a".to_string());
    third.scope_key = Some("scope-a".to_string());

    let mut failed_repeat = cost_row(4, "check", 90, 40.0, Some(r#"["-p","sinex-db"]"#));
    failed_repeat.status = "failed".to_string();
    failed_repeat.tree_fingerprint = Some("fingerprint-a".to_string());
    failed_repeat.scope_key = Some("scope-a".to_string());

    let rows = vec![first, second, third, failed_repeat];
    let summary = build_history_cost_summary(7, vec!["check".into()], &rows, 0.0);

    assert_eq!(summary.repeated_proof_hours, 50.0 / 3600.0);
    assert_eq!(summary.repeated_proof_candidates.len(), 1);
    let candidate = &summary.repeated_proof_candidates[0];
    assert_eq!(candidate.command, "check");
    assert_eq!(candidate.run_count, 3);
    assert_eq!(candidate.repeated_invocation_count, 2);
    assert_eq!(candidate.invocation_ids, vec![1, 2, 3]);
    assert_eq!(candidate.repeated_hours, 50.0 / 3600.0);
    Ok(())
}

#[sinex_test]
async fn test_wrapper_events_path_lives_next_to_history_db() -> ::xtask::sandbox::TestResult<()> {
    let path = wrapper_events_path(Path::new("/tmp/sinex-state/xtask-history.db"));
    assert_eq!(
        path,
        Path::new("/tmp/sinex-state/xtask-wrapper-events.jsonl")
    );
    Ok(())
}

#[sinex_test]
async fn test_read_wrapper_events_filters_and_skips_malformed_lines()
-> ::xtask::sandbox::TestResult<()> {
    let dir = tempdir()?;
    let path = dir.path().join("xtask-wrapper-events.jsonl");
    fs::write(
        &path,
        concat!(
            "{\"event\":\"checkout-local-rebuild\",\"status\":\"success\",",
            "\"started_at\":\"2026-06-06T10:00:00Z\",",
            "\"finished_at\":\"2026-06-06T10:00:02Z\",",
            "\"duration_ms\":2500,\"command\":\"docs\",",
            "\"force_rebuild\":true,",
            "\"rebuild_trigger\":{\"reason\":\"forced\",\"ref_path\":\"/tmp/target/debug/xtask\",",
            "\"inputs\":[{\"path\":\"/repo/flake.nix\",\"rel_path\":\"flake.nix\",",
            "\"kind\":\"extra\",\"status\":\"newer\",\"mtime_epoch\":1770000000}]},",
            "\"stage_durations_ms\":{\"initdb\":100,\"xtask_build\":2000}}\n",
            "not-json\n",
            "{\"event\":\"checkout-local-rebuild\",\"status\":\"success\",",
            "\"started_at\":\"2026-06-05T10:00:00Z\",",
            "\"duration_ms\":1000}\n",
        ),
    )?;

    let cutoff = time::OffsetDateTime::parse(
        "2026-06-06T00:00:00Z",
        &time::format_description::well_known::Rfc3339,
    )?;
    let (events, skipped_lines) = read_wrapper_events(&path, cutoff)?;

    assert_eq!(events.len(), 1);
    assert_eq!(skipped_lines, 1);
    assert_eq!(events[0].event, "checkout-local-rebuild");
    assert_eq!(events[0].status, "success");
    assert_eq!(events[0].duration_secs, Some(2.5));
    assert_eq!(events[0].command.as_deref(), Some("docs"));
    assert!(events[0].force_rebuild);
    assert_eq!(
        events[0]
            .rebuild_trigger
            .as_ref()
            .map(|trigger| trigger.reason.as_str()),
        Some("forced")
    );
    assert_eq!(wrapper_trigger_summary(&events[0]), "forced: flake.nix");
    assert_eq!(
        events[0]
            .top_stage
            .as_ref()
            .map(|stage| stage.name.as_str()),
        Some("xtask_build")
    );
    assert_eq!(
        events[0]
            .top_stage
            .as_ref()
            .map(|stage| stage.duration_secs),
        Some(2.0)
    );
    Ok(())
}

#[sinex_test]
async fn test_wrapper_stage_totals_rank_by_duration() -> ::xtask::sandbox::TestResult<()> {
    let mut first = cost_wrapper_event(10.0);
    first.stage_durations_ms = BTreeMap::from([
        ("xtask_build".to_string(), 8_000),
        ("initdb".to_string(), 500),
    ]);
    let mut second = cost_wrapper_event(5.0);
    second.stage_durations_ms = BTreeMap::from([
        ("xtask_build".to_string(), 4_000),
        ("schema_apply".to_string(), 1_000),
    ]);

    let totals = wrapper_stage_totals(&[first, second], 15.0);

    assert_eq!(totals.len(), 3);
    assert_eq!(totals[0].name, "xtask_build");
    assert_eq!(totals[0].duration_secs, 12.0);
    assert_eq!(totals[0].pct_of_total, 80.0);
    assert_eq!(totals[1].name, "schema_apply");
    assert_eq!(totals[1].duration_secs, 1.0);
    assert_eq!(totals[2].name, "initdb");
    assert_eq!(totals[2].duration_secs, 0.5);
    Ok(())
}

#[sinex_test]
async fn test_wrapper_trigger_totals_rank_by_duration() -> ::xtask::sandbox::TestResult<()> {
    let mut first = cost_wrapper_event(10.0);
    first.rebuild_trigger = Some(WrapperRebuildTrigger {
        reason: "sources_newer".to_string(),
        ref_path: Some("/tmp/target/debug/xtask".to_string()),
        inputs: Vec::new(),
    });
    let mut second = cost_wrapper_event(5.0);
    second.rebuild_trigger = Some(WrapperRebuildTrigger {
        reason: "forced".to_string(),
        ref_path: None,
        inputs: Vec::new(),
    });
    let mut third = cost_wrapper_event(2.0);
    third.rebuild_trigger = Some(WrapperRebuildTrigger {
        reason: "sources_newer".to_string(),
        ref_path: Some("/tmp/target/debug/xtask".to_string()),
        inputs: Vec::new(),
    });

    let totals = wrapper_trigger_totals(&[first, second, third]);

    assert_eq!(totals.len(), 2);
    assert_eq!(totals[0].reason, "sources_newer");
    assert_eq!(totals[0].count, 2);
    assert_eq!(totals[0].duration_secs, 12.0);
    assert_eq!(totals[1].reason, "forced");
    assert_eq!(totals[1].count, 1);
    assert_eq!(totals[1].duration_secs, 5.0);
    Ok(())
}

fn cost_wrapper_event(duration_secs: f64) -> WrapperEvent {
    WrapperEvent {
        event: "checkout-local-rebuild".to_string(),
        status: "success".to_string(),
        started_at: "2026-06-06T10:00:00Z".to_string(),
        finished_at: Some("2026-06-06T10:00:10Z".to_string()),
        duration_secs: Some(duration_secs),
        command: Some("history".to_string()),
        args: None,
        force_rebuild: false,
        log_path: None,
        rebuild_trigger: None,
        stage_durations_ms: BTreeMap::new(),
        top_stage: None,
    }
}

fn invocation_resource_metrics(
    io_full: f64,
    memory_full: f64,
    read_mib: f64,
    write_mib: f64,
    device: &str,
    device_total_mib: f64,
    process_count: u32,
) -> crate::process::InvocationResourceMetrics {
    crate::process::InvocationResourceMetrics {
        process_tree: crate::process::ProcessTreeMetrics {
            cpu_usage_avg: Some(10.0),
            memory_usage_max_mb: Some(512.0),
            root_cpu_usage_avg: Some(2.0),
            root_memory_usage_max_mb: Some(128.0),
            process_count_max: Some(process_count),
            sample_count: 4,
        },
        shared_build: crate::process::SharedBuildMetrics::default(),
        host_pressure: crate::process::HostPressureMetrics {
            io_full_avg10_max: Some(io_full),
            memory_full_avg10_max: Some(memory_full),
            ..Default::default()
        },
        host_block_io: crate::process::HostBlockIoMetrics {
            read_mib_delta: Some(read_mib),
            write_mib_delta: Some(write_mib),
            read_iops_avg: Some(30.0),
            write_iops_avg: Some(12.0),
            busiest_device: Some(device.to_string()),
            busiest_device_total_mib_delta: Some(device_total_mib),
            busiest_device_read_iops_avg: Some(20.0),
            busiest_device_write_iops_avg: Some(8.0),
            busiest_device_weighted_io_ms_per_s: Some(100.0),
        },
    }
}

#[sinex_test]
async fn test_execute_resources_summarizes_commands_and_devices() -> ::xtask::sandbox::TestResult<()>
{
    let db = seeded_history_db("resources-summary.db")?;
    let ctx = silent_ctx();

    let check_id = db.start_invocation("check", None, None, None)?;
    db.record_resource_metrics(
        check_id,
        &invocation_resource_metrics(12.0, 3.0, 100.0, 20.0, "nvme0n1", 120.0, 9),
    )?;
    db.finish_invocation(check_id, InvocationStatus::Success, Some(0), 10.0)?;

    let test_id = db.start_invocation("test", None, None, None)?;
    db.record_resource_metrics(
        test_id,
        &invocation_resource_metrics(64.0, 20.0, 200.0, 30.0, "nvme0n1", 230.0, 28),
    )?;
    db.finish_invocation(test_id, InvocationStatus::Failed, Some(1), 20.0)?;

    let result = execute_resources(&db, None, 1, &[], 10, false, false, &ctx)?;
    let data = result.data.expect("resource report data should be present");
    let rows = data
        .get("rows")
        .and_then(serde_json::Value::as_array)
        .expect("resource command summaries should be present");
    let test_summary = rows
        .iter()
        .find(|row| row.get("command").and_then(serde_json::Value::as_str) == Some("test"))
        .expect("test command summary should exist");

    assert_eq!(
        data.get("invocation_count")
            .and_then(serde_json::Value::as_u64),
        Some(2)
    );
    assert_eq!(
        test_summary
            .get("failed_count")
            .and_then(serde_json::Value::as_u64),
        Some(1)
    );
    assert_eq!(
        test_summary
            .get("host_block_read_mib")
            .and_then(serde_json::Value::as_f64),
        Some(200.0)
    );

    let devices = data
        .get("top_devices")
        .and_then(serde_json::Value::as_array)
        .expect("top devices should be present");
    assert_eq!(
        devices[0].get("device").and_then(serde_json::Value::as_str),
        Some("nvme0n1")
    );
    assert_eq!(
        devices[0]
            .get("total_mib")
            .and_then(serde_json::Value::as_f64),
        Some(350.0)
    );
    Ok(())
}

#[sinex_test]
async fn test_history_command_metadata() -> ::xtask::sandbox::TestResult<()> {
    let cmd = HistoryCommand {
        subcommand: HistorySubcommand::List {
            limit: 10,
            command: None,
            first: false,
            no_limit: false,
            offset: 0,
            after_invocation: None,
            before_invocation: None,
            sort_by: "started".to_string(),
            since: None,
            with_diagnostics: false,
            with_stages: false,
            with_tests: false,
            include_zombies: false,
        },
    };

    let metadata = cmd.metadata();
    assert_eq!(metadata.category, Some("diagnostics"));
    assert!(metadata.timeout.is_some());
    assert!(!metadata.modifies_state); // History commands are read-only
    Ok(())
}

#[sinex_test]
async fn test_history_command_name() -> ::xtask::sandbox::TestResult<()> {
    let cmd = HistoryCommand {
        subcommand: HistorySubcommand::Stats {
            command: Some("test".to_string()),
            days: 7,
            package: None,
            all_packages: false,
            all_commands: false,
        },
    };

    assert_eq!(cmd.name(), "history");
    Ok(())
}

#[sinex_test]
async fn test_apply_diagnostic_filters_honors_all_fields() -> ::xtask::sandbox::TestResult<()> {
    let mut diagnostics = vec![
        sample_diagnostic(
            "warning",
            Some("crate/sinex-db/src/lib.rs"),
            Some("sinex-db"),
            Some("W001"),
            true,
            Some("check"),
        ),
        sample_diagnostic(
            "warning",
            Some("crate/sinexctl/src/main.rs"),
            Some("sinexctl"),
            Some("W001"),
            true,
            Some("check"),
        ),
        sample_diagnostic(
            "error",
            Some("crate/sinex-db/src/lib.rs"),
            Some("sinex-db"),
            Some("E001"),
            false,
            Some("build"),
        ),
    ];

    apply_diagnostic_filters(
        &mut diagnostics,
        DiagnosticFilter::new(
            Some("warning"),
            Some("sinex-db/src"),
            Some("check"),
            Some("sinex-db"),
            Some("W001"),
            true,
        ),
    );

    assert_eq!(diagnostics.len(), 1);
    let diagnostic = &diagnostics[0];
    assert_eq!(diagnostic.package.as_deref(), Some("sinex-db"));
    assert_eq!(diagnostic.code.as_deref(), Some("W001"));
    assert_eq!(diagnostic.source_command.as_deref(), Some("check"));
    assert_eq!(
        diagnostic.fix_applicability.as_deref(),
        Some("MachineApplicable")
    );
    Ok(())
}

#[sinex_test]
async fn test_diagnostic_source_command_counts_are_sorted_and_explicit()
-> ::xtask::sandbox::TestResult<()> {
    let diagnostics = vec![
        sample_diagnostic("warning", None, None, None, false, Some("lint")),
        sample_diagnostic("warning", None, None, None, false, Some("check")),
        sample_diagnostic("warning", None, None, None, false, Some("check")),
        sample_diagnostic("warning", None, None, None, false, None),
    ];

    assert_eq!(
        diagnostic_source_command_counts(&diagnostics),
        vec![
            ("check".to_string(), 2),
            ("lint".to_string(), 1),
            ("unknown".to_string(), 1),
        ]
    );
    assert_eq!(
        format_diagnostic_source_command_counts(&diagnostics),
        "check: 2, lint: 1, unknown: 1"
    );
    Ok(())
}

#[sinex_test]
async fn test_execute_diagnostics_invocation_applies_package_code_and_fixable_filters()
-> ::xtask::sandbox::TestResult<()> {
    let db = seeded_history_db("diag-invocation.db")?;
    let ctx = silent_ctx();

    let inv_id = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 1.0)?;

    db.record_diagnostic(
        inv_id,
        &CompilerDiagnostic {
            level: "warning".into(),
            code: Some("W001".into()),
            message: "target".into(),
            package: Some("sinex-db".into()),
            file_path: Some("crate/sinex-db/src/lib.rs".into()),
            fix_applicability: Some("MachineApplicable".into()),
            ..Default::default()
        },
    )?;
    db.record_diagnostic(
        inv_id,
        &CompilerDiagnostic {
            level: "warning".into(),
            code: Some("W001".into()),
            message: "other package".into(),
            package: Some("sinexctl".into()),
            file_path: Some("crate/sinexctl/src/main.rs".into()),
            fix_applicability: Some("MachineApplicable".into()),
            ..Default::default()
        },
    )?;
    db.record_diagnostic(
        inv_id,
        &CompilerDiagnostic {
            level: "warning".into(),
            code: Some("W001".into()),
            message: "not fixable".into(),
            package: Some("sinex-db".into()),
            file_path: Some("crate/sinex-db/src/state.rs".into()),
            ..Default::default()
        },
    )?;

    let result = execute_diagnostics_invocation(
        &db,
        "latest",
        Some("check"),
        Some("warning"),
        Some("sinex-db/src"),
        Some("sinex-db"),
        true,
        Some("W001"),
        &DiagnosticsFormat::Table,
        &ctx,
    )?;

    assert_eq!(
        result.message.as_deref(),
        Some("Found 1 diagnostics from invocation")
    );
    Ok(())
}

#[sinex_test]
async fn test_execute_tests_analyze_honors_explicit_invocation() -> ::xtask::sandbox::TestResult<()>
{
    let db = seeded_history_db("tests-analyze-explicit.db")?;
    let ctx = silent_ctx();

    let older = db.start_invocation("test", None, None, None)?;
    db.finish_invocation(older, InvocationStatus::Success, Some(0), 1.0)?;
    store_test_result(&db, older, "older_pass", "pkg-a", TestStatus::Pass)?;

    let newer = db.start_invocation("test", None, None, None)?;
    db.finish_invocation(newer, InvocationStatus::Success, Some(0), 2.0)?;
    store_test_result(&db, newer, "newer_fail", "pkg-b", TestStatus::Fail)?;

    let result = execute_tests_analyze(&db, &older.to_string(), &ctx)?;
    let data = result.data.expect("analysis data should be present");
    let invocation_id = data
        .get("invocation_id")
        .and_then(serde_json::Value::as_i64)
        .expect("analysis invocation id should be present");
    let passed = data
        .get("total_passed")
        .and_then(serde_json::Value::as_u64)
        .expect("analysis passed count should be present");
    let failed = data
        .get("total_failed")
        .and_then(serde_json::Value::as_u64)
        .expect("analysis failed count should be present");

    assert_eq!(invocation_id, older);
    assert_eq!(passed, 1);
    assert_eq!(failed, 0);
    Ok(())
}

#[sinex_test]
async fn test_execute_tests_analyze_accepts_background_job_selector()
-> ::xtask::sandbox::TestResult<()> {
    let db = seeded_history_db("tests-analyze-job.db")?;
    let ctx = silent_ctx();

    let (invocation_id, job_id) = db.start_background_job(
        "test",
        &[],
        None,
        std::path::Path::new(""),
        std::path::Path::new(""),
    )?;
    db.finish_invocation(invocation_id, InvocationStatus::Success, Some(0), 1.0)?;
    store_test_result(&db, invocation_id, "job_pass", "pkg-job", TestStatus::Pass)?;

    let result = execute_tests_analyze(&db, &format!("job:{job_id}"), &ctx)?;
    let data = result.data.expect("analysis data should be present");
    let resolved_invocation_id = data
        .get("invocation_id")
        .and_then(serde_json::Value::as_i64)
        .expect("analysis invocation id should be present");
    let expected_message =
        format!("Analysis for invocation #{invocation_id} (job #{job_id}): 1 passed, 0 failed");

    assert_eq!(resolved_invocation_id, invocation_id);
    assert_eq!(result.message.as_deref(), Some(expected_message.as_str()));
    Ok(())
}

#[sinex_test]
async fn test_execute_progress_defaults_to_current_selector() -> ::xtask::sandbox::TestResult<()> {
    let db = seeded_history_db("progress-current-selector.db")?;
    let ctx = silent_ctx();

    let older = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(older, InvocationStatus::Success, Some(0), 1.0)?;

    let stdout = std::path::Path::new("");
    let stderr = std::path::Path::new("");
    let (running_invocation, _job_id) =
        db.start_background_job("test", &[], None, stdout, stderr)?;
    db.write_progress(
        running_invocation,
        Some("tests"),
        Some("compiling targeted crates"),
        Some(12.5),
        Some(5),
        Some(40),
    )?;

    let result = execute_progress(&db, None, &ctx)?;
    let expected_message = format!("Progress for invocation #{running_invocation}");

    assert_eq!(result.message.as_deref(), Some(expected_message.as_str()));
    Ok(())
}

#[sinex_test]
async fn test_execute_tests_slowest_accepts_explicit_invocation() -> ::xtask::sandbox::TestResult<()>
{
    let db = seeded_history_db("tests-slowest-explicit.db")?;
    let ctx = silent_ctx();

    let older = db.start_invocation("test", None, None, None)?;
    db.finish_invocation(older, InvocationStatus::Success, Some(0), 4.0)?;
    db.store_test_results(
        older,
        &[
            StoredTestResult {
                test_name: "older_slowest".into(),
                package: "pkg-a".into(),
                status: TestStatus::Fail,
                duration_secs: Some(4.0),
                attempt: 1,
                output: Some("boom".into()),
            },
            StoredTestResult {
                test_name: "older_fast".into(),
                package: "pkg-a".into(),
                status: TestStatus::Pass,
                duration_secs: Some(0.2),
                attempt: 1,
                output: None,
            },
        ],
    )?;

    let newer = db.start_invocation("test", None, None, None)?;
    db.finish_invocation(newer, InvocationStatus::Success, Some(0), 20.0)?;
    db.store_test_results(
        newer,
        &[StoredTestResult {
            test_name: "newer_only".into(),
            package: "pkg-b".into(),
            status: TestStatus::Pass,
            duration_secs: Some(20.0),
            attempt: 1,
            output: None,
        }],
    )?;

    let result = execute_tests_slowest(&db, Some(&older.to_string()), 10, None, 1, false, &ctx)?;
    let data = result.data.expect("slowest test data should be present");
    let tests = data
        .as_array()
        .expect("run-scoped slowest data should be an array");
    let expected_message = format!("Found 2 slowest tests for invocation #{older}");

    assert_eq!(tests.len(), 2);
    assert_eq!(
        tests[0]
            .get("test_name")
            .and_then(serde_json::Value::as_str),
        Some("older_slowest")
    );
    assert_eq!(
        tests[0].get("status").and_then(serde_json::Value::as_str),
        Some("fail")
    );
    assert_eq!(result.message.as_deref(), Some(expected_message.as_str()));
    Ok(())
}

#[sinex_test]
async fn test_execute_tests_slowest_classifies_optimization_candidates()
-> ::xtask::sandbox::TestResult<()> {
    let db = seeded_history_db("tests-slowest-classification.db")?;
    let ctx = silent_ctx();

    let invocation = db.start_invocation("test", None, None, None)?;
    db.finish_invocation(invocation, InvocationStatus::Success, Some(0), 14.0)?;
    db.store_test_results(
        invocation,
        &[
            StoredTestResult {
                test_name:
                    "xtask::xtask$command_catalog::tests::command_catalog_exposes_core_public_surface"
                        .into(),
                package: "xtask".into(),
                status: TestStatus::Pass,
                duration_secs: Some(9.0),
                attempt: 1,
                output: None,
            },
            StoredTestResult {
                test_name: "sinexd::settlement_fault_injection$opens_circuit".into(),
                package: "sinexd".into(),
                status: TestStatus::Pass,
                duration_secs: Some(4.0),
                attempt: 1,
                output: None,
            },
            StoredTestResult {
                test_name: "sinexd::replay_control::tests::source_runtime_never_reports_completion"
                    .into(),
                package: "sinexd".into(),
                status: TestStatus::Pass,
                duration_secs: Some(2.0),
                attempt: 1,
                output: None,
            },
        ],
    )?;

    let result = execute_tests_slowest(&db, None, 10, None, 1, false, &ctx)?;
    let data = result.data.expect("slowest test data should be present");
    let tests = data
        .as_array()
        .expect("aggregate slowest data should be an array");

    assert_eq!(
        tests.len(),
        1,
        "only setup-overhead candidates are emitted above the default threshold"
    );
    assert_eq!(
        tests[0]
            .get("optimization_kind")
            .and_then(serde_json::Value::as_str),
        Some("setup_overhead_candidate")
    );
    assert!(
        tests[0]
            .get("recommendation")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| value.contains("setup"))
    );
    Ok(())
}

#[sinex_test]
async fn test_execute_diagnostics_delta_respects_command_and_filters()
-> ::xtask::sandbox::TestResult<()> {
    let db = seeded_history_db("diag-delta.db")?;
    let ctx = silent_ctx();

    let build_old = db.start_invocation("build", None, None, None)?;
    db.finish_invocation(build_old, InvocationStatus::Success, Some(0), 1.0)?;
    db.record_diagnostic(
        build_old,
        &CompilerDiagnostic {
            level: "warning".into(),
            code: Some("W001".into()),
            message: "persistent".into(),
            package: Some("sinex-db".into()),
            file_path: Some("crate/sinex-db/src/lib.rs".into()),
            fix_applicability: Some("MachineApplicable".into()),
            ..Default::default()
        },
    )?;

    let build_new = db.start_invocation("build", None, None, None)?;
    db.finish_invocation(build_new, InvocationStatus::Failed, Some(1), 1.0)?;
    db.record_diagnostic(
        build_new,
        &CompilerDiagnostic {
            level: "warning".into(),
            code: Some("W001".into()),
            message: "persistent".into(),
            package: Some("sinex-db".into()),
            file_path: Some("crate/sinex-db/src/lib.rs".into()),
            fix_applicability: Some("MachineApplicable".into()),
            ..Default::default()
        },
    )?;
    db.record_diagnostic(
        build_new,
        &CompilerDiagnostic {
            level: "warning".into(),
            code: Some("W002".into()),
            message: "new build-only".into(),
            package: Some("sinex-db".into()),
            file_path: Some("crate/sinex-db/src/state.rs".into()),
            fix_applicability: Some("MachineApplicable".into()),
            ..Default::default()
        },
    )?;

    let check_new = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(check_new, InvocationStatus::Failed, Some(1), 1.0)?;
    db.record_diagnostic(
        check_new,
        &CompilerDiagnostic {
            level: "warning".into(),
            code: Some("W999".into()),
            message: "check-only".into(),
            package: Some("sinexctl".into()),
            file_path: Some("crate/sinexctl/src/lib.rs".into()),
            fix_applicability: Some("MachineApplicable".into()),
            ..Default::default()
        },
    )?;

    let result = execute_diagnostics_delta(
        &db,
        None,
        None,
        Some("warning"),
        Some("sinex-db/src"),
        Some("build"),
        Some("sinex-db"),
        true,
        Some("W002"),
        &DiagnosticsFormat::Table,
        &ctx,
    )?;

    assert_eq!(
        result.message.as_deref(),
        Some("Delta: 1 new, 0 resolved, 0 persistent")
    );
    Ok(())
}

#[sinex_test]
async fn test_execute_diagnostics_by_code_respects_file_and_fixable()
-> ::xtask::sandbox::TestResult<()> {
    let db = seeded_history_db("diag-by-code.db")?;
    let ctx = silent_ctx();

    let inv_id = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 1.0)?;
    db.record_compiled_packages(
        inv_id,
        &HashSet::from(["sinex-db".to_string(), "sinexctl".to_string()]),
    )?;

    db.record_diagnostic(
        inv_id,
        &CompilerDiagnostic {
            level: "warning".into(),
            code: Some("W001".into()),
            message: "target".into(),
            package: Some("sinex-db".into()),
            file_path: Some("crate/sinex-db/src/lib.rs".into()),
            fix_applicability: Some("MachineApplicable".into()),
            ..Default::default()
        },
    )?;
    db.record_diagnostic(
        inv_id,
        &CompilerDiagnostic {
            level: "warning".into(),
            code: Some("W002".into()),
            message: "other path".into(),
            package: Some("sinex-db".into()),
            file_path: Some("crate/sinex-db/tests/lib.rs".into()),
            fix_applicability: Some("MachineApplicable".into()),
            ..Default::default()
        },
    )?;
    db.record_diagnostic(
        inv_id,
        &CompilerDiagnostic {
            level: "warning".into(),
            code: Some("W001".into()),
            message: "not fixable".into(),
            package: Some("sinexctl".into()),
            file_path: Some("crate/sinexctl/src/lib.rs".into()),
            ..Default::default()
        },
    )?;

    let result = execute_diagnostics_by_code(
        &db,
        Some("warning"),
        Some("sinex-db/src"),
        Some("check"),
        Some("sinex-db"),
        true,
        Some("W001"),
        &ctx,
    )?;

    assert_eq!(result.message.as_deref(), Some("1 unique codes"));
    Ok(())
}

// ────────────────────────────────────────────────────────────────────────
// Property tests — parse_duration_secs and apply_diagnostic_filters
// ────────────────────────────────────────────────────────────────────────

use crate::sandbox::sinex_proptest;
use proptest::prelude::*;

sinex_proptest! {
    /// Larger numeric values with the same unit parse to larger durations (monotonicity).
    ///
    /// This verifies that `parse_duration_secs` acts as a monotone function
    /// within each unit: "30m" > "10m", "2h" > "1h", etc. Violated monotonicity
    /// would cause --since time windows to behave non-intuitively.
    ///
    /// Generates `a = base + delta` (so `a > b = base` by construction), avoiding
    /// prop_assume!-based rejection which causes Reject failures at high rates.
    fn prop_parse_duration_monotonic_within_unit(
        base  in 1i64..=5_000i64,
        delta in 1i64..=5_000i64,
        unit  in prop_oneof![Just('s'), Just('m'), Just('h'), Just('d')]
    ) -> TestResult<()> {
        let a = base + delta;   // a > base = b by construction, no prop_assume! needed
        let b = base;
        let da = parse_duration_secs(&format!("{a}{unit}"))
            .expect("valid format must parse");
        let db = parse_duration_secs(&format!("{b}{unit}"))
            .expect("valid format must parse");
        prop_assert!(da > db, "{a}{unit} must parse to more seconds than {b}{unit}");
        Ok(())
    }

    /// Larger units always produce longer durations for the same multiplier.
    ///
    /// For any positive n: n days > n hours, and n hours > n minutes, etc.
    /// Uses big/small unit partitions that are always ordered (d/h > m/s).
    fn prop_parse_duration_larger_unit_always_bigger(
        n in 1i64..=100i64,
        big_unit   in prop_oneof![Just('d'), Just('h')],
        small_unit in prop_oneof![Just('m'), Just('s')]
    ) -> TestResult<()> {
        let big   = parse_duration_secs(&format!("{n}{big_unit}"))
            .expect("big unit should parse");
        let small = parse_duration_secs(&format!("{n}{small_unit}"))
            .expect("small unit should parse");
        prop_assert!(
            big > small,
            "{n}{big_unit} ({big}s) must be longer than {n}{small_unit} ({small}s)"
        );
        Ok(())
    }

    /// Unknown suffixes return None — no silent misparse.
    ///
    /// The parser must return None for any suffix outside {s, m, h, d}.
    /// Silently parsing an unknown suffix (e.g. treating "100w" as 100)
    /// would corrupt --since time windows.
    fn prop_parse_duration_unknown_suffix_returns_none(
        n in 1i64..=1000i64,
        suffix in prop_oneof![
            Just('x'), Just('y'), Just('z'), Just('w'), Just('q'),
            Just('p'), Just('k'), Just('n'),
        ]
    ) -> TestResult<()> {
        let result = parse_duration_secs(&format!("{n}{suffix}"));
        prop_assert!(
            result.is_none(),
            "suffix '{}' must return None, got {:?}", suffix, result
        );
        Ok(())
    }

    /// All valid formats parse to a positive number of seconds.
    fn prop_parse_duration_valid_inputs_are_positive(
        n in 1i64..=1000i64,
        unit in prop_oneof![Just('s'), Just('m'), Just('h'), Just('d')]
    ) -> TestResult<()> {
        let result = parse_duration_secs(&format!("{n}{unit}"));
        prop_assert!(result.is_some(), "{n}{unit} must parse to Some");
        prop_assert!(result.unwrap() > 0, "parsed duration must be positive");
        Ok(())
    }

    /// Level filter retains exactly the matching diagnostics and drops the rest.
    ///
    /// This verifies AND semantics for the level predicate: every retained
    /// diagnostic must have the exact requested level, and all non-matching
    /// diagnostics are removed.
    fn prop_diagnostic_filter_level_and_semantics(
        matching_count   in 1usize..=8usize,
        unmatching_count in 0usize..=8usize
    ) -> TestResult<()> {
        let target  = "error";
        let other   = "warning";

        let mut diagnostics: Vec<crate::history::StoredDiagnostic> = Vec::new();
        for _ in 0..matching_count {
            diagnostics.push(sample_diagnostic(target, None, None, None, false, None));
        }
        for _ in 0..unmatching_count {
            diagnostics.push(sample_diagnostic(other, None, None, None, false, None));
        }

        apply_diagnostic_filters(
            &mut diagnostics,
            DiagnosticFilter::new(Some(target), None, None, None, None, false),
        );

        prop_assert_eq!(
            diagnostics.len(), matching_count,
            "should retain exactly {} matching-level entries", matching_count
        );
        for d in &diagnostics {
            prop_assert_eq!(&d.level, target, "all retained entries must match level");
        }
        Ok(())
    }

    /// Package filter retains exactly the matching diagnostics.
    fn prop_diagnostic_filter_package_and_semantics(
        count in 1usize..=8usize
    ) -> TestResult<()> {
        let target_pkg = "sinex-db";
        let other_pkg  = "sinex-primitives";

        let mut diagnostics: Vec<crate::history::StoredDiagnostic> = Vec::new();
        for _ in 0..count {
            diagnostics.push(sample_diagnostic("warning", None, Some(target_pkg), None, false, None));
            diagnostics.push(sample_diagnostic("warning", None, Some(other_pkg),  None, false, None));
        }

        apply_diagnostic_filters(
            &mut diagnostics,
            DiagnosticFilter::new(None, None, None, Some(target_pkg), None, false),
        );

        prop_assert_eq!(
            diagnostics.len(), count,
            "should retain exactly {} entries for package '{}'", count, target_pkg
        );
        for d in &diagnostics {
            prop_assert_eq!(
                d.package.as_deref(), Some(target_pkg),
                "all retained entries must match package"
            );
        }
        Ok(())
    }

    /// Combined level + package filters use AND logic, not OR.
    ///
    /// Only entries that satisfy BOTH predicates are retained. This rules out
    /// an accidental OR implementation where either match would be sufficient.
    fn prop_diagnostic_filter_combined_and_semantics(
        extra_matches in 0usize..=6usize
    ) -> TestResult<()> {
        let target_level = "error";
        let target_pkg   = "sinex-db";

        // Four categories: match both, match level only, match pkg only, match neither
        let mut diagnostics = vec![
            sample_diagnostic(target_level, None, Some(target_pkg), None, false, None), // MATCH BOTH
            sample_diagnostic(target_level, None, Some("sinex-primitives"), None, false, None), // level only
            sample_diagnostic("warning",    None, Some(target_pkg), None, false, None), // pkg only
            sample_diagnostic("warning",    None, Some("sinex-primitives"), None, false, None), // neither
        ];
        // Add extra_matches fully-matching entries to parameterize the expected count
        for _ in 0..extra_matches {
            diagnostics.push(sample_diagnostic(target_level, None, Some(target_pkg), None, false, None));
        }

        apply_diagnostic_filters(
            &mut diagnostics,
            DiagnosticFilter::new(Some(target_level), None, None, Some(target_pkg), None, false),
        );

        let expected = 1 + extra_matches;
        prop_assert_eq!(
            diagnostics.len(), expected,
            "combined AND filter must retain exactly {} entries", expected
        );
        for d in &diagnostics {
            prop_assert_eq!(&d.level, target_level, "retained entry must match level");
            prop_assert_eq!(
                d.package.as_deref(), Some(target_pkg),
                "retained entry must match package"
            );
        }
        Ok(())
    }
}

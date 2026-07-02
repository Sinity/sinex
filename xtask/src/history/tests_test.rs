
use super::super::db::StagePressure;
use super::*;
use std::collections::HashMap;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn test_parse_nextest_output() -> TestResult<()> {
    let output = r#"
{"type":"suite","event":"started","test_count":2,"nextest":{"crate":"mypackage"}}
{"type":"test","event":"started","name":"mypackage::mypackage$module::test_one"}
{"type":"test","event":"ok","name":"mypackage::mypackage$module::test_one","exec_time":0.001}
{"type":"test","event":"started","name":"mypackage::mypackage$module::test_two"}
{"type":"test","event":"failed","name":"mypackage::mypackage$module::test_two","exec_time":0.5,"stdout":"test output"}
{"type":"suite","event":"finished","passed":1,"failed":1}
"#;

    let results = parse_nextest_output(output);
    assert_eq!(results.len(), 2);

    assert_eq!(results[0].package, "mypackage");
    assert_eq!(results[0].test_name, "module::test_one");
    assert_eq!(results[0].status, TestStatus::Pass);
    assert!(results[0].duration_secs.unwrap() < 0.01);

    assert_eq!(results[1].package, "mypackage");
    assert_eq!(results[1].test_name, "module::test_two");
    assert_eq!(results[1].status, TestStatus::Fail);
    assert!(results[1].output.is_some());
    Ok(())
}

#[sinex_test]
async fn test_parse_test_name() -> TestResult<()> {
    let (pkg, name) = parse_test_name("xtask::xtask$bench::stats::tests::test_mean", "default");
    assert_eq!(pkg, "xtask");
    assert_eq!(name, "bench::stats::tests::test_mean");

    let (pkg, name) = parse_test_name("no_dollar_sign", "fallback");
    assert_eq!(pkg, "fallback");
    assert_eq!(name, "no_dollar_sign");
    Ok(())
}

#[sinex_test]
async fn test_status_as_str_roundtrip() -> TestResult<()> {
    // Verify as_str and from_str are consistent
    for status in [
        TestStatus::Pass,
        TestStatus::Fail,
        TestStatus::Skip,
        TestStatus::Flaky,
    ] {
        let s = status.as_str();
        let roundtripped = TestStatus::try_from_str(s)?;
        assert_eq!(roundtripped, status, "Roundtrip failed for {s}");
    }
    Ok(())
}

#[sinex_test]
async fn test_status_from_str_aliases() -> TestResult<()> {
    // "ok" is an alias for Pass (nextest output format)
    assert_eq!(TestStatus::try_from_str("ok")?, TestStatus::Pass);
    // "failed" is an alias for Fail (nextest output format)
    assert_eq!(TestStatus::try_from_str("failed")?, TestStatus::Fail);
    // "ignored" is an alias for Skip (nextest output format)
    assert_eq!(TestStatus::try_from_str("ignored")?, TestStatus::Skip);
    // Unknown values are rejected rather than silently coerced.
    assert!(TestStatus::try_from_str("unknown").is_err());
    Ok(())
}

/// Helper: create a fresh in-memory-like HistoryDb with a test invocation.
fn test_db_with_invocation() -> color_eyre::eyre::Result<(tempfile::TempDir, HistoryDb, i64)> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test-history.db");
    let db = HistoryDb::open(&db_path)?;
    let inv_id = db.start_invocation("test", None, None, None)?;
    db.finish_invocation(
        inv_id,
        super::super::db::InvocationStatus::Success,
        Some(0),
        5.0,
    )?;
    Ok((dir, db, inv_id))
}

#[sinex_test]
async fn test_resolve_test_run_skips_invocations_without_results() -> TestResult<()> {
    let (_dir, db, inv_with_results) = test_db_with_invocation()?;
    db.store_test_results(
        inv_with_results,
        &[TestResult {
            test_name: "test_alpha".into(),
            package: "pkg-a".into(),
            status: TestStatus::Pass,
            duration_secs: Some(0.1),
            attempt: 1,
            output: None,
        }],
    )?;

    let inv_without_results = db.start_invocation("test", None, None, None)?;
    db.finish_invocation(
        inv_without_results,
        super::super::db::InvocationStatus::Success,
        Some(0),
        1.0,
    )?;

    let resolved = db
        .resolve_test_run(None)?
        .expect("latest completed run with results should resolve");
    assert_eq!(resolved.invocation_id, inv_with_results);
    assert_eq!(resolved.job_id, None);

    let error = db
        .resolve_test_run(Some(&inv_without_results.to_string()))
        .expect_err("explicit invocation without results should fail");
    assert!(
        error.to_string().contains("has no stored test results"),
        "{error:#}"
    );
    Ok(())
}

#[sinex_test]
async fn test_resolve_test_run_accepts_background_job_selectors() -> TestResult<()> {
    let (_dir, db, _first_inv) = test_db_with_invocation()?;

    let (background_invocation, background_job) = db.start_background_job(
        "test",
        &[],
        None,
        std::path::Path::new(""),
        std::path::Path::new(""),
    )?;
    db.finish_invocation(
        background_invocation,
        super::super::db::InvocationStatus::Success,
        Some(0),
        1.0,
    )?;
    db.store_test_results(
        background_invocation,
        &[TestResult {
            test_name: "test_from_job".into(),
            package: "pkg-job".into(),
            status: TestStatus::Pass,
            duration_secs: Some(0.2),
            attempt: 1,
            output: None,
        }],
    )?;

    let resolved_from_prefix = db
        .resolve_test_run(Some(&format!("job:{background_job}")))?
        .expect("job selector should resolve");
    assert_eq!(resolved_from_prefix.invocation_id, background_invocation);
    assert_eq!(resolved_from_prefix.job_id, Some(background_job));

    let resolved_from_numeric = db
        .resolve_test_run(Some(&background_job.to_string()))?
        .expect("plain numeric selector should fall back to background job id");
    assert_eq!(resolved_from_numeric.invocation_id, background_invocation);
    assert_eq!(resolved_from_numeric.job_id, Some(background_job));

    Ok(())
}

#[sinex_test]
async fn test_recent_test_runs_lists_completed_runs_with_results_newest_first() -> TestResult<()> {
    let (_dir, db, first_inv) = test_db_with_invocation()?;
    db.store_test_results(
        first_inv,
        &[TestResult {
            test_name: "test_first".into(),
            package: "pkg-a".into(),
            status: TestStatus::Pass,
            duration_secs: Some(0.1),
            attempt: 1,
            output: None,
        }],
    )?;

    let without_results = db.start_invocation("test", None, None, None)?;
    db.finish_invocation(without_results, InvocationStatus::Success, Some(0), 1.0)?;

    let check_inv = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(check_inv, InvocationStatus::Success, Some(0), 1.0)?;

    let second_inv = db.start_invocation("test", None, None, None)?;
    db.finish_invocation(second_inv, InvocationStatus::Failed, Some(1), 1.0)?;
    db.store_test_results(
        second_inv,
        &[TestResult {
            test_name: "test_second".into(),
            package: "pkg-b".into(),
            status: TestStatus::Fail,
            duration_secs: Some(0.2),
            attempt: 1,
            output: None,
        }],
    )?;

    let runs = db.recent_test_runs(10)?;
    let ids = runs.iter().map(|run| run.invocation_id).collect::<Vec<_>>();

    assert_eq!(ids, vec![second_inv, first_inv]);
    assert_eq!(db.recent_test_runs(1)?[0].invocation_id, second_inv);
    Ok(())
}

#[sinex_test]
async fn test_resolve_test_run_supports_freshness_selectors() -> TestResult<()> {
    let (_dir, db, first_inv) = test_db_with_invocation()?;
    db.store_test_results(
        first_inv,
        &[TestResult {
            test_name: "test_first".into(),
            package: "pkg-a".into(),
            status: TestStatus::Pass,
            duration_secs: Some(0.1),
            attempt: 1,
            output: None,
        }],
    )?;

    let second_inv = db.start_invocation("test", None, None, None)?;
    db.finish_invocation(second_inv, InvocationStatus::Failed, Some(1), 1.0)?;
    db.store_test_results(
        second_inv,
        &[TestResult {
            test_name: "test_second".into(),
            package: "pkg-b".into(),
            status: TestStatus::Fail,
            duration_secs: Some(0.2),
            attempt: 1,
            output: None,
        }],
    )?;

    let third_inv = db.start_invocation("test", None, None, None)?;
    db.finish_invocation(third_inv, InvocationStatus::Success, Some(0), 1.0)?;
    db.store_test_results(
        third_inv,
        &[TestResult {
            test_name: "test_third".into(),
            package: "pkg-c".into(),
            status: TestStatus::Pass,
            duration_secs: Some(0.3),
            attempt: 1,
            output: None,
        }],
    )?;

    let latest = db
        .resolve_test_run(Some("latest"))?
        .expect("latest run should resolve");
    assert_eq!(latest.invocation_id, third_inv);

    let previous = db
        .resolve_test_run(Some("previous"))?
        .expect("previous run should resolve");
    assert_eq!(previous.invocation_id, second_inv);

    let latest_success = db
        .resolve_test_run(Some("latest-success"))?
        .expect("latest successful run should resolve");
    assert_eq!(latest_success.invocation_id, third_inv);

    let latest_failure = db
        .resolve_test_run(Some("latest-failure"))?
        .expect("latest failed run should resolve");
    assert_eq!(latest_failure.invocation_id, second_inv);

    Ok(())
}

#[sinex_test]
async fn test_analyze_test_run_can_target_non_latest_invocation() -> TestResult<()> {
    let (_dir, db, first_inv) = test_db_with_invocation()?;
    db.store_test_results(
        first_inv,
        &[TestResult {
            test_name: "test_first".into(),
            package: "pkg-a".into(),
            status: TestStatus::Pass,
            duration_secs: Some(0.5),
            attempt: 1,
            output: None,
        }],
    )?;

    let second_inv = db.start_invocation("test", None, None, None)?;
    db.finish_invocation(
        second_inv,
        super::super::db::InvocationStatus::Success,
        Some(0),
        2.0,
    )?;
    db.store_test_results(
        second_inv,
        &[TestResult {
            test_name: "test_second".into(),
            package: "pkg-b".into(),
            status: TestStatus::Fail,
            duration_secs: Some(1.0),
            attempt: 1,
            output: Some("boom".into()),
        }],
    )?;

    let first = db
        .analyze_test_run(first_inv)?
        .expect("first invocation should analyze");
    let second = db
        .analyze_test_run(second_inv)?
        .expect("second invocation should analyze");

    assert_eq!(first.invocation_id, first_inv);
    assert_eq!(first.total_passed, 1);
    assert_eq!(first.total_failed, 0);
    assert_eq!(second.invocation_id, second_inv);
    assert_eq!(second.total_passed, 0);
    assert_eq!(second.total_failed, 1);
    Ok(())
}

#[sinex_test]
async fn test_store_and_get_test_results() -> TestResult<()> {
    let (_dir, db, inv_id) = test_db_with_invocation()?;

    let results = vec![
        TestResult {
            test_name: "test_alpha".into(),
            package: "pkg-a".into(),
            status: TestStatus::Pass,
            duration_secs: Some(0.5),
            attempt: 1,
            output: None,
        },
        TestResult {
            test_name: "test_beta".into(),
            package: "pkg-a".into(),
            status: TestStatus::Fail,
            duration_secs: Some(1.2),
            attempt: 1,
            output: Some("assertion failed".into()),
        },
    ];
    let stored = db.store_test_results(inv_id, &results)?;
    assert_eq!(stored, 2);

    let retrieved = db.get_test_results(inv_id)?;
    assert_eq!(retrieved.len(), 2);
    // Ordered by package, test_name
    assert_eq!(retrieved[0].test_name, "test_alpha");
    assert_eq!(retrieved[0].status, TestStatus::Pass);
    assert_eq!(retrieved[1].test_name, "test_beta");
    assert_eq!(retrieved[1].status, TestStatus::Fail);
    assert_eq!(retrieved[1].output.as_deref(), Some("assertion failed"));
    Ok(())
}

#[sinex_test]
async fn test_get_flaky_tests_detects_retry_pass() -> TestResult<()> {
    let (_dir, db, inv_id) = test_db_with_invocation()?;

    // Simulate: test_flaky fails on attempt 1, passes on attempt 2
    let results = vec![
        TestResult {
            test_name: "test_flaky".into(),
            package: "pkg-a".into(),
            status: TestStatus::Fail,
            duration_secs: Some(0.3),
            attempt: 1,
            output: Some("timeout".into()),
        },
        TestResult {
            test_name: "test_flaky".into(),
            package: "pkg-a".into(),
            status: TestStatus::Pass,
            duration_secs: Some(0.2),
            attempt: 2,
            output: None,
        },
        // Non-flaky test: passes on first attempt
        TestResult {
            test_name: "test_stable".into(),
            package: "pkg-a".into(),
            status: TestStatus::Pass,
            duration_secs: Some(0.1),
            attempt: 1,
            output: None,
        },
    ];
    db.store_test_results(inv_id, &results)?;

    let flaky = db.get_flaky_tests(10)?;
    assert_eq!(flaky.len(), 1, "Should detect exactly one flaky test");
    assert_eq!(flaky[0].0, "test_flaky");
    assert_eq!(flaky[0].1, "pkg-a");
    assert_eq!(flaky[0].2, inv_id);
    Ok(())
}

#[sinex_test]
async fn test_get_flaky_test_count_matches_limited_row_query() -> TestResult<()> {
    let (_dir, db, inv_id) = test_db_with_invocation()?;

    let results = vec![
        TestResult {
            test_name: "test_flaky_a".into(),
            package: "pkg-a".into(),
            status: TestStatus::Fail,
            duration_secs: Some(0.3),
            attempt: 1,
            output: Some("timeout".into()),
        },
        TestResult {
            test_name: "test_flaky_a".into(),
            package: "pkg-a".into(),
            status: TestStatus::Pass,
            duration_secs: Some(0.2),
            attempt: 2,
            output: None,
        },
        TestResult {
            test_name: "test_flaky_b".into(),
            package: "pkg-b".into(),
            status: TestStatus::Fail,
            duration_secs: Some(0.4),
            attempt: 1,
            output: Some("panic".into()),
        },
        TestResult {
            test_name: "test_flaky_b".into(),
            package: "pkg-b".into(),
            status: TestStatus::Pass,
            duration_secs: Some(0.1),
            attempt: 2,
            output: None,
        },
    ];
    db.store_test_results(inv_id, &results)?;

    assert_eq!(db.get_flaky_tests(1)?.len(), db.get_flaky_test_count(1)?);
    assert_eq!(db.get_flaky_tests(10)?.len(), db.get_flaky_test_count(10)?);
    Ok(())
}

#[sinex_test]
async fn test_get_flaky_tests_no_false_positives() -> TestResult<()> {
    let (_dir, db, inv_id) = test_db_with_invocation()?;

    // Test that fails and stays failed — NOT flaky
    let results = vec![
        TestResult {
            test_name: "test_broken".into(),
            package: "pkg-a".into(),
            status: TestStatus::Fail,
            duration_secs: Some(0.3),
            attempt: 1,
            output: None,
        },
        TestResult {
            test_name: "test_broken".into(),
            package: "pkg-a".into(),
            status: TestStatus::Fail,
            duration_secs: Some(0.3),
            attempt: 2,
            output: None,
        },
    ];
    db.store_test_results(inv_id, &results)?;

    let flaky = db.get_flaky_tests(10)?;
    assert!(
        flaky.is_empty(),
        "Consistently failing test should not be flagged as flaky"
    );
    Ok(())
}

#[sinex_test]
async fn test_get_failing_tests() -> TestResult<()> {
    let (_dir, db, inv_id) = test_db_with_invocation()?;

    let results = vec![
        TestResult {
            test_name: "test_ok".into(),
            package: "pkg-a".into(),
            status: TestStatus::Pass,
            duration_secs: Some(0.1),
            attempt: 1,
            output: None,
        },
        TestResult {
            test_name: "test_broken".into(),
            package: "pkg-a".into(),
            status: TestStatus::Fail,
            duration_secs: Some(0.8),
            attempt: 1,
            output: Some("panic!".into()),
        },
        TestResult {
            test_name: "test_also_broken".into(),
            package: "pkg-b".into(),
            status: TestStatus::Fail,
            duration_secs: Some(0.5),
            attempt: 1,
            output: None,
        },
    ];
    db.store_test_results(inv_id, &results)?;

    let failing = db.get_failing_tests(inv_id, 10)?;
    assert_eq!(failing.len(), 2);
    // Ordered by test_name
    assert_eq!(failing[0].0, "test_also_broken");
    assert_eq!(failing[1].0, "test_broken");
    Ok(())
}

#[sinex_test]
async fn test_get_failing_tests_with_output() -> TestResult<()> {
    let (_dir, db, inv_id) = test_db_with_invocation()?;

    let results = vec![
        TestResult {
            test_name: "test_pass".into(),
            package: "pkg-a".into(),
            status: TestStatus::Pass,
            duration_secs: Some(0.1),
            attempt: 1,
            output: None,
        },
        TestResult {
            test_name: "test_fail".into(),
            package: "pkg-a".into(),
            status: TestStatus::Fail,
            duration_secs: Some(2.0),
            attempt: 1,
            output: Some("thread 'main' panicked at 'assertion failed'".into()),
        },
    ];
    db.store_test_results(inv_id, &results)?;

    let failing = db.get_failing_tests_with_output(inv_id, 10)?;
    assert_eq!(failing.len(), 1);
    assert_eq!(failing[0].test_name, "test_fail");
    assert!(failing[0].output.as_deref().unwrap().contains("panicked"));
    Ok(())
}

#[sinex_test]
async fn test_get_slowest_tests() -> TestResult<()> {
    let (_dir, db, inv_id) = test_db_with_invocation()?;

    let results = vec![
        TestResult {
            test_name: "test_fast".into(),
            package: "pkg".into(),
            status: TestStatus::Pass,
            duration_secs: Some(0.01),
            attempt: 1,
            output: None,
        },
        TestResult {
            test_name: "test_slow".into(),
            package: "pkg".into(),
            status: TestStatus::Pass,
            duration_secs: Some(5.0),
            attempt: 1,
            output: None,
        },
        // Failed test should NOT appear in slowest (it inflates with timeout ceiling)
        TestResult {
            test_name: "test_failed_slow".into(),
            package: "pkg".into(),
            status: TestStatus::Fail,
            duration_secs: Some(60.0),
            attempt: 1,
            output: None,
        },
    ];
    db.store_test_results(inv_id, &results)?;

    let slowest = db.get_slowest_tests(10)?;
    assert_eq!(slowest.len(), 2, "Failed test should be excluded");
    assert_eq!(slowest[0].test_name, "test_slow");
    assert!(slowest[0].avg_duration_secs > 4.0); // avg duration > 4s
    assert_eq!(slowest[1].test_name, "test_fast");
    Ok(())
}

#[sinex_test]
async fn test_get_slowest_latest_tests_uses_current_result() -> TestResult<()> {
    let (_dir, db, older_inv) = test_db_with_invocation()?;

    db.store_test_results(
        older_inv,
        &[TestResult {
            test_name: "changed_cost".into(),
            package: "pkg".into(),
            status: TestStatus::Pass,
            duration_secs: Some(20.0),
            attempt: 1,
            output: None,
        }],
    )?;

    let newer_inv = db.start_invocation("test", None, None, None)?;
    db.finish_invocation(newer_inv, InvocationStatus::Success, Some(0), 3.0)?;
    db.store_test_results(
        newer_inv,
        &[
            TestResult {
                test_name: "changed_cost".into(),
                package: "pkg".into(),
                status: TestStatus::Pass,
                duration_secs: Some(0.2),
                attempt: 1,
                output: None,
            },
            TestResult {
                test_name: "currently_slow".into(),
                package: "pkg".into(),
                status: TestStatus::Pass,
                duration_secs: Some(3.0),
                attempt: 1,
                output: None,
            },
        ],
    )?;

    let slowest = db.get_slowest_latest_tests_filtered(10, None)?;

    assert_eq!(slowest.len(), 2);
    assert_eq!(slowest[0].test_name, "currently_slow");
    assert_eq!(slowest[0].avg_duration_secs, 3.0);
    assert_eq!(slowest[1].test_name, "changed_cost");
    assert_eq!(slowest[1].avg_duration_secs, 0.2);
    Ok(())
}

#[sinex_test]
async fn test_get_slowest_tests_for_invocation_keeps_run_scope() -> TestResult<()> {
    let (_dir, db, first_inv) = test_db_with_invocation()?;

    db.store_test_results(
        first_inv,
        &[
            TestResult {
                test_name: "test_medium".into(),
                package: "pkg".into(),
                status: TestStatus::Pass,
                duration_secs: Some(2.0),
                attempt: 1,
                output: None,
            },
            TestResult {
                test_name: "test_slowest".into(),
                package: "pkg".into(),
                status: TestStatus::Fail,
                duration_secs: Some(8.0),
                attempt: 1,
                output: Some("boom".into()),
            },
        ],
    )?;

    let second_inv = db.start_invocation("test", None, None, None)?;
    db.finish_invocation(
        second_inv,
        super::super::db::InvocationStatus::Success,
        Some(0),
        1.0,
    )?;
    db.store_test_results(
        second_inv,
        &[TestResult {
            test_name: "test_other_run".into(),
            package: "pkg".into(),
            status: TestStatus::Pass,
            duration_secs: Some(20.0),
            attempt: 1,
            output: None,
        }],
    )?;

    let slowest = db.get_slowest_tests_for_invocation(first_inv, 10)?;
    assert_eq!(slowest.len(), 2);
    assert_eq!(slowest[0].test_name, "test_slowest");
    assert_eq!(slowest[0].status, "fail");
    assert_eq!(slowest[1].test_name, "test_medium");
    Ok(())
}

#[sinex_test]
async fn test_analyze_last_run_basic() -> TestResult<()> {
    let (_dir, db, inv_id) = test_db_with_invocation()?;
    db.record_stage_timing(
        inv_id,
        "preflight",
        "2026-01-01T00:00:00Z",
        0.5,
        true,
        StagePressure::default(),
    )?;
    db.record_stage_timing(
        inv_id,
        "nextest-stream",
        "2026-01-01T00:00:01Z",
        1.0,
        true,
        StagePressure {
            io_full_avg10: Some(12.5),
            cpu_some_avg10: Some(3.0),
            memory_some_avg10: Some(1.0),
            io_full_stall_us: Some(125_000),
            cpu_some_stall_us: Some(30_000),
            memory_some_stall_us: Some(10_000),
        },
    )?;
    db.record_stage_timing(
        inv_id,
        "nextest-stream",
        "2026-01-01T00:00:02Z",
        0.25,
        true,
        StagePressure::default(),
    )?;

    let results = vec![
        TestResult {
            test_name: "test_one".into(),
            package: "pkg-a".into(),
            status: TestStatus::Pass,
            duration_secs: Some(0.5),
            attempt: 1,
            output: None,
        },
        TestResult {
            test_name: "test_two".into(),
            package: "pkg-a".into(),
            status: TestStatus::Fail,
            duration_secs: Some(1.5),
            attempt: 1,
            output: Some("failed".into()),
        },
        TestResult {
            test_name: "test_three".into(),
            package: "pkg-b".into(),
            status: TestStatus::Skip,
            duration_secs: None,
            attempt: 1,
            output: None,
        },
    ];
    db.store_test_results(inv_id, &results)?;

    let analysis = db.analyze_last_run()?.expect("should have analysis");
    assert_eq!(analysis.total_passed, 1);
    assert_eq!(analysis.total_failed, 1);
    assert_eq!(analysis.total_ignored, 1);
    assert_eq!(analysis.invocation_id, inv_id);
    assert_eq!(analysis.slowest_tests.len(), 3);
    assert_eq!(analysis.slowest_tests[0].test_name, "test_two");
    assert_eq!(analysis.slowest_tests[0].status, "fail");
    let overhead = analysis
        .run_overhead
        .as_ref()
        .expect("finished invocation duration should produce overhead summary");
    assert_eq!(overhead.invocation_duration_secs, 5.0);
    assert_eq!(overhead.test_body_duration_secs, 2.0);
    assert_eq!(overhead.non_test_overhead_secs, 3.0);
    assert_eq!(overhead.classification, "mixed");
    assert_eq!(analysis.stage_breakdown.len(), 2);
    assert_eq!(analysis.stage_breakdown[0].stage_name, "nextest-stream");
    assert_eq!(analysis.stage_breakdown[0].runs, 2);
    assert_eq!(analysis.stage_breakdown[0].total_duration_secs, 1.25);
    assert_eq!(analysis.stage_breakdown[1].stage_name, "preflight");
    assert_eq!(analysis.unstaged_invocation_secs, Some(3.25));

    // Failure summary should have pkg-a with 1 failure
    assert_eq!(analysis.failure_summary.len(), 1);
    assert_eq!(analysis.failure_summary[0].package, "pkg-a");
    assert_eq!(analysis.failure_summary[0].failed_count, 1);
    assert_eq!(analysis.failure_summary[0].passed_count, 1);
    Ok(())
}

#[sinex_test]
async fn test_run_overhead_classifies_parallel_test_body_sums() -> TestResult<()> {
    let (_dir, db, inv_id) = test_db_with_invocation()?;
    db.conn.execute(
        r"
            UPDATE invocations
            SET status = 'success',
                duration_secs = 10.0
            WHERE id = ?1
            ",
        [inv_id],
    )?;
    db.store_test_results(
        inv_id,
        &[
            TestResult {
                test_name: "parallel_one".into(),
                package: "pkg-a".into(),
                status: TestStatus::Pass,
                duration_secs: Some(8.0),
                attempt: 1,
                output: None,
            },
            TestResult {
                test_name: "parallel_two".into(),
                package: "pkg-a".into(),
                status: TestStatus::Pass,
                duration_secs: Some(7.0),
                attempt: 1,
                output: None,
            },
        ],
    )?;

    let analysis = db.analyze_last_run()?.expect("should have analysis");
    let overhead = analysis
        .run_overhead
        .as_ref()
        .expect("finished invocation duration should produce overhead summary");
    assert_eq!(overhead.invocation_duration_secs, 10.0);
    assert_eq!(overhead.test_body_duration_secs, 15.0);
    assert_eq!(overhead.non_test_overhead_secs, 0.0);
    assert_eq!(overhead.test_body_ratio, 1.0);
    assert_eq!(overhead.classification, "parallel_test_bodies");
    Ok(())
}

#[sinex_test]
async fn test_analyze_test_run_estimates_overhead_for_in_flight_invocation() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test-history.db");
    let db = HistoryDb::open(&db_path)?;
    let inv_id = db.start_invocation("test", None, None, None)?;
    let started_at = (OffsetDateTime::now_utc() - time::Duration::seconds(120)).format(&Rfc3339)?;
    db.conn.execute(
        "UPDATE invocations SET started_at = ?1, duration_secs = NULL WHERE id = ?2",
        rusqlite::params![started_at, inv_id],
    )?;
    db.store_test_results(
        inv_id,
        &[TestResult {
            test_name: "tiny_test".into(),
            package: "xtask".into(),
            status: TestStatus::Pass,
            duration_secs: Some(0.2),
            attempt: 1,
            output: None,
        }],
    )?;

    let analysis = db
        .analyze_test_run(inv_id)?
        .expect("in-flight invocation with test results should analyze");
    let overhead = analysis
        .run_overhead
        .as_ref()
        .expect("started_at fallback should produce overhead summary");

    assert!(overhead.invocation_duration_secs >= 119.0);
    assert_eq!(overhead.test_body_duration_secs, 0.2);
    assert_eq!(overhead.classification, "runner_setup_dominated");
    Ok(())
}

#[sinex_test]
async fn test_analyze_last_run_surfaces_corrupted_rows() -> TestResult<()> {
    let (_dir, db, inv_id) = test_db_with_invocation()?;

    db.conn.execute(
        r"
            INSERT INTO test_results (
                invocation_id,
                test_name,
                package,
                status,
                duration_secs,
                attempt
            ) VALUES (?1, 'test_corrupt', 'pkg-a', 'pass', zeroblob(4), 1)
            ",
        rusqlite::params![inv_id],
    )?;

    let error = db
        .analyze_test_run(inv_id)
        .expect_err("corrupted test rows should surface");
    let message = format!("{error:#}");
    assert!(message.contains("failed to read stored test rows for invocation"));
    Ok(())
}

#[sinex_test]
async fn test_analyze_last_run_empty() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test-empty.db");
    let db = HistoryDb::open(&db_path)?;

    // No invocations at all
    let analysis = db.analyze_last_run()?;
    assert!(analysis.is_none());
    Ok(())
}

#[sinex_test]
async fn test_get_infra_timing_summary_surfaces_corrupted_rows() -> TestResult<()> {
    let (_dir, db, inv_id) = test_db_with_invocation()?;

    db.conn.execute(
        r"
            INSERT INTO test_results (
                invocation_id,
                test_name,
                package,
                status,
                duration_secs,
                attempt,
                slot_name,
                slot_wait_ms,
                cleanup_ms
            ) VALUES (
                ?1,
                'test_corrupt_slot',
                'pkg-a',
                'pass',
                0.1,
                1,
                'slot-a',
                zeroblob(4),
                10
            )
            ",
        rusqlite::params![inv_id],
    )?;

    let error = db
        .get_infra_timing_summary(inv_id)
        .expect_err("corrupted infrastructure timing rows should surface");
    let message = format!("{error:#}");
    assert!(message.contains("failed to read stored infrastructure timing rows"));
    Ok(())
}

#[sinex_test]
async fn test_backfill_test_metadata_surfaces_corrupted_output_rows() -> TestResult<()> {
    let (_dir, db, inv_id) = test_db_with_invocation()?;

    db.conn.execute(
        r"
            INSERT INTO test_results (
                invocation_id,
                test_name,
                package,
                status,
                duration_secs,
                attempt,
                output
            ) VALUES (?1, 'test_corrupt_output', 'pkg-a', 'pass', 0.1, 1, zeroblob(4))
            ",
        rusqlite::params![inv_id],
    )?;

    let error = db
        .backfill_test_metadata(inv_id, &HashMap::new())
        .expect_err("corrupted output rows should surface");
    let message = format!("{error:#}");
    assert!(message.contains("failed to read stored sandbox metadata rows for invocation"));
    Ok(())
}

#[sinex_test]
async fn test_get_test_trends_surfaces_corrupted_rows() -> TestResult<()> {
    let (_dir, db, inv_id) = test_db_with_invocation()?;

    db.conn.execute(
        r"
            INSERT INTO test_results (
                invocation_id,
                test_name,
                package,
                status,
                duration_secs,
                attempt
            ) VALUES (?1, 'test_corrupt_trend', 'pkg-a', 'pass', zeroblob(4), 1)
            ",
        rusqlite::params![inv_id],
    )?;

    let error = db
        .get_test_trends(None, None, 10)
        .expect_err("corrupted trend rows should surface");
    let message = format!("{error:#}");
    assert!(message.contains("failed to read stored test trend rows"));
    Ok(())
}

#[sinex_test]
async fn test_analyze_probable_timeouts() -> TestResult<()> {
    let (_dir, db, inv_id) = test_db_with_invocation()?;

    let results = vec![
        // Failed test at exactly 60s — probable timeout
        TestResult {
            test_name: "test_timeout".into(),
            package: "pkg".into(),
            status: TestStatus::Fail,
            duration_secs: Some(59.8),
            attempt: 1,
            output: None,
        },
        // Failed test at 3s — NOT a timeout
        TestResult {
            test_name: "test_real_fail".into(),
            package: "pkg".into(),
            status: TestStatus::Fail,
            duration_secs: Some(3.0),
            attempt: 1,
            output: None,
        },
    ];
    db.store_test_results(inv_id, &results)?;

    let analysis = db.analyze_last_run()?.expect("should have analysis");
    assert_eq!(analysis.probable_timeouts.len(), 1);
    assert_eq!(analysis.probable_timeouts[0].test_name, "test_timeout");
    Ok(())
}

#[sinex_test]
async fn test_analyze_classifies_ready_failures_under_host_pressure() -> TestResult<()> {
    let (_dir, db, inv_id) = test_db_with_invocation()?;

    db.conn.execute(
        r"
            UPDATE invocations
            SET host_io_pressure_full_avg10_max = 84.41,
                host_memory_pressure_full_avg10_max = 65.13,
                host_cpu_pressure_some_avg10_max = 71.20
            WHERE id = ?1
            ",
        rusqlite::params![inv_id],
    )?;
    db.store_test_results(
        inv_id,
        &[TestResult {
            test_name: "event_engine_ready_probe".into(),
            package: "sinexd".into(),
            status: TestStatus::Fail,
            duration_secs: Some(12.0),
            attempt: 1,
            output: Some("event_engine did not reach READY state".into()),
        }],
    )?;

    let analysis = db.analyze_last_run()?.expect("should have analysis");
    let pressure = analysis
        .host_pressure
        .expect("pressure classification should be present");
    assert_eq!(pressure.level, "severe");
    assert!(pressure.timing_failures_may_be_invalidated);
    assert!(pressure.reason.contains("rerun under low contention"));
    assert_eq!(pressure.host_io_pressure_full_avg10_max, Some(84.41));
    Ok(())
}

#[sinex_test]
async fn test_analyze_does_not_invalidate_nontiming_failures() -> TestResult<()> {
    let (_dir, db, inv_id) = test_db_with_invocation()?;

    db.conn.execute(
        r"
            UPDATE invocations
            SET host_io_pressure_full_avg10_max = 12.0
            WHERE id = ?1
            ",
        rusqlite::params![inv_id],
    )?;
    db.store_test_results(
        inv_id,
        &[TestResult {
            test_name: "assert_payload_shape".into(),
            package: "sinex-db".into(),
            status: TestStatus::Fail,
            duration_secs: Some(0.2),
            attempt: 1,
            output: Some("assertion failed: payload mismatch".into()),
        }],
    )?;

    let analysis = db.analyze_last_run()?.expect("should have analysis");
    let pressure = analysis
        .host_pressure
        .expect("pressure context should still be present");
    assert_eq!(pressure.level, "severe");
    assert!(!pressure.timing_failures_may_be_invalidated);
    assert!(pressure.reason.contains("do not look timing-sensitive"));
    Ok(())
}

#[sinex_test]
async fn test_get_test_output() -> TestResult<()> {
    let (_dir, db, inv_id) = test_db_with_invocation()?;

    let results = vec![
        TestResult {
            test_name: "module::test_alpha".into(),
            package: "pkg-a".into(),
            status: TestStatus::Pass,
            duration_secs: Some(0.1),
            attempt: 1,
            output: Some("all good".into()),
        },
        TestResult {
            test_name: "module::test_beta".into(),
            package: "pkg-a".into(),
            status: TestStatus::Fail,
            duration_secs: Some(0.2),
            attempt: 1,
            output: Some("assertion failed".into()),
        },
    ];
    db.store_test_results(inv_id, &results)?;

    // Pattern match
    let output = db.get_test_output(inv_id, "alpha")?;
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].test_name, "module::test_alpha");
    assert_eq!(output[0].output.as_deref(), Some("all good"));

    // Pattern matching multiple
    let output = db.get_test_output(inv_id, "test_")?;
    assert_eq!(output.len(), 2);
    Ok(())
}

#[sinex_test]
async fn test_estimate_runtime() -> TestResult<()> {
    let (_dir, db, inv_id) = test_db_with_invocation()?;

    let results = vec![
        TestResult {
            test_name: "test_a".into(),
            package: "pkg-fast".into(),
            status: TestStatus::Pass,
            duration_secs: Some(0.1),
            attempt: 1,
            output: None,
        },
        TestResult {
            test_name: "test_b".into(),
            package: "pkg-fast".into(),
            status: TestStatus::Pass,
            duration_secs: Some(0.2),
            attempt: 1,
            output: None,
        },
        TestResult {
            test_name: "test_c".into(),
            package: "pkg-slow".into(),
            status: TestStatus::Pass,
            duration_secs: Some(5.0),
            attempt: 1,
            output: None,
        },
    ];
    db.store_test_results(inv_id, &results)?;

    let estimate = db.estimate_runtime()?;
    assert!(estimate.estimated_secs > 0.0);
    assert_eq!(estimate.test_count, 3);
    // Low confidence with < 5 samples
    assert_eq!(estimate.confidence, Confidence::Low);
    // Breakdown should have 2 packages
    assert_eq!(estimate.breakdown.len(), 2);
    Ok(())
}

#[sinex_test]
async fn test_get_tests_getting_slower() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test-slower.db");
    let db = HistoryDb::open(&db_path)?;

    // Create multiple invocations to simulate time progression
    // Older runs: test takes ~1s
    for _ in 0..3 {
        let inv_id = db.start_invocation("test", None, None, None)?;
        db.finish_invocation(
            inv_id,
            super::super::db::InvocationStatus::Success,
            Some(0),
            5.0,
        )?;
        db.store_test_results(
            inv_id,
            &[TestResult {
                test_name: "test_regressing".into(),
                package: "pkg".into(),
                status: TestStatus::Pass,
                duration_secs: Some(1.0),
                attempt: 1,
                output: None,
            }],
        )?;
    }

    // Recent runs: test takes ~3s (200% slower)
    for _ in 0..3 {
        let inv_id = db.start_invocation("test", None, None, None)?;
        db.finish_invocation(
            inv_id,
            super::super::db::InvocationStatus::Success,
            Some(0),
            5.0,
        )?;
        db.store_test_results(
            inv_id,
            &[TestResult {
                test_name: "test_regressing".into(),
                package: "pkg".into(),
                status: TestStatus::Pass,
                duration_secs: Some(3.0),
                attempt: 1,
                output: None,
            }],
        )?;
    }

    let slower = db.get_tests_getting_slower(6, 50.0, 10)?;
    assert!(
        !slower.is_empty(),
        "Should detect test_regressing as getting slower"
    );
    assert_eq!(slower[0].test_name, "test_regressing");
    assert!(slower[0].pct_change > 100.0, "Should show >100% regression");
    Ok(())
}

#[sinex_test]
async fn test_get_tests_getting_slower_zero_window() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test-zero-window.db");
    let db = HistoryDb::open(&db_path)?;

    // Window of 0 or 1 means half_window = 0 → early return
    let result = db.get_tests_getting_slower(0, 50.0, 10)?;
    assert!(result.is_empty());
    let result = db.get_tests_getting_slower(1, 50.0, 10)?;
    assert!(result.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_get_test_trends() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test-trends.db");
    let db = HistoryDb::open(&db_path)?;

    // Create 3 runs with varying durations
    for duration in [1.0, 1.5, 2.0] {
        let inv_id = db.start_invocation("test", None, None, None)?;
        db.finish_invocation(
            inv_id,
            super::super::db::InvocationStatus::Success,
            Some(0),
            5.0,
        )?;
        db.store_test_results(
            inv_id,
            &[TestResult {
                test_name: "test_trending".into(),
                package: "pkg".into(),
                status: TestStatus::Pass,
                duration_secs: Some(duration),
                attempt: 1,
                output: None,
            }],
        )?;
    }

    // Get trends for all tests
    let trends = db.get_test_trends(None, None, 10)?;
    assert_eq!(trends.len(), 1);
    assert_eq!(trends[0].test_name, "test_trending");
    assert_eq!(trends[0].durations.len(), 3);
    assert!(trends[0].avg_duration_secs > 1.0);

    // Filter by pattern
    let trends = db.get_test_trends(Some("trending"), None, 10)?;
    assert_eq!(trends.len(), 1);

    // Filter by non-matching pattern
    let trends = db.get_test_trends(Some("nonexistent"), None, 10)?;
    assert!(trends.is_empty());

    // Filter by package
    let trends = db.get_test_trends(None, Some("pkg"), 10)?;
    assert_eq!(trends.len(), 1);

    // Limit runs per test
    let trends = db.get_test_trends(None, None, 2)?;
    assert_eq!(trends[0].durations.len(), 2);
    Ok(())
}

#[sinex_test]
async fn test_duration_buckets() -> TestResult<()> {
    let (_dir, db, inv_id) = test_db_with_invocation()?;

    let results = vec![
        TestResult {
            test_name: "test_instant".into(),
            package: "pkg".into(),
            status: TestStatus::Pass,
            duration_secs: Some(0.01),
            attempt: 1,
            output: None,
        },
        TestResult {
            test_name: "test_medium".into(),
            package: "pkg".into(),
            status: TestStatus::Pass,
            duration_secs: Some(7.0),
            attempt: 1,
            output: None,
        },
        TestResult {
            test_name: "test_long".into(),
            package: "pkg".into(),
            status: TestStatus::Pass,
            duration_secs: Some(45.0),
            attempt: 1,
            output: None,
        },
    ];
    db.store_test_results(inv_id, &results)?;

    let analysis = db.analyze_last_run()?.expect("should have analysis");

    // Check bucket distribution
    let sub_1s = analysis
        .duration_buckets
        .iter()
        .find(|b| b.label == "< 1s")
        .unwrap();
    assert_eq!(sub_1s.count, 1);
    let five_to_ten = analysis
        .duration_buckets
        .iter()
        .find(|b| b.label == "5-10s")
        .unwrap();
    assert_eq!(five_to_ten.count, 1);
    let thirty_to_sixty = analysis
        .duration_buckets
        .iter()
        .find(|b| b.label == "30-60s")
        .unwrap();
    assert_eq!(thirty_to_sixty.count, 1);
    Ok(())
}

#[sinex_test]
async fn test_confidence_display() -> TestResult<()> {
    assert_eq!(format!("{}", Confidence::Low), "low");
    assert_eq!(format!("{}", Confidence::Medium), "medium");
    assert_eq!(format!("{}", Confidence::High), "high");
    Ok(())
}

#[sinex_test]
async fn test_parse_nextest_output_empty() -> TestResult<()> {
    let results = parse_nextest_output("");
    assert!(results.is_empty());

    let results = parse_nextest_output("not json\njust text\n");
    assert!(results.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_parse_nextest_output_ignores_started_events() -> TestResult<()> {
    let output = r#"
{"type":"suite","event":"started","test_count":1}
{"type":"test","event":"started","name":"pkg::pkg$test_one"}
"#;
    let results = parse_nextest_output(output);
    assert!(results.is_empty(), "Started events should be skipped");
    Ok(())
}

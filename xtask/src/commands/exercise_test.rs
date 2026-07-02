use super::*;
use crate::sandbox::sinex_test;
use ::xtask::sandbox::EnvGuard;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use tempfile::tempdir;

fn write_executable_script(path: &std::path::Path, body: &str) -> ::xtask::sandbox::TestResult<()> {
    fs::write(path, body)?;
    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

// ── Tier enum ─────────────────────────────────────────────────────────────

#[sinex_test]
async fn test_tier_label() -> ::xtask::sandbox::TestResult<()> {
    assert_eq!(Tier::T1.label(), "T1");
    assert_eq!(Tier::T2.label(), "T2");
    assert_eq!(Tier::T3.label(), "T3");
    assert_eq!(Tier::T4.label(), "T4");
    Ok(())
}

#[sinex_test]
async fn test_tier_as_arg() -> ::xtask::sandbox::TestResult<()> {
    assert_eq!(Tier::T1.as_arg(), "1");
    assert_eq!(Tier::T2.as_arg(), "2");
    assert_eq!(Tier::T3.as_arg(), "3");
    assert_eq!(Tier::T4.as_arg(), "4");
    Ok(())
}

#[sinex_test]
async fn test_tier_display() -> ::xtask::sandbox::TestResult<()> {
    assert_eq!(Tier::T1.to_string(), "T1");
    assert_eq!(Tier::T4.to_string(), "T4");
    Ok(())
}

// ── json_path helper ──────────────────────────────────────────────────────

#[sinex_test]
async fn test_json_path_top_level() -> ::xtask::sandbox::TestResult<()> {
    let val = serde_json::json!({"status": "success", "count": 3});
    assert_eq!(
        json_path(&val, "status"),
        Some(&serde_json::json!("success"))
    );
    assert_eq!(json_path(&val, "count"), Some(&serde_json::json!(3)));
    assert_eq!(json_path(&val, "missing"), None);
    Ok(())
}

#[sinex_test]
async fn test_json_path_nested() -> ::xtask::sandbox::TestResult<()> {
    let val = serde_json::json!({"data": {"job_id": 42}});
    assert_eq!(json_path(&val, "data.job_id"), Some(&serde_json::json!(42)));
    assert_eq!(json_path(&val, "data.missing"), None);
    assert_eq!(json_path(&val, "nope.job_id"), None);
    Ok(())
}

// ── parse_last_json ───────────────────────────────────────────────────────

#[sinex_test]
async fn test_parse_last_json_single() -> ::xtask::sandbox::TestResult<()> {
    let result = parse_last_json(r#"{"status":"ok"}"#);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), serde_json::json!({"status": "ok"}));
    Ok(())
}

#[sinex_test]
async fn test_parse_last_json_multiple_returns_last() -> ::xtask::sandbox::TestResult<()> {
    // Two concatenated JSON objects — last wins
    let result = parse_last_json(r#"{"first":1}{"second":2}"#);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), serde_json::json!({"second": 2}));
    Ok(())
}

#[sinex_test]
async fn test_parse_last_json_empty_string() -> ::xtask::sandbox::TestResult<()> {
    let result = parse_last_json("");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("no JSON object found"));
    Ok(())
}

#[sinex_test]
async fn test_parse_last_json_invalid() -> ::xtask::sandbox::TestResult<()> {
    let result = parse_last_json("not json at all");
    assert!(result.is_err());
    Ok(())
}

// ── Validation::check ─────────────────────────────────────────────────────

fn make_output(stdout: &str, stderr: &str, exit_code: i32) -> StepOutput {
    StepOutput {
        stdout: stdout.to_string(),
        stderr: stderr.to_string(),
        exit_code,
        duration: Duration::ZERO,
    }
}

#[sinex_test]
async fn test_validation_json_valid() -> ::xtask::sandbox::TestResult<()> {
    let out = make_output(r#"{"ok":true}"#, "", 0);
    assert!(Validation::JsonValid.check(&out).is_ok());

    let bad = make_output("not json", "", 0);
    assert!(Validation::JsonValid.check(&bad).is_err());
    Ok(())
}

#[sinex_test]
async fn test_validation_json_has_fields() -> ::xtask::sandbox::TestResult<()> {
    let out = make_output(r#"{"status":"ok","data":{}}"#, "", 0);
    let v = v_has(&["status", "data"]);
    assert!(v.check(&out).is_ok());

    let v_missing = v_has(&["status", "missing_field"]);
    assert!(v_missing.check(&out).is_err());
    Ok(())
}

#[sinex_test]
async fn test_validation_json_field_equals() -> ::xtask::sandbox::TestResult<()> {
    let out = make_output(r#"{"status":"success"}"#, "", 0);
    let v = v_eq("status", serde_json::json!("success"));
    assert!(v.check(&out).is_ok());

    let v_wrong = v_eq("status", serde_json::json!("failure"));
    assert!(v_wrong.check(&out).is_err());

    let v_missing = v_eq("nonexistent", serde_json::json!("x"));
    assert!(v_missing.check(&out).is_err());
    Ok(())
}

#[sinex_test]
async fn test_validation_json_array_min_len() -> ::xtask::sandbox::TestResult<()> {
    let out = make_output(r#"{"items":[1,2,3]}"#, "", 0);
    let v = v_arr_min("items", 2);
    assert!(v.check(&out).is_ok());

    let v_too_few = v_arr_min("items", 5);
    assert!(v_too_few.check(&out).is_err());

    // Field missing
    let v_missing = v_arr_min("no_such_field", 1);
    assert!(v_missing.check(&out).is_err());

    // Not an array
    let not_arr = make_output(r#"{"items":"hello"}"#, "", 0);
    assert!(v_arr_min("items", 1).check(&not_arr).is_err());
    Ok(())
}

#[sinex_test]
async fn test_validation_stdout_contains() -> ::xtask::sandbox::TestResult<()> {
    let out = make_output("hello world", "", 0);
    assert!(v_contains("hello").check(&out).is_ok());
    assert!(v_contains("missing").check(&out).is_err());
    Ok(())
}

#[sinex_test]
async fn test_validation_stdout_not_contains() -> ::xtask::sandbox::TestResult<()> {
    let out = make_output("hello world", "", 0);
    let v = Validation::StdoutNotContains("absent".to_string());
    assert!(v.check(&out).is_ok());

    let v_present = Validation::StdoutNotContains("hello".to_string());
    assert!(v_present.check(&out).is_err());
    Ok(())
}

#[sinex_test]
async fn test_validation_stderr_contains() -> ::xtask::sandbox::TestResult<()> {
    let out = make_output("", "No command specified", 1);
    assert!(v_stderr("No command").check(&out).is_ok());
    assert!(v_stderr("missing phrase").check(&out).is_err());
    Ok(())
}

#[sinex_test]
async fn test_validation_stdout_empty() -> ::xtask::sandbox::TestResult<()> {
    let empty = make_output("   \n  ", "", 0);
    assert!(v_empty().check(&empty).is_ok());

    let non_empty = make_output("some output", "", 0);
    assert!(v_empty().check(&non_empty).is_err());
    Ok(())
}

#[sinex_test]
async fn test_validation_stdout_line_count() -> ::xtask::sandbox::TestResult<()> {
    let three_lines = make_output("a\nb\nc", "", 0);

    assert!(v_lines(Some(1), Some(5)).check(&three_lines).is_ok());
    assert!(v_lines(Some(3), Some(3)).check(&three_lines).is_ok());
    assert!(v_lines(Some(4), None).check(&three_lines).is_err());
    assert!(v_lines(None, Some(2)).check(&three_lines).is_err());
    assert!(v_lines(None, None).check(&three_lines).is_ok());
    Ok(())
}

// ── validate_step ─────────────────────────────────────────────────────────

#[sinex_test]
async fn test_validate_step_exit_success_passes() -> ::xtask::sandbox::TestResult<()> {
    let out = make_output("", "", 0);
    let errs = validate_step(&out, &ExpectedExit::Success, &[]);
    assert!(errs.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_validate_step_exit_success_fails_on_nonzero() -> ::xtask::sandbox::TestResult<()> {
    let out = make_output("", "", 1);
    let errs = validate_step(&out, &ExpectedExit::Success, &[]);
    assert_eq!(errs.len(), 1);
    assert!(errs[0].contains("expected exit 0"));
    Ok(())
}

#[sinex_test]
async fn test_validate_step_exit_failure_passes_on_nonzero() -> ::xtask::sandbox::TestResult<()> {
    let out = make_output("", "", 2);
    let errs = validate_step(&out, &ExpectedExit::Failure, &[]);
    assert!(errs.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_validate_step_exit_failure_fails_on_zero() -> ::xtask::sandbox::TestResult<()> {
    let out = make_output("", "", 0);
    let errs = validate_step(&out, &ExpectedExit::Failure, &[]);
    assert_eq!(errs.len(), 1);
    assert!(errs[0].contains("expected non-zero exit"));
    Ok(())
}

#[sinex_test]
async fn test_validate_step_any_accepts_any_exit() -> ::xtask::sandbox::TestResult<()> {
    for code in [0, 1, 2, 127] {
        let out = make_output("", "", code);
        let errs = validate_step(&out, &ExpectedExit::Any, &[]);
        assert!(
            errs.is_empty(),
            "exit code {code} should be accepted by Any"
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_validate_step_collects_multiple_errors() -> ::xtask::sandbox::TestResult<()> {
    let out = make_output("", "", 1); // non-zero exit
    let errs = validate_step(
        &out,
        &ExpectedExit::Success,           // exit error
        &[v_contains("expected phrase")], // validation error
    );
    assert_eq!(errs.len(), 2);
    Ok(())
}

// ── build_catalog ─────────────────────────────────────────────────────────

#[sinex_test]
async fn test_catalog_has_exercises_in_all_tiers() -> ::xtask::sandbox::TestResult<()> {
    let catalog = build_catalog();
    let t1: Vec<_> = catalog.iter().filter(|e| e.tier == Tier::T1).collect();
    let t2: Vec<_> = catalog.iter().filter(|e| e.tier == Tier::T2).collect();
    let t3: Vec<_> = catalog.iter().filter(|e| e.tier == Tier::T3).collect();
    let t4: Vec<_> = catalog.iter().filter(|e| e.tier == Tier::T4).collect();
    assert!(!t1.is_empty(), "T1 should have exercises");
    assert!(!t2.is_empty(), "T2 should have exercises");
    assert!(!t3.is_empty(), "T3 should have exercises");
    assert!(!t4.is_empty(), "T4 should have exercises");
    Ok(())
}

#[sinex_test]
async fn test_catalog_ids_are_unique() -> ::xtask::sandbox::TestResult<()> {
    let catalog = build_catalog();
    let mut seen = std::collections::HashSet::new();
    for ex in &catalog {
        assert!(
            seen.insert(ex.id.clone()),
            "duplicate exercise ID: {}",
            ex.id
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_catalog_ids_match_tier_prefix() -> ::xtask::sandbox::TestResult<()> {
    let catalog = build_catalog();
    for ex in &catalog {
        let expected_prefix = match ex.tier {
            Tier::T1 => "t1.",
            Tier::T2 => "t2.",
            Tier::T3 => "t3.",
            Tier::T4 => "t4.",
        };
        assert!(
            ex.id.starts_with(expected_prefix),
            "exercise '{}' has tier {:?} but id doesn't start with '{}'",
            ex.id,
            ex.tier,
            expected_prefix
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_catalog_descriptions_non_empty() -> ::xtask::sandbox::TestResult<()> {
    let catalog = build_catalog();
    for ex in &catalog {
        assert!(
            !ex.description.is_empty(),
            "exercise '{}' has an empty description",
            ex.id
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_catalog_declarative_exercises_have_steps() -> ::xtask::sandbox::TestResult<()> {
    let catalog = build_catalog();
    for ex in &catalog {
        if let ExerciseKind::Declarative(steps) = &ex.kind {
            assert!(
                !steps.is_empty(),
                "declarative exercise '{}' has no steps",
                ex.id
            );
        }
    }
    Ok(())
}

// ── Command metadata ──────────────────────────────────────────────────────

#[sinex_test]
async fn test_command_name() -> ::xtask::sandbox::TestResult<()> {
    let cmd = ExerciseCommand {
        all: false,
        tiers: vec![],
        exercises: vec![],
        list: false,
        dry_run: false,
        skip_infra: false,
        verbose: false,
        fail_fast: false,
        ..ExerciseCommand::default()
    };
    assert_eq!(cmd.name(), "exercise");
    Ok(())
}

#[sinex_test]
async fn test_command_metadata() -> ::xtask::sandbox::TestResult<()> {
    let cmd = ExerciseCommand {
        all: true,
        ..ExerciseCommand::default()
    };
    let meta = cmd.metadata();
    assert_eq!(meta.category, Some("test"));
    assert!(!meta.modifies_state);
    assert!(meta.track_in_history);
    assert!(meta.timeout.is_some());
    Ok(())
}

#[sinex_test]
async fn test_background_args_preserve_exercise_ids() -> ::xtask::sandbox::TestResult<()> {
    let cmd = ExerciseCommand {
        exercises: vec![
            "t4.coord_attach_check".to_string(),
            "t4.coord_scope_isolation".to_string(),
        ],
        skip_infra: true,
        ..ExerciseCommand::default()
    };

    assert_eq!(
        cmd.background_args(),
        vec![
            "--id",
            "t4.coord_attach_check",
            "--id",
            "t4.coord_scope_isolation",
            "--skip-infra",
        ]
    );
    Ok(())
}

// ── Builder helpers ───────────────────────────────────────────────────────

#[sinex_test]
async fn test_def_builder_defaults() -> ::xtask::sandbox::TestResult<()> {
    use crate::commands::exercise::builders::def;
    let ex = def("t1.test_id", "A test exercise", Tier::T1);
    assert_eq!(ex.id, "t1.test_id");
    assert_eq!(ex.description, "A test exercise");
    assert_eq!(ex.tier, Tier::T1);
    assert_eq!(ex.infra, InfraReq::None);
    // Declarative with no steps by default
    assert!(matches!(ex.kind, ExerciseKind::Declarative(ref s) if s.is_empty()));
    Ok(())
}

#[sinex_test]
async fn test_def_builder_custom() -> ::xtask::sandbox::TestResult<()> {
    use crate::commands::exercise::builders::def;
    let ex = def("t4.custom", "Custom exercise", Tier::T4).custom();
    assert!(matches!(ex.kind, ExerciseKind::Custom));
    Ok(())
}

#[sinex_test]
async fn test_def_builder_infra() -> ::xtask::sandbox::TestResult<()> {
    use crate::commands::exercise::builders::def;
    let ex = def("t3.infra", "With infra", Tier::T3).infra(InfraReq::Postgres);
    assert_eq!(ex.infra, InfraReq::Postgres);
    Ok(())
}

#[sinex_test]
async fn test_step_builder() -> ::xtask::sandbox::TestResult<()> {
    use crate::commands::exercise::builders::step;
    let s = step("my step", &["check", "--json"]);
    assert_eq!(s.label, "my step");
    assert_eq!(s.args, vec!["check", "--json"]);
    assert!(s.validations.is_empty());
    assert!(matches!(s.expected_exit, ExpectedExit::Success));
    Ok(())
}

#[sinex_test]
async fn test_step_builder_with_exit_and_validations() -> ::xtask::sandbox::TestResult<()> {
    use crate::commands::exercise::builders::step;
    let s = step("bad", &["check"])
        .exit(ExpectedExit::Failure)
        .v(v_contains("error"));
    assert!(matches!(s.expected_exit, ExpectedExit::Failure));
    assert_eq!(s.validations.len(), 1);
    Ok(())
}

#[sinex_test]
async fn test_def_builder_step_accumulates() -> ::xtask::sandbox::TestResult<()> {
    use crate::commands::exercise::builders::{def, step};
    let ex = def("t1.multi", "Multi-step", Tier::T1)
        .step(step("step1", &["check"]))
        .step(step("step2", &["test"]));
    match &ex.kind {
        ExerciseKind::Declarative(steps) => {
            assert_eq!(steps.len(), 2);
            assert_eq!(steps[0].label, "step1");
            assert_eq!(steps[1].label, "step2");
        }
        ExerciseKind::Custom => panic!("expected Declarative"),
    }
    Ok(())
}

// ── build_report ─────────────────────────────────────────────────────────

#[sinex_test]
async fn test_build_report_all_passed() -> ::xtask::sandbox::TestResult<()> {
    use crate::commands::exercise::builders::def;
    let catalog = vec![def("t1.a", "Exercise A", Tier::T1)];
    let outcomes = vec![ExerciseOutcome {
        id: "t1.a".to_string(),
        passed: true,
        duration: Duration::from_secs(1),
        steps: vec![],
        error: None,
    }];
    let report = build_report(
        &outcomes,
        &catalog,
        0,
        Duration::from_secs(1),
        std::path::Path::new("/tmp"),
    );
    assert_eq!(report.status, "success");
    assert_eq!(report.passed, 1);
    assert_eq!(report.failed, 0);
    assert_eq!(report.total, 1);
    Ok(())
}

#[sinex_test]
async fn test_build_report_all_failed() -> ::xtask::sandbox::TestResult<()> {
    use crate::commands::exercise::builders::def;
    let catalog = vec![def("t1.a", "Exercise A", Tier::T1)];
    let outcomes = vec![ExerciseOutcome {
        id: "t1.a".to_string(),
        passed: false,
        duration: Duration::from_millis(500),
        steps: vec![],
        error: Some("it broke".to_string()),
    }];
    let report = build_report(
        &outcomes,
        &catalog,
        0,
        Duration::from_secs(1),
        std::path::Path::new("/tmp"),
    );
    assert_eq!(report.status, "failed");
    assert_eq!(report.passed, 0);
    assert_eq!(report.failed, 1);
    Ok(())
}

#[sinex_test]
async fn test_build_report_partial() -> ::xtask::sandbox::TestResult<()> {
    use crate::commands::exercise::builders::def;
    let catalog = vec![
        def("t1.a", "Exercise A", Tier::T1),
        def("t1.b", "Exercise B", Tier::T1),
    ];
    let outcomes = vec![
        ExerciseOutcome {
            id: "t1.a".to_string(),
            passed: true,
            duration: Duration::ZERO,
            steps: vec![],
            error: None,
        },
        ExerciseOutcome {
            id: "t1.b".to_string(),
            passed: false,
            duration: Duration::ZERO,
            steps: vec![],
            error: None,
        },
    ];
    let report = build_report(
        &outcomes,
        &catalog,
        0,
        Duration::from_secs(2),
        std::path::Path::new("/tmp"),
    );
    assert_eq!(report.status, "partial");
    assert_eq!(report.passed, 1);
    assert_eq!(report.failed, 1);
    Ok(())
}

#[sinex_test]
async fn test_build_report_skipped_counted_in_total() -> ::xtask::sandbox::TestResult<()> {
    use crate::commands::exercise::builders::def;
    let catalog = vec![def("t1.a", "A", Tier::T1)];
    let outcomes = vec![ExerciseOutcome {
        id: "t1.a".to_string(),
        passed: true,
        duration: Duration::ZERO,
        steps: vec![],
        error: None,
    }];
    let report = build_report(
        &outcomes,
        &catalog,
        3, // 3 skipped
        Duration::from_secs(1),
        std::path::Path::new("/tmp"),
    );
    // total = outcomes (1) + skipped (3)
    assert_eq!(report.total, 4);
    assert_eq!(report.skipped, 3);
    Ok(())
}

#[sinex_test]
async fn test_build_report_entries_have_tier() -> ::xtask::sandbox::TestResult<()> {
    use crate::commands::exercise::builders::def;
    let catalog = vec![def("t2.foo", "Foo", Tier::T2)];
    let outcomes = vec![ExerciseOutcome {
        id: "t2.foo".to_string(),
        passed: true,
        duration: Duration::from_millis(100),
        steps: vec![StepOutcome {
            label: "step1".to_string(),
            passed: true,
            exit_code: 0,
            duration: Duration::from_millis(50),
            validation_errors: vec![],
        }],
        error: None,
    }];
    let report = build_report(
        &outcomes,
        &catalog,
        0,
        Duration::from_millis(100),
        std::path::Path::new("/tmp"),
    );
    assert_eq!(report.results.len(), 1);
    assert_eq!(report.results[0].tier, "T2");
    assert_eq!(report.results[0].steps.len(), 1);
    assert_eq!(report.results[0].steps[0].label, "step1");
    Ok(())
}

#[sinex_test]
async fn test_save_output_reports_missing_directory() -> ::xtask::sandbox::TestResult<()> {
    let dir = tempdir()?;
    let missing = dir.path().join("missing");
    let output = StepOutput {
        stdout: "stdout".to_string(),
        stderr: "stderr".to_string(),
        exit_code: 0,
        duration: Duration::ZERO,
    };

    let error = save_output(&missing, "step", &output).unwrap_err();
    let message = format!("{error:#}");
    assert!(message.contains("step.stdout.log"));
    Ok(())
}

#[sinex_test]
async fn test_create_exercise_dir_creates_missing_path() -> ::xtask::sandbox::TestResult<()> {
    let dir = tempdir()?;
    let path = dir.path().join("exercise").join("nested");

    create_exercise_dir(&path)?;

    assert!(path.is_dir());
    Ok(())
}

#[sinex_test]
async fn test_create_exercise_dir_reports_parent_creation_failure()
-> ::xtask::sandbox::TestResult<()> {
    let dir = tempdir()?;
    let blocking = dir.path().join("file-parent");
    fs::write(&blocking, "blocker")?;
    let path = blocking.join("exercise");

    let error = create_exercise_dir(&path).unwrap_err();
    let message = format!("{error:#}");
    assert!(message.contains("create exercise output directory"));
    Ok(())
}

#[sinex_test]
async fn test_git_state_guard_fails_when_stash_fails() -> ::xtask::sandbox::TestResult<()> {
    let bin_dir = tempdir()?;
    write_executable_script(
        &bin_dir.path().join("git"),
        r#"#!/bin/sh
if [ "$1" = "status" ]; then
  printf ' M file.txt\n'
  exit 0
fi
if [ "$1" = "stash" ]; then
  echo "stash failed" >&2
  exit 1
fi
echo "unexpected git invocation: $*" >&2
exit 2
"#,
    )?;

    let original_path = std::env::var("PATH").unwrap_or_default();
    let mut env = EnvGuard::new();
    env.set(
        "PATH",
        format!("{}:{original_path}", bin_dir.path().display()),
    );

    let error = runner::GitStateGuard::new()
        .err()
        .expect("git state guard should fail when stash fails");
    let message = format!("{error:#}");
    assert!(message.contains("stash failed"));
    Ok(())
}

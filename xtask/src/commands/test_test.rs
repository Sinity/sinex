// Inline because these helpers are private and are exercised more directly here
// than through a full nextest command harness.
use super::*;
use crate::command::CommandContext;
use crate::history::HistoryDb;
use crate::output::{OutputFormat, OutputWriter};
use crate::sandbox::sinex_test;

fn test_context(db_path: std::path::PathBuf) -> CommandContext {
    test_context_with_invocation(db_path, None)
}

fn test_context_with_invocation(
    db_path: std::path::PathBuf,
    invocation_id: Option<i64>,
) -> CommandContext {
    CommandContext::new_with_db_override(
        OutputWriter::new(OutputFormat::Silent),
        false,
        invocation_id,
        "test",
        db_path,
    )
}

#[sinex_test]
async fn test_load_failing_test_details_surfaces_history_query_failures()
-> ::xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test.db");
    let _db = HistoryDb::open(&db_path)?;
    let conn = rusqlite::Connection::open(&db_path)?;
    conn.execute("DROP TABLE test_results", [])?;

    let (_failures, issue) =
        load_failing_test_details(&test_context_with_invocation(db_path.clone(), Some(1)), 50);
    let issue = issue.expect("query failure should surface");
    assert!(issue.contains("Failed to read failing-test details"));
    assert!(issue.contains(&db_path.display().to_string()));
    Ok(())
}

#[sinex_test]
async fn test_load_flaky_tests_surfaces_history_query_failures() -> ::xtask::sandbox::TestResult<()>
{
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test.db");
    let _db = HistoryDb::open(&db_path)?;
    let conn = rusqlite::Connection::open(&db_path)?;
    conn.execute("DROP TABLE test_results", [])?;

    let (_flaky, issue) = load_flaky_tests(&test_context(db_path.clone()), 5);
    let issue = issue.expect("query failure should surface");
    assert!(issue.contains("Failed to read flaky-test history"));
    assert!(issue.contains(&db_path.display().to_string()));
    Ok(())
}

#[sinex_test]
async fn test_nextest_history_skips_recording_without_invocation()
-> ::xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test.db");
    let db = HistoryDb::open(&db_path)?;
    let ctx = test_context(db_path);

    assert!(super::nextest_history(&ctx, &db).is_none());
    Ok(())
}

#[sinex_test]
async fn test_nextest_history_preserves_real_invocation_id() -> ::xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test.db");
    let db = HistoryDb::open(&db_path)?;
    let ctx = CommandContext::new_with_db_override(
        OutputWriter::new(OutputFormat::Silent),
        false,
        Some(42),
        "test",
        db_path,
    );

    let (_db, invocation_id) =
        super::nextest_history(&ctx, &db).expect("history should keep the real invocation id");
    assert_eq!(invocation_id, 42);
    Ok(())
}

#[sinex_test]
async fn test_vm_subcommand_disables_outer_command_timeout() -> ::xtask::sandbox::TestResult<()> {
    let command = TestCommand {
        subcommand: Some(TestSubcommand::Vm(VmArgs {
            category: Some("smoke".to_string()),
            timeout: crate::commands::vm::DEFAULT_TIMEOUT_SECS,
            keep_failed: false,
            list: false,
            validate: false,
            args: Vec::new(),
        })),
        ..Default::default()
    };

    let metadata = command.metadata();
    assert_eq!(metadata.category, Some("test"));
    assert!(metadata.timeout.is_none());
    Ok(())
}

#[sinex_test]
async fn test_effective_threads_prefers_explicit_override() -> ::xtask::sandbox::TestResult<()> {
    let command = TestCommand {
        heavy: true,
        threads: Some(9),
        ..Default::default()
    };

    assert_eq!(command.effective_threads(), Some(9));
    Ok(())
}

#[sinex_test]
async fn test_semantic_invocation_args_include_heavy_thread_cap() -> ::xtask::sandbox::TestResult<()>
{
    let command = TestCommand {
        heavy: true,
        ..Default::default()
    };

    let args = command.semantic_invocation_args(&WorkloadScope::Workspace, None, &[], false);
    assert!(args.contains(&"--heavy".to_string()));

    // The thread cap is min(available_parallelism, HEAVY_TEST_THREAD_CAP).
    // Asserting exactly "--threads=4" is brittle on machines with fewer than 4
    // logical CPUs.  Instead verify a thread arg is present and within range.
    let thread_arg = args.iter().find(|a| a.starts_with("--threads="));
    assert!(
        thread_arg.is_some(),
        "heavy invocation must include a --threads=N arg, got: {args:?}"
    );
    let n: usize = thread_arg
        .unwrap()
        .strip_prefix("--threads=")
        .unwrap()
        .parse()
        .expect("--threads= value must be numeric");
    assert!(
        (1..=HEAVY_TEST_THREAD_CAP).contains(&n),
        "--threads={n} is outside the expected range 1..={HEAVY_TEST_THREAD_CAP}"
    );
    Ok(())
}

#[sinex_test]
async fn test_semantic_invocation_args_include_cargo_features() -> ::xtask::sandbox::TestResult<()>
{
    let command = TestCommand {
        cargo_features: vec!["extra-feature".to_string()],
        ..Default::default()
    };

    let args = command.semantic_invocation_args(
        &WorkloadScope::Packages(vec!["xtask".to_string()]),
        Some("test(some_case)"),
        &[],
        true,
    );

    assert!(args.contains(&"--features=extra-feature".to_string()));
    Ok(())
}

#[sinex_test]
async fn test_semantic_invocation_args_include_nextest_test_targets()
-> ::xtask::sandbox::TestResult<()> {
    let command = TestCommand::default();

    let args = command.semantic_invocation_args(
        &WorkloadScope::Packages(vec!["sinex-e2e-tests".to_string()]),
        None,
        &["large_payload_test".to_string()],
        false,
    );

    assert!(
        args.contains(&"--test=large_payload_test".to_string()),
        "test binary selector should be part of the coordination identity: {args:?}"
    );
    Ok(())
}

#[sinex_test]
async fn test_explicit_package_scope_preserves_matching_test_binary_inference()
-> ::xtask::sandbox::TestResult<()> {
    let command = TestCommand {
        packages: vec!["sinexd".to_string()],
        filter: Some("test(weechat_descriptor_registered)".to_string()),
        ..Default::default()
    };

    let binaries = command.effective_test_binaries(command.filter.as_deref())?;

    assert_eq!(
        binaries,
        vec!["registry_dispatch_test".to_string()],
        "explicit matching package scope should still infer the exact integration-test binary"
    );
    Ok(())
}

#[sinex_test]
async fn test_explicit_package_scope_rejects_cross_package_test_binary_inference()
-> ::xtask::sandbox::TestResult<()> {
    let command = TestCommand {
        packages: vec!["xtask".to_string()],
        filter: Some("test(mcp_catalog_exactly_covers_live_tools)".to_string()),
        ..Default::default()
    };

    let binaries = command.effective_test_binaries(command.filter.as_deref())?;

    assert!(
        binaries.is_empty(),
        "explicit package scope must not infer integration-test binaries from other packages: {binaries:?}"
    );
    Ok(())
}

#[sinex_test]
async fn test_semantic_invocation_args_include_lib_target() -> ::xtask::sandbox::TestResult<()> {
    let command = TestCommand::default();

    let args = command.semantic_invocation_args(
        &WorkloadScope::Packages(vec!["sinexd".to_string()]),
        None,
        &[],
        true,
    );

    assert!(
        args.contains(&"--lib".to_string()),
        "library target selector should be part of the coordination identity: {args:?}"
    );
    Ok(())
}

#[sinex_test]
async fn test_semantic_invocation_args_include_all_scope() -> ::xtask::sandbox::TestResult<()> {
    let command = TestCommand {
        all: true,
        ..Default::default()
    };

    let args = command.semantic_invocation_args(&WorkloadScope::Workspace, None, &[], false);

    assert!(
        args.contains(&"--all".to_string()),
        "--all must be part of the proof identity: {args:?}"
    );
    Ok(())
}

#[sinex_test]
async fn test_semantic_invocation_args_include_configured_db_pool_size()
-> ::xtask::sandbox::TestResult<()> {
    let _guard = crate::sandbox::prelude::EnvGuard::set_single("SINEX_TEST_DB_POOL_SIZE", "48");
    let command = TestCommand::default();

    let args = command.semantic_invocation_args(
        &WorkloadScope::Packages(vec!["sinexd".to_string()]),
        Some("test(one)"),
        &[],
        true,
    );

    assert!(
        args.contains(&"--db-pool-size-env=48".to_string()),
        "configured DB pool size must be part of the proof identity: {args:?}"
    );
    Ok(())
}

#[sinex_test]
async fn test_semantic_invocation_args_include_runtime_binary_requirements()
-> ::xtask::sandbox::TestResult<()> {
    let command = TestCommand::default();

    let args = command.semantic_invocation_args(
        &WorkloadScope::Packages(vec!["sinex-db".to_string()]),
        None,
        &[],
        false,
    );

    assert!(
        args.contains(&"--runtime-binary=sinexd:sinexd".to_string()),
        "runtime binary requirements must be part of proof identity: {args:?}"
    );
    Ok(())
}

#[sinex_test]
async fn test_semantic_invocation_args_ignore_success_irrelevant_scheduling_flags()
-> ::xtask::sandbox::TestResult<()> {
    let command = TestCommand {
        fail_fast: true,
        ..Default::default()
    };

    let args = command.semantic_invocation_args(
        &WorkloadScope::Packages(vec!["xtask".to_string()]),
        Some("test(example)"),
        &[],
        true,
    );

    assert!(
        !args.contains(&"--fail-fast".to_string()),
        "--fail-fast affects failure scheduling, not successful proof identity: {args:?}"
    );
    Ok(())
}

#[sinex_test]
async fn test_narrow_test_db_pool_size_scales_with_exact_filter() -> ::xtask::sandbox::TestResult<()>
{
    let mut _guard = crate::sandbox::prelude::EnvGuard::new();
    _guard.clear("SINEX_TEST_DB_POOL_SIZE");
    let command = TestCommand::default();
    let plan = NextestExecutionPlan {
        runner_packages: vec!["sinexd".to_string()],
        excluded_packages: Vec::new(),
        workload_scope: WorkloadScope::Packages(vec!["sinexd".to_string()]),
    };

    assert_eq!(
        command.narrow_test_db_pool_size(&plan, Some("test(one) | test(two)"), &[], true,),
        Some(4)
    );
    assert_eq!(
        command.narrow_test_db_pool_size(&plan, Some("test(one)"), &[], true),
        Some(2)
    );

    assert_eq!(
        command.narrow_test_db_pool_size(&plan, Some("test(one)"), &[], false),
        None,
        "package-wide filtered runs should keep the normal pool unless target narrowed"
    );
    Ok(())
}

#[sinex_test]
async fn test_narrow_test_db_pool_size_skips_broad_or_configured_runs()
-> ::xtask::sandbox::TestResult<()> {
    let plan = NextestExecutionPlan {
        runner_packages: vec!["sinexd".to_string()],
        excluded_packages: Vec::new(),
        workload_scope: WorkloadScope::Packages(vec!["sinexd".to_string()]),
    };
    let broad = TestCommand {
        all: true,
        ..Default::default()
    };
    assert_eq!(
        broad.narrow_test_db_pool_size(&plan, Some("test(one)"), &[], true),
        None
    );

    let _guard = crate::sandbox::prelude::EnvGuard::set_single("SINEX_TEST_DB_POOL_SIZE", "48");
    assert_eq!(
        TestCommand::default().narrow_test_db_pool_size(&plan, Some("test(one)"), &[], true,),
        None
    );
    Ok(())
}

#[sinex_test]
async fn test_semantic_invocation_args_include_package_excludes() -> ::xtask::sandbox::TestResult<()>
{
    let command = TestCommand {
        exclude_packages: vec!["sinex-e2e-tests".to_string()],
        ..Default::default()
    };

    let args = command.semantic_invocation_args(&WorkloadScope::Workspace, None, &[], false);
    assert!(
        args.contains(&"--exclude=sinex-e2e-tests".to_string()),
        "package excludes must be part of coordination identity: {args:?}"
    );
    Ok(())
}

#[sinex_test]
async fn test_exact_package_proof_args_match_reusable_package_scope()
-> ::xtask::sandbox::TestResult<()> {
    let command = TestCommand::default();

    let args = command.exact_package_proof_args("xtask");

    assert_eq!(
        crate::coordinator::proof_kind("test", &args),
        "test.nextest.exact"
    );
    assert!(
        args.contains(&"--scope=packages:xtask".to_string()),
        "package proof args should use the same scope marker as executed package tests: {args:?}"
    );
    assert!(
        !args.iter().any(|arg| arg.starts_with("--filter=")),
        "package proof subtraction must not claim coverage of a filtered test plan: {args:?}"
    );
    Ok(())
}

#[sinex_test]
async fn test_subtract_reusable_impact_package_proofs_keeps_unproven_packages()
-> ::xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test.db");
    let db = HistoryDb::open(&db_path)?;
    let command = TestCommand::default();
    let proof_args = command.exact_package_proof_args("xtask");
    let input_fingerprint =
        crate::coordinator::current_scoped_tree_fingerprint("test", &proof_args)?;
    let scope_key = crate::coordinator::compute_scope_key("test", &proof_args);
    let invocation_id = db.start_invocation("test", None, None, None)?;
    db.record_test_proof_unit(
        invocation_id,
        "test.nextest.exact",
        &scope_key,
        &input_fingerprint,
        r#"{"scope":"packages:xtask"}"#,
        true,
    )?;
    db.finish_invocation(
        invocation_id,
        crate::history::InvocationStatus::Success,
        Some(0),
        0.1,
    )?;
    drop(db);

    let ctx = test_context(db_path);
    let (remaining, reused) = command.subtract_reusable_impact_package_proofs(
        &ctx,
        &["xtask".to_string(), "sinex-primitives".to_string()],
    )?;

    assert_eq!(remaining, vec!["sinex-primitives".to_string()]);
    assert_eq!(reused.len(), 1);
    assert_eq!(reused[0].package, "xtask");
    assert_eq!(reused[0].invocation_id, invocation_id);
    assert_eq!(reused[0].proof_kind, "test.nextest.exact");
    assert_eq!(reused[0].scope_key, scope_key);
    Ok(())
}

#[sinex_test]
async fn test_explicit_package_proof_subtraction_keeps_unproven() -> ::xtask::sandbox::TestResult<()>
{
    // When explicit -p lists two packages and only one has a reusable proof,
    // subtract_reusable_impact_package_proofs keeps the unproven package.
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test.db");
    let db = HistoryDb::open(&db_path)?;
    let command = TestCommand::default();
    let proof_args = command.exact_package_proof_args("xtask");
    let input_fingerprint =
        crate::coordinator::current_scoped_tree_fingerprint("test", &proof_args)?;
    let scope_key = crate::coordinator::compute_scope_key("test", &proof_args);
    let invocation_id = db.start_invocation("test", None, None, None)?;
    db.record_test_proof_unit(
        invocation_id,
        "test.nextest.exact",
        &scope_key,
        &input_fingerprint,
        r#"{"scope":"packages:xtask"}"#,
        true,
    )?;
    db.finish_invocation(
        invocation_id,
        crate::history::InvocationStatus::Success,
        Some(0),
        0.1,
    )?;
    drop(db);

    // Test with explicit -p via the subtraction method directly.
    // The command doesn't need packages set — the method receives them.
    let ctx = test_context(db_path);
    let (remaining, reused) = command.subtract_reusable_impact_package_proofs(
        &ctx,
        &["xtask".to_string(), "xtask-macros".to_string()],
    )?;
    assert_eq!(remaining, vec!["xtask-macros".to_string()]);
    assert_eq!(reused.len(), 1);
    assert_eq!(reused[0].package, "xtask");
    Ok(())
}

#[sinex_test]
async fn test_subtract_explicit_package_proof_all_reusable() -> ::xtask::sandbox::TestResult<()> {
    // Verify that when all explicit -p packages have reusable proofs,
    // execution is skipped without running nextest.
    let mut _env = crate::sandbox::prelude::EnvGuard::new();
    _env.clear("NEXTEST_RUN_ID");
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test.db");
    let db = HistoryDb::open(&db_path)?;
    let command = TestCommand {
        packages: vec!["xtask".to_string()],
        skip_preflight: true,
        ..Default::default()
    };
    let proof_args = command.exact_package_proof_args("xtask");
    let input_fingerprint =
        crate::coordinator::current_scoped_tree_fingerprint("test", &proof_args)?;
    let scope_key = crate::coordinator::compute_scope_key("test", &proof_args);
    let invocation_id = db.start_invocation("test", None, None, None)?;
    db.record_test_proof_unit(
        invocation_id,
        "test.nextest.exact",
        &scope_key,
        &input_fingerprint,
        r#"{"scope":"packages:xtask"}"#,
        true,
    )?;
    db.finish_invocation(
        invocation_id,
        crate::history::InvocationStatus::Success,
        Some(0),
        0.1,
    )?;
    drop(db);

    let ctx = test_context_with_invocation(db_path, Some(invocation_id));
    let result = command.execute(&ctx).await?;
    assert_eq!(result.status, crate::output::Status::Success);
    assert_eq!(
        result.message.as_deref(),
        Some("tests skipped by package proofs")
    );
    let reused: Vec<i64> = result
        .data
        .as_ref()
        .and_then(|data| data["reused_package_proofs"].as_array())
        .map(|proofs| {
            proofs
                .iter()
                .filter_map(|p| p["invocation_id"].as_i64())
                .collect()
        })
        .unwrap_or_default();
    assert_eq!(reused, vec![invocation_id]);
    Ok(())
}

#[sinex_test]
async fn test_execute_reuses_exact_test_proof_before_nextest() -> ::xtask::sandbox::TestResult<()> {
    let mut _env = crate::sandbox::prelude::EnvGuard::new();
    _env.clear("NEXTEST_RUN_ID");
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test.db");
    let db = HistoryDb::open(&db_path)?;
    let command = TestCommand {
        packages: vec!["xtask".to_string()],
        skip_preflight: true,
        ..Default::default()
    };
    let proof_args = command.semantic_invocation_args(
        &WorkloadScope::Packages(vec!["xtask".to_string()]),
        None,
        &[],
        false,
    );
    let input_fingerprint =
        crate::coordinator::current_scoped_tree_fingerprint("test", &proof_args)?;
    let scope_key = crate::coordinator::compute_scope_key("test", &proof_args);
    let invocation_id = db.start_invocation("test", None, None, None)?;
    db.record_test_proof_unit(
        invocation_id,
        "test.nextest.exact",
        &scope_key,
        &input_fingerprint,
        r#"{"scope":"packages:xtask"}"#,
        true,
    )?;
    db.finish_invocation(
        invocation_id,
        crate::history::InvocationStatus::Success,
        Some(0),
        0.1,
    )?;
    drop(db);

    let ctx = test_context(db_path);
    let result = command.execute(&ctx).await?;

    assert_eq!(result.status, crate::output::Status::Success);
    // Explicit -p packages now go through package-level subtraction first,
    // so the skip message reflects that path.
    assert!(result.message.as_deref().is_some_and(|msg| {
        msg == "tests skipped by exact proof" || msg == "tests skipped by package proofs"
    }));
    // Proof data may be in reused_proof (exact path) or reused_package_proofs (package path).
    let reused_invocation: Option<i64> = result.data.as_ref().and_then(|data| {
        data["reused_proof"]["invocation_id"].as_i64().or_else(|| {
            data["reused_package_proofs"]
                .as_array()
                .and_then(|proofs| proofs.first())
                .and_then(|p| p["invocation_id"].as_i64())
        })
    });
    assert_eq!(reused_invocation, Some(invocation_id));
    Ok(())
}

#[sinex_test]
async fn test_no_reuse_changes_test_proof_kind_to_plan() -> ::xtask::sandbox::TestResult<()> {
    let command = TestCommand {
        no_reuse: true,
        ..Default::default()
    };

    let args = command.semantic_invocation_args(
        &WorkloadScope::Packages(vec!["xtask".to_string()]),
        None,
        &[],
        false,
    );

    assert!(args.contains(&"--no-reuse".to_string()));
    assert_eq!(
        crate::coordinator::proof_kind("test", &args),
        "test.nextest.plan"
    );
    Ok(())
}

#[sinex_test]
async fn test_list_disables_exact_test_proof_reuse() -> ::xtask::sandbox::TestResult<()> {
    let command = TestCommand {
        list: true,
        packages: vec!["xtask".to_string()],
        ..Default::default()
    };

    let args = command.semantic_invocation_args(
        &WorkloadScope::Packages(vec!["xtask".to_string()]),
        None,
        &[],
        false,
    );

    assert_eq!(
        crate::coordinator::proof_kind("test", &args),
        "test.nextest.exact"
    );
    assert!(!command.can_consume_exact_test_proof());
    Ok(())
}

#[sinex_test]
async fn test_prime_disables_exact_test_proof_reuse() -> ::xtask::sandbox::TestResult<()> {
    for (flag, command) in [(
        "--prime",
        TestCommand {
            prime: true,
            packages: vec!["xtask".to_string()],
            ..Default::default()
        },
    )] {
        let args = command.semantic_invocation_args(
            &WorkloadScope::Packages(vec!["xtask".to_string()]),
            None,
            &[],
            false,
        );

        assert!(args.contains(&flag.to_string()), "{flag} missing: {args:?}");
        assert_eq!(
            crate::coordinator::proof_kind("test", &args),
            "test.nextest.plan",
            "{flag} must not produce an exact reusable proof key"
        );
        assert!(
            !command.can_consume_exact_test_proof(),
            "{flag} must bypass direct exact proof consumption"
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_preflight_mode_uses_compile_only_for_runtime_independent_tests()
-> ::xtask::sandbox::TestResult<()> {
    assert_eq!(
        super::preflight_mode_for_test_plan(false, false, &[]),
        super::TestPreflightMode::CompileOnly
    );
    Ok(())
}

#[sinex_test]
async fn test_preflight_mode_uses_runtime_stack_for_runtime_test_requirements()
-> ::xtask::sandbox::TestResult<()> {
    let requirements = [plan::RuntimeBinaryRequirement {
        package: "sinexd",
        binary: "sinexd",
    }];

    assert_eq!(
        super::preflight_mode_for_test_plan(false, false, &requirements),
        super::TestPreflightMode::RuntimeStack
    );
    Ok(())
}

#[sinex_test]
async fn test_preflight_mode_uses_runtime_stack_for_pool_priming()
-> ::xtask::sandbox::TestResult<()> {
    assert_eq!(
        super::preflight_mode_for_test_plan(false, true, &[]),
        super::TestPreflightMode::RuntimeStack
    );
    Ok(())
}

#[sinex_test]
async fn test_preflight_mode_honors_explicit_skip() -> ::xtask::sandbox::TestResult<()> {
    let requirements = [plan::RuntimeBinaryRequirement {
        package: "sinexd",
        binary: "sinexd",
    }];

    assert_eq!(
        super::preflight_mode_for_test_plan(true, true, &requirements),
        super::TestPreflightMode::Skipped
    );
    Ok(())
}

#[sinex_test]
async fn test_semantic_invocation_args_include_test_binary_args() -> ::xtask::sandbox::TestResult<()>
{
    let command = TestCommand {
        packages: vec!["xtask".to_string()],
        args: vec!["--exact".to_string(), "case-name".to_string()],
        ..Default::default()
    };

    let args = command.semantic_invocation_args(
        &WorkloadScope::Packages(vec!["xtask".to_string()]),
        None,
        &[],
        false,
    );

    assert!(
        args.contains(&"--test-arg=--exact".to_string()),
        "test binary args should be part of the proof identity: {args:?}"
    );
    assert!(
        args.contains(&"--test-arg=case-name".to_string()),
        "test binary args should be part of the proof identity: {args:?}"
    );
    assert_eq!(
        crate::coordinator::proof_kind("test", &args),
        "test.nextest.exact"
    );
    Ok(())
}

#[sinex_test]
async fn test_nextest_invocation_args_include_reuse_and_impact_flags()
-> ::xtask::sandbox::TestResult<()> {
    let command = TestCommand {
        no_reuse: true,
        impact_mode: crate::impact::ImpactMode::Aggressive,
        packages: vec!["xtask".to_string()],
        filter: Some("test(freshness_explain)".to_string()),
        cargo_features: vec!["extra-feature".to_string()],
        ..Default::default()
    };

    let args = command.nextest_invocation_args(false);

    assert!(args.contains(&"--no-reuse".to_string()));
    assert!(args.contains(&"--impact-mode=aggressive".to_string()));
    assert_eq!(
        args.windows(2)
            .find(|window| window[0] == "-p")
            .map(|window| window[1].as_str()),
        Some("xtask")
    );
    assert_eq!(
        args.windows(2)
            .find(|window| window[0] == "-E")
            .map(|window| window[1].as_str()),
        Some("test(freshness_explain)")
    );
    assert_eq!(
        args.windows(2)
            .find(|window| window[0] == "--features")
            .map(|window| window[1].as_str()),
        Some("extra-feature")
    );
    Ok(())
}

#[sinex_test]
async fn test_background_invocation_args_carry_inferred_lib_target()
-> ::xtask::sandbox::TestResult<()> {
    let command = TestCommand {
        packages: vec!["sinexd".to_string()],
        filter: Some("test(source_status)".to_string()),
        ..Default::default()
    };

    let effective_test_binaries = command.effective_test_binaries(command.filter.as_deref())?;
    let effective_lib_target =
        command.effective_lib_target(command.filter.as_deref(), &effective_test_binaries)?;
    let args = command.nextest_background_invocation_args(
        false,
        &effective_test_binaries,
        effective_lib_target,
    );

    assert!(
        effective_lib_target,
        "source_status tests live under src/ and should infer the library target"
    );
    assert!(
        args.contains(&"--lib".to_string()),
        "background execution must carry inferred --lib to avoid compiling every sinexd integration binary: {args:?}"
    );
    Ok(())
}

#[sinex_test]
async fn test_load_current_test_analysis_surfaces_current_invocation_summary()
-> ::xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test.db");
    let db = HistoryDb::open(&db_path)?;
    let invocation_id = db.start_invocation("test", None, None, None)?;
    db.finish_invocation(
        invocation_id,
        crate::history::InvocationStatus::Success,
        Some(0),
        1.0,
    )?;
    db.store_test_results(
        invocation_id,
        &[crate::history::TestResult {
            test_name: "test_alpha".into(),
            package: "pkg-a".into(),
            status: crate::history::TestStatus::Pass,
            duration_secs: Some(0.25),
            attempt: 1,
            output: None,
        }],
    )?;

    let ctx = test_context_with_invocation(db_path, Some(invocation_id));
    let (analysis, issue) = super::load_current_test_analysis(&ctx);

    assert!(issue.is_none());
    let analysis = analysis.expect("analysis should be available");
    assert_eq!(analysis["invocation_id"], invocation_id);
    assert_eq!(analysis["total_passed"], 1);
    assert_eq!(analysis["total_failed"], 0);
    Ok(())
}

#[sinex_test]
async fn test_load_current_test_analysis_requires_invocation_id() -> ::xtask::sandbox::TestResult<()>
{
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test.db");
    let _db = HistoryDb::open(&db_path)?;
    let ctx = test_context(db_path);

    let (analysis, issue) = super::load_current_test_analysis(&ctx);
    assert!(analysis.is_none());
    assert_eq!(
        issue.as_deref(),
        Some("Current test invocation ID unavailable for analysis")
    );
    Ok(())
}

#[sinex_test]
async fn test_classify_package_proof_coverage_covered() -> ::xtask::sandbox::TestResult<()> {
    let mut _env = crate::sandbox::prelude::EnvGuard::new();
    _env.clear("NEXTEST_RUN_ID");
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test.db");
    let db = HistoryDb::open(&db_path)?;
    let command = TestCommand {
        packages: vec!["xtask".to_string()],
        skip_preflight: true,
        ..Default::default()
    };
    let proof_args = command.semantic_invocation_args(
        &WorkloadScope::Packages(vec!["xtask".to_string()]),
        None,
        &[],
        false,
    );
    let input_fingerprint =
        crate::coordinator::current_scoped_tree_fingerprint("test", &proof_args)?;
    let scope_key = crate::coordinator::compute_scope_key("test", &proof_args);
    let invocation_id = db.start_invocation("test", None, None, None)?;
    db.record_test_proof_unit(
        invocation_id,
        "test.nextest.exact",
        &scope_key,
        &input_fingerprint,
        r#"{"scope":"packages:xtask"}"#,
        true,
    )?;
    db.finish_invocation(
        invocation_id,
        crate::history::InvocationStatus::Success,
        Some(0),
        0.1,
    )?;
    drop(db);

    let ctx = test_context(db_path);
    let coverage = command.classify_package_proof_coverage(&ctx, &["xtask".to_string()]);
    assert_eq!(coverage.len(), 1);
    assert_eq!(coverage[0].state, super::ProofCoverageState::Covered);
    assert_eq!(coverage[0].proof_invocation_id, Some(invocation_id));
    Ok(())
}

#[sinex_test]
async fn test_classify_package_proof_coverage_missing() -> ::xtask::sandbox::TestResult<()> {
    let mut _env = crate::sandbox::prelude::EnvGuard::new();
    _env.clear("NEXTEST_RUN_ID");
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test.db");
    let _db = HistoryDb::open(&db_path)?;
    let command = TestCommand {
        packages: vec!["xtask".to_string()],
        skip_preflight: true,
        ..Default::default()
    };
    let ctx = test_context(db_path);
    let coverage = command.classify_package_proof_coverage(&ctx, &["xtask".to_string()]);
    assert_eq!(coverage.len(), 1);
    assert_eq!(coverage[0].state, super::ProofCoverageState::Missing);
    assert!(coverage[0].proof_invocation_id.is_none());
    Ok(())
}

#[sinex_test]
async fn test_classify_package_proof_coverage_ineligible_no_reuse()
-> ::xtask::sandbox::TestResult<()> {
    let mut _env = crate::sandbox::prelude::EnvGuard::new();
    _env.clear("NEXTEST_RUN_ID");
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test.db");
    let _db = HistoryDb::open(&db_path)?;
    let command = TestCommand {
        packages: vec!["xtask".to_string()],
        skip_preflight: true,
        no_reuse: true,
        ..Default::default()
    };
    let ctx = test_context(db_path);
    let coverage = command.classify_package_proof_coverage(&ctx, &["xtask".to_string()]);
    assert_eq!(coverage.len(), 1);
    assert_eq!(coverage[0].state, super::ProofCoverageState::Ineligible);
    assert!(coverage[0].proof_invocation_id.is_none());
    Ok(())
}

#[sinex_test]
async fn test_classify_package_proof_coverage_empty_packages() -> ::xtask::sandbox::TestResult<()> {
    let mut _env = crate::sandbox::prelude::EnvGuard::new();
    _env.clear("NEXTEST_RUN_ID");
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test.db");
    let _db = HistoryDb::open(&db_path)?;
    let command = TestCommand {
        skip_preflight: true,
        ..Default::default()
    };
    let ctx = test_context(db_path);
    let coverage = command.classify_package_proof_coverage(&ctx, &[]);
    assert!(coverage.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_classify_package_proof_coverage_stale() -> ::xtask::sandbox::TestResult<()> {
    // A proof exists for the scope but with a different (old) fingerprint
    // than the current tree — it should be classified as Stale.
    let mut _env = crate::sandbox::prelude::EnvGuard::new();
    _env.clear("NEXTEST_RUN_ID");
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test.db");
    let db = HistoryDb::open(&db_path)?;
    let command = TestCommand {
        packages: vec!["xtask".to_string()],
        skip_preflight: true,
        ..Default::default()
    };
    // Record a proof with a deliberately different (stale) fingerprint
    let proof_args = command.exact_package_proof_args("xtask");
    let scope_key = crate::coordinator::compute_scope_key("test", &proof_args);
    let stale_fingerprint = "0000000000000000000000000000000000000000000000000000000000000000";
    let invocation_id = db.start_invocation("test", None, None, None)?;
    db.record_test_proof_unit(
        invocation_id,
        "test.nextest.exact",
        &scope_key,
        stale_fingerprint,
        r#"{"scope":"packages:xtask"}"#,
        true,
    )?;
    db.finish_invocation(
        invocation_id,
        crate::history::InvocationStatus::Success,
        Some(0),
        0.1,
    )?;
    drop(db);

    let ctx = test_context(db_path);
    let coverage = command.classify_package_proof_coverage(&ctx, &["xtask".to_string()]);
    assert_eq!(coverage.len(), 1);
    assert_eq!(coverage[0].state, super::ProofCoverageState::Stale);
    assert_eq!(coverage[0].proof_invocation_id, Some(invocation_id));
    Ok(())
}

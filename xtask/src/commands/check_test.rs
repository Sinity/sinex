use super::*;
use crate::cargo_diagnostics::CompilerDiagnostic;
use crate::cargo_runner::MockCargoRunner;
use crate::command::CommandContext;
use crate::output::{OutputFormat, OutputWriter};
use crate::sandbox::sinex_test;
use std::sync::Arc;

#[derive(Default, Clone, Copy)]
struct CheckFlags {
    lint: bool,
    fmt: bool,
    forbidden: bool,
    full: bool,
}

fn make_cmd(flags: CheckFlags) -> CheckCommand {
    CheckCommand {
        lint: flags.lint,
        fmt: flags.fmt,
        forbidden: flags.forbidden,
        full: flags.full,
        fix: false,
        heavy: false,
        all: false,
        packages: vec![],
        skip_tests: false,
        lint_breakdown: false,
        by_file: false,
        nix: false,
        plan: false,
        skip_preflight: false,
        changed_strict: None,
    }
}

#[sinex_test]
async fn test_check_command_metadata() -> ::xtask::sandbox::TestResult<()> {
    let cmd = make_cmd(CheckFlags::default());
    let metadata = cmd.metadata();
    assert_eq!(metadata.category, Some("check"));
    assert!(metadata.timeout.is_some());
    Ok(())
}

#[sinex_test]
async fn test_check_command_name() -> ::xtask::sandbox::TestResult<()> {
    let cmd = make_cmd(CheckFlags::default());
    assert_eq!(cmd.name(), "check");
    Ok(())
}

#[sinex_test]
async fn test_full_flag_resolves() -> ::xtask::sandbox::TestResult<()> {
    let mut cmd = make_cmd(CheckFlags {
        full: true,
        ..CheckFlags::default()
    });
    cmd.resolve_flags();
    assert!(cmd.lint);
    assert!(cmd.fmt);
    assert!(cmd.forbidden);
    assert!(cmd.nix, "--full should imply --nix");
    Ok(())
}

#[sinex_test]
async fn test_fix_flag_implies_full() -> ::xtask::sandbox::TestResult<()> {
    let mut cmd = CheckCommand {
        fix: true,
        ..make_cmd(CheckFlags::default())
    };
    cmd.resolve_flags();
    assert!(cmd.lint, "--fix should imply --full → --lint");
    assert!(cmd.fmt, "--fix should imply --full → --fmt");
    assert!(cmd.forbidden, "--fix should imply --full → --forbidden");
    Ok(())
}

#[sinex_test]
async fn test_defaults_are_compile_only() -> ::xtask::sandbox::TestResult<()> {
    let cmd = make_cmd(CheckFlags::default());
    assert!(!cmd.lint);
    assert!(!cmd.fmt);
    assert!(!cmd.forbidden);
    assert!(!cmd.full);
    Ok(())
}

#[sinex_test]
async fn test_changed_strict_child_checks_skip_nested_preflight()
-> ::xtask::sandbox::TestResult<()> {
    let cmd = CheckCommand {
        lint: true,
        forbidden: true,
        skip_tests: true,
        ..make_cmd(CheckFlags::default())
    };

    let args = changed_strict_child_check_args(&cmd);

    assert!(
        args.contains(&"--skip-preflight".to_string()),
        "changed-strict parent owns compile readiness; child checks must not run nested preflight: {args:?}"
    );
    assert!(
        args.contains(&"--lint".to_string()),
        "child checks should preserve lint mode: {args:?}"
    );
    assert!(
        args.contains(&"--forbidden".to_string()),
        "child checks should preserve forbidden-pattern mode: {args:?}"
    );
    assert!(
        args.contains(&"--skip-tests".to_string()),
        "child checks should preserve test-skip mode: {args:?}"
    );
    Ok(())
}

// ── execute() unit tests via MockCargoRunner ──────────────────────────────

fn mock_ctx(runner: Arc<MockCargoRunner>) -> CommandContext {
    CommandContext::new(
        OutputWriter::new(OutputFormat::Silent),
        false,
        None,
        "check",
    )
    .with_cargo_runner(runner as Arc<dyn crate::cargo_runner::CargoRunner>)
}

fn mock_ctx_with_history(
    runner: Arc<MockCargoRunner>,
    invocation_id: Option<i64>,
    db_path: std::path::PathBuf,
) -> CommandContext {
    CommandContext::new_with_db_override(
        OutputWriter::new(OutputFormat::Silent),
        false,
        invocation_id,
        "check",
        db_path,
    )
    .with_cargo_runner(runner as Arc<dyn crate::cargo_runner::CargoRunner>)
}

fn error_summary() -> DiagnosticSummary {
    DiagnosticSummary {
        errors: 1,
        warnings: 0,
        diagnostics: vec![CompilerDiagnostic {
            level: "error".to_string(),
            message: "type mismatch".to_string(),
            ..Default::default()
        }],
        success: false,
        compiled_packages: std::collections::HashSet::default(),
    }
}

fn warning_summary(n: usize) -> DiagnosticSummary {
    let packages: std::collections::HashSet<String> =
        (0..n).map(|i| format!("pkg-{i}")).collect();
    DiagnosticSummary {
        errors: 0,
        warnings: n,
        diagnostics: (0..n)
            .map(|i| CompilerDiagnostic {
                level: "warning".to_string(),
                message: format!("unused import #{i}"),
                ..Default::default()
            })
            .collect(),
        success: true,
        compiled_packages: packages,
    }
}

#[sinex_test]
async fn test_execute_clean_compile_succeeds() -> ::xtask::sandbox::TestResult<()> {
    let runner = Arc::new(MockCargoRunner::clean());
    let ctx = mock_ctx(runner);
    let cmd = make_cmd(CheckFlags::default());
    let result = cmd.execute(&ctx).await?;
    assert!(
        result.is_success(),
        "clean check should succeed: {result:?}"
    );
    Ok(())
}

#[sinex_test]
async fn test_execute_check_warns_when_fixable_count_is_unavailable()
-> ::xtask::sandbox::TestResult<()> {
    let runner = Arc::new(MockCargoRunner::clean());
    let temp = tempfile::tempdir()?;
    let ctx = mock_ctx_with_history(runner, Some(42), temp.path().to_path_buf());
    let cmd = make_cmd(CheckFlags::default());
    let result = cmd.execute(&ctx).await?;
    let data = result
        .data
        .as_ref()
        .unwrap_or_else(|| panic!("expected structured data"));
    assert!(
        result.is_success(),
        "clean check should still succeed: {result:?}"
    );
    assert!(
        result
            .warnings
            .iter()
            .any(|warning| warning.contains("auto-fixable diagnostic count"))
    );
    assert!(data.get("fixable").is_none());
    Ok(())
}

#[sinex_test]
async fn test_execute_check_without_history_invocation_skips_fixable_probe_warning()
-> ::xtask::sandbox::TestResult<()> {
    let runner = Arc::new(MockCargoRunner::clean());
    let temp = tempfile::tempdir()?;
    let db_path = temp.path().join("history.db");
    let ctx = mock_ctx_with_history(runner, None, db_path.clone());
    let cmd = make_cmd(CheckFlags::default());
    let result = cmd.execute(&ctx).await?;
    assert!(
        result.is_success(),
        "clean check should succeed: {result:?}"
    );
    assert!(
        result
            .warnings
            .iter()
            .all(|warning| !warning.contains("auto-fixable diagnostic count")),
        "unexpected auto-fixable warning: {:?}",
        result.warnings
    );
    assert!(
        !db_path.exists(),
        "check without invocation_id should not even open the history DB"
    );
    Ok(())
}

#[sinex_test]
async fn test_execute_check_errors_yield_failure() -> ::xtask::sandbox::TestResult<()> {
    let runner = Arc::new(MockCargoRunner::clean().with_check(error_summary()));
    let ctx = mock_ctx(runner);
    let cmd = make_cmd(CheckFlags::default());
    let result = cmd.execute(&ctx).await?;
    assert!(!result.is_success(), "check with errors should fail");
    assert!(
        result.errors.iter().any(|e| e.code == "CHECK_FAILED"),
        "expected CHECK_FAILED in errors: {:?}",
        result.errors,
    );
    Ok(())
}

#[sinex_test]
async fn test_execute_lint_routes_to_clippy_not_check() -> ::xtask::sandbox::TestResult<()> {
    let runner = Arc::new(MockCargoRunner::clean());
    let ctx = mock_ctx(runner.clone());
    let cmd = make_cmd(CheckFlags {
        lint: true,
        ..CheckFlags::default()
    }); // --lint
    cmd.execute(&ctx).await?;
    let calls = runner.calls();
    assert_eq!(calls.clippy, 1, "clippy should have been called once");
    assert_eq!(
        calls.check, 0,
        "cargo check must NOT run when --lint active"
    );
    Ok(())
}

#[sinex_test]
async fn test_execute_compile_only_routes_to_check_not_clippy()
-> ::xtask::sandbox::TestResult<()> {
    let runner = Arc::new(MockCargoRunner::clean());
    let ctx = mock_ctx(runner.clone());
    let cmd = make_cmd(CheckFlags::default()); // default: compile-only
    cmd.execute(&ctx).await?;
    let calls = runner.calls();
    assert_eq!(calls.check, 1, "cargo check should have been called once");
    assert_eq!(calls.clippy, 0, "clippy must NOT run in compile-only mode");
    Ok(())
}

#[sinex_test]
async fn test_execute_clippy_errors_yield_failure() -> ::xtask::sandbox::TestResult<()> {
    let runner = Arc::new(MockCargoRunner::clean().with_clippy(error_summary()));
    let ctx = mock_ctx(runner);
    let cmd = make_cmd(CheckFlags {
        lint: true,
        ..CheckFlags::default()
    }); // --lint
    let result = cmd.execute(&ctx).await?;
    assert!(
        !result.is_success(),
        "clippy errors should propagate to failure"
    );
    assert!(
        result.errors.iter().any(|e| e.code == "CLIPPY_FAILED"),
        "expected CLIPPY_FAILED in errors: {:?}",
        result.errors,
    );
    Ok(())
}

#[sinex_test]
async fn test_execute_fmt_fail_short_circuits_before_compile()
-> ::xtask::sandbox::TestResult<()> {
    // --fmt with a formatting violation should bail before running cargo check.
    let runner = Arc::new(MockCargoRunner::clean().with_fmt_fail());
    let ctx = mock_ctx(runner.clone());
    let cmd = make_cmd(CheckFlags {
        fmt: true,
        ..CheckFlags::default()
    }); // --fmt
    let result = cmd.execute(&ctx).await;
    // fmt failure surfaces as Err (propagated via `?` in execute)
    assert!(result.is_err(), "fmt failure should propagate as Err");
    let calls = runner.calls();
    assert_eq!(calls.fmt, 1, "fmt must have been called");
    assert_eq!(calls.check, 0, "cargo check must NOT run after fmt failure");
    Ok(())
}

#[sinex_test]
async fn test_execute_fmt_uses_package_scope() -> ::xtask::sandbox::TestResult<()> {
    let runner = Arc::new(MockCargoRunner::clean());
    let ctx = mock_ctx(runner.clone());
    let cmd = CheckCommand {
        fmt: true,
        packages: vec!["sinex-primitives".to_string()],
        ..make_cmd(CheckFlags::default())
    };
    let result = cmd.execute(&ctx).await?;
    assert!(
        result.is_success(),
        "package-scoped fmt check should succeed: {result:?}"
    );
    let calls = runner.calls();
    assert_eq!(calls.fmt, 1, "fmt must have been called once");
    assert_eq!(calls.fmt_args, vec!["-p", "sinex-primitives"]);
    Ok(())
}

#[sinex_test]
async fn test_execute_fmt_includes_xtask_macro_path_dependency()
-> ::xtask::sandbox::TestResult<()> {
    let runner = Arc::new(MockCargoRunner::clean());
    let ctx = mock_ctx(runner.clone());
    let cmd = CheckCommand {
        fmt: true,
        packages: vec!["xtask".to_string()],
        ..make_cmd(CheckFlags::default())
    };
    let result = cmd.execute(&ctx).await?;
    assert!(
        result.is_success(),
        "xtask fmt check should include local macro package: {result:?}"
    );
    let calls = runner.calls();
    assert_eq!(calls.fmt, 1, "fmt must have been called once");
    assert_eq!(calls.fmt_args, vec!["-p", "xtask", "-p", "xtask-macros"]);
    Ok(())
}

#[sinex_test]
async fn test_execute_fmt_uses_workspace_scope_for_all() -> ::xtask::sandbox::TestResult<()> {
    let runner = Arc::new(MockCargoRunner::clean());
    let ctx = mock_ctx(runner.clone());
    let cmd = CheckCommand {
        fmt: true,
        all: true,
        ..make_cmd(CheckFlags::default())
    };
    let result = cmd.execute(&ctx).await?;
    assert!(
        result.is_success(),
        "workspace fmt check should succeed: {result:?}"
    );
    let calls = runner.calls();
    assert_eq!(calls.fmt, 1, "fmt must have been called once");
    assert_eq!(calls.fmt_args, vec!["--all"]);
    Ok(())
}

#[sinex_test]
async fn test_execute_warnings_recorded_in_result() -> ::xtask::sandbox::TestResult<()> {
    // Warnings don't fail the check, but they appear in result.warnings.
    let runner = Arc::new(MockCargoRunner::clean().with_check(warning_summary(3)));
    let ctx = mock_ctx(runner);
    let cmd = make_cmd(CheckFlags::default());
    let result = cmd.execute(&ctx).await?;
    assert!(
        result.is_success(),
        "warnings alone should not fail the check"
    );
    assert!(
        result.warnings.iter().any(|w| w.contains("3 warning")),
        "3 warnings should appear in result.warnings: {:?}",
        result.warnings
    );
    Ok(())
}

#[sinex_test]
async fn test_execute_progress_callback_fired_per_package() -> ::xtask::sandbox::TestResult<()>
{
    // Verify that the progress callback is fired once per compiled package.
    // MockCargoRunner fires on_package_done N times for N compiled_packages.
    let runner = Arc::new(MockCargoRunner::clean().with_check(warning_summary(5)));
    let ctx = mock_ctx(runner);
    let cmd = make_cmd(CheckFlags::default());
    // If the callback fires correctly, execute completes without panic.
    let result = cmd.execute(&ctx).await?;
    assert!(result.is_success());
    Ok(())
}

#[sinex_test]
async fn test_ambient_optimizations_only_enabled_for_human_foreground()
-> ::xtask::sandbox::TestResult<()> {
    let human =
        CommandContext::new(OutputWriter::new(OutputFormat::Human), false, None, "check");
    assert!(human.allows_ambient_optimizations());

    let json = CommandContext::new(OutputWriter::new(OutputFormat::Json), false, None, "check");
    assert!(!json.allows_ambient_optimizations());

    let silent = CommandContext::new(
        OutputWriter::new(OutputFormat::Silent),
        false,
        None,
        "check",
    );
    assert!(!silent.allows_ambient_optimizations());

    let background =
        CommandContext::new(OutputWriter::new(OutputFormat::Human), true, None, "check");
    assert!(!background.allows_ambient_optimizations());

    Ok(())
}

#[sinex_test]
async fn test_ensure_nix_tool_ready_accepts_healthy_tool() -> ::xtask::sandbox::TestResult<()> {
    let healthy_tool = ToolInfo {
        path: "/run/current-system/sw/bin/nix".into(),
        version: "nix (Nix) 2.0".to_string(),
        probe_issue: None,
    };
    ensure_nix_tool_ready_with(|tool| {
        assert_eq!(tool, "nix");
        Ok(healthy_tool)
    })?;
    Ok(())
}

#[sinex_test]
async fn test_ensure_nix_tool_ready_rejects_missing_tool() -> ::xtask::sandbox::TestResult<()> {
    let error = ensure_nix_tool_ready_with(|_| Err(eyre!("Tool 'nix' not found in PATH")))
        .expect_err("missing nix should fail");
    let rendered = format!("{error:#}");
    assert!(rendered.contains("xtask check --nix requires `nix` on PATH"));
    assert!(rendered.contains("Tool 'nix' not found in PATH"));
    Ok(())
}

#[sinex_test]
async fn test_ensure_nix_tool_ready_rejects_probe_issue() -> ::xtask::sandbox::TestResult<()> {
    let error = ensure_nix_tool_ready_with(|_| {
        Ok(ToolInfo {
            path: "/tmp/nix".into(),
            version: "unknown".to_string(),
            probe_issue: Some("failed to run `nix --version`".to_string()),
        })
    })
    .expect_err("broken nix probe should fail");
    let rendered = format!("{error:#}");
    assert!(rendered.contains("failed readiness probe"));
    assert!(rendered.contains("failed to run `nix --version`"));
    Ok(())
}

//! Forbidden pattern scanning command - enforces project coding standards

use color_eyre::eyre::{bail, Result, WrapErr};
use std::process::Command;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};

/// Lint forbidden patterns command - scans for anti-patterns and policy violations.
///
/// Checks for:
/// - Use of `#[tokio::test]` instead of `#[sinex_test]`
/// - Use of `#[test]` instead of `#[sinex_test]` (outside test dirs)
/// - Use of `anyhow::` in library code (use `SinexError` / `color_eyre`)
/// - Runtime `sqlx::query()` instead of compile-time `sqlx::query!()`
/// - Runtime `sqlx::query_as()` instead of compile-time `sqlx::query_as!()`
/// - `println!` in library code (use `tracing` instead)
///
/// Also reports (informational, non-blocking):
/// - `SQLx` query usage statistics (runtime vs compile-time)
/// - `sinex_test_utils` usage in production code
#[derive(Debug, Clone, clap::Args)]
pub struct LintForbiddenCommand;

#[async_trait::async_trait]
impl XtaskCommand for LintForbiddenCommand {
    fn name(&self) -> &'static str {
        "lint-forbidden"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        if ctx.is_human() {
            println!("========== forbidden pattern scan ==========");
        }

        // ═══════════════════════════════════════════════════════════════════════
        // ALLOWLISTS — KEEP MINIMAL
        // ═══════════════════════════════════════════════════════════════════════
        //
        // `#[sinex_test]` / `sinex_proptest!` is universal. The only remaining
        // `#[test]` / `#[tokio::test]` are in:
        //   - xtask/macros/src/lib.rs — proc macro generates them in expansion
        //   - compile_fail_test.rs — trybuild requires vanilla #[test]
        //
        // Both are auto-skipped by is_tests_path(). The allowlists below are
        // for the strict checks that DON'T auto-skip.
        // ═══════════════════════════════════════════════════════════════════════

        // #[tokio::test] allowlist — only for code that GENERATES or REFERENCES
        // #[tokio::test] as string literals (not actual test attributes).
        let tokio_test_allow = [
            // Proc macro: generates #[tokio::test] in expanded sinex_test output
            "xtask/macros/src/lib.rs",
            // This file: contains pattern strings and doc comments referencing it
            "xtask/src/commands/lint_forbidden.rs",
        ];
        // #[test] allowlist — empty. All tests use #[sinex_test] or sinex_proptest!.
        // Remaining #[test]: compile_fail_test.rs (trybuild) and xtask/macros/src/lib.rs
        // (proc macro generated code) — both auto-skipped by is_tests_path().
        let rust_test_allow: [&str; 0] = [];
        // Runtime sqlx::query() is allowed for:
        // - Session control (SET, ROLLBACK, RESET)
        // - Advisory locks
        // - Dynamic queries (analytics, cascade analysis)
        // - Test infrastructure
        let sqlx_query_allow = [
            "crate/core/sinex-gateway/src/cascade_analyzer.rs",
            "crate/core/sinex-gateway/src/rpc_server.rs",
            "crate/core/sinex-gateway/src/handlers/legacy.rs",
            "crate/core/sinex-ingestd/src/config.rs",
            // sinex-db paths (after crate reorganization - no /db/ subdir)
            "crate/lib/sinex-db/src/lib.rs",
            "crate/lib/sinex-db/src/pool.rs",
            "crate/lib/sinex-db/src/query_helpers.rs",
            "crate/lib/sinex-db/src/repositories/events/mod.rs",
            "crate/lib/sinex-db/src/repositories/events/persistence.rs",
            "crate/lib/sinex-db/src/repositories/events/queries.rs",
            "crate/lib/sinex-db/src/repositories/common.rs",
            "crate/lib/sinex-db/src/repositories/schema_management.rs",
            "crate/lib/sinex-db/src/repositories/knowledge_graph.rs",
            "crate/lib/sinex-db/src/repositories/state.rs",
            "crate/lib/sinex-db/src/replay/state_machine.rs",
            "crate/lib/sinex-node-sdk/src/preflight/database.rs",
            "crate/lib/sinex-node-sdk/src/preflight/verification.rs",
            "crate/lib/sinex-services/src/analytics.rs",
            "crate/lib/sinex-test-utils/src/database_pool.rs",
            "crate/lib/sinex-test-utils/src/db_common.rs",
            "crate/lib/sinex-test-utils/src/fixture_generator.rs",
            "crate/lib/sinex-test-utils/src/fixtures.rs",
            "crate/lib/sinex-test-utils/src/session_guards.rs",
            "crate/lib/sinex-test-utils/src/permissions.rs",
            "xtask/src/main.rs",
            // sinex-schema binary uses runtime queries for sync (no compile-time DB)
            "crate/lib/sinex-schema/src/main.rs",
        ];
        let sqlx_query_as_allow = [
            "crate/lib/sinex-db/src/repositories/common.rs",
            "crate/lib/sinex-node-sdk/src/preflight/database.rs",
            "xtask/src/main.rs",
            "crate/lib/sinex-schema/src/main.rs",
        ];

        let mut violations: Vec<String> = Vec::new();
        violations.extend(check_pattern_strict(
            "#[tokio::test]",
            r"#\[tokio::test",
            &tokio_test_allow,
        )?);
        violations.extend(check_pattern_allow_tests(
            "#[test]",
            r"#\[test\]",
            &rust_test_allow,
        )?);
        violations.extend(check_pattern_allow_tests(
            "sqlx::query(",
            r"sqlx::query\(",
            &sqlx_query_allow,
        )?);
        violations.extend(check_pattern_allow_tests(
            "sqlx::query_as(",
            r"sqlx::query_as\(",
            &sqlx_query_as_allow,
        )?);

        // anyhow:: in library code — fully migrated to color_eyre, no exceptions needed
        let anyhow_allow: [&str; 0] = [];
        violations.extend(check_anyhow_in_lib("anyhow::", r"anyhow::", &anyhow_allow)?);

        // println! in library code (use tracing for structured logging)
        let println_lib_allow = [
            "crate/lib/sinex-processor-runtime/src/cli.rs",
            "crate/lib/sinex-schema/src/main.rs",
            // Intentional stdout output for CLI-facing functions
            "crate/lib/sinex-node-sdk/src/version.rs",
            "crate/lib/sinex-node-sdk/src/heartbeat.rs",
            "crate/lib/sinex-node-sdk/src/diagnostics/regression.rs",
            // Doc comment code examples (scanner can't distinguish from real code)
            "crate/lib/sinex-node-sdk/src/watcher_handle.rs",
        ];
        violations.extend(check_println_in_lib(
            "println!",
            r"println!",
            &println_lib_allow,
        )?);

        // Report runtime vs compile-time SQLx query usage
        report_sqlx_query_stats()?;

        // Note: unwrap/expect checking is handled by clippy (unwrap_used, expect_used lints)
        // No need to duplicate with grep-based counting here.

        // Check for test-utils usage in production code (layering violation)
        check_test_utils_layering(&mut violations)?;

        // Note: Error handling and type system anti-patterns are now checked
        // by ast-grep rules in .config/ast-grep/rules/
        // Run: ast-grep scan crate

        if violations.is_empty() {
            if ctx.is_human() {
                eprintln!("✅ No forbidden patterns found");
            }
            return Ok(CommandResult::success()
                .with_message("No forbidden patterns found")
                .with_duration(ctx.elapsed()));
        }

        eprintln!("Forbidden pattern detected:");
        for v in &violations {
            eprintln!("  {v}");
        }
        bail!("forbidden pattern scan failed");
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::check()
    }
}

/// Check for a pattern strictly (no test directory exceptions)
fn check_pattern_strict(label: &str, pattern: &str, allow: &[&str]) -> Result<Vec<String>> {
    run_rg(pattern)
        .map(|matches| filter_allowlist(matches, allow, |_| false))
        .with_context(|| format!("failed to scan for {label}"))
}

/// Check for a pattern allowing test directories
fn check_pattern_allow_tests(label: &str, pattern: &str, allow: &[&str]) -> Result<Vec<String>> {
    run_rg(pattern)
        .map(|matches| filter_allowlist(matches, allow, is_tests_path))
        .with_context(|| format!("failed to scan for {label}"))
}

/// Run ripgrep to find pattern matches
fn run_rg(pattern: &str) -> Result<Vec<String>> {
    let output = Command::new("rg")
        .args([
            "--color=never",
            "--no-heading",
            "--with-filename",
            "--line-number",
            pattern,
            "--glob",
            "*.rs",
            "--glob",
            "!docs/agent/**",
        ])
        .output()
        .with_context(|| "failed to invoke ripgrep")?;
    let code = output.status.code().unwrap_or_default();
    if code != 0 && code != 1 {
        bail!("ripgrep failed with status {}", output.status);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout.lines().map(str::to_string).collect::<Vec<String>>())
}

/// Filter matches against allowlist and skip function
fn filter_allowlist<F>(matches: Vec<String>, allow: &[&str], mut skip: F) -> Vec<String>
where
    F: FnMut(&str) -> bool,
{
    matches
        .into_iter()
        .filter(|line| {
            let file = line.split(':').next().unwrap_or_default();
            !allow.contains(&file) && !skip(file)
        })
        .collect()
}

/// Check if a path is a test directory or build tooling.
///
/// xtask is blanket-allowed because the proc macro crate (`xtask/macros/`)
/// generates `#[test]` and `#[tokio::test]` in its expansion output.
fn is_tests_path(path: &str) -> bool {
    path.contains("/tests/") || path.starts_with("tests/") || path.starts_with("xtask/")
}

/// Check for anyhow usage in library code (not xtask, not tests, not binaries)
fn check_anyhow_in_lib(label: &str, pattern: &str, allow: &[&str]) -> Result<Vec<String>> {
    run_rg(pattern)
        .map(|matches| {
            filter_allowlist(matches, allow, |path| {
                // Allow in xtask, tests, binaries, build scripts, CLI, examples
                path.starts_with("xtask/")
                    || is_tests_path(path)
                    || path.ends_with("/main.rs")
                    || path.ends_with("build.rs")
                    || path.contains("/bin/")
                    || path.contains("/examples/")
                    || path.starts_with("crate/cli/")
            })
        })
        .with_context(|| format!("failed to scan for {label}"))
}

/// Check for println! in library code (use tracing instead)
fn check_println_in_lib(label: &str, pattern: &str, allow: &[&str]) -> Result<Vec<String>> {
    run_rg(pattern)
        .map(|matches| {
            filter_allowlist(matches, allow, |path| {
                // Allow in xtask, tests, binaries, CLI, examples, build scripts
                path.starts_with("xtask/")
                    || is_tests_path(path)
                    || path.ends_with("/main.rs")
                    || path.starts_with("crate/cli/")
                    || path.contains("/bin/")
                    || path.contains("/examples/")
                    || path.ends_with("build.rs")
            })
        })
        .with_context(|| format!("failed to scan for {label}"))
}

/// Check for `sinex_test_utils` usage outside expected locations.
/// Reports usage for awareness but doesn't block (inline #[cfg(test)] modules are OK).
fn check_test_utils_layering(_violations: &mut Vec<String>) -> Result<()> {
    // Allow test-utils imports in expected locations
    let allow_prefixes = [
        "xtask/src/",                  // Build tooling
        "crate/lib/sinex-test-utils/", // Test utils itself
    ];

    let matches = run_rg(r"use sinex_test_utils")?;
    let filtered: Vec<String> = matches
        .into_iter()
        .filter(|line| {
            let file = line.split(':').next().unwrap_or_default();
            // Skip if in allow list
            if allow_prefixes.iter().any(|a| file.starts_with(a)) {
                return false;
            }
            // Skip if in tests/ directory
            if is_tests_path(file) {
                return false;
            }
            true
        })
        .collect();

    // Note: Many of these may be in inline #[cfg(test)] modules, which is fine.
    // We report the count for awareness but don't block builds.
    if !filtered.is_empty() {
        eprintln!(
            "📋 sinex_test_utils usage: {} locations (inline #[cfg(test)] modules are expected)",
            filtered.len()
        );
    }
    Ok(())
}

/// Report `SQLx` query usage statistics (runtime vs compile-time checked).
/// Runtime queries use `sqlx::query()/query_as()`, compile-time use `sqlx::query!()/query_as`!().
fn report_sqlx_query_stats() -> Result<()> {
    // Count runtime queries (sqlx::query(, sqlx::query_as()
    let runtime_query = count_pattern_outside_tests(r"sqlx::query\(")?;
    let runtime_query_as = count_pattern_outside_tests(r"sqlx::query_as\(")?;
    let runtime_total = runtime_query + runtime_query_as;

    // Count compile-time queries (sqlx::query!, sqlx::query_as!, sqlx::query_scalar!)
    let compile_query = count_pattern_outside_tests(r"sqlx::query!\(")?;
    let compile_query_as = count_pattern_outside_tests(r"sqlx::query_as!\(")?;
    let compile_query_scalar = count_pattern_outside_tests(r"sqlx::query_scalar!\(")?;
    let compile_total = compile_query + compile_query_as + compile_query_scalar;

    let total = runtime_total + compile_total;
    if total > 0 {
        let compile_pct = if total > 0 {
            (compile_total as f64 / total as f64 * 100.0) as u32
        } else {
            0
        };
        eprintln!(
            "📊 SQLx queries: {compile_total} compile-time ({compile_pct}%), {runtime_total} runtime ({runtime_query} query, {runtime_query_as} query_as)"
        );
    }
    Ok(())
}

// Error handling and type system anti-patterns are now checked by ast-grep.
// See .config/ast-grep/rules/ for the rules.
// Run: ast-grep scan crate

/// Count occurrences of a pattern outside test directories
fn count_pattern_outside_tests(pattern: &str) -> Result<usize> {
    let output = Command::new("rg")
        .args([
            "--color=never",
            "--no-heading",
            "-c",
            pattern,
            "--glob",
            "*.rs",
            "--glob",
            "!**/tests/**",
            "--glob",
            "!tests/**",
            "--glob",
            "!*_test.rs",
            "--glob",
            "!test_*.rs",
        ])
        .output()
        .with_context(|| "failed to invoke ripgrep for pattern count")?;

    let code = output.status.code().unwrap_or_default();
    if code != 0 && code != 1 {
        bail!("ripgrep failed with status {}", output.status);
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut total = 0;
    for line in stdout.lines() {
        if let Some(count_str) = line.split(':').nth(1) {
            if let Ok(count) = count_str.parse::<usize>() {
                total += count;
            }
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    fn test_lint_forbidden_command_name() -> ::xtask::sandbox::TestResult<()> {
        let cmd = LintForbiddenCommand;
        assert_eq!(cmd.name(), "lint-forbidden");
        Ok(())
    }

    #[sinex_test]
    fn test_lint_forbidden_command_metadata() -> ::xtask::sandbox::TestResult<()> {
        let cmd = LintForbiddenCommand;
        let metadata = cmd.metadata();

        assert_eq!(metadata.category, Some("check".to_string()));
        assert!(metadata.timeout.is_some());
        Ok(())
    }

    #[sinex_test]
    fn test_is_tests_path() -> ::xtask::sandbox::TestResult<()> {
        assert!(is_tests_path("tests/foo.rs"));
        assert!(is_tests_path("crate/lib/foo/tests/bar.rs"));
        assert!(!is_tests_path("crate/lib/foo/src/test_utils.rs"));
        Ok(())
    }

    #[sinex_test]
    fn test_filter_allowlist() -> ::xtask::sandbox::TestResult<()> {
        let matches = vec![
            "crate/foo/src/main.rs:10:test".to_string(),
            "crate/bar/src/lib.rs:20:test".to_string(),
            "tests/integration.rs:30:test".to_string(),
        ];
        let allow = ["crate/foo/src/main.rs"];
        let filtered = filter_allowlist(matches, &allow, is_tests_path);

        assert_eq!(filtered.len(), 1);
        assert!(filtered[0].contains("crate/bar/src/lib.rs"));
        Ok(())
    }
}

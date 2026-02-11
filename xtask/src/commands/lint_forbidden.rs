//! Forbidden pattern scanning command - enforces project coding standards

use anyhow::{bail, Context, Result};
use std::process::Command;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};

/// Lint forbidden patterns command - scans for anti-patterns and policy violations.
///
/// Checks for:
/// - Use of `#[tokio::test]` instead of `#[sinex_test]`
/// - Use of `#[test]` in non-test code
/// - Runtime `sqlx::query()` instead of compile-time `sqlx::query!()`
/// - Runtime `sqlx::query_as()` instead of compile-time `sqlx::query_as!()`
///
/// Also reports (informational, non-blocking):
/// - Count of unwrap/expect calls in production code
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
        // TEST ATTRIBUTE ALLOWLISTS — ONLY FOR IMPOSSIBLE CASES
        // ═══════════════════════════════════════════════════════════════════════
        //
        // `#[sinex_test]` is UNIVERSAL. If a test doesn't need TestContext, just
        // don't take it as an argument — the macro supports this.
        //
        // These allowlists are ONLY for cases where `#[sinex_test]` literally
        // cannot be used:
        // - Testing the test infrastructure itself (sinex-test-utils)
        // - External tools (xtask) that don't have sandbox access
        // - Bootstrap code that runs before sandbox is available
        //
        // If you're adding a file here, you're probably doing something wrong.
        // ═══════════════════════════════════════════════════════════════════════

        // Async tests that don't require database/NATS isolation
        let tokio_test_allow = [
            "crate/lib/sinex-test-utils/macros/src/lib.rs",
            "crate/lib/sinex-test-utils/tests/rstest_integration_example.rs",
            "crate/lib/sinex-test-utils/tests/database_pool_tests.rs",
            "crate/lib/sinex-test-utils/tests/channel_backpressure_test.rs",
            "crate/lib/sinex-test-utils/tests/select_cancellation_test.rs",
            "crate/core/sinex-ingestd/src/service.rs",
            "crate/lib/sinex-node-sdk/src/lifecycle.rs",
            "crate/lib/sinex-node-sdk/src/shutdown.rs",
            "crate/lib/sinex-node-sdk/src/watcher_handle.rs",
            "crate/lib/sinex-node-sdk/src/schema_validator.rs",
            "crate/lib/sinex-node-sdk/examples/git_activity_detector.rs",
            "crate/cli/tests/retry_tests.rs",
            "crate/cli/tests/gateway_client_tests.rs",
            "crate/cli/tests/mock_client_tests.rs",
            "crate/cli/tests/common/mock_client.rs",
            "crate/cli/src/fmt/progress.rs",
            "xtask/src/main.rs",
            "xtask/src/command.rs",
            "xtask/src/commands/lint_forbidden.rs",
            "xtask/src/commands/contracts.rs",
            "xtask/src/commands/fuzz.rs",
            "xtask/src/sandbox/fs/resources.rs",
            "xtask/tests/command_edge_cases.rs",
            "xtask/tests/test_commands.rs",
            "xtask/macros/src/lib.rs",
        ];
        // Pure sync `#[test]` allowed for unit tests that are:
        // - In-memory only (no DB, no NATS, no network)
        // - Synchronous (no async runtime needed)
        // - Testing pure functions, parsing, validation, serialization
        let rust_test_allow = [
            "crate/lib/sinex-test-utils/macros/src/lib.rs",
            "crate/nodes/sinex-desktop-node/src/window_manager.rs",
            "crate/nodes/sinex-desktop-ingestor/src/window_manager.rs",
            // sinex-db paths (after crate reorganization)
            "crate/lib/sinex-db/src/sanitization.rs",
            "crate/lib/sinex-db/src/models/event.rs",
            "crate/lib/sinex-db/src/query_helpers.rs",
            // sinex-primitives paths (after crate reorganization - no /types/ subdir)
            "crate/lib/sinex-primitives/src/testing.rs", // proptest! macro generates #[test]
            "crate/lib/sinex-primitives/src/units.rs",
            "crate/lib/sinex-primitives/src/error.rs",
            "crate/lib/sinex-primitives/src/validation/query_validation.rs",
            "crate/lib/sinex-primitives/src/utils/json_helpers.rs",
            "crate/lib/sinex-primitives/src/utils/timestamp_helpers.rs",
            "crate/lib/sinex-node-sdk/src/version.rs",
            "crate/lib/sinex-test-utils/src/property_testing.rs",
            "crate/lib/sinex-test-utils/src/static_fixtures.rs",
            "crate/lib/sinex-test-utils/src/test_hooks.rs",
            "crate/core/sinex-ingestd/src/material_assembler.rs",
            "crate/core/sinex-ingestd/src/material_assembler/state.rs",
            "crate/core/sinex-gateway/src/native_messaging.rs",
            "crate/core/sinex-gateway/src/rpc_server.rs",
            "crate/core/sinex-gateway/src/rate_limit.rs",
            "crate/core/sinex-gateway/src/gateway_metrics.rs",
            "crate/lib/sinex-schema/src/schema_registry.rs",
            "crate/lib/sinex-test-utils/src/cleanup_config.rs",
            "crate/lib/sinex-test-utils/src/permissions.rs",
            "crate/lib/sinex-node-sdk/src/schema_validator.rs",
            "crate/lib/sinex-node-sdk/src/simple_node.rs",
            "crate/lib/sinex-node-sdk/src/health_reporter.rs",
            "crate/lib/sinex-node-sdk/src/self_observation.rs",
            "crate/lib/sinex-node-sdk/src/shutdown.rs",
            "crate/lib/sinex-node-sdk/src/automaton_base.rs",
            "crate/tools/sx/src/build.rs",
            "crate/tools/sx/src/tether.rs",
            "crate/tools/sx/src/generate.rs",
            "crate/tools/sx/src/watcher.rs",
            "crate/nodes/sinex-terminal-ingestor/src/secret_redaction.rs",
            "crate/nodes/sinex-terminal-ingestor/src/fish_history.rs",
            "crate/cli/src/validation.rs",
            "crate/cli/src/error.rs",
            "crate/cli/src/fmt/syntax.rs",
            "crate/cli/src/fmt/output.rs",
            "crate/cli/src/fmt/progress.rs",
            "crate/cli/src/fmt/table.rs",
            "crate/cli/src/fmt/json.rs",
            "crate/cli/src/fmt/yaml.rs",
            "crate/cli/src/commands/query.rs",
            "xtask/src/main.rs",
            "xtask/src/command.rs",
            "xtask/src/output.rs",
            "xtask/src/process.rs",
            "xtask/src/tools.rs",
            "xtask/macros/src/lib.rs",
        ];
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
        ];
        let sqlx_query_as_allow = [
            "crate/lib/sinex-db/src/repositories/common.rs",
            "crate/lib/sinex-node-sdk/src/preflight/database.rs",
            "xtask/src/main.rs",
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
            println!("✅ No forbidden patterns found");
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

/// Check if a path is a test directory
fn is_tests_path(path: &str) -> bool {
    // Test directories
    path.contains("/tests/") || path.starts_with("tests/")
    // xtask is a build tool - its sync tests are acceptable
    || path.starts_with("xtask/")
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
        println!(
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
        println!(
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

    #[test]
    fn test_lint_forbidden_command_name() {
        let cmd = LintForbiddenCommand;
        assert_eq!(cmd.name(), "lint-forbidden");
    }

    #[test]
    fn test_lint_forbidden_command_metadata() {
        let cmd = LintForbiddenCommand;
        let metadata = cmd.metadata();

        assert_eq!(metadata.category, Some("check".to_string()));
        assert!(metadata.timeout.is_some());
    }

    #[test]
    fn test_is_tests_path() {
        assert!(is_tests_path("tests/foo.rs"));
        assert!(is_tests_path("crate/lib/foo/tests/bar.rs"));
        assert!(!is_tests_path("crate/lib/foo/src/test_utils.rs"));
    }

    #[test]
    fn test_filter_allowlist() {
        let matches = vec![
            "crate/foo/src/main.rs:10:test".to_string(),
            "crate/bar/src/lib.rs:20:test".to_string(),
            "tests/integration.rs:30:test".to_string(),
        ];
        let allow = ["crate/foo/src/main.rs"];
        let filtered = filter_allowlist(matches, &allow, is_tests_path);

        assert_eq!(filtered.len(), 1);
        assert!(filtered[0].contains("crate/bar/src/lib.rs"));
    }
}

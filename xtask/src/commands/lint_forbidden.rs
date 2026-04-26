//! Forbidden pattern scanning command - enforces project coding standards

use color_eyre::eyre::{Result, WrapErr, bail, eyre};
use serde::Deserialize;
use std::process::Command;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::{ast_grep_config_path, workspace_root};

/// Lint forbidden patterns command - scans for anti-patterns and policy violations.
///
/// Checks for blocking policy violations via ripgrep-based scans, and also runs
/// the repo's ast-grep rule catalog in severity-aware mode.
///
/// Blocking checks include:
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
/// - ast-grep warning/hint findings from `.config/ast-grep/rules/`
#[derive(Debug, Clone, clap::Args)]
pub struct LintForbiddenCommand;

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
        // `#[sinex_test]` / `sinex_proptest!` is the preferred harness surface.
        // The raw-attribute scan intentionally auto-skips:
        //   - dedicated test directories/files
        //   - inline `#[cfg(test)] mod tests` modules
        //   - proc-macro generated/doc-string references via allowlists below
        //
        // The allowlists below are only for strict scans that do not already
        // auto-skip those categories.
        // ═══════════════════════════════════════════════════════════════════════

        // #[tokio::test] allowlist — only for code that GENERATES or REFERENCES
        // #[tokio::test] as string literals (not actual test attributes).
        let tokio_test_allow = [
            // Proc macro: generates #[tokio::test] in expanded sinex_test output
            "xtask/macros/src/lib.rs",
            // This file: contains pattern strings and doc comments referencing it
            "xtask/src/commands/lint_forbidden.rs",
        ];
        // #[test] allowlist — empty. Dedicated test files and inline cfg(test)
        // modules are auto-skipped; proc-macro/trybuild cases are covered by
        // path-based skipping.
        let rust_test_allow: [&str; 0] = [];
        // Runtime sqlx::query() is allowed for:
        // - Session control (SET, ROLLBACK, RESET)
        // - Advisory locks
        // - Dynamic queries (analytics, cascade analysis)
        // - Test infrastructure
        let sqlx_query_allow = [
            "crate/core/sinex-gateway/src/cascade_analyzer.rs",
            "crate/core/sinex-gateway/src/rpc_server.rs",
            "crate/core/sinex-gateway/src/service_container.rs",
            "crate/core/sinex-gateway/src/handlers/rpc_handlers.rs",
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
            "crate/core/sinex-gateway/src/handlers/audit.rs",
            "crate/lib/sinex-db/src/repositories/events/composable_query.rs",
            "crate/lib/sinex-db/src/repositories/events/persistence.rs",
            "crate/lib/sinex-node-sdk/src/preflight/database.rs",
            "xtask/src/main.rs",
        ];

        let mut violations: Vec<String> = Vec::new();
        violations.extend(check_rust_test_attr_patterns(
            "#[tokio::test]",
            r"#\[tokio::test",
            &tokio_test_allow,
        )?);
        violations.extend(check_rust_test_attr_patterns(
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

        // anyhow:: in library code is disallowed; libraries use the project error stack.
        let anyhow_allow: [&str; 0] = [];
        violations.extend(check_anyhow_in_lib("anyhow::", r"anyhow::", &anyhow_allow)?);

        // println! in library code (use tracing for structured logging)
        let println_lib_allow = [
            "crate/lib/sinex-node-sdk/src/node_cli.rs",
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

        let ast_grep = run_ast_grep_scan()?;
        if ast_grep.has_findings() && ctx.is_human() {
            eprintln!(
                "ℹ ast-grep: {} error(s), {} warning(s), {} hint(s)",
                ast_grep.error_count(),
                ast_grep.warning_count(),
                ast_grep.hint_count()
            );
        }
        for finding in ast_grep.error_findings() {
            violations.push(format!(
                "{}:{}:{} [{}] {}",
                finding.file, finding.line, finding.column, finding.rule_id, finding.message
            ));
        }

        if violations.is_empty() {
            if ctx.is_human() {
                eprintln!("✅ No forbidden patterns found");
            }
            let mut result = CommandResult::success()
                .with_message("No forbidden patterns found")
                .with_duration(ctx.elapsed())
                .with_data(serde_json::json!({
                    "ast_grep": ast_grep,
                }));
            if ast_grep.warning_count() > 0 || ast_grep.hint_count() > 0 {
                result = result.with_detail(format!(
                    "ast-grep advisory findings: {} warning(s), {} hint(s)",
                    ast_grep.warning_count(),
                    ast_grep.hint_count()
                ));
            }
            return Ok(result);
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

/// Check for a pattern allowing test directories
fn check_pattern_allow_tests(label: &str, pattern: &str, allow: &[&str]) -> Result<Vec<String>> {
    run_rg(pattern)
        .and_then(|matches| filter_allowlist(matches, allow, is_tests_path))
        .with_context(|| format!("failed to scan for {label}"))
}

/// Check test attributes while allowing dedicated test dirs and inline cfg(test) modules.
fn check_rust_test_attr_patterns(
    label: &str,
    pattern: &str,
    allow: &[&str],
) -> Result<Vec<String>> {
    run_rg(pattern)
        .and_then(|matches| {
            filter_allowlist(matches, allow, |path| {
                is_tests_path(path) || file_has_inline_cfg_test_module(path)
            })
        })
        .with_context(|| format!("failed to scan for {label}"))
}

/// Run ripgrep to find pattern matches
fn run_rg(pattern: &str) -> Result<Vec<String>> {
    let output = Command::new("rg")
        .current_dir(workspace_root())
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
    ensure_rg_completed(&output, "ripgrep")?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout.lines().map(str::to_string).collect::<Vec<String>>())
}

fn ensure_rg_completed(output: &std::process::Output, context: &str) -> Result<()> {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    match output.status.code() {
        Some(0 | 1) => Ok(()),
        Some(code) if stderr.is_empty() => bail!("{context} failed with exit code {code}"),
        Some(code) => bail!("{context} failed with exit code {code}: {stderr}"),
        None if stderr.is_empty() => bail!("{context} terminated by signal"),
        None => bail!("{context} terminated by signal: {stderr}"),
    }
}

/// Filter matches against allowlist and skip function
fn parse_match_file(line: &str) -> Result<&str> {
    let (file, _) = line
        .split_once(':')
        .ok_or_else(|| eyre!("ripgrep match line is missing a file prefix: {line}"))?;
    let file = file.trim();
    if file.is_empty() {
        bail!("ripgrep match line reported an empty file path: {line}");
    }
    Ok(file)
}

fn filter_allowlist<F>(matches: Vec<String>, allow: &[&str], mut skip: F) -> Result<Vec<String>>
where
    F: FnMut(&str) -> bool,
{
    let mut filtered = Vec::new();
    for line in matches {
        let file = parse_match_file(&line)?;
        if !allow.contains(&file) && !skip(file) {
            filtered.push(line);
        }
    }
    Ok(filtered)
}

/// Check if a path is a test directory or build tooling.
///
/// xtask is blanket-allowed because the proc macro crate (`xtask/macros/`)
/// generates `#[test]` and `#[tokio::test]` in its expansion output.
fn is_tests_path(path: &str) -> bool {
    path.contains("/tests/") || path.starts_with("tests/") || path.starts_with("xtask/")
}

fn file_has_inline_cfg_test_module(path: &str) -> bool {
    let Ok(contents) = std::fs::read_to_string(path) else {
        return false;
    };
    contents.contains("#[cfg(test)]") && contents.contains("mod tests")
}

/// Check for anyhow usage in library code (not xtask, not tests, not binaries)
fn check_anyhow_in_lib(label: &str, pattern: &str, allow: &[&str]) -> Result<Vec<String>> {
    run_rg(pattern)
        .and_then(|matches| {
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
        .and_then(|matches| {
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
    let filtered = filter_allowlist(matches, &[], |file| {
        allow_prefixes.iter().any(|a| file.starts_with(a)) || is_tests_path(file)
    })?;

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
// `xtask lint-forbidden` executes the catalog and treats only error-severity
// findings as blocking today. The remaining warning/hint findings are advisory.

/// Count occurrences of a pattern outside test directories
fn count_pattern_outside_tests(pattern: &str) -> Result<usize> {
    let output = Command::new("rg")
        .current_dir(workspace_root())
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

    ensure_rg_completed(&output, "ripgrep pattern count")?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut total = 0;
    for line in stdout.lines() {
        if let Some(count_str) = line.split(':').nth(1)
            && let Ok(count) = count_str.parse::<usize>()
        {
            total += count;
        }
    }
    Ok(total)
}

#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize)]
struct AstGrepSummary {
    errors: Vec<AstGrepFinding>,
    warnings: usize,
    hints: usize,
}

impl AstGrepSummary {
    fn has_findings(&self) -> bool {
        !self.errors.is_empty() || self.warnings > 0 || self.hints > 0
    }

    fn error_count(&self) -> usize {
        self.errors.len()
    }

    fn warning_count(&self) -> usize {
        self.warnings
    }

    fn hint_count(&self) -> usize {
        self.hints
    }

    fn error_findings(&self) -> &[AstGrepFinding] {
        &self.errors
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
struct AstGrepFinding {
    file: String,
    line: usize,
    column: usize,
    rule_id: String,
    severity: String,
    message: String,
}

#[derive(Debug, Deserialize)]
struct AstGrepFindingJson {
    file: String,
    #[serde(rename = "ruleId")]
    rule_id: String,
    severity: String,
    message: String,
    range: AstGrepRange,
}

#[derive(Debug, Deserialize)]
struct AstGrepRange {
    start: AstGrepPosition,
}

#[derive(Debug, Deserialize)]
struct AstGrepPosition {
    line: usize,
    column: usize,
}

fn run_ast_grep_scan() -> Result<AstGrepSummary> {
    let workspace = workspace_root();
    let config_path = ast_grep_config_path();
    let output = Command::new("ast-grep")
        .current_dir(&workspace)
        .arg("scan")
        .arg("--config")
        .arg(&config_path)
        .arg("--json=stream")
        .arg("--include-metadata")
        .arg("--globs")
        .arg("!**/tests/**")
        .arg("--globs")
        .arg("!**/tests.rs")
        .arg("--globs")
        .arg("!**/*_test.rs")
        .arg("--globs")
        .arg("!**/test_*.rs")
        .arg("--globs")
        .arg("!**/build.rs")
        .arg(".")
        .output()
        .with_context(|| format!("failed to invoke ast-grep with {}", config_path.display()))?;

    ensure_ast_grep_completed(&output)?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_ast_grep_summary(&stdout)
}

fn ensure_ast_grep_completed(output: &std::process::Output) -> Result<()> {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    match output.status.code() {
        Some(0 | 1) => Ok(()),
        Some(code) if stderr.is_empty() => bail!("ast-grep failed with exit code {code}"),
        Some(code) => bail!("ast-grep failed with exit code {code}: {stderr}"),
        None if stderr.is_empty() => bail!("ast-grep terminated by signal"),
        None => bail!("ast-grep terminated by signal: {stderr}"),
    }
}

fn parse_ast_grep_summary(stdout: &str) -> Result<AstGrepSummary> {
    let mut summary = AstGrepSummary::default();

    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let finding: AstGrepFindingJson =
            serde_json::from_str(line).with_context(|| "failed to parse ast-grep JSON output")?;
        match finding.severity.as_str() {
            "error" => summary.errors.push(AstGrepFinding {
                file: finding.file,
                line: finding.range.start.line,
                column: finding.range.start.column,
                rule_id: finding.rule_id,
                severity: finding.severity,
                message: finding.message,
            }),
            "warning" => summary.warnings += 1,
            "hint" => summary.hints += 1,
            _ => {}
        }
    }

    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;
    use std::os::unix::process::ExitStatusExt;

    #[sinex_test]
    async fn test_lint_forbidden_command_name() -> ::xtask::sandbox::TestResult<()> {
        let cmd = LintForbiddenCommand;
        assert_eq!(cmd.name(), "lint-forbidden");
        Ok(())
    }

    #[sinex_test]
    async fn test_lint_forbidden_command_metadata() -> ::xtask::sandbox::TestResult<()> {
        let cmd = LintForbiddenCommand;
        let metadata = cmd.metadata();

        assert_eq!(metadata.category, Some("check"));
        assert!(metadata.timeout.is_some());
        Ok(())
    }

    #[sinex_test]
    async fn test_is_tests_path() -> ::xtask::sandbox::TestResult<()> {
        assert!(is_tests_path("tests/foo.rs"));
        assert!(is_tests_path("crate/lib/foo/tests/bar.rs"));
        assert!(!is_tests_path("crate/lib/foo/src/test_utils.rs"));
        Ok(())
    }

    #[sinex_test]
    async fn test_file_has_inline_cfg_test_module() -> ::xtask::sandbox::TestResult<()> {
        let dir = std::env::temp_dir().join(format!("sinex-inline-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        let file = dir.join("inline.rs");
        std::fs::write(
            &file,
            "fn helper() {}\n#[cfg(test)]\nmod tests {\n    #[test]\n    fn works() {}\n}\n",
        )?;
        assert!(file_has_inline_cfg_test_module(&file.display().to_string()));
        std::fs::remove_file(&file)?;
        std::fs::remove_dir(&dir)?;
        Ok(())
    }

    #[sinex_test]
    async fn test_filter_allowlist() -> ::xtask::sandbox::TestResult<()> {
        let matches = vec![
            "crate/foo/src/main.rs:10:test".to_string(),
            "crate/bar/src/lib.rs:20:test".to_string(),
            "tests/integration.rs:30:test".to_string(),
        ];
        let allow = ["crate/foo/src/main.rs"];
        let filtered = filter_allowlist(matches, &allow, is_tests_path)?;

        assert_eq!(filtered.len(), 1);
        assert!(filtered[0].contains("crate/bar/src/lib.rs"));
        Ok(())
    }

    #[sinex_test]
    async fn test_filter_allowlist_rejects_malformed_match_lines()
    -> ::xtask::sandbox::TestResult<()> {
        let error = filter_allowlist(vec!["malformed line".to_string()], &[], |_| false)
            .expect_err("malformed ripgrep output should fail");
        assert!(format!("{error:#}").contains("missing a file prefix"));
        Ok(())
    }

    #[sinex_test]
    async fn test_filter_allowlist_rejects_empty_match_paths() -> ::xtask::sandbox::TestResult<()> {
        let error = filter_allowlist(vec![":10:test".to_string()], &[], |_| false)
            .expect_err("empty file path should fail");
        assert!(format!("{error:#}").contains("empty file path"));
        Ok(())
    }

    #[sinex_test]
    async fn test_ensure_rg_completed_reports_signal_termination()
    -> ::xtask::sandbox::TestResult<()> {
        let output = std::process::Output {
            status: std::process::ExitStatus::from_raw(9),
            stdout: Vec::new(),
            stderr: b"killed".to_vec(),
        };

        let error =
            ensure_rg_completed(&output, "ripgrep").expect_err("signal termination should surface");
        assert!(error.to_string().contains("terminated by signal"));
        assert!(error.to_string().contains("killed"));
        Ok(())
    }

    #[sinex_test]
    async fn test_ensure_rg_completed_allows_no_matches() -> ::xtask::sandbox::TestResult<()> {
        let output = std::process::Output {
            status: std::process::ExitStatus::from_raw(1 << 8),
            stdout: Vec::new(),
            stderr: Vec::new(),
        };

        ensure_rg_completed(&output, "ripgrep")?;
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_ast_grep_summary_tracks_blocking_and_advisory_findings()
    -> ::xtask::sandbox::TestResult<()> {
        let summary = parse_ast_grep_summary(
            r#"{"file":"crate/lib/foo.rs","ruleId":"dbg-macro","severity":"error","message":"dbg!()","range":{"start":{"line":7,"column":13}}}
{"file":"crate/lib/bar.rs","ruleId":"context-erasure","severity":"warning","message":"map_err(|_| ...)","range":{"start":{"line":11,"column":5}}}
{"file":"crate/lib/baz.rs","ruleId":"string-from-literal","severity":"hint","message":"String::from","range":{"start":{"line":3,"column":9}}}"#,
        )?;

        assert_eq!(summary.error_count(), 1);
        assert_eq!(summary.warning_count(), 1);
        assert_eq!(summary.hint_count(), 1);
        assert_eq!(summary.error_findings()[0].file, "crate/lib/foo.rs");
        assert_eq!(summary.error_findings()[0].rule_id, "dbg-macro");
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_ast_grep_summary_rejects_invalid_json() -> ::xtask::sandbox::TestResult<()>
    {
        let error =
            parse_ast_grep_summary("not-json").expect_err("invalid ast-grep output should fail");
        assert!(format!("{error:#}").contains("failed to parse ast-grep JSON output"));
        Ok(())
    }
}

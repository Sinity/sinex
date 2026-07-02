//! Cargo diagnostic parsing - extract structured errors from cargo --message-format=json

use color_eyre::eyre::{bail, eyre};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io::{BufRead, BufReader, Read};
use std::process::Stdio;
use std::time::Duration;

use crate::process::{ProcessTimeoutGuard, cargo_command};

/// A parsed compiler diagnostic
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CompilerDiagnostic {
    pub level: String,
    pub code: Option<String>,
    pub message: String,
    pub file_path: Option<String>,
    pub line: Option<u32>,
    pub column: Option<u32>,
    pub rendered: Option<String>,
    pub suggestion: Option<String>,
    /// Which crate this diagnostic belongs to (extracted from cargo's `package_id`)
    pub package: Option<String>,
    /// Exact replacement text for machine-applicable suggestions
    pub fix_replacement: Option<String>,
    /// Applicability level: "MachineApplicable", "MaybeIncorrect", "HasPlaceholders", "Unspecified"
    pub fix_applicability: Option<String>,
    /// Byte offset in the source file where the fix starts
    pub fix_byte_start: Option<u32>,
    /// Byte offset in the source file where the fix ends
    pub fix_byte_end: Option<u32>,
}

/// Summary of compiler output
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticSummary {
    pub errors: usize,
    pub warnings: usize,
    pub diagnostics: Vec<CompilerDiagnostic>,
    pub success: bool,
    /// All packages that were compiled during this invocation (for package-scoped supersession)
    pub compiled_packages: std::collections::HashSet<String>,
}

/// Lint code with its count
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LintCount {
    pub code: String,
    pub count: usize,
}

/// File path with warning count
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileCount {
    pub path: String,
    pub count: usize,
}

fn diagnostic_identity_key(diagnostic: &CompilerDiagnostic) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        diagnostic.level,
        diagnostic.code.as_deref().unwrap_or(""),
        diagnostic.message,
        diagnostic.file_path.as_deref().unwrap_or(""),
        diagnostic
            .line
            .map(|value| value.to_string())
            .unwrap_or_default(),
        diagnostic
            .column
            .map(|value| value.to_string())
            .unwrap_or_default(),
        diagnostic.rendered.as_deref().unwrap_or(""),
        diagnostic.package.as_deref().unwrap_or(""),
        diagnostic.suggestion.as_deref().unwrap_or(""),
        diagnostic.fix_replacement.as_deref().unwrap_or(""),
        diagnostic.fix_applicability.as_deref().unwrap_or(""),
        diagnostic
            .fix_byte_start
            .zip(diagnostic.fix_byte_end)
            .map(|(start, end)| format!("{start}:{end}"))
            .unwrap_or_default(),
    )
}

/// Collapse exact duplicates within a single cargo invocation.
///
/// Cargo may emit the same source diagnostic once per compiled target unit
/// (for example library + tests + benches + examples when xtask requests all
/// of them). xtask reports semantic diagnostics per invocation, so these
/// repeated compiler-messages are intentionally collapsed here.
fn dedupe_diagnostics(diagnostics: Vec<CompilerDiagnostic>) -> Vec<CompilerDiagnostic> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::with_capacity(diagnostics.len());
    for diagnostic in diagnostics {
        if seen.insert(diagnostic_identity_key(&diagnostic)) {
            deduped.push(diagnostic);
        }
    }
    deduped
}

impl DiagnosticSummary {
    /// Get breakdown of warning counts by lint code, sorted by count descending.
    /// Returns only warnings (not errors) with recognized lint codes.
    #[must_use]
    pub fn lint_breakdown(&self) -> Vec<LintCount> {
        use std::collections::HashMap;

        let mut counts: HashMap<String, usize> = HashMap::new();
        for diag in &self.diagnostics {
            if diag.level == "warning"
                && let Some(ref code) = diag.code
            {
                *counts.entry(code.clone()).or_insert(0) += 1;
            }
        }

        let mut result: Vec<LintCount> = counts
            .into_iter()
            .map(|(code, count)| LintCount { code, count })
            .collect();
        result.sort_by_key(|x| std::cmp::Reverse(x.count));
        result
    }

    /// Get the top N lints by count
    #[must_use]
    pub fn top_lints(&self, n: usize) -> Vec<LintCount> {
        self.lint_breakdown().into_iter().take(n).collect()
    }

    /// Get breakdown of warning counts by file path, sorted by count descending.
    #[must_use]
    pub fn file_breakdown(&self) -> Vec<FileCount> {
        use std::collections::HashMap;

        let mut counts: HashMap<String, usize> = HashMap::new();
        for diag in &self.diagnostics {
            if diag.level == "warning"
                && let Some(ref path) = diag.file_path
            {
                *counts.entry(path.clone()).or_insert(0) += 1;
            }
        }

        let mut result: Vec<FileCount> = counts
            .into_iter()
            .map(|(path, count)| FileCount { path, count })
            .collect();
        result.sort_by_key(|x| std::cmp::Reverse(x.count));
        result
    }

    /// Get the top N files by warning count
    #[must_use]
    pub fn top_files(&self, n: usize) -> Vec<FileCount> {
        self.file_breakdown().into_iter().take(n).collect()
    }
}

impl CompilerDiagnostic {
    /// Render a useful fallback when cargo's JSON message lacks a pre-rendered
    /// diagnostic. Without this, `xtask check` can report "cargo check failed"
    /// while hiding the actual source error.
    #[must_use]
    pub fn compact_render(&self) -> String {
        let mut rendered = String::new();
        if let Some(path) = &self.file_path {
            rendered.push_str(path);
            if let Some(line) = self.line {
                rendered.push(':');
                rendered.push_str(&line.to_string());
                if let Some(column) = self.column {
                    rendered.push(':');
                    rendered.push_str(&column.to_string());
                }
            }
            rendered.push_str(": ");
        }
        rendered.push_str(&self.level);
        if let Some(code) = &self.code {
            rendered.push('[');
            rendered.push_str(code);
            rendered.push(']');
        }
        rendered.push_str(": ");
        rendered.push_str(&self.message);
        rendered.push('\n');
        if let Some(suggestion) = &self.suggestion {
            rendered.push_str("help: ");
            rendered.push_str(suggestion);
            rendered.push('\n');
        }
        rendered
    }

    #[must_use]
    pub fn rendered_or_compact(&self) -> String {
        self.rendered
            .clone()
            .unwrap_or_else(|| self.compact_render())
    }
}

/// Run a cargo subcommand with piped output and a configurable timeout.
///
/// Uses `SINEX_CARGO_TIMEOUT` (default: 600s) to prevent indefinite hangs when
/// the cargo target/ lock is held by a concurrent process (e.g., nextest, another
/// `cargo check`). On timeout, kills the child process and returns an error.
fn run_cargo_with_timeout(cargo_args: &[&str]) -> color_eyre::eyre::Result<(Vec<u8>, bool)> {
    let timeout_secs =
        crate::parse_positive_u64_env_or_default("SINEX_CARGO_TIMEOUT", 600, "cargo timeout");

    let mut child = cargo_command()
        .args(cargo_args)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit()) // Stream compiler progress/errors to terminal in real-time
        .spawn()?;

    let mut timeout_guard = ProcessTimeoutGuard::start_for_process_group_leader(
        child.id(),
        "cargo diagnostics",
        Duration::from_secs(timeout_secs),
        "cargo diagnostics timeout",
    );

    // Read stdout while child runs (must drain the pipe or child blocks on full pipe buffer)
    let mut stdout_bytes = Vec::new();
    if let Some(mut out) = child.stdout.take() {
        out.read_to_end(&mut stdout_bytes)?;
    }

    let exit_status = child.wait()?;

    if timeout_guard.finish() {
        return Err(eyre!(
            "cargo timed out after {timeout_secs}s — possible cargo target/ lock contention \
             from a concurrent cargo process. \
             Set SINEX_CARGO_TIMEOUT env var to adjust. \
             Check for other running xtask/cargo processes with: xtask jobs active"
        ));
    }

    Ok((stdout_bytes, exit_status.success()))
}

/// Estimate how many packages would be compiled for the given cargo args.
///
/// For `-p package` args: counts the specified packages directly (instantaneous).
/// For `--workspace`/`--all`: queries `cargo metadata --no-deps` (fast, no rustc).
/// Returns 0 on error or when args are ambiguous (caller keeps progress indeterminate).
#[must_use]
pub fn estimate_package_count(package_args: &[&str]) -> usize {
    progress_target_packages(package_args).map_or(0, |targets| targets.len())
}

fn progress_target_packages(package_args: &[&str]) -> Option<std::collections::HashSet<String>> {
    // Count explicit -p/--package flags in the args (most common case)
    let mut explicit_packages = std::collections::HashSet::new();
    let mut workspace_mode = false;
    let mut next_is_pkg = false;

    for arg in package_args {
        if next_is_pkg {
            explicit_packages.insert((*arg).to_string());
            next_is_pkg = false;
        } else if *arg == "--workspace" || *arg == "--all" {
            workspace_mode = true;
        } else if *arg == "--package" || *arg == "-p" {
            next_is_pkg = true;
        } else if arg.starts_with("--package=") {
            explicit_packages.insert(arg.trim_start_matches("--package=").to_string());
        }
    }

    if !explicit_packages.is_empty() {
        return Some(explicit_packages);
    }

    if workspace_mode {
        // Use cargo metadata to count workspace packages (no rustc involved)
        let Ok(output) = cargo_command()
            .args(["metadata", "--no-deps", "--format-version", "1"])
            .output()
        else {
            return None;
        };
        if !output.status.success() {
            return None;
        }
        let Ok(text) = std::str::from_utf8(&output.stdout) else {
            return None;
        };
        let json: serde_json::Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(_) => return None,
        };
        return Some(
            json["packages"]
                .as_array()
                .map(|pkgs| {
                    pkgs.iter()
                        .filter_map(|pkg| {
                            pkg.get("name")
                                .and_then(serde_json::Value::as_str)
                                .map(std::string::ToString::to_string)
                        })
                        .collect::<std::collections::HashSet<_>>()
                })
                .unwrap_or_default(),
        );
    }

    // Affected mode or unknown scope — cannot estimate without running cargo
    None
}

fn track_progress_artifact(
    line: &str,
    progress_targets: Option<&std::collections::HashSet<String>>,
    seen_packages: &mut std::collections::HashSet<String>,
) -> Option<usize> {
    let json = serde_json::from_str::<serde_json::Value>(line).ok()?;
    if json.get("reason").and_then(serde_json::Value::as_str) != Some("compiler-artifact") {
        return None;
    }

    let package = json
        .get("package_id")
        .and_then(serde_json::Value::as_str)
        .and_then(extract_package_name)?;

    if let Some(targets) = progress_targets
        && !targets.contains(&package)
    {
        return None;
    }

    if seen_packages.insert(package) {
        Some(seen_packages.len())
    } else {
        None
    }
}

/// Run a cargo subcommand streaming lines to a callback as they arrive.
///
/// Identical to `run_cargo_with_timeout` but calls `on_artifact` with the running
/// count of compiled artifacts each time a `"compiler-artifact"` JSON line is seen.
/// This enables real-time progress reporting for check/build/clippy stages.
fn run_cargo_streaming<F>(
    cargo_args: &[&str],
    mut on_artifact: F,
) -> color_eyre::eyre::Result<(Vec<u8>, bool)>
where
    F: FnMut(usize),
{
    let timeout_secs =
        crate::parse_positive_u64_env_or_default("SINEX_CARGO_TIMEOUT", 600, "cargo timeout");

    let mut child = cargo_command()
        .args(cargo_args)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;

    let mut timeout_guard = ProcessTimeoutGuard::start_for_process_group_leader(
        child.id(),
        "cargo streaming diagnostics",
        Duration::from_secs(timeout_secs),
        "cargo streaming diagnostics timeout",
    );

    let mut all_bytes = Vec::new();
    let progress_targets = progress_target_packages(cargo_args);
    let mut seen_packages = std::collections::HashSet::new();

    if let Some(out) = child.stdout.take() {
        for line in BufReader::new(out).lines() {
            let line = line?;
            // Accumulate raw bytes for parse_cargo_json_output
            all_bytes.extend_from_slice(line.as_bytes());
            all_bytes.push(b'\n');
            if let Some(done) =
                track_progress_artifact(&line, progress_targets.as_ref(), &mut seen_packages)
            {
                on_artifact(done);
            }
        }
    }

    let exit_status = child.wait()?;

    if timeout_guard.finish() {
        return Err(eyre!(
            "cargo timed out after {timeout_secs}s — possible cargo target/ lock contention \
             from a concurrent cargo process. \
             Set SINEX_CARGO_TIMEOUT env var to adjust. \
             Check for other running xtask/cargo processes with: xtask jobs active"
        ));
    }

    Ok((all_bytes, exit_status.success()))
}

/// Run cargo check with JSON output and parse diagnostics
pub fn run_cargo_check(args: &[&str]) -> color_eyre::eyre::Result<DiagnosticSummary> {
    // Guard: nextest holds the cargo target/ lock for its entire run.
    // Invoking cargo here would deadlock indefinitely on that lock.
    if crate::config::is_nextest_run() {
        bail!(
            "Cannot invoke `cargo check` from inside a nextest test run — \
             nextest holds the cargo target/ lock and any cargo subprocess \
             will deadlock. Use `xtask check --help` to verify flag presence \
             in tests; test cargo behavior via `xtask check --bg`."
        );
    }

    let mut cmd_args = vec!["check", "--message-format=json"];
    cmd_args.extend(args);

    let (stdout_bytes, success) = run_cargo_with_timeout(&cmd_args)?;
    let stdout = String::from_utf8_lossy(&stdout_bytes);
    parse_cargo_json_output(&stdout, success)
}

/// Run cargo clippy with JSON output and parse diagnostics
pub fn run_cargo_clippy(args: &[&str]) -> color_eyre::eyre::Result<DiagnosticSummary> {
    // Guard: same as run_cargo_check — nextest holds the cargo target/ lock.
    if crate::config::is_nextest_run() {
        bail!(
            "Cannot invoke `cargo clippy` from inside a nextest test run — \
             nextest holds the cargo target/ lock and any cargo subprocess \
             will deadlock. Use `xtask check --help` to verify flag presence \
             in tests; test cargo behavior via `xtask check --lint --bg`."
        );
    }

    let mut cmd_args = vec!["clippy", "--message-format=json"];
    cmd_args.extend(args);

    let (stdout_bytes, success) = run_cargo_with_timeout(&cmd_args)?;
    let stdout = String::from_utf8_lossy(&stdout_bytes);
    parse_cargo_json_output(&stdout, success)
}

/// Run cargo check with streaming artifact count callbacks for progress reporting.
///
/// Calls `on_artifact(n)` each time a new package artifact is compiled, where `n`
/// is the running count. Use this when you want real-time progress during compilation.
pub fn run_cargo_check_streaming<F>(
    args: &[&str],
    on_artifact: F,
) -> color_eyre::eyre::Result<DiagnosticSummary>
where
    F: FnMut(usize),
{
    if crate::config::is_nextest_run() {
        bail!(
            "Cannot invoke `cargo check` from inside a nextest test run — \
             nextest holds the cargo target/ lock and any cargo subprocess \
             will deadlock. Use `xtask check --help` to verify flag presence \
             in tests; test cargo behavior via `xtask check --bg`."
        );
    }

    let mut cmd_args = vec!["check", "--message-format=json"];
    cmd_args.extend(args);

    let (stdout_bytes, success) = run_cargo_streaming(&cmd_args, on_artifact)?;
    let stdout = String::from_utf8_lossy(&stdout_bytes);
    parse_cargo_json_output(&stdout, success)
}

/// Run cargo clippy with streaming artifact count callbacks for progress reporting.
pub fn run_cargo_clippy_streaming<F>(
    args: &[&str],
    on_artifact: F,
) -> color_eyre::eyre::Result<DiagnosticSummary>
where
    F: FnMut(usize),
{
    if crate::config::is_nextest_run() {
        bail!(
            "Cannot invoke `cargo clippy` from inside a nextest test run — \
             nextest holds the cargo target/ lock and any cargo subprocess \
             will deadlock. Use `xtask check --help` to verify flag presence \
             in tests; test cargo behavior via `xtask check --lint --bg`."
        );
    }

    let mut cmd_args = vec!["clippy", "--message-format=json"];
    cmd_args.extend(args);

    let (stdout_bytes, success) = run_cargo_streaming(&cmd_args, on_artifact)?;
    let stdout = String::from_utf8_lossy(&stdout_bytes);
    parse_cargo_json_output(&stdout, success)
}

/// Run cargo build with streaming artifact count callbacks for progress reporting.
pub fn run_cargo_build_streaming<F>(
    args: &[&str],
    on_artifact: F,
) -> color_eyre::eyre::Result<DiagnosticSummary>
where
    F: FnMut(usize),
{
    if crate::config::is_nextest_run() {
        bail!(
            "Cannot invoke `cargo build` from inside a nextest test run — \
             nextest holds the cargo target/ lock and any cargo subprocess \
             will deadlock."
        );
    }

    let mut cmd_args = vec!["build", "--message-format=json"];
    cmd_args.extend(args);

    let (stdout_bytes, success) = run_cargo_streaming(&cmd_args, on_artifact)?;
    let stdout = String::from_utf8_lossy(&stdout_bytes);
    parse_cargo_json_output(&stdout, success)
}

/// Parse cargo's JSON output format
pub fn parse_cargo_json_output(
    output: &str,
    success: bool,
) -> color_eyre::eyre::Result<DiagnosticSummary> {
    let mut diagnostics = Vec::new();
    let mut compiled_packages = std::collections::HashSet::new();

    for line in output.lines() {
        if !line.trim().is_empty()
            && let Ok(json) = serde_json::from_str::<serde_json::Value>(line)
        {
            // Extract package name from package_id (present on all cargo JSON messages)
            let package = json
                .get("package_id")
                .and_then(|p| p.as_str())
                .and_then(extract_package_name);

            if json.get("reason").and_then(|r| r.as_str()) == Some("compiler-artifact")
                && json.get("fresh").and_then(serde_json::Value::as_bool) != Some(true)
                && let Some(ref pkg) = package
            {
                compiled_packages.insert(pkg.clone());
            }

            // Check if this is a compiler message
            if json.get("reason").and_then(|r| r.as_str()) == Some("compiler-message")
                && let Some(message) = json.get("message")
                && let Some(mut diag) = parse_diagnostic_message(message)
            {
                // Attach package attribution from the outer JSON envelope
                if diag.package.is_none() {
                    diag.package.clone_from(&package);
                }
                diagnostics.push(diag);
            }
        }
    }

    let diagnostics = dedupe_diagnostics(diagnostics);
    let mut diagnostics = diagnostics;
    let mut errors = diagnostics
        .iter()
        .filter(|diag| diag.level == "error")
        .count();
    let warnings = diagnostics
        .iter()
        .filter(|diag| diag.level == "warning")
        .count();
    if !success && errors == 0 {
        diagnostics.push(unparsed_cargo_failure_diagnostic(output));
        errors = 1;
    }

    Ok(DiagnosticSummary {
        errors,
        warnings,
        diagnostics,
        success,
        compiled_packages,
    })
}

fn unparsed_cargo_failure_diagnostic(output: &str) -> CompilerDiagnostic {
    let tail = output_tail(output, 24);
    let message = if tail.is_empty() {
        "cargo failed without parseable compiler diagnostics and emitted no stdout".to_string()
    } else {
        format!("cargo failed without parseable compiler diagnostics; raw output tail:\n{tail}")
    };
    CompilerDiagnostic {
        level: "error".to_string(),
        code: Some("XTASK_UNPARSED_CARGO_FAILURE".to_string()),
        message: message.clone(),
        rendered: Some(format!("error[XTASK_UNPARSED_CARGO_FAILURE]: {message}\n")),
        ..CompilerDiagnostic::default()
    }
}

fn output_tail(output: &str, max_lines: usize) -> String {
    let lines: Vec<&str> = output
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    let start = lines.len().saturating_sub(max_lines);
    lines[start..].join("\n")
}

/// Extract the crate name from a cargo `package_id` string.
///
/// Cargo emits package IDs in three current formats:
/// 1. Registry: `"registry+URL#name@version"` (e.g., `registry+...#proc-macro2@1.0.103`)
/// 2. Local (dir = name): `"path+file:///path/to/crate#version"` (name = last path segment)
/// 3. Local (explicit): `"path+file:///path/to/crate#name@version"` (name after `#`, before `@`)
fn extract_package_name(package_id: &str) -> Option<String> {
    if let Some(hash_pos) = package_id.rfind('#') {
        let after_hash = &package_id[hash_pos + 1..];

        if let Some(at_pos) = after_hash.find('@') {
            // Format 1 or 3: "name@version" after #
            let name = &after_hash[..at_pos];
            if !name.is_empty() {
                return Some(name.to_string());
            }
        }

        // Format 2: "#version" only — extract name from last path segment before #
        let before_hash = &package_id[..hash_pos];
        let path_part = before_hash
            .strip_prefix("path+file:///")
            .or_else(|| before_hash.strip_prefix("path+file://"))?;
        let last_segment = path_part.rsplit('/').next()?;
        if !last_segment.is_empty() {
            return Some(last_segment.to_string());
        }
    }
    None
}

/// Parse a single diagnostic message from cargo JSON output
fn parse_diagnostic_message(msg: &serde_json::Value) -> Option<CompilerDiagnostic> {
    let level = msg.get("level")?.as_str()?;
    let message = msg.get("message")?.as_str()?;

    // Skip note-level messages for cleaner output
    if level == "note" || level == "help" {
        return None;
    }

    let code = msg
        .get("code")
        .and_then(|c| c.get("code"))
        .and_then(|c| c.as_str())
        .map(std::string::ToString::to_string);

    let rendered = msg
        .get("rendered")
        .and_then(|r| r.as_str())
        .map(std::string::ToString::to_string);

    // Get primary span location
    let (file_path, line, column) = if let Some(spans) = msg.get("spans").and_then(|s| s.as_array())
    {
        spans
            .iter()
            .find(|s| s.get("is_primary").and_then(serde_json::Value::as_bool) == Some(true))
            .map_or((None, None, None), |span| {
                (
                    span.get("file_name")
                        .and_then(|f| f.as_str())
                        .map(std::string::ToString::to_string),
                    span.get("line_start")
                        .and_then(serde_json::Value::as_u64)
                        .map(|l| l as u32),
                    span.get("column_start")
                        .and_then(serde_json::Value::as_u64)
                        .map(|c| c as u32),
                )
            })
    } else {
        (None, None, None)
    };

    // Extract suggestion text and machine-applicable fix metadata from children
    let fix = extract_fix_from_children(msg);

    Some(CompilerDiagnostic {
        level: level.to_string(),
        code,
        message: message.to_string(),
        file_path,
        line,
        column,
        rendered,
        suggestion: fix.suggestion,
        package: None, // Set by caller from outer JSON envelope
        fix_replacement: fix.replacement,
        fix_applicability: fix.applicability,
        fix_byte_start: fix.byte_start,
        fix_byte_end: fix.byte_end,
    })
}

/// Suggestion and machine-applicable fix metadata extracted from a diagnostic's `children`.
#[derive(Default)]
struct FixSuggestion {
    suggestion: Option<String>,
    replacement: Option<String>,
    applicability: Option<String>,
    byte_start: Option<u32>,
    byte_end: Option<u32>,
}

/// Extract suggestion text and machine-applicable fix metadata from diagnostic children.
///
/// Cargo's JSON format nests fix suggestions inside `children` → `spans[]`, each containing:
/// - `suggested_replacement`: the exact replacement text
/// - `suggestion_applicability`: "MachineApplicable", "MaybeIncorrect", "HasPlaceholders", "Unspecified"
/// - `byte_start` / `byte_end`: byte offsets in the source file
///
/// We prefer `MachineApplicable` suggestions when available, falling back to any help message.
fn extract_fix_from_children(msg: &serde_json::Value) -> FixSuggestion {
    let Some(children) = msg.get("children").and_then(|c| c.as_array()) else {
        return FixSuggestion::default();
    };

    let mut suggestion_text: Option<String> = None;
    let mut best_fix: Option<(String, String, Option<u32>, Option<u32>)> = None;

    for child in children {
        let child_level = child.get("level").and_then(|l| l.as_str());

        // Capture the help message text (existing behavior)
        if child_level == Some("help") && suggestion_text.is_none() {
            suggestion_text = child
                .get("message")
                .and_then(|m| m.as_str())
                .map(ToString::to_string);
        }

        // Walk child spans for machine-applicable fix data
        if let Some(spans) = child.get("spans").and_then(|s| s.as_array()) {
            for span in spans {
                let applicability = span
                    .get("suggestion_applicability")
                    .and_then(|a| a.as_str());
                let replacement = span.get("suggested_replacement").and_then(|r| r.as_str());

                if let (Some(applicability), Some(replacement)) = (applicability, replacement) {
                    let byte_start = span
                        .get("byte_start")
                        .and_then(serde_json::Value::as_u64)
                        .map(|b| b as u32);
                    let byte_end = span
                        .get("byte_end")
                        .and_then(serde_json::Value::as_u64)
                        .map(|b| b as u32);

                    // Prefer MachineApplicable over other applicability levels.
                    // Store fix metadata even without byte offsets — applicability alone
                    // is enough for --smart mode to identify fixable packages.
                    let dominated = best_fix
                        .as_ref()
                        .is_some_and(|(_, a, _, _)| a == "MachineApplicable");

                    if !dominated || applicability == "MachineApplicable" {
                        best_fix = Some((
                            replacement.to_string(),
                            applicability.to_string(),
                            byte_start,
                            byte_end,
                        ));
                    }
                }
            }
        }
    }

    match best_fix {
        Some((replacement, applicability, byte_start, byte_end)) => FixSuggestion {
            suggestion: suggestion_text,
            replacement: Some(replacement),
            applicability: Some(applicability),
            byte_start,
            byte_end,
        },
        None => FixSuggestion {
            suggestion: suggestion_text,
            ..FixSuggestion::default()
        },
    }
}

#[cfg(test)]
#[path = "cargo_diagnostics_test.rs"]
mod tests;

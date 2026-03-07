//! Cargo diagnostic parsing - extract structured errors from cargo --message-format=json

use color_eyre::eyre::{bail, eyre};
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::process::{Command, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

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

/// Run a cargo subcommand with piped output and a configurable timeout.
///
/// Uses `SINEX_CARGO_TIMEOUT` (default: 600s) to prevent indefinite hangs when
/// the cargo target/ lock is held by a concurrent process (e.g., nextest, another
/// `cargo check`). On timeout, kills the child process and returns an error.
fn run_cargo_with_timeout(cargo_args: &[&str]) -> color_eyre::eyre::Result<(Vec<u8>, bool)> {
    let timeout_secs = std::env::var("SINEX_CARGO_TIMEOUT")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(600);

    let mut child = Command::new("cargo")
        .args(cargo_args)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit()) // Stream compiler progress/errors to terminal in real-time
        .spawn()?;

    let pid = child.id();

    // Shared flag: watchdog sets this to true if it fires (timeout exceeded).
    let timed_out = Arc::new(AtomicBool::new(false));
    let timed_out_clone = timed_out.clone();

    // Spawn timeout watchdog: kills child after timeout seconds.
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        if done_rx
            .recv_timeout(Duration::from_secs(timeout_secs))
            .is_err()
        {
            // Timeout fired — record it and kill the child
            timed_out_clone.store(true, Ordering::Relaxed);
            unsafe {
                libc::kill(pid as i32, libc::SIGTERM);
            }
            std::thread::sleep(Duration::from_secs(2));
            unsafe {
                libc::kill(pid as i32, libc::SIGKILL);
            }
        }
    });

    // Read stdout while child runs (must drain the pipe or child blocks on full pipe buffer)
    let mut stdout_bytes = Vec::new();
    if let Some(mut out) = child.stdout.take() {
        out.read_to_end(&mut stdout_bytes)?;
    }

    let exit_status = child.wait()?;
    let _ = done_tx.send(()); // Cancel watchdog

    // Check if we timed out (watchdog set the flag)
    if timed_out.load(Ordering::Relaxed) {
        return Err(eyre!(
            "cargo timed out after {timeout_secs}s — possible cargo target/ lock contention \
             from a concurrent cargo process. \
             Set SINEX_CARGO_TIMEOUT env var to adjust. \
             Check for other running xtask/cargo processes with: xtask jobs active"
        ));
    }

    Ok((stdout_bytes, exit_status.success()))
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

/// Parse cargo's JSON output format
pub fn parse_cargo_json_output(
    output: &str,
    success: bool,
) -> color_eyre::eyre::Result<DiagnosticSummary> {
    let mut diagnostics = Vec::new();
    let mut errors = 0;
    let mut warnings = 0;
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

            if let Some(ref pkg) = package {
                compiled_packages.insert(pkg.clone());
            }

            // Check if this is a compiler message
            if json.get("reason").and_then(|r| r.as_str()) == Some("compiler-message")
                && let Some(message) = json.get("message")
                && let Some(mut diag) = parse_diagnostic_message(message)
            {
                // Attach package attribution from the outer JSON envelope
                if diag.package.is_none() {
                    diag.package = package.clone();
                }
                match diag.level.as_str() {
                    "error" => errors += 1,
                    "warning" => warnings += 1,
                    _ => {}
                }
                diagnostics.push(diag);
            }
        }
    }

    Ok(DiagnosticSummary {
        errors,
        warnings,
        diagnostics,
        success,
        compiled_packages,
    })
}

/// Extract the crate name from a cargo `package_id` string.
///
/// Cargo emits package IDs in three formats:
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

    // Legacy format: "name version (registry)"
    let name = package_id.split_whitespace().next()?;
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
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
    let (suggestion, fix_replacement, fix_applicability, fix_byte_start, fix_byte_end) =
        extract_fix_from_children(msg);

    Some(CompilerDiagnostic {
        level: level.to_string(),
        code,
        message: message.to_string(),
        file_path,
        line,
        column,
        rendered,
        suggestion,
        package: None, // Set by caller from outer JSON envelope
        fix_replacement,
        fix_applicability,
        fix_byte_start,
        fix_byte_end,
    })
}

/// Extract suggestion text and machine-applicable fix metadata from diagnostic children.
///
/// Cargo's JSON format nests fix suggestions inside `children` → `spans[]`, each containing:
/// - `suggested_replacement`: the exact replacement text
/// - `suggestion_applicability`: "MachineApplicable", "MaybeIncorrect", "HasPlaceholders", "Unspecified"
/// - `byte_start` / `byte_end`: byte offsets in the source file
///
/// We prefer `MachineApplicable` suggestions when available, falling back to any help message.
fn extract_fix_from_children(
    msg: &serde_json::Value,
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<u32>,
    Option<u32>,
) {
    let children = match msg.get("children").and_then(|c| c.as_array()) {
        Some(c) => c,
        None => return (None, None, None, None, None),
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
        Some((replacement, applicability, byte_start, byte_end)) => (
            suggestion_text,
            Some(replacement),
            Some(applicability),
            byte_start,
            byte_end,
        ),
        None => (suggestion_text, None, None, None, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn test_parse_empty_output() -> TestResult<()> {
        let result = parse_cargo_json_output("", true)?;
        assert_eq!(result.errors, 0);
        assert_eq!(result.warnings, 0);
        assert!(result.success);
        assert!(result.compiled_packages.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_extract_package_name_registry() -> TestResult<()> {
        // Format 1: registry packages — "registry+URL#name@version"
        let id = "registry+https://github.com/rust-lang/crates.io-index#proc-macro2@1.0.103";
        assert_eq!(extract_package_name(id), Some("proc-macro2".into()));

        let id = "registry+https://github.com/rust-lang/crates.io-index#serde@1.0.200";
        assert_eq!(extract_package_name(id), Some("serde".into()));
        Ok(())
    }

    #[sinex_test]
    async fn test_extract_package_name_local_dir_equals_name() -> TestResult<()> {
        // Format 2: local workspace, directory name = crate name — "#version" only
        let id = "path+file:///realm/project/sinex/crate/lib/sinex-primitives#0.1.0";
        assert_eq!(extract_package_name(id), Some("sinex-primitives".into()));

        let id = "path+file:///realm/project/sinex/xtask#0.1.0";
        assert_eq!(extract_package_name(id), Some("xtask".into()));
        Ok(())
    }

    #[sinex_test]
    async fn test_extract_package_name_local_explicit() -> TestResult<()> {
        // Format 3: local workspace, explicit name — "#name@version"
        let id = "path+file:///realm/project/sinex/xtask/macros#xtask-macros@0.1.0";
        assert_eq!(extract_package_name(id), Some("xtask-macros".into()));

        let id = "path+file:///realm/project/sinex#sinex-db@0.2.0";
        assert_eq!(extract_package_name(id), Some("sinex-db".into()));
        Ok(())
    }

    #[sinex_test]
    async fn test_extract_package_name_legacy_format() -> TestResult<()> {
        // Legacy format: "name version (registry)"
        let id = "sinex-primitives 0.1.0 (path+file:///realm/project/sinex)";
        assert_eq!(extract_package_name(id), Some("sinex-primitives".into()));
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_compiler_message_with_package() -> TestResult<()> {
        let json_line = r#"{"reason":"compiler-message","package_id":"path+file:///realm/project/sinex#sinex-db@0.1.0","message":{"level":"warning","code":{"code":"unused_imports","explanation":null},"message":"unused import","spans":[{"file_name":"src/lib.rs","byte_start":42,"byte_end":55,"line_start":3,"line_end":3,"column_start":5,"column_end":18,"is_primary":true}],"children":[{"level":"help","message":"remove the import","spans":[{"byte_start":42,"byte_end":55,"suggestion_applicability":"MachineApplicable","suggested_replacement":""}]}],"rendered":"warning: unused import"}}"#;

        let result = parse_cargo_json_output(json_line, true)?;
        assert_eq!(result.warnings, 1);
        assert_eq!(result.compiled_packages.len(), 1);
        assert!(result.compiled_packages.contains("sinex-db"));

        let diag = &result.diagnostics[0];
        assert_eq!(diag.package.as_deref(), Some("sinex-db"));
        assert_eq!(diag.code.as_deref(), Some("unused_imports"));
        assert_eq!(diag.fix_applicability.as_deref(), Some("MachineApplicable"));
        assert_eq!(diag.fix_replacement.as_deref(), Some(""));
        assert_eq!(diag.fix_byte_start, Some(42));
        assert_eq!(diag.fix_byte_end, Some(55));
        Ok(())
    }

    #[sinex_test]
    async fn test_compiled_packages_tracked_from_artifacts() -> TestResult<()> {
        // compiler-artifact messages also carry package_id
        let lines = [
            r#"{"reason":"compiler-artifact","package_id":"path+file:///realm/project/sinex#sinex-primitives@0.1.0","target":{"name":"sinex-primitives"}}"#,
            r#"{"reason":"compiler-artifact","package_id":"path+file:///realm/project/sinex#sinex-db@0.1.0","target":{"name":"sinex-db"}}"#,
        ];
        let output = lines.join("\n");
        let result = parse_cargo_json_output(&output, true)?;
        assert_eq!(result.compiled_packages.len(), 2);
        assert!(result.compiled_packages.contains("sinex-primitives"));
        assert!(result.compiled_packages.contains("sinex-db"));
        Ok(())
    }
}

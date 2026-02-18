//! Cargo diagnostic parsing - extract structured errors from cargo --message-format=json

use serde::{Deserialize, Serialize};
use std::process::{Command, Stdio};

/// A parsed compiler diagnostic
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompilerDiagnostic {
    pub level: String,
    pub code: Option<String>,
    pub message: String,
    pub file_path: Option<String>,
    pub line: Option<u32>,
    pub column: Option<u32>,
    pub rendered: Option<String>,
    pub suggestion: Option<String>,
}

/// Summary of compiler output
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticSummary {
    pub errors: usize,
    pub warnings: usize,
    pub diagnostics: Vec<CompilerDiagnostic>,
    pub success: bool,
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
            if diag.level == "warning" {
                if let Some(ref code) = diag.code {
                    *counts.entry(code.clone()).or_insert(0) += 1;
                }
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
            if diag.level == "warning" {
                if let Some(ref path) = diag.file_path {
                    *counts.entry(path.clone()).or_insert(0) += 1;
                }
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

/// Run cargo check with JSON output and parse diagnostics
pub fn run_cargo_check(args: &[&str]) -> color_eyre::eyre::Result<DiagnosticSummary> {
    let mut cmd_args = vec!["check", "--message-format=json"];
    cmd_args.extend(args);

    let output = Command::new("cargo")
        .args(&cmd_args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_cargo_json_output(&stdout, output.status.success())
}

/// Run cargo clippy with JSON output and parse diagnostics
pub fn run_cargo_clippy(args: &[&str]) -> color_eyre::eyre::Result<DiagnosticSummary> {
    let mut cmd_args = vec!["clippy", "--message-format=json"];
    cmd_args.extend(args);

    let output = Command::new("cargo")
        .args(&cmd_args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_cargo_json_output(&stdout, output.status.success())
}

/// Parse cargo's JSON output format
pub fn parse_cargo_json_output(output: &str, success: bool) -> color_eyre::eyre::Result<DiagnosticSummary> {
    let mut diagnostics = Vec::new();
    let mut errors = 0;
    let mut warnings = 0;

    for line in output.lines() {
        if !line.trim().is_empty() {
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
                // Check if this is a compiler message
                if json.get("reason").and_then(|r| r.as_str()) == Some("compiler-message") {
                    if let Some(message) = json.get("message") {
                        if let Some(diag) = parse_diagnostic_message(message) {
                            match diag.level.as_str() {
                                "error" => errors += 1,
                                "warning" => warnings += 1,
                                _ => {}
                            }
                            diagnostics.push(diag);
                        }
                    }
                }
            }
        }
    }

    Ok(DiagnosticSummary {
        errors,
        warnings,
        diagnostics,
        success,
    })
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

    // Check for suggestions
    let suggestion = msg
        .get("children")
        .and_then(|c| c.as_array())
        .and_then(|children| {
            children
                .iter()
                .find(|child| child.get("level").and_then(|l| l.as_str()) == Some("help"))
                .and_then(|help| help.get("message").and_then(|m| m.as_str()))
                .map(std::string::ToString::to_string)
        });

    Some(CompilerDiagnostic {
        level: level.to_string(),
        code,
        message: message.to_string(),
        file_path,
        line,
        column,
        rendered,
        suggestion,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    fn test_parse_empty_output() -> TestResult<()> {
        let result = parse_cargo_json_output("", true)?;
        assert_eq!(result.errors, 0);
        assert_eq!(result.warnings, 0);
        assert!(result.success);
        Ok(())
    }
}

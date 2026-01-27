//! Output formatting for xtask commands.
//!
//! Provides structured output in multiple formats (Human, JSON, Compact, Silent)
//! to support both interactive use and machine consumption (AI agents, scripts).

use chrono::{DateTime, Utc};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::io::{self, Write};

/// Output format for command results.
#[derive(Debug, Clone, Copy, Default, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    /// Human-readable output with colors and headers (default)
    #[default]
    Human,
    /// Machine-parseable JSON output
    Json,
    /// Single-line compact summary
    Compact,
    /// No output, exit code only
    Silent,
}

/// Execution status of a command.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    /// Command completed successfully
    Success,
    /// Command failed
    Failed,
    /// Command partially succeeded (some subtasks failed)
    Partial,
    /// Command is still running (for async operations)
    Running,
    /// Command was cancelled
    Cancelled,
}

impl Status {
    #[allow(dead_code)]
    pub fn is_success(&self) -> bool {
        matches!(self, Status::Success)
    }

    pub fn symbol(&self) -> &'static str {
        match self {
            Status::Success => "✓",
            Status::Failed => "✗",
            Status::Partial => "⚠",
            Status::Running => "⋯",
            Status::Cancelled => "⊘",
        }
    }

    pub fn color_code(&self) -> &'static str {
        match self {
            Status::Success => "\x1b[32m",   // Green
            Status::Failed => "\x1b[31m",    // Red
            Status::Partial => "\x1b[33m",   // Yellow
            Status::Running => "\x1b[36m",   // Cyan
            Status::Cancelled => "\x1b[90m", // Gray
        }
    }
}

/// A structured error with optional location and suggestion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuredError {
    /// Error code (e.g., "E0308", "DB_UNREACHABLE", "SCHEMA_STALE")
    pub code: String,
    /// Human-readable error message
    pub message: String,
    /// Optional file location (path:line:column)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    /// Optional suggested fix or next action
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<String>,
}

impl StructuredError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            location: None,
            suggestion: None,
        }
    }

    #[allow(dead_code)]
    pub fn with_location(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }

    #[allow(dead_code)]
    pub fn with_suggestion(mut self, suggestion: impl Into<String>) -> Self {
        self.suggestion = Some(suggestion.into());
        self
    }
}

/// Result of executing a command, suitable for JSON serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResult {
    /// Command name (e.g., "test", "check", "lint")
    pub command: String,
    /// Optional subcommand (e.g., "html" for "coverage html")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subcommand: Option<String>,
    /// Optional summary message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Execution status
    pub status: Status,
    /// Duration in seconds
    pub duration_secs: f64,
    /// Timestamp when command completed
    pub timestamp: DateTime<Utc>,
    /// Command-specific details
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
    /// Structured data
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    /// Whether to suppress all output in human/compact modes
    #[serde(skip)]
    pub is_silent: bool,
    /// Structured errors encountered
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub errors: Vec<StructuredError>,
    /// Suggested fixes for common issues
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub suggested_fixes: Vec<String>,
}

impl CommandResult {
    /// Create a new successful result.
    pub fn success(command: impl Into<String>, duration_secs: f64) -> Self {
        Self {
            command: command.into(),
            subcommand: None,
            message: None,
            status: Status::Success,
            duration_secs,
            timestamp: Utc::now(),
            details: None,
            data: None,
            is_silent: false,
            errors: Vec::new(),
            suggested_fixes: Vec::new(),
        }
    }

    /// Create a new failed result.
    pub fn failed(command: impl Into<String>, duration_secs: f64) -> Self {
        Self {
            command: command.into(),
            subcommand: None,
            message: None,
            status: Status::Failed,
            duration_secs,
            timestamp: Utc::now(),
            details: None,
            data: None,
            is_silent: false,
            errors: Vec::new(),
            suggested_fixes: Vec::new(),
        }
    }

    #[allow(dead_code)]
    pub fn with_silent(mut self) -> Self {
        self.is_silent = true;
        self
    }

    #[allow(dead_code)]
    pub fn with_subcommand(mut self, subcommand: impl Into<String>) -> Self {
        self.subcommand = Some(subcommand.into());
        self
    }

    #[allow(dead_code)]
    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }

    pub fn with_data(mut self, data: serde_json::Value) -> Self {
        self.data = Some(data);
        self
    }

    pub fn with_error(mut self, error: StructuredError) -> Self {
        self.errors.push(error);
        self
    }

    #[allow(dead_code)]
    pub fn with_errors(mut self, errors: Vec<StructuredError>) -> Self {
        self.errors.extend(errors);
        self
    }

    #[allow(dead_code)]
    pub fn with_suggestion(mut self, fix: impl Into<String>) -> Self {
        self.suggested_fixes.push(fix.into());
        self
    }
}

/// Output writer that respects the configured format.
pub struct OutputWriter {
    format: OutputFormat,
    /// Whether stdout is a TTY (for color support)
    is_tty: bool,
}

impl OutputWriter {
    pub fn new(format: OutputFormat) -> Self {
        Self {
            format,
            is_tty: atty::is(atty::Stream::Stdout),
        }
    }

    /// Get the output format.
    pub fn format(&self) -> OutputFormat {
        self.format
    }

    /// Write a command result in the configured format.
    pub fn write_result(&self, result: &CommandResult) -> io::Result<()> {
        match self.format {
            OutputFormat::Human => self.write_human(result),
            OutputFormat::Json => self.write_json(result),
            OutputFormat::Compact => {
                if result.is_silent {
                    Ok(())
                } else {
                    self.write_compact(result)
                }
            }
            OutputFormat::Silent => Ok(()), // No output
        }
    }

    fn write_human(&self, result: &CommandResult) -> io::Result<()> {
        let mut out = io::stdout().lock();

        if !result.is_silent {
            // Status line
            let status_str = if self.is_tty {
                format!(
                    "{}{}{} ",
                    result.status.color_code(),
                    result.status.symbol(),
                    "\x1b[0m"
                )
            } else {
                format!("{} ", result.status.symbol())
            };

            write!(out, "{}", status_str)?;

            // Command name
            let cmd_name = match &result.subcommand {
                Some(sub) => format!("{} {}", result.command, sub),
                None => result.command.clone(),
            };
            writeln!(out, "{} ({:.2}s)", cmd_name, result.duration_secs)?;

            if let Some(msg) = &result.message {
                writeln!(out, "{}", msg)?;
            }
        }

        // Details
        if let Some(details) = &result.details {
            if let Some(details_array) = details.as_array() {
                for detail in details_array {
                    writeln!(out, "  • {}", detail.as_str().unwrap_or(""))?;
                }
            }
        }

        // Data (if it's a string, print it directly; if object/array, print as pretty JSON)
        if let Some(data) = &result.data {
            match data {
                serde_json::Value::String(s) => writeln!(out, "{}", s)?,
                serde_json::Value::Null => {}
                _ => {
                    let json = serde_json::to_string_pretty(data)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    writeln!(out, "{}", json)?;
                }
            }
        }

        // Errors
        for error in &result.errors {
            if self.is_tty {
                write!(out, "  \x1b[31m{}\x1b[0m: ", error.code)?;
            } else {
                write!(out, "  {}: ", error.code)?;
            }
            writeln!(out, "{}", error.message)?;
            if let Some(loc) = &error.location {
                writeln!(out, "    at {}", loc)?;
            }
            if let Some(sug) = &error.suggestion {
                writeln!(out, "    suggestion: {}", sug)?;
            }
        }

        // Suggestions
        if !result.suggested_fixes.is_empty() {
            writeln!(out)?;
            writeln!(out, "Suggested fixes:")?;
            for fix in &result.suggested_fixes {
                writeln!(out, "  - {}", fix)?;
            }
        }

        Ok(())
    }

    fn write_json(&self, result: &CommandResult) -> io::Result<()> {
        let json = serde_json::to_string_pretty(result)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        println!("{}", json);
        Ok(())
    }

    fn write_compact(&self, result: &CommandResult) -> io::Result<()> {
        let cmd_name = match &result.subcommand {
            Some(sub) => format!("{} {}", result.command, sub),
            None => result.command.clone(),
        };

        let error_count = result.errors.len();
        let detail = if error_count > 0 {
            format!(
                " ({} error{})",
                error_count,
                if error_count == 1 { "" } else { "s" }
            )
        } else {
            String::new()
        };

        if self.is_tty {
            println!(
                "{}{}{} {}: {:.1}s{}",
                result.status.color_code(),
                result.status.symbol(),
                "\x1b[0m",
                cmd_name,
                result.duration_secs,
                detail
            );
        } else {
            println!(
                "{} {}: {:.1}s{}",
                result.status.symbol(),
                cmd_name,
                result.duration_secs,
                detail
            );
        }

        Ok(())
    }

    /// Write a progress update (for streaming output).
    #[allow(dead_code)]
    pub fn write_progress(
        &self,
        stage: &str,
        current: usize,
        total: usize,
        message: &str,
    ) -> io::Result<()> {
        match self.format {
            OutputFormat::Json => {
                let progress = serde_json::json!({
                    "type": "progress",
                    "stage": stage,
                    "current": current,
                    "total": total,
                    "message": message,
                });
                println!("{}", progress);
            }
            OutputFormat::Human if self.is_tty => {
                // Overwrite current line with progress bar
                let pct = if total > 0 {
                    (current * 100) / total
                } else {
                    0
                };
                let bar_width = 30;
                let filled = (pct * bar_width) / 100;
                let empty = bar_width - filled;
                print!(
                    "\r[{}{}] {}/{} {}",
                    "=".repeat(filled),
                    " ".repeat(empty),
                    current,
                    total,
                    message
                );
                io::stderr().flush()?;
            }
            OutputFormat::Human => {
                // Non-TTY: simple line
                eprintln!("[{}/{}] {} {}", current, total, stage, message);
            }
            OutputFormat::Compact | OutputFormat::Silent => {}
        }
        Ok(())
    }

    /// Clear the progress line (for Human format with TTY).
    #[allow(dead_code)]
    pub fn clear_progress(&self) -> io::Result<()> {
        if matches!(self.format, OutputFormat::Human) && self.is_tty {
            eprint!("\r{}\r", " ".repeat(80));
            io::stderr().flush()?;
        }
        Ok(())
    }
}

/// Check if we're running with a TTY (useful for deciding on colors).
#[allow(dead_code)]
pub fn is_tty() -> bool {
    atty::is(atty::Stream::Stdout)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_symbol() {
        assert_eq!(Status::Success.symbol(), "✓");
        assert_eq!(Status::Failed.symbol(), "✗");
    }

    #[test]
    fn test_command_result_json() {
        let result = CommandResult::success("test", 1.5)
            .with_subcommand("fast")
            .with_error(StructuredError::new("E001", "Test failed"));

        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"command\":\"test\""));
        assert!(json.contains("\"subcommand\":\"fast\""));
        assert!(json.contains("\"status\":\"success\""));
    }
}

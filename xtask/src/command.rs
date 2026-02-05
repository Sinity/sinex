//! Command trait and execution framework for xtask.
//!
//! Provides a consistent interface for all xtask commands with:
//! - Standardized execution flow
//! - Automatic history tracking
//! - Metadata for timeouts and categories
//! - Structured output formatting
//!
//! # Architecture
//!
//! Commands implement the `XtaskCommand` trait and are dispatched through
//! a central `CommandContext` that handles:
//! - Output formatting (JSON, human-readable, compact, silent)
//! - History tracking in `SQLite`
//! - Error handling and recovery
//!
//! # Example
//!
//! ```no_run
//! use xtask::command::{XtaskCommand, CommandContext, ExecutionResult};
//! use anyhow::Result;
//!
//! struct MyCommand {
//!     verbose: bool,
//! }
//!
//! #[async_trait::async_trait]
//! impl XtaskCommand for MyCommand {
//!     fn name(&self) -> &str {
//!         "my-command"
//!     }
//!
//!     async fn execute(&self, ctx: &CommandContext) -> Result<ExecutionResult> {
//!         // Command logic here
//!         Ok(ExecutionResult::success())
//!     }
//! }
//! ```

use anyhow::Result;
use serde::{Deserialize, Serialize};
use sinex_schema::primitives::Timestamp;
use std::time::Duration;

use crate::output::{OutputWriter, Status, StructuredError};

/// Metadata about a command's execution requirements and characteristics.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct CommandMetadata {
    /// Command category for organization (e.g., "build", "test", "database")
    pub category: Option<String>,
    /// Expected timeout duration (None = no timeout)
    pub timeout: Option<Duration>,
    /// Whether this command modifies state (vs read-only)
    pub modifies_state: bool,
    /// Whether to track this command in history
    pub track_in_history: bool,
}

impl Default for CommandMetadata {
    fn default() -> Self {
        Self {
            category: None,
            timeout: None,
            modifies_state: false,
            track_in_history: true,
        }
    }
}

#[allow(dead_code)]
impl CommandMetadata {
    /// Create metadata for a build/compilation command.
    #[must_use]
    pub fn build() -> Self {
        Self {
            category: Some("build".to_string()),
            timeout: Some(Duration::from_mins(5)), // 5 minutes
            modifies_state: true,
            track_in_history: true,
        }
    }

    /// Create metadata for a test command.
    #[must_use]
    pub fn test() -> Self {
        Self {
            category: Some("test".to_string()),
            timeout: Some(Duration::from_mins(10)), // 10 minutes
            modifies_state: false,
            track_in_history: true,
        }
    }

    /// Create metadata for a database command.
    #[must_use]
    pub fn database() -> Self {
        Self {
            category: Some("database".to_string()),
            timeout: Some(Duration::from_mins(2)), // 2 minutes
            modifies_state: true,
            track_in_history: true,
        }
    }

    /// Create metadata for a quick check/lint command.
    #[must_use]
    pub fn check() -> Self {
        Self {
            category: Some("check".to_string()),
            timeout: Some(Duration::from_mins(5)), // 5 minutes (preflight + fmt + check + clippy)
            modifies_state: false,
            track_in_history: true,
        }
    }

    /// Create metadata for utility commands (completions, help, etc.).
    #[must_use]
    pub fn utility() -> Self {
        Self {
            category: Some("utility".to_string()),
            timeout: None,
            modifies_state: false,
            track_in_history: false,
        }
    }

    /// Create metadata for diagnostic commands (doctor, health checks).
    #[must_use]
    pub fn diagnostics() -> Self {
        Self {
            category: Some("diagnostics".to_string()),
            timeout: Some(Duration::from_mins(2)), // 2 minutes
            modifies_state: false,
            track_in_history: true,
        }
    }
}

/// Result of command execution.
///
/// Note: This was renamed from `CommandResult` to `ExecutionResult` to avoid confusion
/// with `output::CommandResult`. The `CommandResult` name is preserved as a type alias
/// for backwards compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionResult {
    /// Execution status
    pub status: Status,
    /// Optional success/summary message
    pub message: Option<String>,
    /// Additional details (e.g., list of checks passed)
    pub details: Vec<String>,
    /// Optional structured data
    pub data: Option<serde_json::Value>,
    /// Whether to suppress all output in human/compact modes
    pub is_silent: bool,
    /// Errors that occurred (empty if success)
    pub errors: Vec<StructuredError>,
    /// Warnings (non-fatal issues)
    pub warnings: Vec<String>,
    /// Execution duration in seconds
    pub duration_secs: Option<f64>,
    /// Timestamp when command completed
    pub timestamp: Option<Timestamp>,
}

/// Backwards compatibility alias for `ExecutionResult`.
///
/// Prefer using `ExecutionResult` in new code to avoid confusion with `output::CommandResult`.
pub type CommandResult = ExecutionResult;

impl ExecutionResult {
    /// Create a successful result.
    #[must_use]
    pub fn success() -> Self {
        Self {
            status: Status::Success,
            message: None,
            details: Vec::new(),
            data: None,
            is_silent: false,
            errors: Vec::new(),
            warnings: Vec::new(),
            duration_secs: None,
            timestamp: Some(Timestamp::now()),
        }
    }

    /// Create a failed result with an error.
    #[must_use]
    pub fn failure(error: StructuredError) -> Self {
        Self {
            status: Status::Failed,
            message: None,
            details: Vec::new(),
            data: None,
            is_silent: false,
            errors: vec![error],
            warnings: Vec::new(),
            duration_secs: None,
            timestamp: Some(Timestamp::now()),
        }
    }

    /// Create a partial success result (some subtasks failed).
    #[allow(dead_code)]
    #[must_use]
    pub fn partial() -> Self {
        Self {
            status: Status::Partial,
            message: None,
            details: Vec::new(),
            data: None,
            is_silent: false,
            errors: Vec::new(),
            warnings: Vec::new(),
            duration_secs: None,
            timestamp: Some(Timestamp::now()),
        }
    }

    /// Suppress all output in human/compact modes
    #[allow(dead_code)]
    #[must_use]
    pub fn with_silent(mut self) -> Self {
        self.is_silent = true;
        self
    }

    /// Add a success message.
    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    /// Add structured data.
    #[must_use]
    pub fn with_data(mut self, data: serde_json::Value) -> Self {
        self.data = Some(data);
        self
    }

    /// Add detail items.
    pub fn with_details<I, S>(mut self, details: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.details
            .extend(details.into_iter().map(std::convert::Into::into));
        self
    }

    /// Add a single detail item.
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.details.push(detail.into());
        self
    }

    /// Add warnings.
    #[allow(dead_code)]
    pub fn with_warnings<I, S>(mut self, warnings: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.warnings
            .extend(warnings.into_iter().map(std::convert::Into::into));
        self
    }

    /// Add a single warning.
    pub fn with_warning(mut self, warning: impl Into<String>) -> Self {
        self.warnings.push(warning.into());
        self
    }

    /// Add an error.
    #[allow(dead_code)]
    #[must_use]
    pub fn with_error(mut self, error: StructuredError) -> Self {
        self.errors.push(error);
        if self.status == Status::Success {
            self.status = Status::Failed;
        }
        self
    }

    /// Set the duration.
    #[must_use]
    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.duration_secs = Some(duration.as_secs_f64());
        self
    }

    /// Check if the result represents success.
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.status == Status::Success
    }

    /// Check if the result represents failure.
    #[allow(dead_code)]
    #[must_use]
    pub fn is_failure(&self) -> bool {
        self.status == Status::Failed
    }
    /// Print the result using the given writer.
    pub fn print(&self, writer: &OutputWriter, command_name: &str) {
        let output_res = crate::output::CommandResult {
            command: command_name.to_string(),
            subcommand: None, // Could parse from name if space? But passed name is top-level.
            message: self.message.clone(),
            status: self.status,
            duration_secs: self.duration_secs.unwrap_or(0.0),
            timestamp: self.timestamp.unwrap_or_else(Timestamp::now),
            details: if self.details.is_empty() {
                None
            } else {
                Some(serde_json::to_value(&self.details).unwrap_or(serde_json::json!([])))
            },
            data: self.data.clone(),
            is_silent: self.is_silent,
            errors: self.errors.clone(),
            suggested_fixes: self.warnings.clone(),
        };
        writer.write_result(&output_res).ok();
    }
}

/// Context passed to commands during execution.
pub struct CommandContext {
    start_time: std::time::Instant,
    writer: crate::output::OutputWriter,
    // json: bool, // Removed unused field
    background: bool,
    invocation_id: Option<i64>,
}

impl CommandContext {
    #[must_use]
    pub fn new(
        writer: crate::output::OutputWriter,
        _json: bool,
        background: bool,
        invocation_id: Option<i64>,
    ) -> Self {
        Self {
            start_time: std::time::Instant::now(),
            writer,
            // json,
            background,
            invocation_id,
        }
    }

    #[must_use]
    pub fn is_verbose(&self) -> bool {
        // Verbosity implied by format or specific flags if we add them later
        false
    }

    #[must_use]
    pub fn is_background(&self) -> bool {
        self.background
    }

    #[must_use]
    pub fn invocation_id(&self) -> Option<i64> {
        self.invocation_id
    }

    #[must_use]
    pub fn writer(&self) -> &crate::output::OutputWriter {
        &self.writer
    }

    /// Get elapsed time since command started.
    #[must_use]
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Check if output format is human-readable.
    #[must_use]
    pub fn is_human(&self) -> bool {
        matches!(self.writer.format(), crate::output::OutputFormat::Human)
    }

    /// Check if output format is JSON.
    #[allow(dead_code)]
    #[must_use]
    pub fn is_json(&self) -> bool {
        matches!(self.writer.format(), crate::output::OutputFormat::Json)
    }

    /// Print a section heading (only in human-readable mode).
    pub fn heading(&self, title: &str) {
        if self.is_human() {
            println!("========== {title} ==========");
        }
    }

    /// Record a diagnostic to the history database.
    ///
    /// This is used by check/build commands to capture compiler warnings/errors.
    pub fn record_diagnostic(
        &self,
        diag: &crate::cargo_diagnostics::CompilerDiagnostic,
    ) -> Result<()> {
        use crate::config::config;
        use crate::history::HistoryDb;

        if let Some(inv_id) = self.invocation_id {
            let cfg = config();
            if let Ok(db) = HistoryDb::open(&cfg.history_db_path()) {
                db.record_diagnostic(
                    inv_id,
                    &diag.level,
                    diag.code.as_deref(),
                    &diag.message,
                    diag.file_path.as_deref(),
                    diag.line,
                    diag.column,
                    diag.rendered.as_deref(),
                )?;
            }
        }
        Ok(())
    }

    /// Record multiple diagnostics to the history database.
    pub fn record_diagnostics(
        &self,
        diagnostics: &[crate::cargo_diagnostics::CompilerDiagnostic],
    ) -> Result<()> {
        for diag in diagnostics {
            self.record_diagnostic(diag)?;
        }
        Ok(())
    }

    /// Spawn a command as a background job.
    ///
    /// Returns a `CommandResult` with the job ID and log paths. The actual command
    /// execution happens in a separate process.
    pub async fn spawn_background(
        &self,
        subcommand: &str,
        args: &[String],
    ) -> Result<ExecutionResult> {
        use crate::config::config;
        use crate::jobs::JobManager;

        let cfg = config();
        let manager = JobManager::new(cfg.jobs_dir())?;
        let job = manager.spawn_xtask(subcommand, args).await?;

        let result = ExecutionResult::success()
            .with_message(format!("Started background job {}", job.id))
            .with_data(serde_json::json!({
                "job_id": job.id,
                "stdout": job.stdout_path.display().to_string(),
                "stderr": job.stderr_path.display().to_string(),
                "command": subcommand,
                "args": args,
                "hint": format!("Monitor with: cargo xtask jobs status {}", job.id),
            }));

        if self.is_human() {
            println!("🚀 Started background job {}", job.id);
            println!("   Command: cargo xtask {} {}", subcommand, args.join(" "));
            println!("   Logs: {}", job.stdout_path.display());
            println!();
            println!("   Monitor: cargo xtask jobs status {}", job.id);
            println!("   Output:  cargo xtask jobs output {}", job.id);
            println!("   Cancel:  cargo xtask jobs cancel {}", job.id);
        }

        Ok(result.with_duration(self.elapsed()))
    }
}

#[async_trait::async_trait]
pub trait XtaskCommand {
    /// Get the command name (used for history tracking and error messages).
    fn name(&self) -> &str;

    /// Execute the command with the given context.
    ///
    /// Implementations should:
    /// - Use `ctx.writer()` for output formatting
    /// - Use `ProcessBuilder` for spawning processes
    /// - Return `CommandResult` with appropriate status and details
    async fn execute(&self, ctx: &CommandContext) -> Result<ExecutionResult>;

    /// Get command metadata (optional, defaults to basic metadata).
    #[allow(dead_code)]
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestCommand {
        should_fail: bool,
    }

    #[async_trait::async_trait]
    impl XtaskCommand for TestCommand {
        fn name(&self) -> &'static str {
            "test-command"
        }

        async fn execute(&self, _ctx: &CommandContext) -> Result<ExecutionResult> {
            if self.should_fail {
                Ok(CommandResult::failure(StructuredError {
                    code: "TEST_ERROR".to_string(),
                    message: "Test failure".to_string(),
                    location: None,
                    suggestion: None,
                }))
            } else {
                Ok(CommandResult::success().with_message("Test passed"))
            }
        }

        fn metadata(&self) -> CommandMetadata {
            CommandMetadata::check()
        }
    }

    #[tokio::test]
    async fn test_command_success() {
        let cmd = TestCommand { should_fail: false };
        let ctx = CommandContext::new(
            OutputWriter::new(crate::output::OutputFormat::Silent),
            false,
            false,
            None,
        );
        let result = cmd.execute(&ctx).await.expect("should not error");

        assert!(result.is_success());
        assert_eq!(result.message, Some("Test passed".to_string()));
    }

    #[tokio::test]
    async fn test_command_failure() {
        let cmd = TestCommand { should_fail: true };
        let ctx = CommandContext::new(
            OutputWriter::new(crate::output::OutputFormat::Silent),
            false,
            false,
            None,
        );
        let result = cmd.execute(&ctx).await.expect("should not error");

        assert!(result.is_failure());
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.errors[0].code, "TEST_ERROR");
    }

    #[test]
    fn test_command_metadata() {
        let cmd = TestCommand { should_fail: false };
        let metadata = cmd.metadata();

        assert_eq!(metadata.category, Some("check".to_string()));
        assert!(metadata.timeout.is_some());
    }

    #[test]
    fn test_command_result_builder() {
        let result = CommandResult::success()
            .with_message("All checks passed")
            .with_details(vec!["Check 1", "Check 2"])
            .with_warnings(vec!["Warning 1"]);

        assert!(result.is_success());
        assert_eq!(result.message, Some("All checks passed".to_string()));
        assert_eq!(result.details.len(), 2);
        assert_eq!(result.warnings.len(), 1);
    }

    #[test]
    fn test_command_result_partial() {
        let result = CommandResult::partial()
            .with_message("Some checks failed")
            .with_detail("Completed: 3/5");

        assert_eq!(result.status, Status::Partial);
        assert_eq!(result.details.len(), 1);
    }

    #[test]
    fn test_command_result_with_error() {
        let result = CommandResult::success().with_error(StructuredError {
            code: "ERR001".to_string(),
            message: "Test error".to_string(),
            location: None,
            suggestion: None,
        });

        assert!(result.is_failure());
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.errors[0].code, "ERR001");
    }

    #[test]
    fn test_command_result_duration() {
        let duration = std::time::Duration::from_secs(5);
        let result = CommandResult::success().with_duration(duration);

        assert_eq!(result.duration_secs, Some(5.0));
    }

    #[test]
    fn test_command_context_elapsed() {
        let ctx = CommandContext::new(
            OutputWriter::new(crate::output::OutputFormat::Silent),
            false,
            false,
            None,
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
        let elapsed = ctx.elapsed();

        assert!(elapsed.as_millis() >= 10);
    }

    #[test]
    fn test_command_context_is_human() {
        let ctx_human = CommandContext::new(
            OutputWriter::new(crate::output::OutputFormat::Human),
            false,
            false,
            None,
        );
        assert!(ctx_human.is_human());

        let ctx_json = CommandContext::new(
            OutputWriter::new(crate::output::OutputFormat::Json),
            true,
            false,
            None,
        );
        assert!(!ctx_json.is_human());
    }

    #[test]
    fn test_command_context_is_json() {
        let ctx_json = CommandContext::new(
            OutputWriter::new(crate::output::OutputFormat::Json),
            true,
            false,
            None,
        );
        assert!(ctx_json.is_json());

        let ctx_human = CommandContext::new(
            OutputWriter::new(crate::output::OutputFormat::Human),
            false,
            false,
            None,
        );
        assert!(!ctx_human.is_json());
    }

    #[test]
    fn test_command_metadata_builders() {
        let build_meta = CommandMetadata::build();
        assert_eq!(build_meta.category, Some("build".to_string()));
        assert!(build_meta.modifies_state);
        assert!(build_meta.timeout.is_some());

        let test_meta = CommandMetadata::test();
        assert_eq!(test_meta.category, Some("test".to_string()));
        assert!(!test_meta.modifies_state);

        let db_meta = CommandMetadata::database();
        assert_eq!(db_meta.category, Some("database".to_string()));
        assert!(db_meta.modifies_state);
    }

    #[test]
    fn test_command_result_with_detail() {
        let result = CommandResult::success()
            .with_detail("First detail")
            .with_detail("Second detail");

        assert_eq!(result.details.len(), 2);
        assert_eq!(result.details[0], "First detail");
        assert_eq!(result.details[1], "Second detail");
    }

    #[test]
    fn test_command_result_with_warning() {
        let result = CommandResult::success().with_warning("This is a warning");

        assert_eq!(result.warnings.len(), 1);
        assert_eq!(result.warnings[0], "This is a warning");
    }
}

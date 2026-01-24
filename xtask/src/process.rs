//! Process execution helpers for xtask commands.
//!
//! Provides a fluent builder API for spawning external processes with:
//! - Consistent error handling and context
//! - Automatic output capture and formatting
//! - NixOS-aware PATH handling
//! - Special handling for common tools (git, cargo, postgres)
//!
//! # Examples
//!
//! ```no_run
//! use xtask::process::ProcessBuilder;
//!
//! // Simple command execution
//! let output = ProcessBuilder::new("ls")
//!     .args(&["-la", "/tmp"])
//!     .run()?;
//!
//! // Git command with automatic context
//! let output = ProcessBuilder::git()
//!     .args(&["status", "--short"])
//!     .run()?;
//!
//! // Cargo command
//! let output = ProcessBuilder::cargo()
//!     .args(&["build", "--release"])
//!     .run()?;
//! ```

use anyhow::{Context, Result};
use std::process::{Command, Stdio};

/// Output from a process execution.
#[derive(Debug)]
pub struct ProcessOutput {
    /// Standard output as UTF-8 string
    pub stdout: String,
    /// Standard error as UTF-8 string
    #[allow(dead_code)]
    pub stderr: String,
    /// Exit status code
    pub exit_code: i32,
}

impl ProcessOutput {
    /// Check if the process succeeded (exit code 0).
    pub fn success(&self) -> bool {
        self.exit_code == 0
    }

    /// Get combined output (stdout + stderr).
    #[allow(dead_code)]
    pub fn combined(&self) -> String {
        format!("{}{}", self.stdout, self.stderr)
    }
}

/// Builder for executing external processes with consistent error handling.
pub struct ProcessBuilder {
    command: Command,
    description: Option<String>,
    capture_output: bool,
}

impl ProcessBuilder {
    /// Create a new process builder for the given program.
    pub fn new(program: impl AsRef<str>) -> Self {
        let mut command = Command::new(program.as_ref());
        command.stdin(Stdio::null());

        Self {
            command,
            description: None,
            capture_output: true,
        }
    }

    /// Create a git command builder with automatic context.
    pub fn git() -> Self {
        Self::new("git").with_description("git command")
    }

    /// Create a cargo command builder with automatic context.
    pub fn cargo() -> Self {
        Self::new("cargo").with_description("cargo command")
    }

    /// Create a psql (PostgreSQL) command builder.
    pub fn psql() -> Self {
        Self::new("psql").with_description("PostgreSQL command")
    }

    /// Create a nix command builder.
    #[allow(dead_code)]
    pub fn nix() -> Self {
        Self::new("nix").with_description("nix command")
    }

    /// Set command arguments.
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for arg in args {
            self.command.arg(arg.as_ref());
        }
        self
    }

    /// Set a single argument.
    pub fn arg(mut self, arg: impl AsRef<str>) -> Self {
        self.command.arg(arg.as_ref());
        self
    }

    /// Set an environment variable.
    #[allow(dead_code)]
    pub fn env(mut self, key: impl AsRef<str>, val: impl AsRef<str>) -> Self {
        self.command.env(key.as_ref(), val.as_ref());
        self
    }

    /// Set the working directory.
    #[allow(dead_code)]
    pub fn current_dir(mut self, dir: impl AsRef<std::path::Path>) -> Self {
        self.command.current_dir(dir);
        self
    }

    /// Set a description for error messages.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Disable output capture (inherit stdio from parent).
    pub fn inherit_output(mut self) -> Self {
        self.capture_output = false;
        self.command.stdout(Stdio::inherit());
        self.command.stderr(Stdio::inherit());
        self
    }

    /// Execute the command and return the output.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The command fails to spawn
    /// - The command exits with non-zero status
    /// - Output cannot be decoded as UTF-8
    pub fn run(mut self) -> Result<ProcessOutput> {
        let program = self.command.get_program().to_string_lossy().to_string();
        let args: Vec<String> = self
            .command
            .get_args()
            .map(|s| s.to_string_lossy().to_string())
            .collect();

        let context_msg = self.description.unwrap_or_else(|| {
            if args.is_empty() {
                format!("running {}", program)
            } else {
                format!("running {} {}", program, args.join(" "))
            }
        });

        if self.capture_output {
            self.command.stdout(Stdio::piped());
            self.command.stderr(Stdio::piped());
        }

        let output = self
            .command
            .output()
            .with_context(|| format!("failed to spawn: {}", context_msg))?;

        let exit_code = output.status.code().unwrap_or(-1);

        if self.capture_output {
            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();

            if !output.status.success() {
                anyhow::bail!(
                    "{} failed with exit code {}\nstderr: {}",
                    context_msg,
                    exit_code,
                    stderr.trim()
                );
            }

            Ok(ProcessOutput {
                stdout,
                stderr,
                exit_code,
            })
        } else {
            // For inherited output, we can't capture but can check status
            if !output.status.success() {
                anyhow::bail!("{} failed with exit code {}", context_msg, exit_code);
            }

            Ok(ProcessOutput {
                stdout: String::new(),
                stderr: String::new(),
                exit_code,
            })
        }
    }

    /// Execute the command and return only if it succeeds (discarding output).
    pub fn run_ok(self) -> Result<()> {
        self.run().map(|_| ())
    }

    /// Execute the command and return stdout as a trimmed string.
    #[allow(dead_code)]
    pub fn run_stdout(self) -> Result<String> {
        self.run().map(|output| output.stdout.trim().to_string())
    }

    /// Execute the command and check if it succeeds, returning a boolean.
    ///
    /// Unlike `run()`, this doesn't fail on non-zero exit - it returns false instead.
    #[allow(dead_code)]
    pub fn run_success(mut self) -> Result<bool> {
        self.command.stdout(Stdio::null());
        self.command.stderr(Stdio::null());

        let status = self.command.status().context("failed to spawn command")?;

        Ok(status.success())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_builder_basic() {
        let output = ProcessBuilder::new("echo")
            .arg("hello")
            .run()
            .expect("echo should succeed");

        assert!(output.success());
        assert_eq!(output.stdout.trim(), "hello");
    }

    #[test]
    fn test_process_builder_git() {
        let output = ProcessBuilder::git()
            .args(&["--version"])
            .run()
            .expect("git --version should succeed");

        assert!(output.success());
        assert!(output.stdout.contains("git version"));
    }

    #[test]
    fn test_process_builder_failure() {
        let result = ProcessBuilder::new("false").run();
        assert!(result.is_err());
    }

    #[test]
    fn test_process_builder_run_success() {
        let success = ProcessBuilder::new("true")
            .run_success()
            .expect("should not error");
        assert!(success);

        let failure = ProcessBuilder::new("false")
            .run_success()
            .expect("should not error");
        assert!(!failure);
    }

    #[test]
    fn test_process_builder_stdout() {
        let output = ProcessBuilder::new("echo")
            .arg("test output")
            .run_stdout()
            .expect("should succeed");

        assert_eq!(output, "test output");
    }

    #[test]
    fn test_process_builder_multiple_args() {
        let output = ProcessBuilder::new("echo")
            .args(&["one", "two", "three"])
            .run()
            .expect("should succeed");

        assert!(output.success());
        assert_eq!(output.stdout.trim(), "one two three");
    }

    #[test]
    fn test_process_builder_cargo_helper() {
        let output = ProcessBuilder::cargo()
            .args(&["--version"])
            .run()
            .expect("cargo --version should succeed");

        assert!(output.success());
        assert!(output.stdout.contains("cargo"));
    }

    #[test]
    fn test_process_builder_with_description() {
        let result = ProcessBuilder::new("nonexistent_command_xyz")
            .with_description("test command")
            .run();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("test command"));
    }

    #[test]
    fn test_process_builder_env() {
        let output = ProcessBuilder::new("sh")
            .args(&["-c", "echo $TEST_VAR"])
            .env("TEST_VAR", "test_value")
            .run()
            .expect("should succeed");

        assert!(output.success());
        assert_eq!(output.stdout.trim(), "test_value");
    }

    #[test]
    fn test_process_builder_current_dir() {
        let output = ProcessBuilder::new("pwd")
            .current_dir("/tmp")
            .run()
            .expect("should succeed");

        assert!(output.success());
        assert!(output.stdout.contains("/tmp"));
    }

    #[test]
    fn test_process_output_combined() {
        let output = ProcessBuilder::new("sh")
            .args(&["-c", "echo stdout; echo stderr >&2"])
            .run()
            .expect("should succeed");

        let combined = output.combined();
        assert!(combined.contains("stdout"));
        assert!(combined.contains("stderr"));
    }

    #[test]
    fn test_process_builder_run_ok() {
        ProcessBuilder::new("true")
            .run_ok()
            .expect("true should succeed");

        let result = ProcessBuilder::new("false").run_ok();
        assert!(result.is_err());
    }
}

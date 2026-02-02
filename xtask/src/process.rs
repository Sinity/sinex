//! Process execution helpers for xtask commands.
//!
//! Provides a fluent builder API for spawning external processes with:
//! - Consistent error handling and context (via xshell)
//! - Automatic output capture and formatting
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
use std::path::PathBuf;
use std::process::{Command, Stdio};
use xshell::{cmd, Shell};

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
    #[must_use]
    pub fn success(&self) -> bool {
        self.exit_code == 0
    }

    /// Get combined output (stdout + stderr).
    #[allow(dead_code)]
    #[must_use]
    pub fn combined(&self) -> String {
        format!("{}{}", self.stdout, self.stderr)
    }
}

/// Builder for executing external processes with consistent error handling.
///
/// Internally uses xshell for cleaner command execution with automatic
/// error context including the command line.
pub struct ProcessBuilder {
    program: String,
    args: Vec<String>,
    env_vars: Vec<(String, String)>,
    working_dir: Option<PathBuf>,
    description: Option<String>,
    capture_output: bool,
}

impl ProcessBuilder {
    /// Create a new process builder for the given program.
    pub fn new(program: impl AsRef<str>) -> Self {
        Self {
            program: program.as_ref().to_string(),
            args: Vec::new(),
            env_vars: Vec::new(),
            working_dir: None,
            description: None,
            capture_output: true,
        }
    }

    /// Create a git command builder with automatic context.
    #[must_use]
    pub fn git() -> Self {
        Self::new("git").with_description("git command")
    }

    /// Create a cargo command builder with automatic context.
    #[must_use]
    pub fn cargo() -> Self {
        Self::new("cargo").with_description("cargo command")
    }

    /// Create a psql (`PostgreSQL`) command builder.
    #[must_use]
    pub fn psql() -> Self {
        Self::new("psql").with_description("PostgreSQL command")
    }

    /// Create a nix command builder.
    #[allow(dead_code)]
    #[must_use]
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
            self.args.push(arg.as_ref().to_string());
        }
        self
    }

    /// Set a single argument.
    pub fn arg(mut self, arg: impl AsRef<str>) -> Self {
        self.args.push(arg.as_ref().to_string());
        self
    }

    /// Set an environment variable.
    #[allow(dead_code)]
    pub fn env(mut self, key: impl AsRef<str>, val: impl AsRef<str>) -> Self {
        self.env_vars
            .push((key.as_ref().to_string(), val.as_ref().to_string()));
        self
    }

    /// Set the working directory.
    #[allow(dead_code)]
    pub fn current_dir(mut self, dir: impl AsRef<std::path::Path>) -> Self {
        self.working_dir = Some(dir.as_ref().to_path_buf());
        self
    }

    /// Set a description for error messages.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Disable output capture (inherit stdio from parent).
    #[must_use]
    pub fn inherit_output(mut self) -> Self {
        self.capture_output = false;
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
    pub fn run(self) -> Result<ProcessOutput> {
        let sh = Shell::new().context("failed to create shell")?;

        // Change to working directory if specified
        if let Some(ref dir) = self.working_dir {
            sh.change_dir(dir);
        }

        // Build command description for error context
        let cmd_display = if self.args.is_empty() {
            self.program.clone()
        } else {
            format!("{} {}", self.program, self.args.join(" "))
        };

        let context_msg = self
            .description
            .unwrap_or_else(|| format!("running {cmd_display}"));

        // Build the xshell command (xshell 0.2.7 interpolates from local vars)
        let program = &self.program;
        let args = &self.args;
        let mut command = cmd!(sh, "{program} {args...}");

        // Add environment variables
        for (key, val) in &self.env_vars {
            command = command.env(key, val);
        }

        if self.capture_output {
            // Capture output - xshell captures by default
            let output = command
                .ignore_status()
                .output()
                .with_context(|| format!("failed to spawn: {context_msg}"))?;

            let exit_code = output.status.code().unwrap_or(-1);
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
            // Inherit output - use std::process::Command for streaming output with exit code
            // xshell doesn't provide a clean way to get exit code with inherited stdio
            let mut cmd = Command::new(&self.program);
            cmd.args(&self.args)
                .stdin(Stdio::null())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit());

            if let Some(ref dir) = self.working_dir {
                cmd.current_dir(dir);
            }

            for (key, val) in &self.env_vars {
                cmd.env(key, val);
            }

            let status = cmd
                .status()
                .with_context(|| format!("failed to spawn: {context_msg}"))?;

            let exit_code = status.code().unwrap_or(-1);

            if !status.success() {
                anyhow::bail!("{context_msg} failed with exit code {exit_code}");
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
    pub fn run_success(self) -> Result<bool> {
        let sh = Shell::new().context("failed to create shell")?;

        if let Some(ref dir) = self.working_dir {
            sh.change_dir(dir);
        }

        let program = &self.program;
        let args = &self.args;
        let mut command = cmd!(sh, "{program} {args...}");

        for (key, val) in &self.env_vars {
            command = command.env(key, val);
        }

        let output = command
            .ignore_status()
            .quiet()
            .output()
            .context("failed to spawn command")?;

        Ok(output.status.success())
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
            .args(["--version"])
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
            .args(["one", "two", "three"])
            .run()
            .expect("should succeed");

        assert!(output.success());
        assert_eq!(output.stdout.trim(), "one two three");
    }

    #[test]
    fn test_process_builder_cargo_helper() {
        let output = ProcessBuilder::cargo()
            .args(["--version"])
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
            .args(["-c", "echo $TEST_VAR"])
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
            .args(["-c", "echo stdout; echo stderr >&2"])
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

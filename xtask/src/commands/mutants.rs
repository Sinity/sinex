//! Mutation testing command - runs cargo-mutants for mutation analysis

use anyhow::{anyhow, Result};
use std::process::Command;
use std::time::Duration;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::process::ProcessBuilder;

/// Mutation testing command configuration
#[derive(Debug, Clone, clap::Args)]
pub struct MutantsCommand {
    #[arg(short, long)]
    pub package: Option<String>,
    #[arg(short, long)]
    pub file: Option<String>,
    #[arg(long, default_value = "300")]
    pub timeout: u64,
    #[arg(short, long, default_value = "1")]
    pub jobs: usize,
    pub args: Vec<String>,
}

#[async_trait::async_trait]
impl XtaskCommand for MutantsCommand {
    fn name(&self) -> &'static str {
        "mutants"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        // Check if cargo-mutants is available
        let check_result = Command::new("cargo")
            .arg("mutants")
            .arg("--version")
            .output();

        if check_result.is_err() || !check_result.unwrap().status.success() {
            return Err(anyhow!(
                "cargo-mutants not found. Setup with: cargo binstall cargo-mutants"
            ));
        }

        let mut builder = ProcessBuilder::cargo().arg("mutants");

        // Add timeout per mutant
        builder = builder.arg("--timeout").arg(format!("{}", self.timeout));

        // Add parallelism
        builder = builder.arg("--jobs").arg(format!("{}", self.jobs));

        // Add package filter if specified
        if let Some(pkg) = &self.package {
            builder = builder.arg("--package").arg(pkg);
        }

        // Add file filter if specified
        if let Some(f) = &self.file {
            builder = builder.arg("--file").arg(f);
        }

        // Add any additional arguments
        for arg in &self.args {
            builder = builder.arg(arg);
        }

        // Build description for logging
        let description = match (&self.package, &self.file) {
            (Some(pkg), _) => format!("cargo mutants --package {pkg}"),
            (None, Some(f)) => format!("cargo mutants --file {f}"),
            (None, None) => "cargo mutants (full workspace)".to_string(),
        };

        builder
            .with_description(&description)
            .inherit_output()
            .run()?;

        Ok(CommandResult::success()
            .with_message("Mutation testing completed successfully")
            .with_detail(format!("Timeout per mutant: {}s", self.timeout))
            .with_detail(format!("Parallel jobs: {}", self.jobs)))
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            category: Some("test".to_string()),
            timeout: Some(Duration::from_mins(30)), // 30 minutes for mutation testing
            modifies_state: false,
            track_in_history: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mutants_command_metadata() {
        let cmd = MutantsCommand {
            package: None,
            file: None,
            timeout: 300,
            jobs: 4,
            args: vec![],
        };

        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("test".to_string()));
        assert!(metadata.timeout.is_some());
        assert_eq!(metadata.timeout.unwrap().as_secs(), 1800);
    }

    #[test]
    fn test_mutants_command_name() {
        let cmd = MutantsCommand {
            package: Some("sinex-db".to_string()),
            file: None,
            timeout: 300,
            jobs: 4,
            args: vec![],
        };

        assert_eq!(cmd.name(), "mutants");
    }

    #[test]
    fn test_mutants_command_with_filters() {
        let cmd = MutantsCommand {
            package: Some("sinex-db".to_string()),
            file: Some("src/lib.rs".to_string()),
            timeout: 600,
            jobs: 8,
            args: vec!["--verbose".to_string()],
        };

        assert_eq!(cmd.name(), "mutants");
        assert_eq!(cmd.package.as_deref(), Some("sinex-db"));
        assert_eq!(cmd.file.as_deref(), Some("src/lib.rs"));
        assert_eq!(cmd.timeout, 600);
        assert_eq!(cmd.jobs, 8);
        assert_eq!(cmd.args.len(), 1);
    }
}

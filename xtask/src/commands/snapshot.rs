//! Codebase snapshot command - promoted from analyze snapshot

use anyhow::{Context, Result};
use serde::Serialize;
use std::path::PathBuf;
use std::process::Command;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};

/// Generate a codebase snapshot for AI context (via repomix)
#[derive(Debug, Clone, clap::Args)]
pub struct SnapshotCommand {
    /// Output file path
    #[arg(short, long)]
    pub output: Option<PathBuf>,
    /// Include patterns (glob)
    #[arg(long)]
    pub include: Vec<String>,
    /// Exclude patterns (glob)
    #[arg(long)]
    pub exclude: Vec<String>,
    /// Use Tree-sitter to extract essential code structure (smaller output)
    #[arg(long)]
    pub compress: bool,
    /// Remove code comments from output
    #[arg(long)]
    pub remove_comments: bool,
}

/// Snapshot metadata
#[derive(Debug, Serialize)]
struct SnapshotResult {
    output_file: String,
    file_count: usize,
    total_bytes: usize,
    compressed: bool,
}

impl XtaskCommand for SnapshotCommand {
    fn name(&self) -> &str {
        "snapshot"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        // Check if repomix is available
        let repomix_check = Command::new("which").arg("repomix").output();
        if repomix_check.is_err() || !repomix_check.unwrap().status.success() {
            return Ok(CommandResult::failure(crate::output::StructuredError {
                code: "TOOL_NOT_FOUND".to_string(),
                message: "repomix not found. Install with: npm install -g repomix".to_string(),
                location: None,
                suggestion: Some("npm install -g repomix".to_string()),
            }));
        }

        let output_path = self
            .output
            .as_ref()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| "context.xml".to_string());

        let mut args = vec!["--output".to_string(), output_path.clone()];

        // Tree-sitter semantic compression (extracts code structure)
        if self.compress {
            args.push("--compress".to_string());
        }

        // Remove comments
        if self.remove_comments {
            args.push("--remove-comments".to_string());
        }

        // Add includes
        for inc in &self.include {
            args.push("--include".to_string());
            args.push(inc.clone());
        }

        // Add excludes (with sensible defaults for sinex)
        let default_excludes = [
            "target/",
            "node_modules/",
            ".git/",
            "*.lock",
            "*.log",
            "test-results/",
        ];

        for exc in default_excludes
            .iter()
            .map(|s| s.to_string())
            .chain(self.exclude.iter().cloned())
        {
            args.push("--ignore".to_string());
            args.push(exc);
        }

        if ctx.is_human() {
            println!("Generating codebase snapshot...");
            if self.compress {
                println!("  Mode: Tree-sitter structure extraction");
            }
            println!("  Output: {}", output_path);
        }

        let result = Command::new("repomix")
            .args(&args)
            .output()
            .context("Failed to run repomix")?;

        if !result.status.success() {
            let stderr = String::from_utf8_lossy(&result.stderr);
            return Ok(CommandResult::failure(crate::output::StructuredError {
                code: "REPOMIX_FAILED".to_string(),
                message: format!("repomix failed: {}", stderr),
                location: None,
                suggestion: None,
            }));
        }

        // Get file info
        let file_meta = std::fs::metadata(&output_path).ok();
        let file_size = file_meta.map(|m| m.len() as usize).unwrap_or(0);

        // Count files in output (rough estimate from XML)
        let content = std::fs::read_to_string(&output_path).unwrap_or_default();
        let file_count = content.matches("<file ").count();

        let snapshot_result = SnapshotResult {
            output_file: output_path.clone(),
            file_count,
            total_bytes: file_size,
            compressed: self.compress,
        };

        if ctx.is_human() {
            println!("\nSnapshot created:");
            println!("  File: {}", snapshot_result.output_file);
            println!("  Files included: {}", snapshot_result.file_count);
            println!(
                "  Size: {} bytes{}",
                snapshot_result.total_bytes,
                if self.compress {
                    " (structure-only)"
                } else {
                    ""
                }
            );

            Ok(CommandResult::success()
                .with_message("Snapshot created")
                .with_duration(ctx.elapsed()))
        } else {
            Ok(CommandResult::success()
                .with_data(serde_json::to_value(&snapshot_result)?)
                .with_duration(ctx.elapsed()))
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::build()
    }
}

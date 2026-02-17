//! Code coverage reporting commands

use anyhow::{bail, Context, Result};
use serde_json;
use std::fs;
use std::path::Path;
use std::process::Command;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};

/// Coverage command configuration
#[derive(Debug, Clone, clap::Args)]
pub struct CoverageCommand {
    #[command(subcommand)]
    pub subcommand: CoverageSubcommand,
}

/// Coverage subcommands
#[derive(Debug, Clone, clap::Subcommand)]
pub enum CoverageSubcommand {
    /// Generate HTML coverage report
    Html {
        #[arg(short, long, default_value = "target/coverage")]
        output: String,
        #[arg(long)]
        open: bool,
        #[arg(short, long)]
        package: Option<String>,
    },
    /// Generate LCOV coverage report (for CI integration)
    Lcov {
        #[arg(short, long, default_value = "lcov.info")]
        output: String,
        #[arg(short, long)]
        package: Option<String>,
    },
    /// Print coverage summary to stdout
    Summary {
        #[arg(short, long)]
        package: Option<String>,
        #[arg(long)]
        files: bool,
    },
    /// Measure coverage and enforce minimum threshold
    Enforce {
        #[arg(short, long)]
        threshold: f64,
        #[arg(short, long)]
        package: Option<String>,
        #[arg(long)]
        html: bool,
        #[arg(short, long, default_value = "target/coverage")]
        output: String,
    },
    /// Clean coverage artifacts
    Clean,
}

#[async_trait::async_trait]
impl XtaskCommand for CoverageCommand {
    fn name(&self) -> &'static str {
        "coverage"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            CoverageSubcommand::Html {
                output,
                open,
                package,
            } => execute_html(output, *open, package.as_deref(), ctx),
            CoverageSubcommand::Lcov { output, package } => {
                execute_lcov(output, package.as_deref(), ctx)
            }
            CoverageSubcommand::Summary { package, files } => {
                execute_summary(package.as_deref(), *files, ctx)
            }
            CoverageSubcommand::Enforce {
                threshold,
                package,
                html,
                output,
            } => execute_enforce(*threshold, package.as_deref(), *html, output, ctx),
            CoverageSubcommand::Clean => execute_clean(ctx),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            category: Some("test".to_string()),
            timeout: Some(std::time::Duration::from_mins(5)), // 5 minutes
            modifies_state: false,
            track_in_history: true,
        }
    }
}

fn execute_html(
    output: &str,
    open: bool,
    package: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("coverage html report");

    check_llvm_cov_installed()?;

    let mut cmd = Command::new("cargo");
    cmd.arg("llvm-cov")
        .arg("--html")
        .arg("--output-dir")
        .arg(output);

    if let Some(pkg) = package {
        cmd.arg("--package").arg(pkg);
    } else {
        cmd.arg("--workspace");
    }

    // Exclude test utilities from coverage measurement
    cmd.arg("--exclude").arg("sinex-test-utils");
    cmd.arg("--exclude").arg("xtask");

    run_cmd_ctx("cargo llvm-cov --html", cmd, ctx)?;

    if ctx.is_human() {
        println!("Coverage report generated at: {output}/html/index.html");
    }

    if open {
        let index_path = Path::new(output).join("html").join("index.html");
        if index_path.exists() {
            let _ = Command::new("xdg-open")
                .arg(&index_path)
                .spawn()
                .or_else(|_| Command::new("open").arg(&index_path).spawn());
        } else {
            bail!("HTML report not found at {}", index_path.display());
        }
    }

    Ok(CommandResult::success()
        .with_message(format!("HTML report: {output}/html/index.html"))
        .with_duration(ctx.elapsed()))
}

fn execute_lcov(
    output: &str,
    package: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("coverage lcov report");

    check_llvm_cov_installed()?;

    let mut cmd = Command::new("cargo");
    cmd.arg("llvm-cov")
        .arg("--lcov")
        .arg("--output-path")
        .arg(output);

    if let Some(pkg) = package {
        cmd.arg("--package").arg(pkg);
    } else {
        cmd.arg("--workspace");
    }

    // Exclude test utilities from coverage measurement
    cmd.arg("--exclude").arg("sinex-test-utils");
    cmd.arg("--exclude").arg("xtask");

    run_cmd_ctx("cargo llvm-cov --lcov", cmd, ctx)?;

    if ctx.is_human() {
        println!("LCOV report generated at: {output}");
    }

    Ok(CommandResult::success()
        .with_message(format!("LCOV report: {output}"))
        .with_duration(ctx.elapsed()))
}

fn execute_summary(
    package: Option<&str>,
    files: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("coverage summary");

    check_llvm_cov_installed()?;

    let mut cmd = Command::new("cargo");
    cmd.arg("llvm-cov");

    if let Some(pkg) = package {
        cmd.arg("--package").arg(pkg);
    } else {
        cmd.arg("--workspace");
    }

    // Exclude test utilities from coverage measurement
    cmd.arg("--exclude").arg("sinex-test-utils");
    cmd.arg("--exclude").arg("xtask");

    if files {
        cmd.arg("--summary-only");
    }

    run_cmd_ctx("cargo llvm-cov", cmd, ctx)?;

    Ok(CommandResult::success()
        .with_message("coverage summary displayed")
        .with_duration(ctx.elapsed()))
}

fn execute_enforce(
    threshold: f64,
    package: Option<&str>,
    generate_html: bool,
    html_output: &str,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("coverage enforcement");

    // Validate threshold
    if !(0.0..=100.0).contains(&threshold) {
        bail!("Threshold must be between 0 and 100 (got {threshold})");
    }

    check_llvm_cov_installed()?;

    // Build coverage command with JSON output for parsing
    let mut cmd = Command::new("cargo");
    cmd.arg("llvm-cov").arg("--json");

    if let Some(pkg) = package {
        cmd.arg("--package").arg(pkg);
    } else {
        cmd.arg("--workspace");
    }

    // Exclude test utilities from coverage measurement
    cmd.arg("--exclude").arg("sinex-test-utils");
    cmd.arg("--exclude").arg("xtask");

    // Run coverage measurement
    if ctx.is_human() {
        println!("Running coverage measurement...");
    }

    let output = cmd
        .output()
        .with_context(|| "Failed to execute cargo llvm-cov")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Ok(CommandResult::failure(crate::output::StructuredError {
            code: "COVERAGE_FAILED".to_string(),
            message: format!("Coverage measurement failed: {stderr}"),
            location: Some("coverage::enforce".to_string()),
            suggestion: Some("Run tests first: xtask test --all".to_string()),
        }));
    }

    // Parse JSON output
    let stdout = String::from_utf8_lossy(&output.stdout);
    let coverage_data: serde_json::Value =
        serde_json::from_str(&stdout).with_context(|| "Failed to parse coverage JSON output")?;

    // Extract total coverage percentage
    let total_coverage = coverage_data["data"][0]["totals"]["lines"]["percent"]
        .as_f64()
        .ok_or_else(|| anyhow::anyhow!("Failed to extract coverage percentage from JSON"))?;

    // Optionally generate HTML report
    if generate_html {
        if ctx.is_human() {
            println!("Generating HTML report...");
        }
        execute_html(html_output, false, package, ctx)?;
    }

    // Determine pass/fail
    let passed = total_coverage >= threshold;

    // Build result
    let mut result = if passed {
        CommandResult::success()
    } else {
        CommandResult::failure(crate::output::StructuredError {
            code: "COVERAGE_BELOW_THRESHOLD".to_string(),
            message: format!("Coverage {total_coverage:.2}% is below threshold {threshold:.2}%"),
            location: Some("coverage::enforce".to_string()),
            suggestion: Some("Write unit tests for uncovered code paths".to_string()),
        })
    };

    result = result
        .with_detail(format!("Total coverage: {total_coverage:.1}%"))
        .with_detail(format!("Threshold: {threshold:.1}%"));

    if !passed {
        result = result.with_detail(format!(
            "Below threshold by {:.1}%",
            threshold - total_coverage
        ));
    }

    // Human-readable output
    if ctx.is_human() {
        println!();
        println!("Code Coverage Report");
        println!("====================");
        println!("Total coverage: {total_coverage:.1}%");
        println!("Threshold:      {threshold:.1}%");
        println!();

        if passed {
            println!("\u{2713} Coverage meets threshold");
        } else {
            println!(
                "\u{2717} Coverage below threshold by {:.1}%",
                threshold - total_coverage
            );
        }
    }

    Ok(result.with_duration(ctx.elapsed()))
}

fn execute_clean(ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading("clean coverage artifacts");

    let mut cmd = Command::new("cargo");
    cmd.arg("llvm-cov").arg("clean").arg("--workspace");
    run_cmd_ctx("cargo llvm-cov clean", cmd, ctx)?;

    // Also remove the output directory
    let coverage_dir = Path::new("target/coverage");
    if coverage_dir.exists() {
        fs::remove_dir_all(coverage_dir)?;
        if ctx.is_human() {
            println!("Removed {}", coverage_dir.display());
        }
    }

    Ok(CommandResult::success()
        .with_message("coverage artifacts cleaned")
        .with_duration(ctx.elapsed()))
}

/// Check if cargo-llvm-cov is installed
fn check_llvm_cov_installed() -> Result<()> {
    let output = Command::new("cargo")
        .args(["llvm-cov", "--version"])
        .output();

    match output {
        Ok(out) if out.status.success() => Ok(()),
        _ => bail!(
            "cargo-llvm-cov is not installed. Install with:\n  \
             cargo install cargo-llvm-cov\n  \
             or via nix: nix-env -iA nixpkgs.cargo-llvm-cov"
        ),
    }
}

/// Helper function to run a command with context
fn run_cmd_ctx(desc: &str, mut cmd: Command, ctx: &CommandContext) -> Result<()> {
    let output = cmd
        .output()
        .with_context(|| format!("Failed to execute {desc}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("{desc} failed: {stderr}");
    }

    if ctx.is_human() && !output.stdout.is_empty() {
        print!("{}", String::from_utf8_lossy(&output.stdout));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::OutputFormat;

    #[test]
    fn test_command_name() {
        let cmd = CoverageCommand {
            subcommand: CoverageSubcommand::Clean,
        };
        assert_eq!(cmd.name(), "coverage");
    }

    #[test]
    fn test_command_metadata() {
        let cmd = CoverageCommand {
            subcommand: CoverageSubcommand::Summary {
                package: None,
                files: false,
            },
        };
        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("test".to_string()));
        assert!(metadata.timeout.is_some());
        assert!(!metadata.modifies_state);
    }

    #[test]
    fn test_threshold_validation() {
        let ctx = CommandContext::new(
            crate::output::OutputWriter::new(OutputFormat::Silent),
            false,
            false,
            None,
        );

        let result = execute_enforce(150.0, None, false, "target/coverage/html", &ctx);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("between 0 and 100"));
    }

    #[test]
    fn test_clean_command() {
        let cmd = CoverageCommand {
            subcommand: CoverageSubcommand::Clean,
        };
        assert_eq!(cmd.name(), "coverage");
    }
}

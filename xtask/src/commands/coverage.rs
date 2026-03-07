//! Code coverage reporting commands

use color_eyre::eyre::{Result, WrapErr, bail, eyre};
use serde_json;
use std::fs;
use std::path::Path;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::process::ProcessBuilder;

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

/// Build a `cargo llvm-cov` command with common args (package scope + exclusions).
fn llvm_cov_cmd(extra_args: &[&str], package: Option<&str>) -> ProcessBuilder {
    let mut cmd = ProcessBuilder::cargo().arg("llvm-cov");

    for arg in extra_args {
        cmd = cmd.arg(*arg);
    }

    if let Some(pkg) = package {
        cmd = cmd.arg("--package").arg(pkg);
    } else {
        cmd = cmd.arg("--workspace");
    }

    // Exclude test utilities from coverage measurement
    cmd.arg("--exclude")
        .arg("sinex-test-utils")
        .arg("--exclude")
        .arg("xtask")
}

fn execute_html(
    output: &str,
    open: bool,
    package: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("coverage html report");

    check_llvm_cov_installed()?;

    let stage = ctx.start_stage("coverage_html");
    let result = llvm_cov_cmd(&["--html", "--output-dir", output], package)
        .with_description("cargo llvm-cov --html")
        .run();
    ctx.finish_stage(stage, result.is_ok());
    let cov_output = result?;

    if ctx.is_human() {
        if !cov_output.stdout.is_empty() {
            print!("{}", cov_output.stdout);
        }
        println!("Coverage report generated at: {output}/html/index.html");
    }

    if open {
        let index_path = Path::new(output).join("html").join("index.html");
        if index_path.exists() {
            let _ = std::process::Command::new("xdg-open")
                .arg(&index_path)
                .spawn()
                .or_else(|_| std::process::Command::new("open").arg(&index_path).spawn());
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

    let stage = ctx.start_stage("coverage_lcov");
    let result = llvm_cov_cmd(&["--lcov", "--output-path", output], package)
        .with_description("cargo llvm-cov --lcov")
        .run();
    ctx.finish_stage(stage, result.is_ok());
    let cov_output = result?;

    if ctx.is_human() {
        if !cov_output.stdout.is_empty() {
            print!("{}", cov_output.stdout);
        }
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

    let extra = if files {
        vec!["--summary-only"]
    } else {
        vec![]
    };

    let result = llvm_cov_cmd(&extra.clone(), package)
        .with_description("cargo llvm-cov summary")
        .run()?;

    if ctx.is_human() && !result.stdout.is_empty() {
        print!("{}", result.stdout);
    }

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

    // Run coverage measurement with JSON output for parsing
    if ctx.is_human() {
        println!("Running coverage measurement...");
    }

    let stage = ctx.start_stage("coverage_enforce");
    let output = llvm_cov_cmd(&["--json"], package)
        .with_description("cargo llvm-cov --json")
        .run_capture()?;
    ctx.finish_stage(stage, output.success());

    if !output.success() {
        return Ok(CommandResult::failure(crate::output::StructuredError {
            code: "COVERAGE_FAILED".to_string(),
            message: format!("Coverage measurement failed: {}", output.stderr.trim()),
            location: Some("coverage::enforce".to_string()),
            suggestion: Some("Run tests first: xtask test --all".to_string()),
        }));
    }

    // Parse JSON output
    let coverage_data: serde_json::Value = serde_json::from_str(&output.stdout)
        .with_context(|| "Failed to parse coverage JSON output")?;

    // Extract total coverage percentage
    let total_coverage = coverage_data["data"][0]["totals"]["lines"]["percent"]
        .as_f64()
        .ok_or_else(|| eyre!("Failed to extract coverage percentage from JSON"))?;

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

    ProcessBuilder::cargo()
        .args(["llvm-cov", "clean", "--workspace"])
        .with_description("cargo llvm-cov clean")
        .run()?;

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
    if !ProcessBuilder::cargo()
        .args(["llvm-cov", "--version"])
        .run_success()?
    {
        bail!(
            "cargo-llvm-cov is not installed. Install with:\n  \
             cargo install cargo-llvm-cov\n  \
             or via nix: nix-env -iA nixpkgs.cargo-llvm-cov"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::OutputFormat;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn test_command_name() -> ::xtask::sandbox::TestResult<()> {
        let cmd = CoverageCommand {
            subcommand: CoverageSubcommand::Clean,
        };
        assert_eq!(cmd.name(), "coverage");
        Ok(())
    }

    #[sinex_test]
    async fn test_command_metadata() -> ::xtask::sandbox::TestResult<()> {
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
        Ok(())
    }

    #[sinex_test]
    async fn test_threshold_validation() -> ::xtask::sandbox::TestResult<()> {
        let ctx = CommandContext::new(
            crate::output::OutputWriter::new(OutputFormat::Silent),
            false,
            false,
            None,
        );

        let result = execute_enforce(150.0, None, false, "target/coverage/html", &ctx);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("between 0 and 100")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_clean_command() -> ::xtask::sandbox::TestResult<()> {
        let cmd = CoverageCommand {
            subcommand: CoverageSubcommand::Clean,
        };
        assert_eq!(cmd.name(), "coverage");
        Ok(())
    }
}

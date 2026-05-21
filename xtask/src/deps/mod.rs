//! Dependency analysis and health checking
//!
//! This module provides commands for analyzing workspace dependencies,
//! detecting unused dependencies, analyzing build times, and assessing
//! rebuild impact.

use clap::Subcommand;
use color_eyre::eyre::{Result, WrapErr, bail};
use std::time::Duration;

// Submodules
pub(crate) mod active;
pub mod analyzer; // Created in P1.W3.T2
pub mod reports; // Created in P1.W3.T3
pub mod timing;
pub mod unused; // Created in P2.W3.T1 // Created in P2.W3.T4

pub use timing::{TimingAnalyzer, TimingReport};
pub use unused::UnusedReport;

/// Dependency analysis commands
#[derive(Debug, Clone, Subcommand)]
pub enum DepsCommand {
    /// List all workspace packages
    List {
        /// Include transitive dependencies
        #[arg(long)]
        all: bool,
    },

    /// Show dependency tree for a package
    Tree {
        /// Target package (defaults to all workspace packages)
        #[arg(long)]
        package: Option<String>,

        /// Maximum tree depth
        #[arg(long, default_value = "10")]
        depth: usize,
    },

    /// Find duplicate dependencies (multiple versions)
    Duplicates {
        /// Minimum number of versions to report
        #[arg(long, default_value = "2")]
        threshold: usize,

        /// Only report duplicates directly requested by workspace manifests
        #[arg(long, conflicts_with = "transitive_only")]
        direct_only: bool,

        /// Only report duplicates introduced through transitive dependencies
        #[arg(long, conflicts_with = "direct_only")]
        transitive_only: bool,
    },

    /// Detect unused dependencies
    Unused {
        /// Fail build if unused dependencies found (for CI)
        #[arg(long)]
        ci: bool,
    },

    /// Analyze build timings
    Timings {
        /// Compare with previous build
        #[arg(long)]
        compare: Option<String>,

        /// Number of slowest crates to show
        #[arg(long, default_value = "10")]
        top: usize,
    },

    /// Analyze rebuild impact of package changes
    Impact {
        /// Target package to analyze (defaults to all)
        #[arg(long)]
        package: Option<String>,
    },

    /// Update Cargo.lock through the xtask dependency surface
    Update {
        /// Package spec to update; forwarded as repeated `cargo update -p <SPEC>`
        #[arg(short = 'p', long = "package")]
        packages: Vec<String>,

        /// Update dependencies recursively for the selected packages
        #[arg(long)]
        recursive: bool,

        /// Preview the update without writing Cargo.lock
        #[arg(long)]
        dry_run: bool,

        /// Update the whole lockfile instead of named packages
        #[arg(long, conflicts_with = "packages")]
        all: bool,
    },

    /// Visualize dependency graph
    Graph {
        /// Render format (dot, json, ascii)
        #[arg(long, default_value = "ascii")]
        render_format: String,

        /// Focus on specific package
        #[arg(long)]
        focus: Option<String>,

        /// Show reverse dependencies
        #[arg(long)]
        reverse: bool,

        /// Maximum depth
        #[arg(long, default_value = "10")]
        depth: usize,

        /// Output file (if not specified, writes to stdout)
        #[arg(short, long)]
        output: Option<String>,
    },
}

impl DepsCommand {
    /// Execute the deps command
    pub fn run(
        &self,
        ctx: &crate::command::CommandContext,
    ) -> Result<crate::command::CommandResult> {
        use crate::command::CommandResult;
        match self {
            Self::List { all: _ } => {
                use crate::deps::analyzer::WorkspaceAnalyzer;
                use crate::deps::reports::{OutputFormat, write_dependency_list};

                // Create analyzer
                let analyzer =
                    WorkspaceAnalyzer::new().context("Failed to create workspace analyzer")?;

                // Get workspace packages
                let packages = analyzer
                    .workspace_packages()
                    .context("Failed to get workspace packages")?;

                // Parse output format
                let output_format = if ctx.is_json() {
                    OutputFormat::Json
                } else {
                    OutputFormat::Human
                };

                // Capture output to string
                let mut buffer = Vec::new();
                write_dependency_list(&mut buffer, &packages, output_format)?;
                let rendered = String::from_utf8(buffer)?;

                if ctx.is_json() {
                    let json_data: serde_json::Value = serde_json::from_str(&rendered)?;
                    Ok(CommandResult::success()
                        .with_data(json_data)
                        .with_silent()
                        .with_duration(ctx.elapsed()))
                } else {
                    Ok(CommandResult::success()
                        .with_data(serde_json::Value::String(rendered))
                        .with_silent()
                        .with_duration(ctx.elapsed()))
                }
            }

            Self::Tree { package, depth } => {
                use crate::graph::render::{AsciiRenderer, Renderer};
                use crate::graph::workspace::WorkspaceGraph;

                let graph = WorkspaceGraph::new().context("Failed to create workspace graph")?;

                // Verify package exists if specified
                if let Some(pkg_name) = package {
                    let packages = graph
                        .workspace_packages()
                        .context("Failed to get workspace packages")?;

                    let found = packages.iter().any(|p| p.name() == pkg_name);

                    if !found {
                        bail!(
                            "Package '{}' not found in workspace.\n\nAvailable packages:\n{}",
                            pkg_name,
                            packages
                                .iter()
                                .map(|p| format!("  - {}", p.name()))
                                .collect::<Vec<_>>()
                                .join("\n")
                        );
                    }
                }

                let rendered = AsciiRenderer::new(&graph, package.clone(), *depth)
                    .with_style(!ctx.is_json())
                    .render()
                    .context("Failed to render dependency tree")?;

                let data = if ctx.is_json() {
                    serde_json::json!({
                        "tree": rendered,
                        "package": package,
                        "depth": depth,
                    })
                } else {
                    serde_json::Value::String(rendered)
                };

                Ok(CommandResult::success()
                    .with_data(data)
                    .with_silent()
                    .with_duration(ctx.elapsed()))
            }

            Self::Duplicates {
                threshold,
                direct_only,
                transitive_only,
            } => {
                use crate::deps::analyzer::WorkspaceAnalyzer;
                use crate::deps::reports::{OutputFormat, write_duplicates_report};

                // Create analyzer
                let analyzer =
                    WorkspaceAnalyzer::new().context("Failed to create workspace analyzer")?;

                // Find duplicates
                let mut duplicates = analyzer
                    .find_duplicates()
                    .context("Failed to find duplicate dependencies")?;

                // Filter by threshold
                duplicates.retain(|d| d.versions.len() >= *threshold);
                if *direct_only {
                    duplicates.retain(|d| d.direct_workspace_debt);
                }
                if *transitive_only {
                    duplicates.retain(|d| d.transitive_only);
                }

                if ctx.is_json() {
                    return Ok(CommandResult::success()
                        .with_data(serde_json::json!({
                            "duplicates": duplicates,
                            "count": duplicates.len(),
                            "threshold": threshold,
                            "direct_only": direct_only,
                            "transitive_only": transitive_only,
                        }))
                        .with_silent()
                        .with_duration(ctx.elapsed()));
                }

                // Write report to buffer
                let mut buffer = Vec::new();
                write_duplicates_report(&mut buffer, &duplicates, OutputFormat::Human)?;
                let rendered = String::from_utf8(buffer)?;
                Ok(CommandResult::success()
                    .with_data(serde_json::Value::String(rendered))
                    .with_silent()
                    .with_duration(ctx.elapsed()))
            }

            Self::Unused { ci } => {
                use crate::deps::unused::UnusedDetector;

                // Detect unused dependencies
                let report =
                    UnusedDetector::detect().context("Failed to detect unused dependencies")?;

                // In CI mode, fail if unused dependencies found
                if *ci && !report.unused.is_empty() {
                    return Ok(CommandResult::failure(crate::output::StructuredError::new(
                        "UNUSED_DEPS",
                        format!("Found {} unused dependencies", report.unused.len()),
                    ))
                    .with_data(serde_json::to_value(&report)?));
                }

                if ctx.is_json() {
                    // JSON output - return structured report
                    Ok(CommandResult::success()
                        .with_data(serde_json::to_value(&report)?)
                        .with_silent()
                        .with_duration(ctx.elapsed()))
                } else {
                    // Human output
                    let mut buffer = Vec::new();
                    crate::deps::reports::write_unused_report_to_buffer(
                        &mut buffer,
                        &report,
                        "human",
                    )?;
                    let rendered = String::from_utf8(buffer)?;

                    Ok(CommandResult::success()
                        .with_data(serde_json::Value::String(rendered))
                        .with_silent()
                        .with_duration(ctx.elapsed()))
                }
            }

            Self::Timings { compare: _, top } => {
                let report = TimingAnalyzer::analyze()?;

                if ctx.is_json() {
                    // JSON output - return the structured report
                    Ok(CommandResult::success()
                        .with_data(serde_json::to_value(&report)?)
                        .with_silent()
                        .with_duration(ctx.elapsed()))
                } else {
                    // Human output
                    let mut buffer = Vec::new();
                    crate::deps::reports::write_timing_report_to_buffer(
                        &mut buffer,
                        &report,
                        *top,
                    )?;
                    let rendered = String::from_utf8(buffer)?;

                    Ok(CommandResult::success()
                        .with_data(serde_json::Value::String(rendered))
                        .with_silent()
                        .with_duration(ctx.elapsed()))
                }
            }

            Self::Impact { package } => {
                use crate::graph::impact::generate_report;
                use crate::graph::workspace::WorkspaceGraph;

                let graph = WorkspaceGraph::new()?;

                if let Some(pkg_name) = package {
                    // Single package analysis
                    let metrics = graph.compute_impact_metrics(pkg_name)?;

                    if ctx.is_json() {
                        Ok(CommandResult::success()
                            .with_data(serde_json::to_value(metrics)?)
                            .with_silent()
                            .with_duration(ctx.elapsed()))
                    } else {
                        let mut rendered = String::new();
                        rendered.push_str(&format!("Impact Analysis for {pkg_name}\n"));
                        rendered.push_str(&format!(
                            "  Dependent packages: {}\n",
                            metrics.dependent_count
                        ));
                        rendered.push_str(&format!(
                            "  Direct dependencies: {}\n",
                            metrics.dependency_count
                        ));
                        rendered.push_str(&format!(
                            "  Criticality: {:.2}% ({:?})\n",
                            metrics.criticality * 100.0,
                            metrics.criticality_level()
                        ));
                        Ok(CommandResult::success()
                            .with_data(serde_json::Value::String(rendered))
                            .with_silent()
                            .with_duration(ctx.elapsed()))
                    }
                } else {
                    // Full workspace report
                    let report = generate_report(&graph)?;

                    if ctx.is_json() {
                        Ok(CommandResult::success()
                            .with_data(serde_json::to_value(report)?)
                            .with_silent()
                            .with_duration(ctx.elapsed()))
                    } else {
                        let mut rendered = String::new();
                        if !report.critical_packages.is_empty() {
                            rendered.push_str("Critical Packages (>80% rebuild impact):\n");
                            for pkg in &report.critical_packages {
                                rendered.push_str(&format!("  - {pkg}\n"));
                            }
                            rendered.push('\n');
                        }

                        if !report.high_impact_packages.is_empty() {
                            rendered.push_str("High Impact Packages (50-80% rebuild impact):\n");
                            for pkg in &report.high_impact_packages {
                                rendered.push_str(&format!("  - {pkg}\n"));
                            }
                        }
                        Ok(CommandResult::success()
                            .with_data(serde_json::Value::String(rendered))
                            .with_silent()
                            .with_duration(ctx.elapsed()))
                    }
                }
            }

            Self::Update {
                packages,
                recursive,
                dry_run,
                all,
            } => run_update(packages, *recursive, *dry_run, *all, ctx),

            Self::Graph {
                render_format,
                focus,
                reverse,
                depth,
                output,
            } => {
                use crate::graph::render::{AsciiRenderer, DotRenderer, JsonRenderer, Renderer};
                use crate::graph::workspace::WorkspaceGraph;

                let graph = WorkspaceGraph::new()?;

                let rendered = match render_format.as_str() {
                    "dot" => {
                        let mut renderer = DotRenderer::new(graph);
                        if let Some(focus_pkg) = focus {
                            renderer = renderer.with_focus(focus_pkg.clone(), *reverse);
                        }
                        renderer.render()?
                    }
                    "json" => {
                        let renderer = JsonRenderer::new(graph);
                        renderer.render()?
                    }
                    _ => {
                        let renderer = AsciiRenderer::new(&graph, focus.clone(), *depth);
                        renderer.render()?
                    }
                };

                if let Some(output_path) = output {
                    std::fs::write(output_path, &rendered)
                        .with_context(|| format!("Failed to write to {output_path}"))?;
                    Ok(CommandResult::success()
                        .with_message(format!("Graph written to {output_path}"))
                        .with_duration(ctx.elapsed()))
                } else {
                    // Graph render formats (ascii, dot, json) output their content
                    // directly to stdout regardless of the global --format flag.
                    // DOT and ASCII are not wrappable in a JSON envelope; json
                    // render format produces self-contained graph JSON that callers
                    // can parse directly (nodes/edges at the top level).
                    print!("{rendered}");
                    Ok(CommandResult::success()
                        .with_silent()
                        .with_duration(ctx.elapsed()))
                }
            }
        }
    }
}

fn cargo_update_args(
    packages: &[String],
    recursive: bool,
    dry_run: bool,
    all: bool,
) -> Result<Vec<String>> {
    if packages.is_empty() && !all {
        bail!("deps update requires at least one --package or explicit --all");
    }

    let mut args = vec!["update".to_string()];
    for package in packages {
        args.push("-p".to_string());
        args.push(package.clone());
    }
    if recursive {
        args.push("--recursive".to_string());
    }
    if dry_run {
        args.push("--dry-run".to_string());
    }
    Ok(args)
}

fn run_update(
    packages: &[String],
    recursive: bool,
    dry_run: bool,
    all: bool,
    ctx: &crate::command::CommandContext,
) -> Result<crate::command::CommandResult> {
    let args = cargo_update_args(packages, recursive, dry_run, all)?;
    let output = crate::process::ProcessBuilder::cargo()
        .args(args.iter().map(String::as_str))
        .with_description("cargo update")
        .with_timeout(Duration::from_mins(15))
        .run_capture()
        .context("failed to run cargo update")?;
    if output.exit_code != 0 {
        bail!(
            "cargo update failed with exit code {}\nstdout:\n{}\nstderr:\n{}",
            output.exit_code,
            output.stdout.trim(),
            output.stderr.trim()
        );
    }

    let command = std::iter::once("cargo".to_string())
        .chain(args.iter().cloned())
        .collect::<Vec<_>>();
    let mut result = crate::command::CommandResult::success()
        .with_message(if dry_run {
            "dependency update dry-run completed"
        } else {
            "dependency update completed"
        })
        .with_data(serde_json::json!({
            "command": command,
            "packages": packages,
            "recursive": recursive,
            "dry_run": dry_run,
            "all": all,
            "exit_code": output.exit_code,
            "stdout": output.stdout,
            "stderr": output.stderr,
        }))
        .with_duration(ctx.elapsed());

    if !ctx.is_json() && !output.stdout.trim().is_empty() {
        result = result.with_detail(output.stdout.trim().to_string());
    }
    if !ctx.is_json() && !output.stderr.trim().is_empty() {
        result = result.with_warning(output.stderr.trim().to_string());
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn deps_update_requires_package_or_all() -> crate::TestResult<()> {
        let error = cargo_update_args(&[], false, false, false)
            .expect_err("empty targeted update should be rejected");
        assert!(error.to_string().contains("--package"));
        Ok(())
    }

    #[sinex_test]
    async fn deps_update_builds_targeted_recursive_dry_run_args() -> crate::TestResult<()> {
        let args = cargo_update_args(&["reqwest".to_string()], true, true, false)?;
        assert_eq!(
            args,
            ["update", "-p", "reqwest", "--recursive", "--dry-run"]
        );
        Ok(())
    }

    #[sinex_test]
    async fn deps_update_builds_all_lockfile_args() -> crate::TestResult<()> {
        let args = cargo_update_args(&[], false, true, true)?;
        assert_eq!(args, ["update", "--dry-run"]);
        Ok(())
    }
}

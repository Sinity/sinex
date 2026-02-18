//! Dependency analysis and health checking
//!
//! This module provides commands for analyzing workspace dependencies,
//! detecting unused dependencies, analyzing build times, and assessing
//! rebuild impact.

use color_eyre::eyre::{bail, Result, WrapErr};
use clap::Subcommand;

// Submodules
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
                use crate::deps::reports::{write_dependency_list, OutputFormat};

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
                use crate::deps::analyzer::WorkspaceAnalyzer;

                // Create analyzer
                let analyzer =
                    WorkspaceAnalyzer::new().context("Failed to create workspace analyzer")?;

                let mut rendered = String::new();

                // Verify package exists if specified
                if let Some(pkg_name) = package {
                    let packages = analyzer
                        .workspace_packages()
                        .context("Failed to get workspace packages")?;

                    let found = packages.iter().any(|p| p.name == *pkg_name);

                    if !found {
                        bail!(
                            "Package '{}' not found in workspace.\n\nAvailable packages:\n{}",
                            pkg_name,
                            packages
                                .iter()
                                .map(|p| format!("  - {}", p.name))
                                .collect::<Vec<_>>()
                                .join("\n")
                        );
                    }

                    rendered.push_str(&format!(
                        "Dependency tree for '{pkg_name}' (depth: {depth}):\n"
                    ));
                    rendered.push_str("(Full tree visualization will be available in Phase 3)\n");
                } else {
                    rendered.push_str(&format!("Workspace dependency tree (depth: {depth}):\n"));
                    rendered.push_str("(Full tree visualization will be available in Phase 3)\n");

                    // Show workspace packages as placeholder
                    let packages = analyzer
                        .workspace_packages()
                        .context("Failed to get workspace packages")?;

                    rendered.push_str("\nWorkspace packages:\n");
                    for pkg in packages {
                        rendered.push_str(&format!("  - {} v{}\n", pkg.name, pkg.version));
                    }
                }

                Ok(CommandResult::success()
                    .with_data(serde_json::Value::String(rendered))
                    .with_silent()
                    .with_duration(ctx.elapsed()))
            }

            Self::Duplicates { threshold } => {
                use crate::deps::analyzer::WorkspaceAnalyzer;
                use crate::deps::reports::{write_duplicates_report, OutputFormat};

                // Create analyzer
                let analyzer =
                    WorkspaceAnalyzer::new().context("Failed to create workspace analyzer")?;

                // Find duplicates
                let mut duplicates = analyzer
                    .find_duplicates()
                    .context("Failed to find duplicate dependencies")?;

                // Filter by threshold
                duplicates.retain(|d| d.versions.len() >= *threshold);

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

                // Respect global --json flag: override render_format when JSON output requested
                let effective_format = if ctx.is_json() && render_format == "ascii" {
                    "json"
                } else {
                    render_format.as_str()
                };

                let rendered = match effective_format {
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
                } else if ctx.is_json() {
                    // Parse rendered JSON and put into CommandResult for framework output
                    let graph_data: serde_json::Value =
                        serde_json::from_str(&rendered).context("Failed to parse graph JSON")?;
                    Ok(CommandResult::success()
                        .with_data(graph_data)
                        .with_duration(ctx.elapsed()))
                } else {
                    // Print to stdout directly (human/dot/ascii)
                    print!("{rendered}");
                    Ok(CommandResult::success()
                        .with_silent()
                        .with_duration(ctx.elapsed()))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    // Tests will be added as functionality is implemented
}

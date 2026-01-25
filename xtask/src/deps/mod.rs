//! Dependency analysis and health checking
//!
//! This module provides commands for analyzing workspace dependencies,
//! detecting unused dependencies, analyzing build times, and assessing
//! rebuild impact.

use anyhow::{Context, Result};
use clap::Subcommand;

// Submodules
pub mod analyzer; // Created in P1.W3.T2
pub mod reports; // Created in P1.W3.T3
pub mod timing;
pub mod unused; // Created in P2.W3.T1 // Created in P2.W3.T4

pub use timing::{TimingAnalyzer, TimingReport};
pub use unused::UnusedReport;

use reports::{write_timing_report, write_unused_report};

/// Dependency analysis commands
#[derive(Debug, Subcommand)]
pub enum DepsCommand {
    /// List all workspace packages
    List {
        /// Output format (human or json)
        #[arg(long, default_value = "human", value_parser = ["human", "json"])]
        format: String,

        /// Include transitive dependencies
        #[arg(long)]
        all: bool,
    },

    /// Show dependency tree for a package
    Tree {
        /// Target package (defaults to all workspace packages)
        #[arg(value_name = "PACKAGE")]
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

        /// Output format (human or json)
        #[arg(long, default_value = "human", value_parser = ["human", "json"])]
        format: String,
    },

    /// Detect unused dependencies
    Unused {
        /// Fail build if unused dependencies found (for CI)
        #[arg(long)]
        ci: bool,

        /// Output format (human or json)
        #[arg(long, default_value = "human", value_parser = ["human", "json"])]
        format: String,
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
        #[arg(value_name = "PACKAGE")]
        package: Option<String>,

        /// Output format (human or json)
        #[arg(long, default_value = "human", value_parser = ["human", "json"])]
        format: String,
    },
}

impl DepsCommand {
    /// Execute the deps command
    pub fn run(&self) -> Result<()> {
        match self {
            Self::List { format, all: _ } => {
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
                let output_format = OutputFormat::from_str(format);

                // Write report to stdout
                let stdout = std::io::stdout();
                let mut handle = stdout.lock();
                write_dependency_list(&mut handle, &packages, output_format)?;

                Ok(())
            }

            Self::Tree { package, depth } => {
                use crate::deps::analyzer::WorkspaceAnalyzer;

                // Create analyzer
                let analyzer =
                    WorkspaceAnalyzer::new().context("Failed to create workspace analyzer")?;

                // Verify package exists if specified
                if let Some(pkg_name) = package {
                    let packages = analyzer
                        .workspace_packages()
                        .context("Failed to get workspace packages")?;

                    let found = packages.iter().any(|p| p.name == *pkg_name);

                    if !found {
                        anyhow::bail!(
                            "Package '{}' not found in workspace.\n\nAvailable packages:\n{}",
                            pkg_name,
                            packages
                                .iter()
                                .map(|p| format!("  - {}", p.name))
                                .collect::<Vec<_>>()
                                .join("\n")
                        );
                    }

                    println!("Dependency tree for '{}' (depth: {}):", pkg_name, depth);
                    println!("(Full tree visualization will be available in Phase 3)");
                } else {
                    println!("Workspace dependency tree (depth: {}):", depth);
                    println!("(Full tree visualization will be available in Phase 3)");

                    // Show workspace packages as placeholder
                    let packages = analyzer
                        .workspace_packages()
                        .context("Failed to get workspace packages")?;

                    println!("\nWorkspace packages:");
                    for pkg in packages {
                        println!("  - {} v{}", pkg.name, pkg.version);
                    }
                }

                Ok(())
            }

            Self::Duplicates {
                threshold,
                format: _,
            } => {
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

                // Write report to stdout (always human format for now)
                let stdout = std::io::stdout();
                let mut handle = stdout.lock();
                write_duplicates_report(&mut handle, &duplicates, OutputFormat::Human)?;

                Ok(())
            }

            Self::Unused { ci, format } => {
                use crate::deps::unused::UnusedDetector;

                // Detect unused dependencies
                let report =
                    UnusedDetector::detect().context("Failed to detect unused dependencies")?;

                // Write report to stdout
                write_unused_report(&report, format)?;

                // In CI mode, fail if unused dependencies found
                if *ci && !report.unused.is_empty() {
                    anyhow::bail!("Found {} unused dependencies", report.unused.len());
                }

                Ok(())
            }

            Self::Timings { compare: _, top } => {
                let report = TimingAnalyzer::analyze()?;
                write_timing_report(&report, *top)?;
                Ok(())
            }

            Self::Impact { package, format } => {
                use crate::graph::impact::generate_report;
                use crate::graph::workspace::WorkspaceGraph;

                let graph = WorkspaceGraph::new()?;

                if let Some(pkg_name) = package {
                    // Single package analysis
                    let metrics = graph.compute_impact_metrics(&pkg_name)?;

                    match format.as_str() {
                        "json" => {
                            let json = serde_json::to_string_pretty(&metrics)?;
                            println!("{}", json);
                        }
                        _ => {
                            println!("Impact Analysis for {}", pkg_name);
                            println!("  Dependent packages: {}", metrics.dependent_count);
                            println!("  Direct dependencies: {}", metrics.dependency_count);
                            println!(
                                "  Criticality: {:.2}% ({:?})",
                                metrics.criticality * 100.0,
                                metrics.criticality_level()
                            );
                        }
                    }
                } else {
                    // Full workspace report
                    let report = generate_report(&graph)?;

                    match format.as_str() {
                        "json" => {
                            let json = serde_json::to_string_pretty(&report)?;
                            println!("{}", json);
                        }
                        _ => {
                            if !report.critical_packages.is_empty() {
                                println!("Critical Packages (>80% rebuild impact):");
                                for pkg in &report.critical_packages {
                                    println!("  - {}", pkg);
                                }
                                println!();
                            }

                            if !report.high_impact_packages.is_empty() {
                                println!("High Impact Packages (50-80% rebuild impact):");
                                for pkg in &report.high_impact_packages {
                                    println!("  - {}", pkg);
                                }
                            }
                        }
                    }
                }

                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests will be added as functionality is implemented
}

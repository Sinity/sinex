//! Report formatting for dependency analysis

use anyhow::Result;
use serde_json::json;
use std::io::Write;

use super::analyzer::{DependencyInfo, DuplicateDependency, PackageInfo};

/// Output format
#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    /// Human-readable output
    Human,
    /// JSON output
    Json,
}

impl OutputFormat {
    /// Parse from string
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "json" => Self::Json,
            _ => Self::Human,
        }
    }
}

/// Write dependency list
///
/// Will be implemented in P1.W5.T4
pub fn write_dependency_list<W: Write>(
    writer: &mut W,
    packages: &[PackageInfo],
    format: OutputFormat,
) -> Result<()> {
    match format {
        OutputFormat::Json => {
            let output = json!({
                "packages": packages,
                "count": packages.len(),
            });
            writeln!(writer, "{}", serde_json::to_string_pretty(&output)?)?;
        }
        OutputFormat::Human => {
            writeln!(writer, "Workspace packages ({} total):", packages.len())?;
            writeln!(writer)?;

            for pkg in packages {
                writeln!(writer, "  {} v{}", pkg.name, pkg.version)?;
            }
        }
    }

    Ok(())
}

/// Write duplicates report
///
/// Will be implemented in P1.W5.T4
pub fn write_duplicates_report<W: Write>(
    writer: &mut W,
    duplicates: &[DuplicateDependency],
    format: OutputFormat,
) -> Result<()> {
    match format {
        OutputFormat::Json => {
            let output = json!({
                "duplicates": duplicates,
                "count": duplicates.len(),
            });
            writeln!(writer, "{}", serde_json::to_string_pretty(&output)?)?;
        }
        OutputFormat::Human => {
            if duplicates.is_empty() {
                writeln!(writer, "No duplicate dependencies found.")?;
            } else {
                writeln!(
                    writer,
                    "Duplicate dependencies ({} total):",
                    duplicates.len()
                )?;
                writeln!(writer)?;

                for dup in duplicates {
                    writeln!(
                        writer,
                        "  {} has {} versions:",
                        dup.name,
                        dup.versions.len()
                    )?;
                    for version in &dup.versions {
                        writeln!(writer, "    - {}", version)?;
                    }
                }

                writeln!(writer)?;
                writeln!(
                    writer,
                    "Total: {} packages with duplicates",
                    duplicates.len()
                )?;
            }
        }
    }

    Ok(())
}

/// Write full workspace report
///
/// Will be implemented in P1.W5.T4
#[allow(dead_code)]
pub fn write_workspace_report<W: Write>(
    writer: &mut W,
    packages: &[PackageInfo],
    dependencies: &[DependencyInfo],
    format: OutputFormat,
) -> Result<()> {
    match format {
        OutputFormat::Json => {
            let output = json!({
                "packages": packages,
                "dependencies": dependencies,
                "package_count": packages.len(),
                "dependency_count": dependencies.len(),
            });
            writeln!(writer, "{}", serde_json::to_string_pretty(&output)?)?;
        }
        OutputFormat::Human => {
            writeln!(writer, "Workspace Analysis")?;
            writeln!(writer, "==================")?;
            writeln!(writer)?;
            writeln!(writer, "Packages: {}", packages.len())?;
            writeln!(writer, "Dependencies: {}", dependencies.len())?;
            writeln!(writer)?;

            writeln!(writer, "Workspace Members:")?;
            for pkg in packages {
                writeln!(writer, "  - {} v{}", pkg.name, pkg.version)?;
            }
        }
    }

    Ok(())
}

/// Write unused dependencies report
///
/// Formats and outputs unused dependencies in either human-readable or JSON format.
/// Human format groups dependencies by package for easy navigation.
/// JSON format includes all structured data for programmatic processing.
///
/// # Arguments
/// * `report` - The unused dependencies report to format
/// * `format` - Output format: "json" or "human" (default)
///
/// # Example
/// ```no_run
/// # use anyhow::Result;
/// # use xtask::deps::UnusedDetector;
/// let report = UnusedDetector::detect()?;
/// xtask::deps::write_unused_report(&report, "human")?;
/// # Ok::<(), anyhow::Error>(())
/// ```
pub fn write_unused_report(report: &crate::deps::UnusedReport, format: &str) -> Result<()> {
    match format {
        "json" => {
            let json = serde_json::to_string_pretty(report)?;
            println!("{}", json);
        }
        "human" | _ => {
            if report.unused.is_empty() {
                println!("✓ No unused dependencies found (tool: {})", report.tool);
            } else {
                println!(
                    "Found {} unused dependencies (tool: {}):\n",
                    report.unused.len(),
                    report.tool
                );

                let mut by_package: std::collections::BTreeMap<&str, Vec<&str>> =
                    std::collections::BTreeMap::new();
                for dep in &report.unused {
                    by_package
                        .entry(&dep.package)
                        .or_insert_with(Vec::new)
                        .push(&dep.dependency);
                }

                for (package, deps) in by_package {
                    println!("  {}:", package);
                    for dep in deps {
                        println!("    - {}", dep);
                    }
                }
            }
        }
    }
    Ok(())
}

/// Write build timing report
///
/// Formats and outputs build timing analysis showing the slowest crates
/// and their percentage of total build time.
///
/// # Arguments
/// * `report` - The timing report to format
/// * `top` - Number of top slowest crates to display
///
/// # Example
/// ```no_run
/// # use anyhow::Result;
/// # use xtask::deps::TimingAnalyzer;
/// let report = TimingAnalyzer::analyze()?;
/// xtask::deps::write_timing_report(&report, 10)?;
/// # Ok::<(), anyhow::Error>(())
/// ```
pub fn write_timing_report(report: &crate::deps::TimingReport, top: usize) -> Result<()> {
    println!("Build Timing Analysis");
    println!("Total build time: {:.2}s\n", report.total_time_secs);

    println!("Top {} slowest crates:", top);
    for (i, crate_info) in report.crate_times.iter().take(top).enumerate() {
        let percent = (crate_info.duration_secs / report.total_time_secs) * 100.0;
        println!(
            "  {}. {} - {:.2}s ({:.1}%)",
            i + 1,
            crate_info.name,
            crate_info.duration_secs,
            percent
        );
    }

    if let Some(html_path) = &report.html_report {
        println!("\nHTML report: {}", html_path.display());
    }

    Ok(())
}

#[cfg(test)]
mod tests {

    // Tests will be added as needed
}

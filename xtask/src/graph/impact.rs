//! Dependency impact analysis
//!
//! This module provides analysis of how changes to packages would impact the
//! broader workspace. It categorizes packages by criticality and generates
//! comprehensive impact reports.
//!
//! # Criticality Levels
//!
//! Packages are classified into four levels based on their criticality score:
//!
//! | Level | Score Range | Meaning |
//! |-------|-------------|---------|
//! | Low | 0.0 - 0.2 | Few packages affected |
//! | Medium | 0.2 - 0.5 | Moderate impact |
//! | High | 0.5 - 0.8 | Large portion affected |
//! | Critical | 0.8 - 1.0 | Most/all packages affected |
//!
//! # Example
//!
//! ```no_run
//! use xtask::graph::{WorkspaceGraph, generate_report};
//!
//! let graph = WorkspaceGraph::new()?;
//! let report = generate_report(&graph)?;
//!
//! println!("Critical packages: {:?}", report.critical_packages);
//! println!("High-impact packages: {:?}", report.high_impact_packages);
//! # Ok::<(), anyhow::Error>(())
//! ```

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Impact metrics for a package.
///
/// Quantifies the impact that changes to a specific package would have on the
/// workspace. The key metric is the criticality score, which ranges from 0.0
/// (low impact, few packages depend on it) to 1.0 (high impact, most/all packages depend on it).
///
/// # Fields
///
/// - **package**: The name of the package being analyzed
/// - **dependent_count**: How many packages depend on this one (rebuild radius)
/// - **dependency_count**: How many packages this one depends on
/// - **criticality**: Computed score (0.0 to 1.0) representing relative impact
///
/// # Example
///
/// ```no_run
/// use xtask::graph::WorkspaceGraph;
///
/// let graph = WorkspaceGraph::new()?;
/// let metrics = graph.compute_impact_metrics("sinex-core")?;
///
/// // Check if this is a critical package
/// match metrics.criticality_level() {
///     xtask::graph::Criticality::Critical => println!("High risk!"),
///     xtask::graph::Criticality::High => println!("Significant impact"),
///     _ => println!("Low to moderate impact"),
/// }
/// # Ok::<(), anyhow::Error>(())
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImpactMetrics {
    /// Package name
    pub package: String,
    /// Number of packages that depend on this one (rebuild radius)
    pub dependent_count: usize,
    /// Number of transitive dependencies (depth)
    pub dependency_count: usize,
    /// Criticality score (0.0 - 1.0), higher = more critical
    pub criticality: f64,
}

/// Impact report for all workspace packages.
///
/// Provides a comprehensive analysis of package impact across the entire workspace.
/// Packages are categorized into critical and high-impact groups for easy identification
/// of sensitive dependencies.
///
/// # Fields
///
/// - **metrics**: Complete metrics for all packages
/// - **high_impact_packages**: Packages with criticality 0.5-0.8 (require careful changes)
/// - **critical_packages**: Packages with criticality >= 0.8 (very sensitive)
///
/// # Example
///
/// ```no_run
/// use xtask::graph::{WorkspaceGraph, generate_report};
///
/// let graph = WorkspaceGraph::new()?;
/// let report = generate_report(&graph)?;
///
/// println!("Total packages analyzed: {}", report.metrics.len());
/// println!("Critical packages: {}", report.critical_packages.len());
/// println!("High-impact packages: {}", report.high_impact_packages.len());
///
/// // Identify the most critical package
/// if let Some(most_critical) = report.metrics.iter()
///     .max_by(|a, b| a.criticality.partial_cmp(&b.criticality).unwrap())
/// {
///     println!("Most critical: {} (score: {:.2})",
///              most_critical.package, most_critical.criticality);
/// }
/// # Ok::<(), anyhow::Error>(())
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImpactReport {
    /// All impact metrics, one per workspace package
    pub metrics: Vec<ImpactMetrics>,
    /// High-impact packages (criticality 0.5-0.8)
    pub high_impact_packages: Vec<String>,
    /// Critical packages (criticality >= 0.8)
    pub critical_packages: Vec<String>,
}

/// Criticality level classification for a package.
///
/// Packages are classified into one of four criticality levels based on their
/// criticality score. This provides an easy-to-understand categorization of
/// how much impact changes to a package would have on the workspace.
///
/// # Examples
///
/// ```no_run
/// use xtask::graph::Criticality;
///
/// assert_eq!(Criticality::from_score(0.9), Criticality::Critical);
/// assert_eq!(Criticality::from_score(0.6), Criticality::High);
/// assert_eq!(Criticality::from_score(0.3), Criticality::Medium);
/// assert_eq!(Criticality::from_score(0.1), Criticality::Low);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Criticality {
    /// Low impact: fewer than 20% of packages affected
    ///
    /// Changes to this package are relatively safe. Few other packages
    /// depend on it, so modifications have a limited blast radius.
    Low,
    /// Medium impact: 20-50% of packages affected
    ///
    /// Changes to this package could affect a moderate portion of the
    /// workspace. Some care should be taken, but impact is manageable.
    Medium,
    /// High impact: 50-80% of packages affected
    ///
    /// Changes to this package would affect a large portion of the workspace.
    /// Significant testing and coordination may be needed.
    High,
    /// Critical impact: 80-100% of packages affected
    ///
    /// This is a critical package. Changes require extreme care, comprehensive
    /// testing, and likely coordination across multiple teams or review phases.
    Critical,
}

impl Criticality {
    /// Determine criticality level from a numeric score.
    ///
    /// Converts a criticality score (0.0 to 1.0) into a categorical level.
    ///
    /// # Arguments
    ///
    /// * `score` - Numeric score between 0.0 and 1.0
    ///
    /// # Returns
    ///
    /// The corresponding `Criticality` level based on thresholds:
    /// - score >= 0.8 → Critical
    /// - score >= 0.5 → High
    /// - score >= 0.2 → Medium
    /// - score < 0.2 → Low
    ///
    /// # Example
    ///
    /// ```no_run
    /// use xtask::graph::Criticality;
    ///
    /// assert_eq!(Criticality::from_score(0.9), Criticality::Critical);
    /// assert_eq!(Criticality::from_score(0.6), Criticality::High);
    /// ```
    pub fn from_score(score: f64) -> Self {
        if score >= 0.8 {
            Self::Critical
        } else if score >= 0.5 {
            Self::High
        } else if score >= 0.2 {
            Self::Medium
        } else {
            Self::Low
        }
    }
}

impl ImpactMetrics {
    /// Create new impact metrics with computed criticality.
    ///
    /// # Arguments
    ///
    /// * `package` - Name of the package being analyzed
    /// * `dependent_count` - Number of packages that depend on this one
    /// * `dependency_count` - Number of packages this one depends on
    ///
    /// # Returns
    ///
    /// A new `ImpactMetrics` with criticality automatically computed from
    /// the dependent count. The criticality score is capped at 1.0.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use xtask::graph::ImpactMetrics;
    ///
    /// let metrics = ImpactMetrics::new(
    ///     "my-package".to_string(),
    ///     50,  // 50 packages depend on this
    ///     10   // This package depends on 10 others
    /// );
    ///
    /// println!("Criticality: {:.2}", metrics.criticality);
    /// ```
    #[allow(dead_code)]
    pub fn new(package: String, dependent_count: usize, dependency_count: usize) -> Self {
        // Calculate criticality based on dependent count
        // Simple heuristic: score = dependent_count / max_possible_dependents
        // For now, use a simplified calculation
        let criticality = (dependent_count as f64) / 100.0;
        let criticality = criticality.min(1.0); // Cap at 1.0

        Self {
            package,
            dependent_count,
            dependency_count,
            criticality,
        }
    }

    /// Get the criticality level classification for this package.
    ///
    /// Converts the numeric criticality score into an easy-to-understand
    /// categorical level (Low, Medium, High, Critical).
    ///
    /// # Returns
    ///
    /// The `Criticality` level based on the package's criticality score.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use xtask::graph::{ImpactMetrics, Criticality};
    ///
    /// let metrics = ImpactMetrics::new("core-lib".to_string(), 80, 5);
    /// match metrics.criticality_level() {
    ///     Criticality::Critical => println!("This is a critical package!"),
    ///     Criticality::High => println!("High impact package"),
    ///     _ => {}
    /// }
    /// ```
    pub fn criticality_level(&self) -> Criticality {
        Criticality::from_score(self.criticality)
    }
}

/// Generate a comprehensive impact report for all workspace packages.
///
/// Analyzes every package in the workspace and computes impact metrics for each.
/// Packages are automatically categorized into critical (>=0.8) and high-impact
/// (>=0.5) groups for easy identification of sensitive dependencies.
///
/// This is the primary entry point for impact analysis. Use it to get a full
/// understanding of the workspace's dependency criticality.
///
/// # Arguments
///
/// * `graph` - A `WorkspaceGraph` containing the workspace dependency structure
///
/// # Returns
///
/// An `ImpactReport` containing:
/// - **metrics**: Complete metrics for every package in the workspace
/// - **critical_packages**: Packages with criticality >= 0.8 (highest risk)
/// - **high_impact_packages**: Packages with criticality 0.5-0.8 (high risk)
///
/// # Errors
///
/// Returns an error if impact metrics cannot be computed for any package,
/// which typically indicates an invalid workspace structure.
///
/// # Example
///
/// ```no_run
/// use xtask::graph::{WorkspaceGraph, generate_report, Criticality};
///
/// let graph = WorkspaceGraph::new()?;
/// let report = generate_report(&graph)?;
///
/// println!("Workspace Analysis");
/// println!("==================");
/// println!("Total packages: {}", report.metrics.len());
/// println!("Critical packages: {}", report.critical_packages.len());
/// println!("High-impact packages: {}", report.high_impact_packages.len());
///
/// // Show the most critical package
/// if let Some(most_critical) = report.metrics.iter()
///     .max_by(|a, b| a.criticality.partial_cmp(&b.criticality).unwrap())
/// {
///     println!("\nMost critical package: {}", most_critical.package);
///     println!("Criticality: {:.2}% (affects {} packages)",
///              most_critical.criticality * 100.0,
///              most_critical.dependent_count);
/// }
/// # Ok::<(), anyhow::Error>(())
/// ```
///
/// # Performance
///
/// This function performs a complete impact analysis of the workspace and can be
/// expensive for very large workspaces. The result can be cached and reused.
pub fn generate_report(graph: &crate::graph::workspace::WorkspaceGraph) -> Result<ImpactReport> {
    let workspace_packages: Vec<String> = graph
        .workspace_packages()
        .into_iter()
        .map(|p| p.name().to_string())
        .collect();

    let mut metrics = Vec::new();
    let mut high_impact_packages = Vec::new();
    let mut critical_packages = Vec::new();

    for package_name in workspace_packages {
        let metric = graph.compute_impact_metrics(&package_name)?;
        let level = metric.criticality_level();

        match level {
            Criticality::Critical => critical_packages.push(package_name.clone()),
            Criticality::High => high_impact_packages.push(package_name.clone()),
            _ => {}
        }

        metrics.push(metric);
    }

    Ok(ImpactReport {
        metrics,
        high_impact_packages,
        critical_packages,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_criticality_from_score() {
        assert_eq!(Criticality::from_score(0.9), Criticality::Critical);
        assert_eq!(Criticality::from_score(0.6), Criticality::High);
        assert_eq!(Criticality::from_score(0.3), Criticality::Medium);
        assert_eq!(Criticality::from_score(0.1), Criticality::Low);
    }

    #[test]
    fn test_impact_metrics_new() {
        let metrics = ImpactMetrics::new("test-pkg".to_string(), 50, 10);
        assert_eq!(metrics.package, "test-pkg");
        assert_eq!(metrics.dependent_count, 50);
        assert_eq!(metrics.dependency_count, 10);
        assert_eq!(metrics.criticality_level(), Criticality::High);
    }
}

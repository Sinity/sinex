//! Workspace graph analysis for dependency visualization
//!
//! This module provides a high-level interface to analyze workspace dependencies
//! using guppy's `PackageGraph`. It enables querying relationships between packages,
//! computing impact metrics, and analyzing the dependency structure of a Cargo workspace.
//!
//! # Example
//!
//! ```no_run
//! use xtask::graph::WorkspaceGraph;
//!
//! // Load the workspace dependency graph
//! let graph = WorkspaceGraph::new()?;
//!
//! // Get all packages in the workspace
//! let packages = graph.workspace_packages();
//! println!("Workspace has {} packages", packages.len());
//!
//! // Analyze a specific package
//! let metrics = graph.compute_impact_metrics("sinex-db")?;
//! println!("Package depends on {} packages", metrics.dependency_count);
//! println!("Package is depended on by {} packages", metrics.dependent_count);
//! println!("Criticality: {:.2}", metrics.criticality);
//! # Ok::<(), color_eyre::eyre::Report>(())
//! ```

use color_eyre::eyre::{ContextCompat, Result, WrapErr};
use guppy::MetadataCommand;
use guppy::graph::PackageGraph;

use crate::graph::impact::ImpactMetrics;

/// Information about a dependency
///
/// Represents a single direct dependency of a package within the workspace.
/// This struct is used when querying the dependencies of a specific package.
///
/// # Example
///
/// ```no_run
/// use xtask::graph::WorkspaceGraph;
///
/// let graph = WorkspaceGraph::new()?;
/// let deps = graph.all_dependencies("sinex-gateway")?;
///
/// for dep in deps {
///     println!("Depends on: {}", dep.name);
/// }
/// # Ok::<(), color_eyre::eyre::Report>(())
/// ```
#[derive(Debug, Clone)]
pub struct DependencyInfo {
    /// The name of the dependency
    pub name: String,
}

/// A workspace-aware dependency graph built from Cargo metadata.
///
/// Wraps guppy's `PackageGraph` with workspace-specific analysis methods.
/// Provides convenient access to the complete dependency graph of a Cargo workspace
/// and enables computing impact metrics, finding paths, and analyzing transitive
/// dependencies.
///
/// # Construction
///
/// Create a new `WorkspaceGraph` by calling `WorkspaceGraph::new()`. This will
/// execute `cargo metadata` to load the workspace structure and build the graph:
///
/// ```no_run
/// use xtask::graph::WorkspaceGraph;
///
/// let graph = WorkspaceGraph::new()?;
/// # Ok::<(), color_eyre::eyre::Report>(())
/// ```
///
/// # Common Operations
///
/// - **Get all workspace packages**: `workspace_packages()`
/// - **Analyze impact**: `compute_impact_metrics(package_name)`
/// - **Find dependents**: `transitive_dependents(package_name)`
/// - **Check reachability**: `shortest_path(from, to)`
/// - **Inspect dependencies**: `all_dependencies(package_name)`
///
/// # Caching and Cloning
///
/// `WorkspaceGraph` is `Clone`. Multiple copies will all point to the same
/// underlying `PackageGraph` data structure, making cloning cheap.
///
/// # Example
///
/// ```no_run
/// use xtask::graph::WorkspaceGraph;
///
/// let graph = WorkspaceGraph::new()?;
///
/// // Find critical packages
/// for pkg in graph.workspace_packages() {
///     let metrics = graph.compute_impact_metrics(pkg.name())?;
///     if metrics.criticality > 0.5 {
///         println!("High-impact package: {} (criticality: {:.2})", pkg.name(), metrics.criticality);
///     }
/// }
/// # Ok::<(), color_eyre::eyre::Report>(())
/// ```
#[derive(Clone)]
pub struct WorkspaceGraph {
    graph: PackageGraph,
}

impl WorkspaceGraph {
    /// Create a new workspace graph from Cargo metadata.
    ///
    /// Executes `cargo metadata` to load the workspace structure and constructs
    /// a dependency graph using guppy. This operation is relatively expensive,
    /// so the resulting `WorkspaceGraph` should be reused where possible.
    ///
    /// # Returns
    ///
    /// A new `WorkspaceGraph` with the complete dependency graph loaded from
    /// the current workspace. The graph includes all packages, both workspace
    /// members and their transitive dependencies.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `cargo metadata` fails (e.g., invalid Cargo.toml, build script errors)
    /// - The workspace structure is invalid
    /// - The package graph cannot be constructed from the metadata
    ///
    /// # Example
    ///
    /// ```no_run
    /// use xtask::graph::WorkspaceGraph;
    ///
    /// let graph = WorkspaceGraph::new()?;
    /// println!("Loaded workspace with {} packages", graph.workspace_packages().len());
    /// # Ok::<(), color_eyre::eyre::Report>(())
    /// ```
    pub fn new() -> Result<Self> {
        // Run cargo metadata to get workspace information
        let metadata = MetadataCommand::new()
            .exec()
            .context("Failed to execute cargo metadata")?;

        // Build the package graph using guppy
        let graph = PackageGraph::from_metadata(metadata)
            .context("Failed to build package graph from metadata")?;

        Ok(Self { graph })
    }

    /// Get the underlying guppy `PackageGraph`.
    ///
    /// Provides direct access to the guppy `PackageGraph` for advanced operations
    /// not covered by the `WorkspaceGraph` API. Most users should prefer the
    /// higher-level methods on this type.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use xtask::graph::WorkspaceGraph;
    ///
    /// let graph = WorkspaceGraph::new()?;
    /// let pkg_graph = graph.graph();
    ///
    /// // Iterate all packages
    /// for pkg in pkg_graph.packages() {
    ///     println!("Package: {}", pkg.name());
    /// }
    /// # Ok::<(), color_eyre::eyre::Report>(())
    /// ```
    pub fn graph(&self) -> &PackageGraph {
        &self.graph
    }

    /// Inject timing weights into the graph from bench timing data.
    ///
    /// If Phase 2 timing data is available, loads compile times from the history
    /// database and associates them with packages for weighted analysis. Otherwise,
    /// uses uniform weights (1.0) for all packages.
    ///
    /// This method currently behaves as a no-op placeholder. It
    /// returns the graph unchanged with uniform weights and prints a note to stderr.
    ///
    /// # Returns
    ///
    /// A new `WorkspaceGraph` with timing weights applied. Currently returns `self`
    /// since timing-weight integration is not yet complete.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use xtask::graph::WorkspaceGraph;
    ///
    /// let graph = WorkspaceGraph::new()?
    ///     .with_timing_weights()?;
    /// # Ok::<(), color_eyre::eyre::Report>(())
    /// ```
    ///
    /// # Future Behavior
    ///
    /// When timing-weight integration is complete, this method will:
    /// 1. Check if timing history database exists
    /// 2. Load most recent timing data for each crate
    /// 3. Associate compile times with graph nodes for weighted analysis
    pub fn with_timing_weights(self) -> Result<Self> {
        // Check if the bench history database exists and has timing data.
        // The bench module stores results in a SQLite DB at the configured state dir.
        let cfg = crate::config::config();
        let bench_db_path = cfg.state_dir.join("bench-history.db");

        if !bench_db_path.exists() {
            eprintln!(
                "Note: No bench history database found at {}",
                bench_db_path.display()
            );
            eprintln!("Using uniform weights (1.0) for all packages");
            eprintln!("Run `xtask bench` to populate timing data");
            return Ok(self);
        }

        // Database exists but we can't query per-crate compile times from it yet
        // (bench history tracks thread-count scenarios, not per-crate compile times).
        // For now, acknowledge the database exists but use uniform weights.
        eprintln!("Note: Bench history found but per-crate timing data not yet available");
        eprintln!("Using uniform weights (1.0) for all packages");

        Ok(self)
    }

    /// Get all transitive dependents of a package.
    ///
    /// Returns all packages that directly or indirectly depend on the given package.
    /// This is useful for understanding the "blast radius" of changes to a package:
    /// modifying this package could affect all returned packages.
    ///
    /// # Arguments
    ///
    /// * `package_name` - Name of the package to find dependents for
    ///
    /// # Returns
    ///
    /// A vector of package names (excluding the input package itself) that depend
    /// on the given package. Empty vector if no packages depend on it.
    ///
    /// # Errors
    ///
    /// Returns an error if the package is not found in the workspace.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use xtask::graph::WorkspaceGraph;
    ///
    /// let graph = WorkspaceGraph::new()?;
    /// let dependents = graph.transitive_dependents("sinex-db")?;
    ///
    /// println!("Packages affected by changes to sinex-db:");
    /// for pkg in dependents {
    ///     println!("  - {}", pkg);
    /// }
    /// # Ok::<(), color_eyre::eyre::Report>(())
    /// ```
    pub fn transitive_dependents(&self, package_name: &str) -> Result<Vec<String>> {
        // Find the package in the graph
        let package = self
            .graph
            .packages()
            .find(|p| p.name() == package_name)
            .with_context(|| format!("Package '{package_name}' not found in workspace"))?;

        // Get all packages that depend on this one (reverse dependencies)
        let query = self.graph.query_reverse(vec![package.id()])?;
        let dependents: Vec<String> = query
            .resolve()
            .packages(guppy::graph::DependencyDirection::Reverse)
            .map(|p| p.name().to_string())
            .filter(|name| name != package_name) // Exclude self
            .collect();

        Ok(dependents)
    }

    /// Find shortest dependency path between two packages.
    ///
    /// Determines if a dependency path exists from the source package to the target
    /// package, and returns the path if one exists. This helps understand how changes
    /// might propagate through the dependency graph.
    ///
    /// # Arguments
    ///
    /// * `from` - Source package name
    /// * `to` - Target package name
    ///
    /// # Returns
    ///
    /// * `Ok(Some(path))` - A vector of package names representing the dependency
    ///   path from `from` to `to` (inclusive)
    /// * `Ok(None)` - If the target is not reachable from the source
    ///
    /// # Errors
    ///
    /// Returns an error if either the source or target package is not found in the workspace.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use xtask::graph::WorkspaceGraph;
    ///
    /// let graph = WorkspaceGraph::new()?;
    ///
    /// match graph.shortest_path("sinex-gateway", "sinex-db")? {
    ///     Some(path) => {
    ///         println!("Dependency path: {}", path.join(" -> "));
    ///     }
    ///     None => {
    ///         println!("No dependency path exists");
    ///     }
    /// }
    /// # Ok::<(), color_eyre::eyre::Report>(())
    /// ```
    ///
    /// # Implementation Note
    ///
    /// The current implementation is simplified and may not return the actual shortest
    /// path in terms of steps. Future versions will implement proper BFS for optimal
    /// path finding.
    pub fn shortest_path(&self, from: &str, to: &str) -> Result<Option<Vec<String>>> {
        // Find both packages
        let from_pkg = self
            .graph
            .packages()
            .find(|p| p.name() == from)
            .with_context(|| format!("Source package '{from}' not found"))?;

        let _to_pkg = self
            .graph
            .packages()
            .find(|p| p.name() == to)
            .with_context(|| format!("Target package '{to}' not found"))?;

        // Use guppy to find the path
        // This is a simplified version - full implementation would use graph traversal
        let query = self.graph.query_forward(vec![from_pkg.id()])?;
        let resolved = query.resolve();

        // Check if target is reachable
        let is_reachable = resolved
            .package_ids(guppy::graph::DependencyDirection::Forward)
            .any(|id| self.graph.metadata(id).is_ok_and(|p| p.name() == to));

        if is_reachable {
            // For now, return a simple direct path marker
            // Full implementation in later waves would do actual path finding
            Ok(Some(vec![from.to_string(), to.to_string()]))
        } else {
            Ok(None)
        }
    }

    /// Get all workspace packages with their metadata.
    ///
    /// Returns information about all member packages in the workspace. This includes
    /// only direct workspace members, not transitive dependencies.
    ///
    /// # Returns
    ///
    /// A vector of `PackageMetadata` objects for all workspace member packages.
    /// Each metadata object provides access to the package name, version, manifest
    /// path, and other properties.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use xtask::graph::WorkspaceGraph;
    ///
    /// let graph = WorkspaceGraph::new()?;
    ///
    /// println!("Workspace packages:");
    /// for pkg in graph.workspace_packages() {
    ///     println!("  {} @ {}", pkg.name(), pkg.version());
    /// }
    /// # Ok::<(), color_eyre::eyre::Report>(())
    /// ```
    pub fn workspace_packages(&self) -> Vec<guppy::graph::PackageMetadata<'_>> {
        self.graph
            .workspace()
            .member_ids()
            .filter_map(|id| self.graph.metadata(id).ok())
            .collect()
    }

    /// Get all direct dependencies of a package.
    ///
    /// Returns information about packages that are directly depended upon by
    /// the specified package. This includes both workspace members and external
    /// dependencies.
    ///
    /// # Arguments
    ///
    /// * `package_name` - Name of the package to get dependencies for
    ///
    /// # Returns
    ///
    /// A vector of `DependencyInfo` objects representing the direct dependencies.
    /// Returns an empty vector if the package has no dependencies.
    ///
    /// # Errors
    ///
    /// Returns an error if the package is not found in the workspace.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use xtask::graph::WorkspaceGraph;
    ///
    /// let graph = WorkspaceGraph::new()?;
    /// let deps = graph.all_dependencies("sinex-gateway")?;
    ///
    /// println!("Dependencies of sinex-gateway:");
    /// for dep in deps {
    ///     println!("  - {}", dep.name);
    /// }
    /// # Ok::<(), color_eyre::eyre::Report>(())
    /// ```
    pub fn all_dependencies(&self, package_name: &str) -> Result<Vec<DependencyInfo>> {
        // Find the package in the graph
        let package = self
            .graph
            .packages()
            .find(|p| p.name() == package_name)
            .with_context(|| format!("Package '{package_name}' not found in workspace"))?;

        // Get direct dependencies
        let deps: Vec<DependencyInfo> = package
            .direct_links()
            .map(|link| {
                let dep_pkg = link.to();
                DependencyInfo {
                    name: dep_pkg.name().to_string(),
                }
            })
            .collect();

        Ok(deps)
    }

    /// Compute impact metrics for a package.
    ///
    /// Calculates comprehensive impact metrics for a package:
    ///
    /// - **Rebuild radius**: Number of packages that depend on this one (directly or indirectly)
    /// - **Dependency depth**: Number of packages this one depends on (directly or indirectly)
    /// - **Criticality score**: Fraction of the workspace affected by changes (0.0 to 1.0)
    ///
    /// The criticality score is the key metric for understanding how "critical" a package is:
    /// a score of 1.0 means all packages in the workspace depend on it, while 0.0 means
    /// no packages depend on it.
    ///
    /// # Arguments
    ///
    /// * `package_name` - Name of the package to analyze
    ///
    /// # Returns
    ///
    /// An `ImpactMetrics` object containing:
    /// - `package`: The package name
    /// - `dependent_count`: Number of packages affected by changes to this package
    /// - `dependency_count`: Number of packages this one depends on
    /// - `criticality`: Score from 0.0 (low impact) to 1.0 (critical)
    ///
    /// # Errors
    ///
    /// Returns an error if the package is not found in the workspace.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use xtask::graph::WorkspaceGraph;
    ///
    /// let graph = WorkspaceGraph::new()?;
    /// let metrics = graph.compute_impact_metrics("sinex-db")?;
    ///
    /// println!("Package: {}", metrics.package);
    /// println!("Packages affected by changes: {}", metrics.dependent_count);
    /// println!("Criticality: {:.2}%", metrics.criticality * 100.0);
    ///
    /// if metrics.criticality > 0.8 {
    ///     println!("WARNING: This is a critical package!");
    /// }
    /// # Ok::<(), color_eyre::eyre::Report>(())
    /// ```
    pub fn compute_impact_metrics(&self, package_name: &str) -> Result<ImpactMetrics> {
        // Get all transitive dependents of this package
        let dependents = self.transitive_dependents(package_name)?;
        let dependent_count = dependents.len();

        // Find the package in the graph
        let package = self
            .graph
            .packages()
            .find(|p| p.name() == package_name)
            .with_context(|| format!("Package '{package_name}' not found"))?;

        // Count direct dependencies using forward query
        let query = self.graph.query_forward(vec![package.id()])?;
        let dependency_count = query
            .resolve()
            .packages(guppy::graph::DependencyDirection::Forward)
            .count()
            - 1; // Exclude self

        // Calculate criticality based on rebuild radius
        // Higher dependent count = higher criticality
        let total_packages = self.graph.workspace().member_ids().count();
        let criticality = if total_packages > 0 {
            (dependent_count as f64) / (total_packages as f64)
        } else {
            0.0
        };

        Ok(ImpactMetrics {
            package: package_name.to_string(),
            dependent_count,
            dependency_count,
            criticality,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn test_workspace_graph_new() -> TestResult<()> {
        let result = WorkspaceGraph::new();
        assert!(result.is_ok(), "Failed to create WorkspaceGraph");
        Ok(())
    }
}

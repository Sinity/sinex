//! Workspace dependency analysis using guppy

use color_eyre::eyre::{Result, WrapErr};
use guppy::MetadataCommand;
use guppy::PackageId;
use guppy::graph::PackageGraph;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::deps::active::{
    active_direct_dependencies, active_direct_dependents_by_package,
    active_package_ids_for_package, cargo_set_package_ids, workspace_cargo_set,
};

/// Workspace analyzer using guppy
pub struct WorkspaceAnalyzer {
    graph: PackageGraph,
}

/// Information about a package
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageInfo {
    /// Package name
    pub name: String,
    /// Version
    pub version: String,
    /// Whether this is a workspace member
    pub is_workspace: bool,
}

/// Dependency relationship information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DependencyInfo {
    /// Package that depends on something
    pub dependent: String,
    /// Package being depended on
    pub dependency: String,
    /// Dependency kind (normal, dev, build)
    pub kind: String,
}

/// A duplicate dependency (same package, multiple versions)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicateDependency {
    /// Package name
    pub name: String,
    /// List of versions present
    pub versions: Vec<String>,
    /// Whether this duplicate is directly requested by workspace manifests or
    /// introduced only by upstream transitive dependencies.
    pub classification: DuplicateDependencyClass,
    /// Number of workspace packages that directly request any reported version.
    pub direct_workspace_root_count: usize,
    /// Per-version reachability from workspace roots.
    pub version_details: Vec<DuplicateVersionDetail>,
}

/// How actionable a duplicate dependency is from workspace manifests.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DuplicateDependencyClass {
    /// At least one workspace manifest directly requests one of the versions.
    DirectWorkspace,
    /// No workspace manifest directly requests the duplicate; upstream crates
    /// introduce the version split.
    TransitiveUpstream,
}

impl DuplicateDependencyClass {
    #[must_use]
    pub const fn is_direct_workspace(self) -> bool {
        matches!(self, Self::DirectWorkspace)
    }

    #[must_use]
    pub const fn is_transitive_upstream(self) -> bool {
        matches!(self, Self::TransitiveUpstream)
    }

    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::DirectWorkspace => "direct workspace debt",
            Self::TransitiveUpstream => "transitive upstream",
        }
    }
}

/// Reachability detail for one version of a duplicate dependency.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicateVersionDetail {
    /// Package version.
    pub version: String,
    /// Workspace packages whose dependency closure reaches this version.
    pub workspace_roots: Vec<String>,
    /// Workspace packages that directly request this exact version.
    pub direct_workspace_roots: Vec<String>,
    /// Active packages that immediately depend on this exact version.
    pub direct_dependents: Vec<String>,
}

impl WorkspaceAnalyzer {
    /// Create a new workspace analyzer
    ///
    /// Loads workspace metadata using `cargo_metadata` and builds a dependency
    /// graph using guppy's `PackageGraph`.
    ///
    /// # Returns
    /// A new `WorkspaceAnalyzer` with loaded dependency graph
    ///
    /// # Errors
    /// Returns error if:
    /// - cargo metadata execution fails
    /// - Metadata parsing fails
    /// - `PackageGraph` construction fails
    ///
    /// # Example
    /// ```no_run
    /// use xtask::deps::analyzer::WorkspaceAnalyzer;
    /// let analyzer = WorkspaceAnalyzer::new()?;
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

    /// Get all workspace packages
    ///
    /// Returns information about all packages that are members of the workspace.
    ///
    /// # Returns
    /// Vector of `PackageInfo` for each workspace member
    ///
    /// # Example
    /// ```no_run
    /// use xtask::deps::analyzer::WorkspaceAnalyzer;
    /// let analyzer = WorkspaceAnalyzer::new()?;
    /// let packages = analyzer.workspace_packages()?;
    /// for pkg in packages {
    ///     println!("{} v{}", pkg.name, pkg.version);
    /// }
    /// # Ok::<(), color_eyre::eyre::Report>(())
    /// ```
    pub fn workspace_packages(&self) -> Result<Vec<PackageInfo>> {
        let workspace = self.graph.workspace();

        let mut packages = Vec::new();

        for package_id in workspace.member_ids() {
            let package = self
                .graph
                .metadata(package_id)
                .context("Failed to get package metadata")?;

            packages.push(PackageInfo {
                name: package.name().to_string(),
                version: package.version().to_string(),
                is_workspace: true,
            });
        }

        Ok(packages)
    }

    /// Get all dependencies (including transitive)
    ///
    /// Returns dependency relationships for all packages in the workspace.
    /// Includes normal, dev, and build dependencies.
    ///
    /// # Returns
    /// Vector of `DependencyInfo` describing all dependency relationships
    ///
    /// # Example
    /// ```no_run
    /// use xtask::deps::analyzer::WorkspaceAnalyzer;
    /// let analyzer = WorkspaceAnalyzer::new()?;
    /// let deps = analyzer.all_dependencies()?;
    /// for dep in deps {
    ///     println!("{} -> {} ({})", dep.dependent, dep.dependency, dep.kind);
    /// }
    /// # Ok::<(), color_eyre::eyre::Report>(())
    /// ```
    pub fn all_dependencies(&self) -> Result<Vec<DependencyInfo>> {
        let workspace = self.graph.workspace();
        let mut dependencies = Vec::new();

        // Iterate over all workspace packages
        for package_id in workspace.member_ids() {
            let _package = self
                .graph
                .metadata(package_id)
                .context("Failed to get package metadata")?;

            for dependency in active_direct_dependencies(&self.graph, package_id, true)? {
                dependencies.push(DependencyInfo {
                    dependent: _package.name().to_string(),
                    dependency: dependency.name,
                    kind: dependency.kind.to_string(),
                });
            }
        }

        Ok(dependencies)
    }

    /// Find duplicate dependencies (same name, different versions)
    ///
    /// Identifies packages that exist in multiple versions within the workspace.
    /// This can indicate potential version conflicts or opportunities for consolidation.
    ///
    /// # Returns
    /// Vector of `DuplicateDependency`, one for each package name that has multiple versions
    ///
    /// # Example
    /// ```no_run
    /// use xtask::deps::analyzer::WorkspaceAnalyzer;
    /// let analyzer = WorkspaceAnalyzer::new()?;
    /// let duplicates = analyzer.find_duplicates()?;
    /// for dup in duplicates {
    ///     println!("{} has versions: {}", dup.name, dup.versions.join(", "));
    /// }
    /// # Ok::<(), color_eyre::eyre::Report>(())
    /// ```
    pub fn find_duplicates(&self) -> Result<Vec<DuplicateDependency>> {
        // Map package name -> version -> package IDs for that version.
        let mut version_map: BTreeMap<String, BTreeMap<String, Vec<PackageId>>> = BTreeMap::new();
        let workspace_roots_by_package = self.workspace_roots_by_reached_package()?;
        let direct_roots_by_package = self.workspace_direct_roots_by_package()?;
        let direct_dependents_by_package = active_direct_dependents_by_package(&self.graph, true)?;
        let active_package_ids = cargo_set_package_ids(&workspace_cargo_set(&self.graph, true)?);

        for package in self.graph.packages() {
            if !active_package_ids.contains(package.id()) {
                continue;
            }

            let name = package.name().to_string();
            let version = package.version().to_string();

            version_map
                .entry(name)
                .or_default()
                .entry(version)
                .or_default()
                .push(package.id().clone());
        }

        // Find packages with multiple versions
        let mut duplicates = Vec::new();

        for (name, versions_by_id) in version_map {
            if versions_by_id.len() > 1 {
                let versions_vec: Vec<String> = versions_by_id.keys().cloned().collect();
                let mut version_details = Vec::with_capacity(versions_by_id.len());

                for (version, package_ids) in versions_by_id {
                    version_details.push(DuplicateVersionDetail {
                        version,
                        workspace_roots: self.workspace_roots_for_package_ids(
                            &package_ids,
                            &workspace_roots_by_package,
                        ),
                        direct_workspace_roots: self.workspace_roots_for_package_ids(
                            &package_ids,
                            &direct_roots_by_package,
                        ),
                        direct_dependents: self.workspace_roots_for_package_ids(
                            &package_ids,
                            &direct_dependents_by_package,
                        ),
                    });
                }
                let direct_workspace_roots = Self::direct_workspace_roots(&version_details);
                let direct_workspace_root_count = direct_workspace_roots.len();
                let classification = if direct_workspace_root_count > 0 {
                    DuplicateDependencyClass::DirectWorkspace
                } else {
                    DuplicateDependencyClass::TransitiveUpstream
                };

                duplicates.push(DuplicateDependency {
                    name,
                    versions: versions_vec,
                    classification,
                    direct_workspace_root_count,
                    version_details,
                });
            }
        }

        // Sort by package name for consistent output
        duplicates.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(duplicates)
    }

    fn direct_workspace_roots(version_details: &[DuplicateVersionDetail]) -> Vec<String> {
        let mut roots = Vec::new();

        for detail in version_details {
            roots.extend(detail.direct_workspace_roots.iter().cloned());
        }

        roots.sort();
        roots.dedup();
        roots
    }

    fn workspace_roots_by_reached_package(&self) -> Result<BTreeMap<PackageId, Vec<String>>> {
        let workspace = self.graph.workspace();
        let mut roots_by_package: BTreeMap<PackageId, Vec<String>> = BTreeMap::new();

        for workspace_id in workspace.member_ids() {
            let package = self
                .graph
                .metadata(workspace_id)
                .context("Failed to get workspace package metadata")?;
            let active_package_ids =
                active_package_ids_for_package(&self.graph, workspace_id, true)?;

            for reached_id in active_package_ids {
                roots_by_package
                    .entry(reached_id)
                    .or_default()
                    .push(package.name().to_string());
            }
        }

        for roots in roots_by_package.values_mut() {
            roots.sort();
            roots.dedup();
        }

        Ok(roots_by_package)
    }

    fn workspace_direct_roots_by_package(&self) -> Result<BTreeMap<PackageId, Vec<String>>> {
        let workspace = self.graph.workspace();
        let mut roots_by_package: BTreeMap<PackageId, Vec<String>> = BTreeMap::new();

        for workspace_id in workspace.member_ids() {
            let package = self
                .graph
                .metadata(workspace_id)
                .context("Failed to get workspace package metadata")?;
            for dependency in active_direct_dependencies(&self.graph, workspace_id, true)? {
                roots_by_package
                    .entry(dependency.package_id)
                    .or_default()
                    .push(package.name().to_string());
            }
        }

        for roots in roots_by_package.values_mut() {
            roots.sort();
            roots.dedup();
        }

        Ok(roots_by_package)
    }

    fn workspace_roots_for_package_ids(
        &self,
        package_ids: &[PackageId],
        roots_by_package: &BTreeMap<PackageId, Vec<String>>,
    ) -> Vec<String> {
        let mut roots = Vec::new();

        for package_id in package_ids {
            if let Some(package_roots) = roots_by_package.get(package_id) {
                roots.extend(package_roots.iter().cloned());
            }
        }

        roots.sort();
        roots.dedup();
        roots
    }
}

#[cfg(test)]
#[path = "analyzer_test.rs"]
mod tests;

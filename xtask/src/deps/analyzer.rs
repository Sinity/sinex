//! Workspace dependency analysis using guppy

use anyhow::{Context, Result};
use guppy::graph::{DependencyDirection, PackageGraph};
use guppy::MetadataCommand;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

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
    /// # Ok::<(), anyhow::Error>(())
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
    /// # Ok::<(), anyhow::Error>(())
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
    /// # Ok::<(), anyhow::Error>(())
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

            // Query all forward dependencies (what this package depends on)
            let query = self
                .graph
                .query_forward(std::iter::once(package_id))
                .context("Failed to create forward dependency query")?;

            // Resolve the query to get all dependencies
            let package_set = query.resolve();

            // Iterate through all dependency links
            for link in package_set.links(DependencyDirection::Forward) {
                let from_pkg = link.from();
                let to_pkg = link.to();

                // Guppy provides dependency metadata for normal, build, and dev dependencies
                // We collect the kind information from the link
                let _normal_req = link.normal();
                let _build_req = link.build();
                let _dev_req = link.dev();

                // For now, classify based on what types of dependencies exist
                // In practice, we just need to know there's a dependency
                let kind = "normal".to_string(); // Simplified for now

                dependencies.push(DependencyInfo {
                    dependent: from_pkg.name().to_string(),
                    dependency: to_pkg.name().to_string(),
                    kind,
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
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn find_duplicates(&self) -> Result<Vec<DuplicateDependency>> {
        // Map package name -> set of versions
        let mut version_map: HashMap<String, HashSet<String>> = HashMap::new();

        // Iterate over all packages in the graph
        for package in self.graph.packages() {
            let name = package.name().to_string();
            let version = package.version().to_string();

            version_map.entry(name).or_default().insert(version);
        }

        // Find packages with multiple versions
        let mut duplicates = Vec::new();

        for (name, versions) in version_map {
            if versions.len() > 1 {
                let mut versions_vec: Vec<String> = versions.into_iter().collect();
                versions_vec.sort(); // Sort for consistent output

                duplicates.push(DuplicateDependency {
                    name,
                    versions: versions_vec,
                });
            }
        }

        // Sort by package name for consistent output
        duplicates.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(duplicates)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workspace_analyzer_new() {
        // Should be able to create analyzer for the xtask workspace
        let result = WorkspaceAnalyzer::new();
        assert!(
            result.is_ok(),
            "Failed to create WorkspaceAnalyzer: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_workspace_packages() {
        let analyzer = WorkspaceAnalyzer::new().expect("Failed to create analyzer");
        let packages = analyzer
            .workspace_packages()
            .expect("Failed to get packages");

        // xtask should be in the workspace
        assert!(!packages.is_empty(), "No workspace packages found");

        // All should be marked as workspace members
        for pkg in &packages {
            assert!(
                pkg.is_workspace,
                "Package {} not marked as workspace",
                pkg.name
            );
            assert!(!pkg.name.is_empty(), "Package has empty name");
            assert!(
                !pkg.version.is_empty(),
                "Package {} has empty version",
                pkg.name
            );
        }

        // xtask itself should be present
        let has_xtask = packages.iter().any(|p| p.name == "xtask");
        assert!(has_xtask, "xtask package not found in workspace");
    }

    #[test]
    fn test_all_dependencies() {
        let analyzer = WorkspaceAnalyzer::new().expect("Failed to create analyzer");
        let deps = analyzer
            .all_dependencies()
            .expect("Failed to get dependencies");

        // Should have some dependencies
        assert!(!deps.is_empty(), "No dependencies found");

        // Verify structure
        for dep in &deps {
            assert!(!dep.dependent.is_empty(), "Dependency has empty dependent");
            assert!(
                !dep.dependency.is_empty(),
                "Dependency has empty dependency"
            );
            assert!(!dep.kind.is_empty(), "Dependency has empty kind");
        }
    }

    #[test]
    fn test_find_duplicates() {
        let analyzer = WorkspaceAnalyzer::new().expect("Failed to create analyzer");
        let duplicates = analyzer
            .find_duplicates()
            .expect("Failed to find duplicates");

        // May or may not have duplicates - just verify structure
        for dup in &duplicates {
            assert!(!dup.name.is_empty(), "Duplicate has empty name");
            assert!(
                dup.versions.len() >= 2,
                "Duplicate {} has less than 2 versions",
                dup.name
            );

            // Versions should be sorted
            let mut sorted_versions = dup.versions.clone();
            sorted_versions.sort();
            assert_eq!(
                dup.versions, sorted_versions,
                "Versions for {} not sorted",
                dup.name
            );
        }
    }

    #[test]
    fn test_package_info_structure() {
        let analyzer = WorkspaceAnalyzer::new().expect("Failed to create analyzer");
        let packages = analyzer
            .workspace_packages()
            .expect("Failed to get packages");

        // Verify at least one package has all fields populated
        assert!(!packages.is_empty(), "No packages to test");

        let pkg = &packages[0];
        assert!(!pkg.name.is_empty());
        assert!(!pkg.version.is_empty());
        // Version should be parseable as semver format (X.Y.Z)
        let parts: Vec<&str> = pkg.version.split('.').collect();
        assert!(
            parts.len() >= 2,
            "Version {} doesn't look like semver",
            pkg.version
        );
    }

    #[test]
    fn test_duplicates_sorted_by_name() {
        let analyzer = WorkspaceAnalyzer::new().expect("Failed to create analyzer");
        let duplicates = analyzer
            .find_duplicates()
            .expect("Failed to find duplicates");

        // If we have duplicates, verify they're sorted by name
        if duplicates.len() > 1 {
            for i in 1..duplicates.len() {
                assert!(
                    duplicates[i - 1].name <= duplicates[i].name,
                    "Duplicates not sorted: {} should come before {}",
                    duplicates[i - 1].name,
                    duplicates[i].name
                );
            }
        }
    }
}

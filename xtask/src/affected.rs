//! Smart test selection based on changed files.
//!
//! Analyzes git diff and workspace dependency graph to determine which packages
//! are affected by current changes, then generates a nextest filter expression.

use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};
use std::process::Command;

/// Get the list of packages affected by current git changes.
pub fn affected_packages() -> Result<Vec<String>> {
    // Get changed files
    let changed = changed_files()?;

    if changed.is_empty() {
        return Ok(vec![]);
    }

    // Map changed files to packages
    let changed_pkgs = files_to_packages(&changed)?;

    if changed_pkgs.is_empty() {
        return Ok(vec![]);
    }

    // Build dependency graph (reverse: package -> packages that depend on it)
    let graph = build_reverse_dependency_graph()?;

    // Compute transitive dependents
    let affected = transitive_dependents(&changed_pkgs, &graph);

    Ok(affected.into_iter().collect())
}

/// Build a nextest filter expression for the affected packages.
pub fn build_nextest_filter(packages: &[String]) -> String {
    if packages.is_empty() {
        return String::new();
    }

    packages
        .iter()
        .map(|p| format!("package({p})"))
        .collect::<Vec<_>>()
        .join(" | ")
}

/// Get list of changed files from git.
fn changed_files() -> Result<Vec<String>> {
    // Get both staged and unstaged changes
    let output = Command::new("git")
        .args(["diff", "--name-only", "HEAD"])
        .output()
        .context("failed to run git diff")?;

    if !output.status.success() {
        // Fall back to uncommitted changes only
        let output = Command::new("git")
            .args(["diff", "--name-only"])
            .output()
            .context("failed to run git diff")?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        return Ok(stdout
            .lines()
            .map(std::string::ToString::to_string)
            .collect());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut files: Vec<String> = stdout
        .lines()
        .map(std::string::ToString::to_string)
        .collect();

    // Also include untracked files in crate/ directory
    let untracked = Command::new("git")
        .args(["ls-files", "--others", "--exclude-standard", "crate/"])
        .output()
        .ok();

    if let Some(out) = untracked {
        let stdout = String::from_utf8_lossy(&out.stdout);
        files.extend(stdout.lines().map(std::string::ToString::to_string));
    }

    Ok(files)
}

/// Map file paths to their containing packages.
fn files_to_packages(files: &[String]) -> Result<HashSet<String>> {
    let mut packages = HashSet::new();

    for file in files {
        if let Some(pkg) = path_to_package(file) {
            packages.insert(pkg);
        }
    }

    Ok(packages)
}

/// Map a file path to its package name.
fn path_to_package(path: &str) -> Option<String> {
    // Match crate path patterns:
    // crate/lib/<name>/...  -> sinex-<name> (with hyphens)
    // crate/core/<name>/... -> sinex-<name>
    // crate/nodes/<name>/... -> sinex-<name>
    // crate/tools/<name>/... -> <name>

    let parts: Vec<&str> = path.split('/').collect();

    if parts.len() < 3 || parts[0] != "crate" {
        return None;
    }

    let category = parts[1];
    let name = parts[2];

    // Convert underscores to hyphens in package name
    let pkg_name = name.replace('_', "-");

    match category {
        "lib" | "core" | "nodes" => Some(pkg_name),
        "tools" => Some(pkg_name),
        _ => None,
    }
}

/// Build reverse dependency graph: package -> packages that depend on it.
fn build_reverse_dependency_graph() -> Result<HashMap<String, HashSet<String>>> {
    let mut reverse_deps: HashMap<String, HashSet<String>> = HashMap::new();

    // Read workspace members and their dependencies
    let output = Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .output()
        .context("failed to run cargo metadata")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("cargo metadata failed: {stderr}");
    }

    let metadata: serde_json::Value =
        serde_json::from_slice(&output.stdout).context("failed to parse cargo metadata")?;

    // Get workspace packages
    let packages = metadata["packages"]
        .as_array()
        .context("no packages in metadata")?;

    // Build forward dependency map first
    let mut forward_deps: HashMap<String, HashSet<String>> = HashMap::new();

    for pkg in packages {
        let name = pkg["name"].as_str().unwrap_or_default().to_string();
        let deps = pkg["dependencies"]
            .as_array()
            .map(|deps| {
                deps.iter()
                    .filter_map(|d| d["name"].as_str().map(std::string::ToString::to_string))
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();

        forward_deps.insert(name, deps);
    }

    // Build reverse dependency map
    for (pkg, deps) in &forward_deps {
        for dep in deps {
            reverse_deps
                .entry(dep.clone())
                .or_default()
                .insert(pkg.clone());
        }
    }

    Ok(reverse_deps)
}

/// Compute transitive dependents of the given packages.
fn transitive_dependents(
    changed: &HashSet<String>,
    reverse_deps: &HashMap<String, HashSet<String>>,
) -> HashSet<String> {
    let mut affected = changed.clone();
    let mut to_process: Vec<String> = changed.iter().cloned().collect();

    while let Some(pkg) = to_process.pop() {
        if let Some(dependents) = reverse_deps.get(&pkg) {
            for dep in dependents {
                if affected.insert(dep.clone()) {
                    to_process.push(dep.clone());
                }
            }
        }
    }

    affected
}

/// Get a summary of affected packages for display.
pub fn affected_summary(packages: &[String]) -> String {
    if packages.is_empty() {
        return "No packages affected by current changes".to_string();
    }

    let mut summary = format!("{} packages affected:\n", packages.len());
    for pkg in packages {
        summary.push_str(&format!("  - {pkg}\n"));
    }
    summary
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_to_package() {
        assert_eq!(
            path_to_package("crate/lib/sinex-db/src/lib.rs"),
            Some("sinex-db".to_string())
        );
        assert_eq!(
            path_to_package("crate/core/sinex-gateway/src/main.rs"),
            Some("sinex-gateway".to_string())
        );
        assert_eq!(
            path_to_package("crate/nodes/sinex-fs-ingestor/src/lib.rs"),
            Some("sinex-fs-ingestor".to_string())
        );
        assert_eq!(path_to_package("docs/README.md"), None);
        assert_eq!(path_to_package("Cargo.toml"), None);
    }

    #[test]
    fn test_build_nextest_filter() {
        let packages = vec!["sinex-db".to_string(), "sinex-gateway".to_string()];
        let filter = build_nextest_filter(&packages);
        assert!(filter.contains("package(sinex-db)"));
        assert!(filter.contains("package(sinex-gateway)"));
    }

    #[test]
    fn test_transitive_dependents() {
        let mut reverse_deps = HashMap::new();
        reverse_deps.insert(
            "a".to_string(),
            HashSet::from(["b".to_string(), "c".to_string()]),
        );
        reverse_deps.insert("b".to_string(), HashSet::from(["d".to_string()]));

        let changed = HashSet::from(["a".to_string()]);
        let affected = transitive_dependents(&changed, &reverse_deps);

        assert!(affected.contains("a"));
        assert!(affected.contains("b"));
        assert!(affected.contains("c"));
        assert!(affected.contains("d"));
    }
}

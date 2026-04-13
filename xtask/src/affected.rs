//! Smart test selection based on changed files.
//!
//! Analyzes git diff and workspace dependency graph to determine which packages
//! are affected by current changes, then generates a nextest filter expression.

use crate::process::ProcessBuilder;
use color_eyre::eyre::{ContextCompat, Result, WrapErr, eyre};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::process::Command;
use std::sync::OnceLock;

/// Cached cargo metadata to avoid running the command multiple times.
#[derive(Clone, Debug)]
struct WorkspaceMetadata {
    packages: Vec<String>,
    reverse_deps: HashMap<String, HashSet<String>>,
}

/// Process-lifetime cache for workspace metadata (R3 accepted).
///
/// `cargo metadata --no-deps` is deterministic within a single process invocation
/// — Cargo.toml/Cargo.lock don't change while xtask runs. The OnceLock is not
/// invalidated during the process lifetime, which is intentional: xtask is a
/// short-lived CLI tool that exits after each command.
static WORKSPACE_METADATA: OnceLock<WorkspaceMetadata> = OnceLock::new();

impl WorkspaceMetadata {
    fn parse_metadata(metadata: &serde_json::Value) -> Result<Self> {
        let packages_array = metadata["packages"]
            .as_array()
            .context("no packages in metadata")?;

        let mut packages = Vec::with_capacity(packages_array.len());
        let mut forward_deps: HashMap<String, HashSet<String>> = HashMap::new();

        for (package_index, pkg) in packages_array.iter().enumerate() {
            let name = pkg["name"].as_str().with_context(|| {
                format!("cargo metadata package[{package_index}] is missing a string name")
            })?;
            let deps = pkg["dependencies"].as_array().with_context(|| {
                format!("cargo metadata package[{name}] is missing dependencies array")
            })?;
            let deps = deps
                .iter()
                .enumerate()
                .map(|(dependency_index, dep)| {
                    dep["name"].as_str().map(str::to_owned).with_context(|| {
                        format!(
                            "cargo metadata package[{name}] dependency[{dependency_index}] is missing a string name"
                        )
                    })
                })
                .collect::<Result<HashSet<_>>>()?;

            let name = name.to_owned();
            packages.push(name.clone());
            forward_deps.insert(name, deps);
        }

        // Build reverse dependency map
        let mut reverse_deps: HashMap<String, HashSet<String>> = HashMap::new();
        for (pkg, deps) in &forward_deps {
            for dep in deps {
                reverse_deps
                    .entry(dep.clone())
                    .or_default()
                    .insert(pkg.clone());
            }
        }

        Ok(Self {
            packages,
            reverse_deps,
        })
    }

    /// Load workspace metadata from cargo (single call).
    fn load() -> Result<Self> {
        let output = ProcessBuilder::cargo()
            .args(["metadata", "--format-version", "1", "--no-deps"])
            .with_description("cargo metadata")
            .run()
            .context("failed to run cargo metadata")?;

        let metadata: serde_json::Value =
            serde_json::from_str(&output.stdout).context("failed to parse cargo metadata")?;
        Self::parse_metadata(&metadata)
    }
}

/// Get the list of packages affected by current git changes.
pub fn affected_packages() -> Result<Vec<String>> {
    // Get changed files
    let changed = changed_files()?;

    if changed.is_empty() {
        return Ok(vec![]);
    }

    // Load workspace metadata once (cached for process lifetime)
    let metadata = if let Some(m) = WORKSPACE_METADATA.get() {
        m
    } else {
        let m = WorkspaceMetadata::load()?;
        WORKSPACE_METADATA.get_or_init(|| m)
    };

    // Check for workspace-wide changes that affect everything
    let workspace_wide = changed
        .iter()
        .any(|f| f == "Cargo.toml" || f == "Cargo.lock" || f.starts_with(".config/"));

    if workspace_wide {
        return Ok(metadata.packages.clone());
    }

    // Map changed files to packages
    let changed_pkgs = files_to_packages(&changed);

    if changed_pkgs.is_empty() {
        return Ok(vec![]);
    }

    // Compute transitive dependents using pre-loaded reverse dependency graph
    let affected = transitive_dependents(&changed_pkgs, &metadata.reverse_deps);

    Ok(affected.into_iter().collect())
}

/// Build a nextest filter expression for the affected packages.
pub fn build_nextest_filter(packages: &[String]) -> String {
    if packages.is_empty() {
        return String::new();
    }

    packages
        .iter()
        .filter(|package| is_valid_nextest_package_name(package))
        .map(|p| format!("package({p})"))
        .collect::<Vec<_>>()
        .join(" | ")
}

fn is_valid_nextest_package_name(package: &str) -> bool {
    !package.is_empty() && !package.starts_with('.')
}

/// Get list of changed files from git.
fn changed_files() -> Result<Vec<String>> {
    let repo_root =
        std::env::current_dir().context("failed to determine current working directory")?;
    changed_files_in(&repo_root)
}

fn changed_files_in(repo_root: &Path) -> Result<Vec<String>> {
    // Get both staged and unstaged changes
    let output = Command::new("git")
        .args(["diff", "--name-only", "HEAD"])
        .current_dir(repo_root)
        .output()
        .context("failed to run git diff --name-only HEAD")?;

    if !output.status.success() {
        // X12: `git diff --name-only HEAD` fails on initial commit (no HEAD yet).
        // Fall back to `git diff --name-only` (unstaged changes only). If that
        // also fails, surface the failure instead of silently suppressing scope drift.
        let fallback = Command::new("git")
            .args(["diff", "--name-only"])
            .current_dir(repo_root)
            .output()
            .context("failed to run git diff --name-only")?;

        if !fallback.status.success() {
            return Err(eyre!(
                "{}; fallback {}",
                format_git_failure("git diff --name-only HEAD", &output),
                format_git_failure("git diff --name-only", &fallback),
            ));
        }

        let stdout = String::from_utf8_lossy(&fallback.stdout);
        let mut files: Vec<String> = stdout
            .lines()
            .map(std::string::ToString::to_string)
            .collect();
        files.extend(untracked_files_in(repo_root)?);
        return Ok(files);
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut files: Vec<String> = stdout
        .lines()
        .map(std::string::ToString::to_string)
        .collect();

    // Also include untracked files in crate/, xtask/, and tests/ directories
    files.extend(untracked_files_in(repo_root)?);

    Ok(files)
}

fn untracked_files_in(repo_root: &Path) -> Result<Vec<String>> {
    let output = Command::new("git")
        .args([
            "ls-files",
            "--others",
            "--exclude-standard",
            "crate/",
            "xtask/",
            "tests/",
            "nixos/",
            ".config/",
            "Cargo.toml",
            "Cargo.lock",
            "flake.nix",
            "flake.lock",
        ])
        .current_dir(repo_root)
        .output()
        .context("failed to run git ls-files --others --exclude-standard")?;

    if !output.status.success() {
        return Err(eyre!(format_git_failure(
            "git ls-files --others --exclude-standard crate/ xtask/ tests/",
            &output,
        )));
    }

    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(std::string::ToString::to_string)
        .collect())
}

fn format_git_failure(command: &str, output: &std::process::Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stderr = stderr.trim();

    if stderr.is_empty() {
        format!("{command} failed with status {}", output.status)
    } else {
        format!("{command} failed with status {}: {stderr}", output.status)
    }
}

/// Map file paths to their containing packages.
fn files_to_packages(files: &[String]) -> HashSet<String> {
    let mut packages = HashSet::new();

    for file in files {
        if let Some(pkg) = path_to_package(file) {
            packages.insert(pkg);
        }
    }

    packages
}

/// Map a file path to its package name.
fn path_to_package(path: &str) -> Option<String> {
    let parts: Vec<&str> = path.split('/').collect();

    // crate/{lib,core,nodes,tools,cli}/<name>/... -> package name (with hyphens)
    if parts.len() >= 3 && parts[0] == "crate" {
        let category = parts[1];
        let name = parts[2];
        if name.starts_with('.') {
            return None;
        }
        let pkg_name = name.replace('_', "-");

        return match category {
            "lib" | "core" | "nodes" | "tools" => Some(pkg_name),
            // crate/cli/ contains the sinexctl binary
            "cli" => Some("sinexctl".to_string()),
            _ => None,
        };
    }

    // xtask/ changes affect xtask itself
    if parts.first() == Some(&"xtask") {
        return Some("xtask".to_string());
    }

    // tests/e2e/ changes affect the e2e test package
    if parts.len() >= 2 && parts[0] == "tests" && parts[1] == "e2e" {
        return Some("sinex-e2e-tests".to_string());
    }

    // Workspace-level files (Cargo.toml, Cargo.lock, .config/) are handled
    // upstream in affected_packages() as workspace-wide changes.
    None
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

/// Returns true when any `nixos/**/*.nix` or `flake.nix`/`flake.lock` file is dirty.
///
/// Used by `xtask check --full` to suggest running the NixOS compatibility gate:
///   `xtask test vm --category smoke`
pub fn nixos_modules_dirty() -> Result<bool> {
    let repo_root =
        std::env::current_dir().context("failed to determine current working directory")?;
    nixos_modules_dirty_in(&repo_root)
}

fn nixos_modules_dirty_in(repo_root: &Path) -> Result<bool> {
    Ok(changed_files_in(repo_root)?.iter().any(|f| {
        (f.starts_with("nixos/") && f.ends_with(".nix")) || f == "flake.nix" || f == "flake.lock"
    }))
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
    use crate::sandbox::{TestResult, sinex_test};
    use std::path::Path;
    use tempfile::tempdir;

    fn run_git(args: &[&str], cwd: &Path) -> TestResult<()> {
        let output = Command::new("git").args(args).current_dir(cwd).output()?;
        if !output.status.success() {
            return Err(color_eyre::eyre::eyre!(
                "git {:?} failed: {}",
                args,
                String::from_utf8_lossy(&output.stderr).trim()
            )
            .into());
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_path_to_package() -> TestResult<()> {
        // Standard crate paths
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

        // CLI crate
        assert_eq!(
            path_to_package("crate/cli/src/main.rs"),
            Some("sinexctl".to_string())
        );
        assert_eq!(
            path_to_package("crate/cli/Cargo.toml"),
            Some("sinexctl".to_string())
        );

        // xtask
        assert_eq!(
            path_to_package("xtask/src/lib.rs"),
            Some("xtask".to_string())
        );
        assert_eq!(
            path_to_package("xtask/Cargo.toml"),
            Some("xtask".to_string())
        );

        // e2e tests
        assert_eq!(
            path_to_package("tests/e2e/tests/some_test.rs"),
            Some("sinex-e2e-tests".to_string())
        );
        assert_eq!(
            path_to_package("tests/e2e/Cargo.toml"),
            Some("sinex-e2e-tests".to_string())
        );

        // Non-package paths return None (workspace-level handled upstream)
        assert_eq!(path_to_package("README.md"), None);
        assert_eq!(path_to_package("Cargo.toml"), None);
        assert_eq!(path_to_package("Cargo.lock"), None);
        assert_eq!(path_to_package(".config/nextest.toml"), None);
        assert_eq!(
            path_to_package("crate/lib/.sinex/test-artifacts/report.json"),
            None
        );
        assert_eq!(
            path_to_package("crate/cli/.sinex/test-artifacts/report.json"),
            None
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_build_nextest_filter() -> TestResult<()> {
        let packages = vec!["sinex-db".to_string(), "sinex-gateway".to_string()];
        let filter = build_nextest_filter(&packages);
        assert!(filter.contains("package(sinex-db)"));
        assert!(filter.contains("package(sinex-gateway)"));
        Ok(())
    }

    #[sinex_test]
    async fn test_transitive_dependents() -> TestResult<()> {
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
        Ok(())
    }

    #[sinex_test]
    async fn test_files_to_packages_maps_multiple() -> TestResult<()> {
        let files = vec![
            "crate/lib/sinex-db/src/lib.rs".into(),
            "crate/core/sinex-gateway/src/main.rs".into(),
            "xtask/src/affected.rs".into(),
        ];
        let pkgs = files_to_packages(&files);
        assert!(pkgs.contains("sinex-db"));
        assert!(pkgs.contains("sinex-gateway"));
        assert!(pkgs.contains("xtask"));
        assert_eq!(pkgs.len(), 3);
        Ok(())
    }

    #[sinex_test]
    async fn test_files_to_packages_deduplicates() -> TestResult<()> {
        let files = vec![
            "crate/lib/sinex-db/src/lib.rs".into(),
            "crate/lib/sinex-db/src/pool.rs".into(),
            "crate/lib/sinex-db/Cargo.toml".into(),
        ];
        let pkgs = files_to_packages(&files);
        assert_eq!(pkgs.len(), 1);
        assert!(pkgs.contains("sinex-db"));
        Ok(())
    }

    #[sinex_test]
    async fn test_files_to_packages_ignores_non_package_files() -> TestResult<()> {
        let files = vec![
            "README.md".into(),
            ".github/workflows/ci.yml".into(),
            "README.md".into(),
        ];
        let pkgs = files_to_packages(&files);
        assert!(pkgs.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_build_nextest_filter_empty() -> TestResult<()> {
        let filter = build_nextest_filter(&[]);
        assert!(filter.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_build_nextest_filter_single_package() -> TestResult<()> {
        let filter = build_nextest_filter(&["sinex-db".into()]);
        assert_eq!(filter, "package(sinex-db)");
        Ok(())
    }

    #[sinex_test]
    async fn test_build_nextest_filter_ignores_hidden_checkout_state() -> TestResult<()> {
        let filter = build_nextest_filter(&["sinex-db".into(), ".sinex".into()]);
        assert_eq!(filter, "package(sinex-db)");
        Ok(())
    }

    #[sinex_test]
    async fn test_affected_summary_empty() -> TestResult<()> {
        let summary = affected_summary(&[]);
        assert!(summary.contains("No packages affected"));
        Ok(())
    }

    #[sinex_test]
    async fn test_affected_summary_with_packages() -> TestResult<()> {
        let pkgs = vec!["sinex-db".into(), "xtask".into()];
        let summary = affected_summary(&pkgs);
        assert!(summary.contains("2 packages affected"));
        assert!(summary.contains("sinex-db"));
        assert!(summary.contains("xtask"));
        Ok(())
    }

    #[sinex_test]
    async fn test_transitive_dependents_no_deps() -> TestResult<()> {
        let reverse_deps = HashMap::new();
        let changed = HashSet::from(["orphan".to_string()]);
        let affected = transitive_dependents(&changed, &reverse_deps);
        assert_eq!(affected.len(), 1);
        assert!(affected.contains("orphan"));
        Ok(())
    }

    #[sinex_test]
    async fn test_transitive_dependents_diamond() -> TestResult<()> {
        // Diamond: A depends on B and C, both B and C depend on D
        //   A
        //  / \
        // B   C
        //  \ /
        //   D
        let mut reverse_deps = HashMap::new();
        reverse_deps.insert(
            "d".to_string(),
            HashSet::from(["b".to_string(), "c".to_string()]),
        );
        reverse_deps.insert("b".to_string(), HashSet::from(["a".to_string()]));
        reverse_deps.insert("c".to_string(), HashSet::from(["a".to_string()]));

        let changed = HashSet::from(["d".to_string()]);
        let affected = transitive_dependents(&changed, &reverse_deps);
        // All four should be affected
        assert_eq!(affected.len(), 4);
        assert!(affected.contains("a"));
        assert!(affected.contains("b"));
        assert!(affected.contains("c"));
        assert!(affected.contains("d"));
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_workspace_metadata_rejects_missing_package_name() -> TestResult<()> {
        let metadata = serde_json::json!({
            "packages": [
                {
                    "dependencies": []
                }
            ]
        });

        let error = WorkspaceMetadata::parse_metadata(&metadata)
            .expect_err("missing package name should surface");
        assert!(format!("{error:#}").contains("package[0] is missing a string name"));
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_workspace_metadata_rejects_missing_dependency_name() -> TestResult<()> {
        let metadata = serde_json::json!({
            "packages": [
                {
                    "name": "xtask",
                    "dependencies": [
                        {}
                    ]
                }
            ]
        });

        let error = WorkspaceMetadata::parse_metadata(&metadata)
            .expect_err("missing dependency name should surface");
        assert!(
            format!("{error:#}").contains("package[xtask] dependency[0] is missing a string name")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_workspace_metadata_builds_reverse_deps() -> TestResult<()> {
        let metadata = serde_json::json!({
            "packages": [
                {
                    "name": "xtask",
                    "dependencies": [
                        { "name": "sinex-primitives" }
                    ]
                },
                {
                    "name": "sinex-primitives",
                    "dependencies": []
                }
            ]
        });

        let parsed = WorkspaceMetadata::parse_metadata(&metadata)?;
        assert_eq!(
            parsed.packages,
            vec!["xtask".to_string(), "sinex-primitives".to_string()]
        );
        assert_eq!(
            parsed.reverse_deps.get("sinex-primitives"),
            Some(&HashSet::from(["xtask".to_string()]))
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_path_to_package_underscore_to_hyphen() -> TestResult<()> {
        // Package directories with underscores should map to hyphenated package names
        assert_eq!(
            path_to_package("crate/lib/sinex_primitives/src/lib.rs"),
            Some("sinex-primitives".to_string())
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_changed_files_includes_untracked_workspace_files() -> TestResult<()> {
        let repo = tempdir()?;
        run_git(&["init", "-q"], repo.path())?;
        run_git(&["config", "user.name", "Sinex Test"], repo.path())?;
        run_git(&["config", "user.email", "sinex@example.test"], repo.path())?;
        std::fs::write(repo.path().join("README.md"), "hello\n")?;
        run_git(&["add", "README.md"], repo.path())?;
        run_git(&["commit", "-qm", "init"], repo.path())?;

        std::fs::create_dir_all(repo.path().join("xtask/src"))?;
        std::fs::write(repo.path().join("xtask/src/new.rs"), "// untracked\n")?;

        let changed = changed_files_in(repo.path())?;
        assert!(changed.contains(&"xtask/src/new.rs".to_string()));
        Ok(())
    }

    #[sinex_test]
    async fn test_changed_files_includes_untracked_workspace_scope_files() -> TestResult<()> {
        let repo = tempdir()?;
        run_git(&["init", "-q"], repo.path())?;
        run_git(&["config", "user.name", "Sinex Test"], repo.path())?;
        run_git(&["config", "user.email", "sinex@example.test"], repo.path())?;
        std::fs::write(repo.path().join("README.md"), "hello\n")?;
        run_git(&["add", "README.md"], repo.path())?;
        run_git(&["commit", "-qm", "init"], repo.path())?;

        std::fs::create_dir_all(repo.path().join(".config"))?;
        std::fs::write(
            repo.path().join(".config/nextest.toml"),
            "[profile.default]\n",
        )?;
        std::fs::write(repo.path().join("flake.nix"), "{ }\n")?;

        let changed = changed_files_in(repo.path())?;
        assert!(changed.contains(&".config/nextest.toml".to_string()));
        assert!(changed.contains(&"flake.nix".to_string()));
        Ok(())
    }

    #[sinex_test]
    async fn test_changed_files_surfaces_git_failures() -> TestResult<()> {
        let dir = tempfile::Builder::new()
            .prefix("xtask-nongit-")
            .tempdir_in("/tmp")?;

        let error = changed_files_in(dir.path()).expect_err("non-git directory should surface");
        assert!(format!("{error:#}").contains("git diff --name-only HEAD failed"));
        Ok(())
    }

    #[sinex_test]
    async fn test_nixos_modules_dirty_detects_untracked_nixos_files() -> TestResult<()> {
        let repo = tempdir()?;
        run_git(&["init", "-q"], repo.path())?;
        run_git(&["config", "user.name", "Sinex Test"], repo.path())?;
        run_git(&["config", "user.email", "sinex@example.test"], repo.path())?;
        std::fs::write(repo.path().join("README.md"), "hello\n")?;
        run_git(&["add", "README.md"], repo.path())?;
        run_git(&["commit", "-qm", "init"], repo.path())?;

        std::fs::create_dir_all(repo.path().join("nixos/modules"))?;
        std::fs::write(repo.path().join("nixos/modules/example.nix"), "{}\n")?;

        assert!(nixos_modules_dirty_in(repo.path())?);
        Ok(())
    }
}

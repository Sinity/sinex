//! Smart test selection based on changed files.
//!
//! Analyzes git diff and workspace dependency graph to determine which packages
//! are affected by current changes, then generates a nextest filter expression.

use crate::process::ProcessBuilder;
use color_eyre::eyre::{ContextCompat, Result, WrapErr, eyre};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::OnceLock;
use walkdir::WalkDir;

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

/// Infer package scope from a simple `nextest -E` test-name filter.
///
/// Supports expressions composed only of `test(name)` terms plus boolean
/// operators/parentheses. More complex filters deliberately return an empty
/// package set so the caller can fall back to workspace scope.
pub fn infer_packages_for_test_filter(filter: &str) -> Result<Vec<String>> {
    let repo_root = crate::config::workspace_root();
    infer_packages_for_test_filter_in(&repo_root, filter)
}

fn infer_packages_for_test_filter_in(repo_root: &Path, filter: &str) -> Result<Vec<String>> {
    let Some(test_names) = extract_simple_test_name_terms(filter) else {
        return Ok(Vec::new());
    };

    let mut packages = HashSet::new();
    for relative_path in candidate_rust_paths(repo_root)? {
        let full_path = repo_root.join(&relative_path);
        let content = fs::read_to_string(&full_path)
            .wrap_err_with(|| format!("failed to read {}", full_path.display()))?;

        if test_names
            .iter()
            .any(|test_name| content_mentions_test_name(&content, test_name))
            && let Some(package) = package_for_path(&relative_path)
        {
            packages.insert(package);
        }
    }

    let mut packages: Vec<String> = packages.into_iter().collect();
    packages.sort();
    Ok(packages)
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
        if let Some(pkg) = package_for_path(file) {
            packages.insert(pkg);
        }
    }

    packages
}

/// Map a file path to its package name.
pub(crate) fn package_for_path(path: &str) -> Option<String> {
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

    if parts.len() >= 2 && parts[0] == "tests" {
        return match parts[1] {
            "e2e" => Some("sinex-e2e-tests".to_string()),
            "workspace" => Some("sinex-workspace-tests".to_string()),
            _ => None,
        };
    }

    // Workspace-level files (Cargo.toml, Cargo.lock, .config/) are handled
    // upstream in affected_packages() as workspace-wide changes.
    None
}

fn extract_simple_test_name_terms(filter: &str) -> Option<Vec<String>> {
    let mut names = Vec::new();
    let mut stripped = String::with_capacity(filter.len());
    let mut cursor = 0usize;

    while let Some(relative_start) = filter[cursor..].find("test(") {
        let start = cursor + relative_start;
        stripped.push_str(&filter[cursor..start]);

        let name_start = start + "test(".len();
        let relative_end = filter[name_start..].find(')')?;
        let end = name_start + relative_end;
        let name = &filter[name_start..end];

        if name.is_empty() || !name.chars().all(is_simple_test_name_char) {
            return None;
        }

        names.push(name.to_string());
        stripped.push(' ');
        cursor = end + 1;
    }

    if names.is_empty() {
        return None;
    }

    stripped.push_str(&filter[cursor..]);
    if stripped
        .chars()
        .all(|ch| ch.is_whitespace() || matches!(ch, '(' | ')' | '|' | '&' | '!'))
    {
        Some(names)
    } else {
        None
    }
}

fn is_simple_test_name_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | ':' | '$' | '-')
}

fn candidate_rust_paths(repo_root: &Path) -> Result<Vec<String>> {
    let mut paths = Vec::new();
    for root in ["crate", "tests", "xtask"] {
        let root_path = repo_root.join(root);
        if !root_path.exists() {
            continue;
        }

        for entry in WalkDir::new(&root_path) {
            let entry = entry?;
            if !entry.file_type().is_file() {
                continue;
            }
            if entry.path().extension().and_then(|ext| ext.to_str()) != Some("rs") {
                continue;
            }

            let relative = entry
                .path()
                .strip_prefix(repo_root)
                .wrap_err_with(|| format!("failed to relativize {}", entry.path().display()))?;
            paths.push(relative.to_string_lossy().replace('\\', "/"));
        }
    }

    Ok(paths)
}

fn content_mentions_test_name(content: &str, test_name: &str) -> bool {
    signature_mentions_test_name(content, &format!("fn {test_name}"))
        || signature_mentions_test_name(content, &format!("async fn {test_name}"))
}

fn signature_mentions_test_name(content: &str, needle: &str) -> bool {
    let mut offset = 0usize;
    while let Some(relative_index) = content[offset..].find(needle) {
        let index = offset + relative_index;
        let after = content[index + needle.len()..].chars().next();
        if after.is_none_or(|ch| !(ch.is_ascii_alphanumeric() || ch == '_')) {
            return true;
        }
        offset = index + needle.len();
    }
    false
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
            package_for_path("crate/lib/sinex-db/src/lib.rs"),
            Some("sinex-db".to_string())
        );
        assert_eq!(
            package_for_path("crate/core/sinex-gateway/src/main.rs"),
            Some("sinex-gateway".to_string())
        );
        assert_eq!(
            package_for_path("crate/nodes/sinex-fs-ingestor/src/lib.rs"),
            Some("sinex-fs-ingestor".to_string())
        );

        // CLI crate
        assert_eq!(
            package_for_path("crate/cli/src/main.rs"),
            Some("sinexctl".to_string())
        );
        assert_eq!(
            package_for_path("crate/cli/Cargo.toml"),
            Some("sinexctl".to_string())
        );

        // xtask
        assert_eq!(
            package_for_path("xtask/src/lib.rs"),
            Some("xtask".to_string())
        );
        assert_eq!(
            package_for_path("xtask/Cargo.toml"),
            Some("xtask".to_string())
        );

        // e2e tests
        assert_eq!(
            package_for_path("tests/e2e/tests/some_test.rs"),
            Some("sinex-e2e-tests".to_string())
        );
        assert_eq!(
            package_for_path("tests/e2e/Cargo.toml"),
            Some("sinex-e2e-tests".to_string())
        );

        // Non-package paths return None (workspace-level handled upstream)
        assert_eq!(package_for_path("README.md"), None);
        assert_eq!(package_for_path("Cargo.toml"), None);
        assert_eq!(package_for_path("Cargo.lock"), None);
        assert_eq!(package_for_path(".config/nextest.toml"), None);
        assert_eq!(
            package_for_path("crate/lib/.sinex/test-artifacts/report.json"),
            None
        );
        assert_eq!(
            package_for_path("crate/cli/.sinex/test-artifacts/report.json"),
            None
        );
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
    async fn test_extract_simple_test_name_terms_accepts_boolean_test_names() -> TestResult<()> {
        let names = extract_simple_test_name_terms("(test(alpha_case) | test(beta::gamma$delta))")
            .expect("simple boolean test-name filter should parse");
        assert_eq!(names, vec!["alpha_case", "beta::gamma$delta"]);
        Ok(())
    }

    #[sinex_test]
    async fn test_extract_simple_test_name_terms_rejects_complex_filter_predicates()
    -> TestResult<()> {
        assert!(extract_simple_test_name_terms("package(xtask) & test(alpha_case)").is_none());
        assert!(extract_simple_test_name_terms("not test(alpha_case)").is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_infer_packages_for_test_filter_maps_matching_sources() -> TestResult<()> {
        let repo = tempfile::tempdir()?;
        let graceful = repo
            .path()
            .join("tests/e2e/tests/graceful_shutdown_test.rs");
        let xtask_test = repo.path().join("xtask/src/example_test.rs");
        let workspace_test = repo.path().join("tests/workspace/tests/smoke.rs");
        fs::create_dir_all(graceful.parent().expect("graceful parent"))?;
        fs::create_dir_all(xtask_test.parent().expect("xtask parent"))?;
        fs::create_dir_all(workspace_test.parent().expect("workspace parent"))?;
        fs::write(
            &graceful,
            "#[sinex_test]\nasync fn test_concurrent_service_shutdown() {}\n",
        )?;
        fs::write(&xtask_test, "#[sinex_test]\nasync fn test_compile_scope() {}\n")?;
        fs::write(
            &workspace_test,
            "#[sinex_test]\nasync fn workspace_smoke_test() {}\n",
        )?;

        let inferred = infer_packages_for_test_filter_in(
            repo.path(),
            "test(test_concurrent_service_shutdown) | test(workspace_smoke_test) | test(test_compile_scope)",
        )?;
        assert_eq!(
            inferred,
            vec![
                "sinex-e2e-tests".to_string(),
                "sinex-workspace-tests".to_string(),
                "xtask".to_string(),
            ]
        );
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
            package_for_path("crate/lib/sinex_primitives/src/lib.rs"),
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

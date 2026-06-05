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
    forward_deps: HashMap<String, HashSet<String>>,
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
            forward_deps,
            reverse_deps,
        })
    }

    /// Load workspace metadata from cargo (single call).
    fn load() -> Result<Self> {
        Self::load_in(Path::new("."))
    }

    /// Load workspace metadata from cargo in a specific workspace root.
    fn load_in(cwd: &Path) -> Result<Self> {
        let output = ProcessBuilder::cargo()
            .args(["metadata", "--format-version", "1", "--no-deps"])
            .current_dir(cwd)
            .with_description("cargo metadata")
            .run()
            .context("failed to run cargo metadata")?;

        let metadata: serde_json::Value =
            serde_json::from_str(&output.stdout).context("failed to parse cargo metadata")?;
        Self::parse_metadata(&metadata)
    }
}

/// Return the requested workspace packages plus their transitive workspace dependencies.
pub fn package_dependency_closure(packages: &[String]) -> Result<Vec<String>> {
    let metadata = if let Some(m) = WORKSPACE_METADATA.get() {
        m
    } else {
        let m = WorkspaceMetadata::load()?;
        WORKSPACE_METADATA.get_or_init(|| m)
    };
    Ok(package_dependency_closure_from_forward(
        packages,
        &metadata.forward_deps,
    ))
}

/// Return the dependency closure using metadata loaded from a specific workspace root.
pub fn package_dependency_closure_in(cwd: &Path, packages: &[String]) -> Result<Vec<String>> {
    let metadata = WorkspaceMetadata::load_in(cwd)?;
    Ok(package_dependency_closure_from_forward(
        packages,
        &metadata.forward_deps,
    ))
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

/// Infer nextest integration-test binary targets from a simple test-name filter.
///
/// This is intentionally conservative. It only returns targets for tests found
/// in integration-test files (`*/tests/*.rs`). If any matched test lives in an
/// inline/unit-test module, the returned set omits it and the caller should run
/// without adding `--test` unless all intended matches are covered.
pub fn infer_test_binaries_for_test_filter(filter: &str) -> Result<Vec<String>> {
    let repo_root = crate::config::workspace_root();
    infer_test_binaries_for_test_filter_in(&repo_root, filter)
}

/// Infer whether a simple test-name filter targets only library unit tests.
///
/// This complements [`infer_test_binaries_for_test_filter`]. Inline tests in
/// `src/` do not have a nextest `--test` target, but nextest can narrow them
/// with `--lib`; doing so avoids enumerating every integration-test binary in a
/// package for a single library unit test.
pub fn infer_lib_target_for_test_filter(filter: &str) -> Result<bool> {
    let repo_root = crate::config::workspace_root();
    infer_lib_target_for_test_filter_in(&repo_root, filter)
}

/// Count simple `test(name)` terms in a nextest filter.
///
/// Returns `None` for complex filters where xtask deliberately should not infer
/// exact test shape.
pub fn simple_test_name_term_count(filter: &str) -> Option<usize> {
    extract_simple_test_name_terms(filter).map(|terms| terms.len())
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

fn infer_test_binaries_for_test_filter_in(repo_root: &Path, filter: &str) -> Result<Vec<String>> {
    let Some(test_names) = extract_simple_test_name_terms(filter) else {
        return Ok(Vec::new());
    };

    let mut binaries = HashSet::new();
    let mut covered_test_names = HashSet::new();
    for relative_path in candidate_rust_paths(repo_root)? {
        let full_path = repo_root.join(&relative_path);
        let content = fs::read_to_string(&full_path)
            .wrap_err_with(|| format!("failed to read {}", full_path.display()))?;

        let matched_test_names: Vec<&String> = test_names
            .iter()
            .filter(|test_name| content_mentions_test_name(&content, test_name))
            .collect();

        if !matched_test_names.is_empty() {
            let binary = if let Some(binary) = test_binary_for_path(&relative_path) {
                Some(binary)
            } else {
                test_binary_for_nested_integration_module(repo_root, &relative_path)?
            };

            if let Some(binary) = binary {
                covered_test_names.extend(matched_test_names.into_iter().cloned());
                binaries.insert(binary);
            }
        }
    }

    if test_names
        .iter()
        .any(|test_name| !covered_test_names.contains(test_name))
    {
        return Ok(Vec::new());
    }

    let mut binaries: Vec<String> = binaries.into_iter().collect();
    binaries.sort();
    Ok(binaries)
}

fn infer_lib_target_for_test_filter_in(repo_root: &Path, filter: &str) -> Result<bool> {
    let Some(test_names) = extract_simple_test_name_terms(filter) else {
        return Ok(false);
    };

    let mut covered_test_names = HashSet::new();
    let mut non_lib_match = false;

    for relative_path in candidate_rust_paths(repo_root)? {
        let full_path = repo_root.join(&relative_path);
        let content = fs::read_to_string(&full_path)
            .wrap_err_with(|| format!("failed to read {}", full_path.display()))?;

        let matched_test_names: Vec<&String> = test_names
            .iter()
            .filter(|test_name| content_mentions_test_name(&content, test_name))
            .collect();

        if matched_test_names.is_empty() {
            continue;
        }

        if is_library_unit_test_path(&relative_path) {
            covered_test_names.extend(matched_test_names.into_iter().cloned());
        } else {
            non_lib_match = true;
        }
    }

    Ok(!non_lib_match
        && test_names
            .iter()
            .all(|test_name| covered_test_names.contains(test_name)))
}

/// Get list of changed files from git.
pub fn changed_files() -> Result<Vec<String>> {
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

    // Runtime crates live under crate/<package>/... while workspace-member
    // test crates live under tests/<kind>/....
    if parts.len() >= 2 && parts[0] == "crate" {
        let name = parts[1];
        if name.starts_with('.') {
            return None;
        }
        return Some(name.replace('_', "-"));
    }

    if parts.len() >= 2 && parts[0] == "tests" {
        return match parts[1] {
            "e2e" => Some("sinex-e2e-tests".to_string()),
            "workspace" => Some("sinex-workspace-tests".to_string()),
            "vm-suite" => Some("sinex-vm-test-suite".to_string()),
            _ => None,
        };
    }

    // xtask/ changes affect xtask itself
    if parts.first() == Some(&"xtask") {
        return Some("xtask".to_string());
    }

    // Workspace-level files (Cargo.toml, Cargo.lock, .config/) and shared
    // test fixtures are handled upstream as workspace-wide changes rather
    // than mapped to a single package.
    None
}

fn test_binary_for_path(path: &str) -> Option<String> {
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() < 3 || parts.last().copied() == Some("mod.rs") {
        return None;
    }

    let file_name = parts.last()?;
    let stem = file_name.strip_suffix(".rs")?;

    // Workspace integration-test crates: tests/e2e/tests/foo.rs and
    // tests/workspace/tests/foo.rs compile to nextest target `foo`.
    if parts.len() == 4 && parts[0] == "tests" && parts[2] == "tests" {
        return Some(stem.to_string());
    }

    // Crate-local integration tests: crate/<category>/<crate>/tests/foo.rs.
    if parts.len() == 5 && parts[0] == "crate" && parts[3] == "tests" {
        return Some(stem.to_string());
    }

    // xtask integration tests: xtask/tests/foo.rs.
    if parts.len() == 3 && parts[0] == "xtask" && parts[1] == "tests" {
        return Some(stem.to_string());
    }

    None
}

fn test_binary_for_nested_integration_module(
    repo_root: &Path,
    path: &str,
) -> Result<Option<String>> {
    let parts: Vec<&str> = path.split('/').collect();
    let Some(file_name) = parts.last() else {
        return Ok(None);
    };
    if file_name.strip_suffix(".rs").is_none() {
        return Ok(None);
    }

    let Some((root_relative_path, binary, _module_relative_path)) = nested_integration_root(&parts)
    else {
        return Ok(None);
    };

    if !repo_root.join(&root_relative_path).exists() {
        if let Some(aggregator_binary) =
            nested_integration_aggregator(repo_root, &root_relative_path, &binary)?
        {
            return Ok(Some(aggregator_binary));
        }
        return Ok(None);
    }

    Ok(Some(binary))
}

fn nested_integration_aggregator(
    repo_root: &Path,
    root_relative_path: &str,
    module_name: &str,
) -> Result<Option<String>> {
    let Some((tests_dir, root_file)) = root_relative_path.rsplit_once('/') else {
        return Ok(None);
    };
    let Some(root_stem) = root_file.strip_suffix(".rs") else {
        return Ok(None);
    };

    let candidate = format!("{tests_dir}/{root_stem}_tests.rs");
    let candidate_path = repo_root.join(&candidate);
    if !candidate_path.exists() {
        return Ok(None);
    }
    let content = fs::read_to_string(&candidate_path)
        .wrap_err_with(|| format!("failed to read {}", candidate_path.display()))?;
    if content.contains(&format!("mod {module_name};")) {
        Ok(Some(format!("{root_stem}_tests")))
    } else {
        Ok(None)
    }
}

fn nested_integration_root(parts: &[&str]) -> Option<(String, String, String)> {
    // Workspace integration-test crates: tests/e2e/tests/foo/bar.rs belongs
    // to nextest target `foo` when tests/e2e/tests/foo.rs includes it.
    if parts.len() > 4
        && parts.first().copied() == Some("tests")
        && parts.get(2).copied() == Some("tests")
    {
        let root = *parts.get(3)?;
        return Some((
            format!("{}/{}/tests/{root}.rs", parts[0], parts[1]),
            root.to_string(),
            parts[3..].join("/"),
        ));
    }

    // Crate-local integration tests: crate/<category>/<crate>/tests/foo/bar.rs
    // belongs to nextest target `foo` when tests/foo.rs includes it.
    if parts.len() > 5
        && parts.first().copied() == Some("crate")
        && parts.get(3).copied() == Some("tests")
    {
        let root = *parts.get(4)?;
        return Some((
            format!("{}/{}/{}/tests/{root}.rs", parts[0], parts[1], parts[2]),
            root.to_string(),
            parts[4..].join("/"),
        ));
    }

    // xtask integration tests: xtask/tests/foo/bar.rs belongs to target `foo`.
    if parts.len() > 3
        && parts.first().copied() == Some("xtask")
        && parts.get(1).copied() == Some("tests")
    {
        let root = *parts.get(2)?;
        return Some((
            format!("xtask/tests/{root}.rs"),
            root.to_string(),
            parts[2..].join("/"),
        ));
    }

    None
}

fn is_library_unit_test_path(path: &str) -> bool {
    let parts: Vec<&str> = path.split('/').collect();

    // Crate library unit tests: crate/<category>/<crate>/src/*.rs, excluding
    // binary entrypoints and src/bin targets which are not covered by --lib.
    if parts.len() >= 5 && parts[0] == "crate" && parts[3] == "src" {
        return parts.get(4).copied() != Some("bin")
            && parts.last().copied() != Some("main.rs")
            && parts.last().copied() != Some("bin.rs");
    }

    // xtask is a crate rooted at xtask/src.
    if parts.len() >= 3 && parts[0] == "xtask" && parts[1] == "src" {
        return parts.get(2).copied() != Some("bin")
            && parts.last().copied() != Some("main.rs")
            && parts.last().copied() != Some("bin.rs");
    }

    false
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
    // Allow only whitespace and boolean connectives (|, &, parentheses).
    // Do NOT allow `!` (negation): `!test(foo)` means "skip foo", so inferring
    // packages that define `foo` and sending them to nextest is wrong — it would
    // target the packages whose tests we want to EXCLUDE.
    if stripped
        .chars()
        .all(|ch| ch.is_whitespace() || matches!(ch, '(' | ')' | '|' | '&'))
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

fn package_dependency_closure_from_forward(
    packages: &[String],
    forward_deps: &HashMap<String, HashSet<String>>,
) -> Vec<String> {
    let workspace_packages: HashSet<&str> = forward_deps.keys().map(String::as_str).collect();
    let mut closure: HashSet<String> = packages.iter().cloned().collect();
    let mut to_process: Vec<String> = packages.to_vec();

    while let Some(pkg) = to_process.pop() {
        let Some(deps) = forward_deps.get(&pkg) else {
            continue;
        };
        for dep in deps {
            if workspace_packages.contains(dep.as_str()) && closure.insert(dep.clone()) {
                to_process.push(dep.clone());
            }
        }
    }

    let mut closure: Vec<String> = closure.into_iter().collect();
    closure.sort();
    closure
}

/// Returns true when any `nixos/**/*.nix` or `flake.nix`/`flake.lock` file is dirty.
///
/// Used by `xtask check --full` to suggest running the NixOS VM deployment gate:
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
            ));
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_path_to_package() -> TestResult<()> {
        // Post-fold flat layout: crate/<package>/... -> <package>
        assert_eq!(
            package_for_path("crate/sinex-db/src/lib.rs"),
            Some("sinex-db".to_string())
        );
        assert_eq!(
            package_for_path("crate/sinexd/src/main.rs"),
            Some("sinexd".to_string())
        );
        assert_eq!(
            package_for_path("crate/sinex-primitives/src/lib.rs"),
            Some("sinex-primitives".to_string())
        );

        // CLI crate is crate/sinexctl directly (no more crate/cli/)
        assert_eq!(
            package_for_path("crate/sinexctl/src/main.rs"),
            Some("sinexctl".to_string())
        );
        assert_eq!(
            package_for_path("crate/sinexctl/Cargo.toml"),
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

        // Test crates are workspace members under the top-level tests/ tree.
        assert_eq!(
            package_for_path("tests/e2e/tests/some_test.rs"),
            Some("sinex-e2e-tests".to_string())
        );
        assert_eq!(
            package_for_path("tests/workspace/Cargo.toml"),
            Some("sinex-workspace-tests".to_string())
        );

        // Non-package paths return None (workspace-level handled upstream)
        assert_eq!(package_for_path("README.md"), None);
        assert_eq!(package_for_path("Cargo.toml"), None);
        assert_eq!(package_for_path("Cargo.lock"), None);
        assert_eq!(package_for_path(".config/nextest.toml"), None);
        // Other top-level tests/ entries are shared fixtures, not packages.
        assert_eq!(package_for_path("tests/fixtures/tls/ca.pem"), None);
        // A dotfile directly under crate/ is not a package.
        assert_eq!(
            package_for_path("crate/.sinex/test-artifacts/report.json"),
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
            "crate/sinex-db/src/lib.rs".into(),
            "crate/sinexd/src/main.rs".into(),
            "xtask/src/affected.rs".into(),
        ];
        let pkgs = files_to_packages(&files);
        assert!(pkgs.contains("sinex-db"));
        assert!(pkgs.contains("sinexd"));
        assert!(pkgs.contains("xtask"));
        assert_eq!(pkgs.len(), 3);
        Ok(())
    }

    #[sinex_test]
    async fn test_files_to_packages_deduplicates() -> TestResult<()> {
        let files = vec![
            "crate/sinex-db/src/lib.rs".into(),
            "crate/sinex-db/src/pool.rs".into(),
            "crate/sinex-db/Cargo.toml".into(),
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
        fs::write(
            &xtask_test,
            "#[sinex_test]\nasync fn test_compile_scope() {}\n",
        )?;
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
    async fn test_infer_test_binaries_for_test_filter_maps_integration_tests() -> TestResult<()> {
        let repo = tempfile::tempdir()?;
        let large_payload = repo.path().join("tests/e2e/tests/large_payload_test.rs");
        let workspace_test = repo.path().join("tests/workspace/tests/smoke.rs");
        fs::create_dir_all(large_payload.parent().expect("large payload parent"))?;
        fs::create_dir_all(workspace_test.parent().expect("workspace parent"))?;
        fs::write(
            &large_payload,
            "#[sinex_test]\nasync fn test_batch_large_payloads() {}\n",
        )?;
        fs::write(
            &workspace_test,
            "#[sinex_test]\nasync fn workspace_smoke_test() {}\n",
        )?;

        let inferred = infer_test_binaries_for_test_filter_in(
            repo.path(),
            "test(test_batch_large_payloads) | test(workspace_smoke_test)",
        )?;
        assert_eq!(
            inferred,
            vec!["large_payload_test".to_string(), "smoke".to_string()]
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_infer_test_binaries_for_test_filter_requires_complete_coverage() -> TestResult<()>
    {
        let repo = tempfile::tempdir()?;
        let large_payload = repo.path().join("tests/e2e/tests/large_payload_test.rs");
        let inline_test = repo.path().join("xtask/src/inline_tests.rs");
        fs::create_dir_all(large_payload.parent().expect("large payload parent"))?;
        fs::create_dir_all(inline_test.parent().expect("inline parent"))?;
        fs::write(
            &large_payload,
            "#[sinex_test]\nasync fn test_batch_large_payloads() {}\n",
        )?;
        fs::write(
            &inline_test,
            "#[sinex_test]\nasync fn inline_unit_test() {}\n",
        )?;

        let inferred = infer_test_binaries_for_test_filter_in(
            repo.path(),
            "test(test_batch_large_payloads) | test(inline_unit_test)",
        )?;
        assert!(inferred.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_infer_lib_target_for_test_filter_maps_inline_unit_tests() -> TestResult<()> {
        let repo = tempfile::tempdir()?;
        let inline_test = repo.path().join("crate/sinexd/src/coordination.rs");
        fs::create_dir_all(inline_test.parent().expect("inline parent"))?;
        fs::write(
            &inline_test,
            "#[sinex_test]\nasync fn leader_maintenance_heartbeat_refreshes_registered_metadata() {}\n",
        )?;

        assert!(infer_lib_target_for_test_filter_in(
            repo.path(),
            "test(leader_maintenance_heartbeat_refreshes_registered_metadata)",
        )?);
        Ok(())
    }

    #[sinex_test]
    async fn test_infer_lib_target_rejects_mixed_integration_coverage() -> TestResult<()> {
        let repo = tempfile::tempdir()?;
        let inline_test = repo.path().join("xtask/src/inline_tests.rs");
        let integration_test = repo.path().join("xtask/tests/command_contract.rs");
        fs::create_dir_all(inline_test.parent().expect("inline parent"))?;
        fs::create_dir_all(integration_test.parent().expect("integration parent"))?;
        fs::write(
            &inline_test,
            "#[sinex_test]\nasync fn inline_unit_test() {}\n",
        )?;
        fs::write(
            &integration_test,
            "#[sinex_test]\nasync fn command_catalog_exposes_core_public_surface() {}\n",
        )?;

        assert!(!infer_lib_target_for_test_filter_in(
            repo.path(),
            "test(inline_unit_test) | test(command_catalog_exposes_core_public_surface)",
        )?);
        Ok(())
    }

    #[sinex_test]
    async fn test_simple_test_name_term_count_rejects_complex_filters() -> TestResult<()> {
        assert_eq!(
            simple_test_name_term_count("test(one) | test(two)"),
            Some(2)
        );
        assert_eq!(simple_test_name_term_count("!test(one)"), None);
        assert_eq!(simple_test_name_term_count("package(foo)"), None);
        Ok(())
    }

    #[sinex_test]
    async fn test_infer_test_binaries_maps_nested_integration_modules() -> TestResult<()> {
        let repo = tempfile::tempdir()?;
        let root = repo
            .path()
            .join("crate/sinexd/tests/sources/production_path.rs");
        let nested = repo
            .path()
            .join("crate/sinexd/tests/sources/production_path/browser.rs");
        fs::create_dir_all(nested.parent().expect("nested parent"))?;
        fs::write(
            &root,
            "#[path = \"production_path/browser.rs\"] mod browser;\n",
        )?;
        fs::write(
            &nested,
            "#[sinex_test]\nasync fn browser_history_qutebrowser_initial_ingestion() {}\n",
        )?;

        let inferred = infer_test_binaries_for_test_filter_in(
            repo.path(),
            "test(browser_history_qutebrowser_initial_ingestion)",
        )?;
        assert_eq!(inferred, vec!["production_path".to_string()]);
        Ok(())
    }

    #[sinex_test]
    async fn test_infer_test_binaries_maps_deep_nested_integration_modules() -> TestResult<()> {
        let repo = tempfile::tempdir()?;
        let root = repo
            .path()
            .join("crate/sinexd/tests/sources/production_path.rs");
        let nested = repo
            .path()
            .join("crate/sinexd/tests/sources/production_path/obligations/initial_ingestion.rs");
        fs::create_dir_all(nested.parent().expect("nested parent"))?;
        fs::write(
            &root,
            "#[path = \"production_path/obligations/mod.rs\"] mod obligations;\n",
        )?;
        fs::write(
            &nested,
            "#[sinex_test]\nasync fn source_driver_host_scan_private_mode_matrix() {}\n",
        )?;

        let inferred = infer_test_binaries_for_test_filter_in(
            repo.path(),
            "test(source_driver_host_scan_private_mode_matrix)",
        )?;
        assert_eq!(inferred, vec!["production_path".to_string()]);
        Ok(())
    }

    #[sinex_test]
    async fn test_infer_test_binaries_maps_aggregated_nested_integration_modules() -> TestResult<()>
    {
        let repo = tempfile::tempdir()?;
        let root = repo.path().join("crate/sinexd/tests/integration_tests.rs");
        let nested = repo
            .path()
            .join("crate/sinexd/tests/integration/runtime_lifecycle_test.rs");
        fs::create_dir_all(nested.parent().expect("nested parent"))?;
        fs::write(&root, "mod integration;\nmod support;\n")?;
        fs::write(
            &nested,
            "#[sinex_test]\nasync fn test_runtime_concurrent_lifecycle() {}\n",
        )?;

        let inferred = infer_test_binaries_for_test_filter_in(
            repo.path(),
            "test(test_runtime_concurrent_lifecycle)",
        )?;
        assert_eq!(inferred, vec!["integration_tests".to_string()]);
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
        assert_eq!(
            parsed.forward_deps.get("xtask"),
            Some(&HashSet::from(["sinex-primitives".to_string()]))
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_package_dependency_closure_includes_transitive_workspace_deps() -> TestResult<()>
    {
        let forward_deps = HashMap::from([
            (
                "sinex-db".to_string(),
                HashSet::from(["sinex-primitives".to_string(), "sqlx".to_string()]),
            ),
            (
                "sinex-primitives".to_string(),
                HashSet::from(["sinex-macros".to_string()]),
            ),
            ("sinex-macros".to_string(), HashSet::new()),
        ]);

        assert_eq!(
            package_dependency_closure_from_forward(&["sinex-db".to_string()], &forward_deps),
            vec![
                "sinex-db".to_string(),
                "sinex-macros".to_string(),
                "sinex-primitives".to_string(),
            ],
            "package-scoped proof fingerprints must include dirty workspace dependencies but not external crates"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_path_to_package_underscore_to_hyphen() -> TestResult<()> {
        // Package directories with underscores should map to hyphenated package names
        assert_eq!(
            package_for_path("crate/sinex_primitives/src/lib.rs"),
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

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
    dependency_specs: HashMap<String, Vec<DependencySpec>>,
    features: HashMap<String, HashMap<String, Vec<String>>>,
}

#[derive(Clone, Debug)]
struct DependencySpec {
    name: String,
    optional: bool,
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
        let mut dependency_specs_by_package: HashMap<String, Vec<DependencySpec>> = HashMap::new();
        let mut features_by_package: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();

        for (package_index, pkg) in packages_array.iter().enumerate() {
            let name = pkg["name"].as_str().with_context(|| {
                format!("cargo metadata package[{package_index}] is missing a string name")
            })?;
            let deps = pkg["dependencies"].as_array().with_context(|| {
                format!("cargo metadata package[{name}] is missing dependencies array")
            })?;
            let package_dependency_specs = deps
                .iter()
                .enumerate()
                .map(|(dependency_index, dep)| {
                    let name = dep["name"].as_str().map(str::to_owned).with_context(|| {
                        format!(
                            "cargo metadata package[{name}] dependency[{dependency_index}] is missing a string name"
                        )
                    })?;
                    let optional = dep["optional"].as_bool().unwrap_or(false);
                    Ok(DependencySpec { name, optional })
                })
                .collect::<Result<Vec<_>>>()?;
            let deps = package_dependency_specs
                .iter()
                .map(|dep| dep.name.clone())
                .collect::<HashSet<_>>();

            let feature_map = pkg["features"]
                .as_object()
                .map(|features| {
                    features
                        .iter()
                        .map(|(feature, members)| {
                            let members = members
                                .as_array()
                                .map(|members| {
                                    members
                                        .iter()
                                        .filter_map(|member| member.as_str().map(str::to_owned))
                                        .collect::<Vec<_>>()
                                })
                                .unwrap_or_default();
                            (feature.clone(), members)
                        })
                        .collect::<HashMap<_, _>>()
                })
                .unwrap_or_default();

            let name = name.to_owned();
            packages.push(name.clone());
            forward_deps.insert(name.clone(), deps);
            dependency_specs_by_package.insert(name.clone(), package_dependency_specs);
            features_by_package.insert(name, feature_map);
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
            dependency_specs: dependency_specs_by_package,
            features: features_by_package,
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

/// Return the requested workspace packages plus dependencies active for the
/// selected packages' default features and explicitly requested Cargo features.
pub fn active_package_dependency_closure_in(
    cwd: &Path,
    packages: &[String],
    requested_features: &[String],
) -> Result<Vec<String>> {
    let metadata = WorkspaceMetadata::load_in(cwd)?;
    Ok(active_package_dependency_closure_from_metadata(
        packages,
        requested_features,
        &metadata.dependency_specs,
        &metadata.features,
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

/// Infer `(package, test_binary)` pairs from a simple test-name filter.
pub(crate) fn infer_test_binary_packages_for_test_filter(
    filter: &str,
) -> Result<Vec<(String, String)>> {
    let repo_root = crate::config::workspace_root();
    infer_test_binary_packages_for_test_filter_in(&repo_root, filter)
}

/// Infer whether a simple test-name filter targets only library unit tests.
///
/// This complements [`infer_test_binary_packages_for_test_filter`]. Inline tests in
/// `src/` do not have a nextest `--test` target, but nextest can narrow them
/// with `--lib`; doing so avoids enumerating every integration-test binary in a
/// package for a single library unit test.
pub fn infer_lib_target_for_test_filter(filter: &str) -> Result<bool> {
    let repo_root = crate::config::workspace_root();
    infer_lib_target_for_test_filter_in(&repo_root, filter)
}

pub fn infer_lib_target_for_test_filter_packages(
    filter: &str,
    packages: &[String],
) -> Result<bool> {
    let repo_root = crate::config::workspace_root();
    let selected_packages: HashSet<&str> = packages.iter().map(String::as_str).collect();
    infer_lib_target_for_test_filter_in_packages(&repo_root, filter, Some(&selected_packages))
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
            .any(|test_name| {
                path_or_content_mentions_test_name(&relative_path, &content, test_name)
            })
            && let Some(package) = package_for_path(&relative_path)
        {
            packages.insert(package);
        }
    }

    let mut packages: Vec<String> = packages.into_iter().collect();
    packages.sort();
    Ok(packages)
}

#[cfg(test)]
fn infer_test_binaries_for_test_filter_in(repo_root: &Path, filter: &str) -> Result<Vec<String>> {
    let binary_packages = infer_test_binary_packages_for_test_filter_in(repo_root, filter)?;
    let mut binaries: Vec<String> = binary_packages
        .into_iter()
        .map(|(_package, binary)| binary)
        .collect();
    binaries.sort();
    binaries.dedup();
    Ok(binaries)
}

fn infer_test_binary_packages_for_test_filter_in(
    repo_root: &Path,
    filter: &str,
) -> Result<Vec<(String, String)>> {
    let Some(test_names) = extract_simple_test_name_terms(filter) else {
        return Ok(Vec::new());
    };

    let mut binary_packages = HashSet::new();
    let mut covered_test_names = HashSet::new();
    for relative_path in candidate_rust_paths(repo_root)? {
        let full_path = repo_root.join(&relative_path);
        let content = fs::read_to_string(&full_path)
            .wrap_err_with(|| format!("failed to read {}", full_path.display()))?;

        let matched_test_names: Vec<&String> = test_names
            .iter()
            .filter(|test_name| {
                path_or_content_mentions_test_name(&relative_path, &content, test_name)
            })
            .collect();

        if !matched_test_names.is_empty() {
            let binary = if let Some(binary) = test_binary_for_path(&relative_path) {
                Some(binary)
            } else {
                test_binary_for_nested_integration_module(repo_root, &relative_path)?
            };

            if let Some(binary) = binary {
                if let Some(package) = package_for_path(&relative_path) {
                    covered_test_names.extend(matched_test_names.into_iter().cloned());
                    binary_packages.insert((package, binary));
                }
            }
        }
    }

    if test_names
        .iter()
        .any(|test_name| !covered_test_names.contains(test_name))
    {
        return Ok(Vec::new());
    }

    let mut binary_packages: Vec<(String, String)> = binary_packages.into_iter().collect();
    binary_packages.sort();
    Ok(binary_packages)
}

fn infer_lib_target_for_test_filter_in(repo_root: &Path, filter: &str) -> Result<bool> {
    infer_lib_target_for_test_filter_in_packages(repo_root, filter, None)
}

fn infer_lib_target_for_test_filter_in_packages(
    repo_root: &Path,
    filter: &str,
    selected_packages: Option<&HashSet<&str>>,
) -> Result<bool> {
    let Some(test_names) = extract_simple_test_name_terms(filter) else {
        return Ok(false);
    };

    let mut covered_test_names = HashSet::new();
    let mut non_lib_match = false;

    for relative_path in candidate_rust_paths(repo_root)? {
        if let Some(selected_packages) = selected_packages {
            let Some(package) = package_for_path(&relative_path) else {
                continue;
            };
            if !selected_packages.contains(package.as_str()) {
                continue;
            }
        }

        let full_path = repo_root.join(&relative_path);
        let content = fs::read_to_string(&full_path)
            .wrap_err_with(|| format!("failed to read {}", full_path.display()))?;

        let matched_test_names: Vec<&String> = test_names
            .iter()
            .filter(|test_name| {
                path_or_content_mentions_test_name(&relative_path, &content, test_name)
            })
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

    // Crate-local integration tests: crate/<crate>/tests/foo.rs.
    if parts.len() == 4 && parts[0] == "crate" && parts[2] == "tests" {
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
    if let Some(binary) = test_binary_from_crate_manifest(repo_root, path)? {
        return Ok(Some(binary));
    }

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

fn test_binary_from_crate_manifest(repo_root: &Path, path: &str) -> Result<Option<String>> {
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() < 4 || parts.first().copied() != Some("crate") {
        return Ok(None);
    }

    let crate_root = format!("{}/{}", parts[0], parts[1]);
    let relative_test_path = parts[2..].join("/");
    let manifest_path = repo_root.join(&crate_root).join("Cargo.toml");
    if !manifest_path.exists() {
        return Ok(None);
    }

    let manifest = fs::read_to_string(&manifest_path)
        .wrap_err_with(|| format!("failed to read {}", manifest_path.display()))?;
    let manifest: toml::Value = toml::from_str(&manifest)
        .wrap_err_with(|| format!("failed to parse {}", manifest_path.display()))?;
    let Some(tests) = manifest.get("test").and_then(toml::Value::as_array) else {
        return Ok(None);
    };

    for test in tests {
        let Some(configured_path) = test.get("path").and_then(toml::Value::as_str) else {
            continue;
        };
        if configured_path != relative_test_path {
            continue;
        }
        if let Some(name) = test.get("name").and_then(toml::Value::as_str) {
            return Ok(Some(name.to_string()));
        }
    }

    Ok(None)
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

    // Crate-local integration tests: crate/<crate>/tests/foo/bar.rs
    // belongs to nextest target `foo` when tests/foo.rs includes it.
    if parts.len() > 4
        && parts.first().copied() == Some("crate")
        && parts.get(2).copied() == Some("tests")
    {
        let root = *parts.get(3)?;
        return Some((
            format!("{}/{}/tests/{root}.rs", parts[0], parts[1]),
            root.to_string(),
            parts[3..].join("/"),
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

    // Crate library unit tests: crate/<crate>/src/*.rs, excluding binary
    // entrypoints and src/bin targets which are not covered by --lib.
    if parts.len() >= 4 && parts[0] == "crate" && parts[2] == "src" {
        return parts.get(3).copied() != Some("bin")
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
    test_function_names(content).any(|name| name.contains(test_name))
}

fn path_or_content_mentions_test_name(path: &str, content: &str, test_name: &str) -> bool {
    content_mentions_test_name(content, test_name) || test_path_mentions_test_name(path, test_name)
}

fn test_path_mentions_test_name(path: &str, test_name: &str) -> bool {
    let Some(file_name) = path.rsplit('/').next() else {
        return false;
    };
    let Some(stem) = file_name.strip_suffix(".rs") else {
        return false;
    };

    (stem.ends_with("_test") || stem.ends_with("_tests") || path.contains("/tests/"))
        && stem.contains(test_name)
}

fn test_function_names(content: &str) -> impl Iterator<Item = &str> {
    content
        .lines()
        .scan(false, |pending_test_attr, line| {
            let trimmed = line.trim_start();
            if is_test_attr_line(trimmed) {
                *pending_test_attr = true;
                return Some(None);
            }

            let test_name = if *pending_test_attr {
                function_name_from_line(trimmed)
            } else {
                None
            };

            if !trimmed.is_empty() && !trimmed.starts_with("#[") {
                *pending_test_attr = false;
            }
            Some(test_name)
        })
        .flatten()
}

fn is_test_attr_line(line: &str) -> bool {
    line.starts_with("#[test")
        || line.starts_with("#[tokio::test")
        || line.starts_with("#[sinex_test")
}

fn function_name_from_line(line: &str) -> Option<&str> {
    let (_, after_fn) = line.split_once("fn ")?;
    let name_end =
        after_fn.find(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_' || ch == ':'))?;
    Some(&after_fn[..name_end])
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

fn active_package_dependency_closure_from_metadata(
    packages: &[String],
    requested_features: &[String],
    dependency_specs: &HashMap<String, Vec<DependencySpec>>,
    features: &HashMap<String, HashMap<String, Vec<String>>>,
) -> Vec<String> {
    let workspace_packages: HashSet<&str> = dependency_specs.keys().map(String::as_str).collect();
    let root_packages: HashSet<&str> = packages.iter().map(String::as_str).collect();
    let mut closure: HashSet<String> = packages.iter().cloned().collect();
    let mut to_process: Vec<String> = packages.to_vec();

    while let Some(pkg) = to_process.pop() {
        let Some(deps) = dependency_specs.get(&pkg) else {
            continue;
        };
        let requested = if root_packages.contains(pkg.as_str()) {
            requested_features
        } else {
            &[]
        };
        let active_optional_deps =
            active_optional_dependencies(deps, features.get(&pkg), requested);

        for dep in deps {
            if dep.optional && !active_optional_deps.contains(dep.name.as_str()) {
                continue;
            }
            if workspace_packages.contains(dep.name.as_str()) && closure.insert(dep.name.clone()) {
                to_process.push(dep.name.clone());
            }
        }
    }

    let mut closure: Vec<String> = closure.into_iter().collect();
    closure.sort();
    closure
}

fn active_optional_dependencies(
    deps: &[DependencySpec],
    features: Option<&HashMap<String, Vec<String>>>,
    requested_features: &[String],
) -> HashSet<String> {
    let optional_deps: HashSet<&str> = deps
        .iter()
        .filter(|dep| dep.optional)
        .map(|dep| dep.name.as_str())
        .collect();
    if optional_deps.is_empty() {
        return HashSet::new();
    }

    let mut active_features = HashSet::new();
    let mut to_process = vec!["default".to_string()];
    to_process.extend(requested_features.iter().cloned());
    let mut active_deps = HashSet::new();

    while let Some(feature) = to_process.pop() {
        if !active_features.insert(feature.clone()) {
            continue;
        }
        let Some(members) = features.and_then(|features| features.get(&feature)) else {
            if optional_deps.contains(feature.as_str()) {
                active_deps.insert(feature);
            }
            continue;
        };

        for member in members {
            if let Some(dep) = member.strip_prefix("dep:") {
                if optional_deps.contains(dep) {
                    active_deps.insert(dep.to_string());
                }
            } else if let Some((dep, _feature)) = member.split_once('/') {
                let dep = dep.strip_suffix('?').unwrap_or(dep);
                if optional_deps.contains(dep) {
                    active_deps.insert(dep.to_string());
                } else {
                    to_process.push(member.clone());
                }
            } else if optional_deps.contains(member.as_str()) {
                active_deps.insert(member.clone());
            } else {
                to_process.push(member.clone());
            }
        }
    }

    active_deps
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
#[path = "affected_test.rs"]
mod tests;

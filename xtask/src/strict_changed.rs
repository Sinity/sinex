//! API drift guard: computes the set of Cargo packages that own changed Rust
//! files in `HEAD` relative to a merge-base, then re-checks only those packages.
//!
//! Motivation: PR #1268 shipped `library.rs:289` with `ParserError::validation(..)`
//! — a method that doesn't exist on the thiserror enum. The agent's local
//! `xtask check` missed it because `CARGO_TARGET_DIR` was stale. This guard runs
//! a fresh per-package check limited to the PR surface, independent of the
//! inherited environment.
//!
//! # Usage
//!
//! ```text
//! xtask check --changed-strict             # base = "origin/master"
//! xtask check --changed-strict main        # explicit base ref
//! ```

use color_eyre::eyre::{Result, eyre};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Compute the merge-base SHA between `base` and `HEAD`.
fn merge_base(base: &str) -> Result<String> {
    let output = Command::new("git")
        .args(["merge-base", base, "HEAD"])
        .output()
        .map_err(|e| eyre!("failed to spawn git merge-base: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(eyre!(
            "git merge-base {base} HEAD failed: {stderr}"
        ));
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Return the list of `*.rs` files changed between `base_ref` (merge-base) and
/// `HEAD`. Paths are relative to the workspace root.
///
/// `base_ref` may be any git ref or SHA accepted by `git diff --name-only`.
/// The caller is responsible for resolving the merge-base if needed.
pub fn changed_rust_files(base_ref: &str) -> Result<Vec<PathBuf>> {
    let mb = merge_base(base_ref)?;

    let output = Command::new("git")
        .args(["diff", "--name-only", &mb, "HEAD", "--", "*.rs"])
        .output()
        .map_err(|e| eyre!("failed to spawn git diff: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(eyre!(
            "git diff --name-only {mb} HEAD -- *.rs failed: {stderr}"
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout
        .lines()
        .filter(|l| !l.is_empty())
        .map(PathBuf::from)
        .collect())
}

/// Walk from `file` up toward `workspace_root` looking for a `Cargo.toml` that
/// contains a `[package]` section, and return the package name declared there.
///
/// Returns `None` when no owning `Cargo.toml` is found or the file is at the
/// workspace root (the root `Cargo.toml` is a workspace manifest, not a package).
pub fn owning_package(file: &Path, workspace_root: &Path) -> Option<String> {
    // Start from the file's parent directory, walk up stopping before workspace root.
    let start_dir = if file.is_absolute() {
        file.parent()?.to_path_buf()
    } else {
        workspace_root.join(file).parent()?.to_path_buf()
    };

    let mut dir = start_dir.as_path();
    loop {
        // Don't read the workspace-root Cargo.toml — it's a [workspace] manifest.
        if dir == workspace_root {
            break;
        }

        let candidate = dir.join("Cargo.toml");
        if candidate.is_file() {
            if let Some(name) = extract_package_name(&candidate) {
                return Some(name);
            }
        }

        dir = dir.parent()?;
    }

    None
}

/// Parse `[package] name = "..."` from a `Cargo.toml` file.
fn extract_package_name(cargo_toml: &Path) -> Option<String> {
    let content = std::fs::read_to_string(cargo_toml).ok()?;

    // We want the `name` key inside a `[package]` section, not `[workspace.package]`.
    let mut in_package = false;
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed == "[package]" {
            in_package = true;
            continue;
        }
        // Any other `[` header exits the package section.
        if trimmed.starts_with('[') {
            in_package = false;
            continue;
        }
        if in_package {
            // Match:  name = "foo"   or   name = 'foo'
            if let Some(rest) = trimmed.strip_prefix("name") {
                let rest = rest.trim_start().strip_prefix('=')?;
                let rest = rest.trim();
                // Strip surrounding quotes
                let name = if (rest.starts_with('"') && rest.ends_with('"'))
                    || (rest.starts_with('\'') && rest.ends_with('\''))
                {
                    rest[1..rest.len() - 1].to_string()
                } else {
                    continue;
                };
                return Some(name);
            }
        }
    }

    None
}

/// Return the deduplicated, sorted set of Cargo package names that own at least
/// one of the Rust files changed between `base_ref` and `HEAD`.
pub fn affected_packages(base_ref: &str, workspace_root: &Path) -> Result<BTreeSet<String>> {
    let files = changed_rust_files(base_ref)?;
    let mut packages = BTreeSet::new();
    for file in &files {
        if let Some(pkg) = owning_package(file, workspace_root) {
            packages.insert(pkg);
        }
    }
    Ok(packages)
}

/// Result of running `xtask check --changed-strict`.
#[derive(Debug, serde::Serialize)]
pub struct ChangedStrictReport {
    /// Git base ref used for the diff (the user-supplied value, e.g. `"origin/master"`).
    pub base_ref: String,
    /// Resolved merge-base SHA.
    pub merge_base: String,
    /// Changed Rust files (relative paths).
    pub changed_files: Vec<PathBuf>,
    /// Affected packages (deduped, sorted).
    pub affected_packages: Vec<String>,
    /// Per-package check results.
    pub package_results: Vec<PackageCheckResult>,
    /// True iff all per-package checks passed.
    pub success: bool,
}

/// Per-package check result.
#[derive(Debug, serde::Serialize)]
pub struct PackageCheckResult {
    pub package: String,
    pub success: bool,
    pub exit_code: Option<i32>,
    /// First lines of stderr/stdout on failure (capped at 20 lines for compactness).
    pub output_excerpt: Option<String>,
}

/// Run `xtask check -p <pkg>` for each affected package, aggregate results.
///
/// `xtask_bin` is the path (or name) of the xtask binary to invoke. The caller
/// resolves this — typically `std::env::current_exe()`.
pub fn run_changed_strict(
    base_ref: &str,
    workspace_root: &Path,
    xtask_bin: &Path,
    extra_check_args: &[String],
) -> Result<ChangedStrictReport> {
    let mb = merge_base(base_ref)?;
    let changed_files = changed_rust_files(base_ref)?;
    let pkgs = affected_packages(base_ref, workspace_root)?;
    let pkg_list: Vec<String> = pkgs.into_iter().collect();

    if pkg_list.is_empty() {
        return Ok(ChangedStrictReport {
            base_ref: base_ref.to_string(),
            merge_base: mb,
            changed_files,
            affected_packages: vec![],
            package_results: vec![],
            success: true,
        });
    }

    let mut package_results = Vec::with_capacity(pkg_list.len());
    let mut all_ok = true;

    for pkg in &pkg_list {
        let mut args = vec!["check".to_string(), "-p".to_string(), pkg.clone()];
        args.extend_from_slice(extra_check_args);

        let output = Command::new(xtask_bin)
            .args(&args)
            .current_dir(workspace_root)
            .output()
            .map_err(|e| eyre!("failed to spawn xtask check -p {pkg}: {e}"))?;

        let success = output.status.success();
        if !success {
            all_ok = false;
        }

        let output_excerpt = if !success {
            // Combine stdout + stderr, cap at 20 lines
            let combined = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
            let lines: Vec<&str> = combined.lines().collect();
            let excerpt = if lines.len() > 20 {
                format!("{}\n[... {} more lines]", lines[..20].join("\n"), lines.len() - 20)
            } else {
                lines.join("\n")
            };
            Some(excerpt)
        } else {
            None
        };

        package_results.push(PackageCheckResult {
            package: pkg.clone(),
            success,
            exit_code: output.status.code(),
            output_excerpt,
        });
    }

    Ok(ChangedStrictReport {
        base_ref: base_ref.to_string(),
        merge_base: mb,
        changed_files,
        affected_packages: pkg_list,
        package_results,
        success: all_ok,
    })
}


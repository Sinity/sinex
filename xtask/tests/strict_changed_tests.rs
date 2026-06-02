//! Unit tests for the `strict_changed` API drift guard.
//!
//! These tests verify the changed-file → owning-package mapping using a
//! synthetic tempdir workspace, without touching git or running any xtask
//! sub-invocation.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use tempfile::TempDir;
use xtask::sandbox::sinex_test;
use xtask::strict_changed::owning_package;

// ============================================================================
// Scaffold helpers
// ============================================================================

/// Create a minimal two-package workspace:
///
/// ```text
/// <tmp>/
///   Cargo.toml                       [workspace]
///   crate/alpha/Cargo.toml           [package] name = "alpha"
///   crate/alpha/src/lib.rs
///   crate/beta/Cargo.toml            [package] name = "beta"
///   crate/beta/src/lib.rs
/// ```
fn scaffold_workspace() -> ::xtask::sandbox::TestResult<TempDir> {
    let tmp = tempfile::tempdir()?;
    let root = tmp.path();

    fs::write(
        root.join("Cargo.toml"),
        "[workspace]\nmembers = [\"crate/alpha\", \"crate/beta\"]\n",
    )?;

    let alpha_src = root.join("crate/alpha/src");
    fs::create_dir_all(&alpha_src)?;
    fs::write(
        root.join("crate/alpha/Cargo.toml"),
        "[package]\nname = \"alpha\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )?;
    fs::write(alpha_src.join("lib.rs"), "// alpha\n")?;

    let beta_src = root.join("crate/beta/src");
    fs::create_dir_all(&beta_src)?;
    fs::write(
        root.join("crate/beta/Cargo.toml"),
        "[package]\nname = \"beta\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )?;
    fs::write(beta_src.join("lib.rs"), "// beta\n")?;

    Ok(tmp)
}

// ============================================================================
// owning_package — direct parent
// ============================================================================

#[sinex_test]
async fn test_owning_package_finds_direct_parent() -> ::xtask::sandbox::TestResult<()> {
    let tmp = scaffold_workspace()?;
    let root = tmp.path();

    let pkg = owning_package(Path::new("crate/alpha/src/lib.rs"), root);
    assert_eq!(pkg.as_deref(), Some("alpha"));
    Ok(())
}

// ============================================================================
// owning_package — nested file within a package
// ============================================================================

#[sinex_test]
async fn test_owning_package_finds_nested_file() -> ::xtask::sandbox::TestResult<()> {
    let tmp = scaffold_workspace()?;
    let root = tmp.path();

    let nested = root.join("crate/beta/src/submod");
    fs::create_dir_all(&nested)?;
    fs::write(nested.join("helper.rs"), "// helper\n")?;

    let pkg = owning_package(Path::new("crate/beta/src/submod/helper.rs"), root);
    assert_eq!(pkg.as_deref(), Some("beta"));
    Ok(())
}

// ============================================================================
// owning_package — workspace-root file has no owning package
// ============================================================================

#[sinex_test]
async fn test_owning_package_returns_none_for_workspace_root_file()
-> ::xtask::sandbox::TestResult<()> {
    let tmp = scaffold_workspace()?;
    let root = tmp.path();

    // A file at the workspace root level has no package owner;
    // we stop before reading the workspace-level Cargo.toml.
    let pkg = owning_package(Path::new("build.rs"), root);
    assert!(
        pkg.is_none(),
        "workspace-root file should return None, got {pkg:?}"
    );
    Ok(())
}

// ============================================================================
// owning_package — workspace-only manifest is not treated as a package
// ============================================================================

#[sinex_test]
async fn test_owning_package_ignores_workspace_only_manifest() -> ::xtask::sandbox::TestResult<()> {
    let tmp = scaffold_workspace()?;
    let root = tmp.path();

    // Create a subdirectory with only a [workspace] manifest (no [package]).
    let sub = root.join("tools/nopkg");
    fs::create_dir_all(sub.join("src"))?;
    fs::write(
        sub.join("Cargo.toml"),
        "[workspace]\nmembers = []\n\n[workspace.package]\nversion = \"0.1.0\"\n",
    )?;
    fs::write(sub.join("src").join("main.rs"), "fn main() {}\n")?;

    // The walk should climb past this manifest without yielding a name.
    let pkg = owning_package(Path::new("tools/nopkg/src/main.rs"), root);
    assert!(
        pkg.is_none(),
        "workspace-only manifest should yield None, got {pkg:?}"
    );
    Ok(())
}

// ============================================================================
// owning_package — nested independent workspace (cargo-fuzz) is disowned
// ============================================================================

#[sinex_test]
async fn test_owning_package_disowns_nested_workspace_crate() -> ::xtask::sandbox::TestResult<()> {
    let tmp = scaffold_workspace()?;
    let root = tmp.path();

    // A `cargo-fuzz`-style crate nested under a member: it has BOTH a [package]
    // section AND its own [workspace] table, marking it an independent workspace
    // root excluded from the outer one. `cargo check -p <name>` would fail with
    // "cannot specify features for packages outside of workspace", so the drift
    // guard must not attribute its files to any outer package.
    let fuzz = root.join("crate/alpha/fuzz");
    fs::create_dir_all(fuzz.join("fuzz_targets"))?;
    fs::write(
        fuzz.join("Cargo.toml"),
        "[package]\nname = \"alpha-fuzz\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n\
         [package.metadata]\ncargo-fuzz = true\n\n[workspace]\n",
    )?;
    fs::write(
        fuzz.join("fuzz_targets").join("fuzz_alpha.rs"),
        "// fuzz target\n",
    )?;

    let pkg = owning_package(
        Path::new("crate/alpha/fuzz/fuzz_targets/fuzz_alpha.rs"),
        root,
    );
    assert!(
        pkg.is_none(),
        "files inside a nested independent workspace must be disowned, got {pkg:?}"
    );
    Ok(())
}

// ============================================================================
// changed-file → package mapping: single file, no duplicates
// ============================================================================

#[sinex_test]
async fn test_changed_strict_maps_files_to_packages() -> ::xtask::sandbox::TestResult<()> {
    let tmp = scaffold_workspace()?;
    let root = tmp.path();

    // Simulate two changed files: one in alpha, one with no owning package.
    let changed = [
        Path::new("crate/alpha/src/lib.rs"),
        Path::new("README.md"), // no owner
    ];

    let mut pkgs = BTreeSet::new();
    for f in &changed {
        if let Some(pkg) = owning_package(f, root) {
            pkgs.insert(pkg);
        }
    }

    assert_eq!(pkgs.len(), 1, "only alpha should be found");
    assert!(pkgs.contains("alpha"), "alpha should be in the set");
    Ok(())
}

// ============================================================================
// changed-file → package mapping: deduplication
// ============================================================================

#[sinex_test]
async fn test_changed_strict_deduplicates_packages() -> ::xtask::sandbox::TestResult<()> {
    let tmp = scaffold_workspace()?;
    let root = tmp.path();

    // Two files in alpha — should collapse to one package entry.
    fs::write(root.join("crate/alpha/src/extra.rs"), "// extra\n")?;

    let changed = [
        Path::new("crate/alpha/src/lib.rs"),
        Path::new("crate/alpha/src/extra.rs"),
    ];

    let mut pkgs = BTreeSet::new();
    for f in &changed {
        if let Some(pkg) = owning_package(f, root) {
            pkgs.insert(pkg);
        }
    }

    assert_eq!(
        pkgs.len(),
        1,
        "two alpha files should collapse to one entry"
    );
    assert!(pkgs.contains("alpha"));
    Ok(())
}

// ============================================================================
// changed-file → package mapping: cross-package
// ============================================================================

#[sinex_test]
async fn test_changed_strict_cross_package() -> ::xtask::sandbox::TestResult<()> {
    let tmp = scaffold_workspace()?;
    let root = tmp.path();

    let changed = [
        Path::new("crate/alpha/src/lib.rs"),
        Path::new("crate/beta/src/lib.rs"),
    ];

    let mut pkgs = BTreeSet::new();
    for f in &changed {
        if let Some(pkg) = owning_package(f, root) {
            pkgs.insert(pkg);
        }
    }

    assert_eq!(pkgs.len(), 2, "both alpha and beta should appear");
    assert!(pkgs.contains("alpha"));
    assert!(pkgs.contains("beta"));
    Ok(())
}

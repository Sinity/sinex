//! Semantic freshness keys for foreground Sinex proofs.
//!
//! AgentCTL owns declared-operation lifecycle. This module retains only the
//! command-aware fingerprint, scope, and proof-key calculations that Sinex
//! planning and HistoryDB reuse require.

use color_eyre::eyre::{Result, WrapErr, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;

const SHARED_FINGERPRINT_INPUTS: &[&str] = &[
    "Cargo.toml",
    "Cargo.lock",
    "rust-toolchain",
    "rust-toolchain.toml",
    "flake.nix",
    "flake.lock",
    ".config/nextest.toml",
];

/// Human/machine-readable explanation of a command freshness key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshnessExplanation {
    pub command: String,
    pub args: Vec<String>,
    pub should_coordinate: bool,
    /// Whether the command's foreground semantic path can reuse an exact proof.
    pub fresh_reuse_enabled: bool,
    pub proof_kind: String,
    pub scope_key: String,
    pub tree_fingerprint: String,
    pub substrate_seal: String,
    pub scope: FreshnessScopeExplanation,
    pub shared_inputs: Vec<String>,
}

const SEAL_ENVIRONMENT: &[&str] = &[
    "DATABASE_URL",
    "DATABASE_URL_APP",
    "DATABASE_URL_SUPERUSER",
    "SINEX_NATS_URL",
    "RUSTFLAGS",
    "RUSTC_WRAPPER",
    "CARGO_TARGET_DIR",
    "SINEX_CACHE_DIR",
    "SINEX_DEV_STATE_DIR",
    "IN_NIX_SHELL",
    "name",
];

fn command_version(command: &str, args: &[&str]) -> String {
    std::process::Command::new(command)
        .args(args)
        .output()
        .map(|output| {
            if output.status.success() {
                String::from_utf8_lossy(&output.stdout).trim().to_string()
            } else {
                format!("unavailable:{}", output.status)
            }
        })
        .unwrap_or_else(|error| format!("unavailable:{error}"))
}

fn live_schema_digest() -> String {
    let Some(database_url) = std::env::var_os("DATABASE_URL") else {
        return "unavailable:no-database-url".to_string();
    };
    let query = "SELECT COALESCE(string_agg(format('%s.%s:%s:%s', table_schema, table_name, column_name, data_type), ',' ORDER BY table_schema, table_name, ordinal_position), '') FROM information_schema.columns WHERE table_schema IN ('core', 'raw', 'audit', 'reflection', 'sinex_schemas', 'sinex_telemetry')";
    let output = std::process::Command::new("psql")
        .args([
            "--no-psqlrc",
            "--tuples-only",
            "--no-align",
            "--command",
            query,
        ])
        .arg(database_url)
        .output();
    match output {
        Ok(output) if output.status.success() => {
            format!("{:x}", Sha256::digest(output.stdout))
        }
        Ok(output) => format!("unavailable:{}", summarize_git_error(&output)),
        Err(error) => format!("unavailable:{error}"),
    }
}

fn hash_repo_inputs(hasher: &mut Sha256, paths: &[&str]) {
    for path in paths {
        let value = Path::new(path)
            .is_file()
            .then(|| fs::read(path).ok())
            .flatten()
            .unwrap_or_default();
        hash_labeled_bytes(hasher, path, &value);
    }
}

/// Return the identity of the substrate against which a proof is valid.
///
/// Values are hashed so connection URLs and other environment values never enter
/// freshness explanations or history rows in plaintext.
pub fn substrate_seal() -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(b"sinex-substrate-seal-v1\0");
    for name in SEAL_ENVIRONMENT {
        hash_labeled_bytes(
            &mut hasher,
            name,
            std::env::var(name).unwrap_or_default().trim().as_bytes(),
        );
    }
    for (command, args) in [
        ("rustc", vec!["-Vv"]),
        ("cargo", vec!["--version"]),
        ("cargo-nextest", vec!["--version"]),
    ] {
        hash_labeled_bytes(
            &mut hasher,
            command,
            command_version(command, &args).as_bytes(),
        );
    }
    hash_repo_inputs(
        &mut hasher,
        &[
            "Cargo.toml",
            "Cargo.lock",
            "flake.lock",
            "crate/sinex-schema/src/apply.rs",
            "crate/sinex-schema/src/registry.rs",
            "nixos/modules/nats.nix",
            "crate/sinexd/src/event_engine/jetstream_consumer/bootstrap.rs",
        ],
    );
    let preflight_cache = crate::config::config()
        .preflight_state_dir()
        .join("preflight-cache.json");
    hash_labeled_bytes(
        &mut hasher,
        "preflight-outcome-id",
        &fs::read(preflight_cache).unwrap_or_default(),
    );
    hash_labeled_bytes(
        &mut hasher,
        "live-schema-digest",
        live_schema_digest().as_bytes(),
    );
    hash_labeled_bytes(
        &mut hasher,
        "postgres-ready",
        crate::preflight::is_postgres_ready().to_string().as_bytes(),
    );
    hash_labeled_bytes(
        &mut hasher,
        "nats-ready",
        crate::preflight::is_nats_ready().to_string().as_bytes(),
    );
    Ok(format!("{:x}", hasher.finalize()))
}

/// Scope inputs that feed a freshness fingerprint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FreshnessScopeExplanation {
    Workspace,
    Packages { packages: Vec<PackageScopeInput> },
}

/// Package-to-path mapping used by scoped fingerprints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageScopeInput {
    pub package: String,
    pub path: String,
}

fn summarize_git_error(output: &std::process::Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !stderr.is_empty() {
        return stderr;
    }

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !stdout.is_empty() {
        return stdout;
    }

    format!("exit status {}", output.status)
}

fn git_output(cwd: &Path, args: &[&str], description: &str) -> Result<std::process::Output> {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .with_context(|| format!("failed to run git {description}"))?;

    if !output.status.success() {
        bail!("git {description} failed: {}", summarize_git_error(&output));
    }

    Ok(output)
}

fn refresh_git_index(cwd: &Path) -> Result<()> {
    let output = std::process::Command::new("git")
        .args(["update-index", "-q", "--refresh"])
        .current_dir(cwd)
        .output()
        .with_context(|| "failed to run git update-index -q --refresh".to_string())?;

    if !output.status.success() {
        bail!(
            "git update-index -q --refresh failed: {}",
            summarize_git_error(&output)
        );
    }

    Ok(())
}

fn hash_labeled_bytes(hasher: &mut Sha256, label: &str, bytes: &[u8]) {
    hasher.update(label.as_bytes());
    hasher.update(b"\x00");
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(b"\x00");
    hasher.update(bytes);
    hasher.update(b"\x00");
}

fn hash_git_output(
    cwd: &Path,
    hasher: &mut Sha256,
    label: &str,
    args: &[&str],
    description: &str,
) -> Result<()> {
    let output = git_output(cwd, args, description)?;
    hash_labeled_bytes(hasher, label, &output.stdout);
    Ok(())
}

fn hash_untracked_file_contents(cwd: &Path, hasher: &mut Sha256, pathspecs: &[&str]) -> Result<()> {
    let mut args = vec!["ls-files", "--others", "--exclude-standard", "-z", "--"];
    args.extend_from_slice(pathspecs);
    let output = git_output(
        cwd,
        &args,
        "ls-files --others --exclude-standard -z for fingerprint",
    )?;
    let mut paths = output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
        .collect::<Vec<_>>();
    paths.sort_unstable();

    for path_bytes in paths {
        hash_labeled_bytes(hasher, "untracked-path", path_bytes);
        let rel_path = String::from_utf8_lossy(path_bytes);
        let contents = fs::read(cwd.join(rel_path.as_ref())).with_context(|| {
            format!("failed to read untracked file for fingerprint: {rel_path}")
        })?;
        hash_labeled_bytes(hasher, "untracked-content", &contents);
    }

    Ok(())
}

fn hash_dirty_content(cwd: &Path, hasher: &mut Sha256, pathspecs: &[&str]) -> Result<()> {
    let mut cached_args = vec![
        "diff",
        "--binary",
        "--no-ext-diff",
        "--cached",
        "HEAD",
        "--",
    ];
    cached_args.extend_from_slice(pathspecs);
    hash_git_output(
        cwd,
        hasher,
        "staged-diff",
        &cached_args,
        "diff --binary --cached HEAD for fingerprint",
    )?;

    let mut unstaged_args = vec!["diff", "--binary", "--no-ext-diff", "--"];
    unstaged_args.extend_from_slice(pathspecs);
    hash_git_output(
        cwd,
        hasher,
        "unstaged-diff",
        &unstaged_args,
        "diff --binary for fingerprint",
    )?;

    hash_untracked_file_contents(cwd, hasher, pathspecs)
}

/// Compute tree fingerprint: sha256 of committed tree identity plus dirty content.
///
/// Properties: deterministic (same tree → same hash), conservative, and
/// content-sensitive for staged, unstaged, and untracked changes.
fn tree_fingerprint_in(cwd: &Path) -> Result<String> {
    // Refresh the git index so status reflects actual filesystem state.
    // Without this, rapid edits within the same second can go undetected
    // because git caches stat data (mtime, size) in the index.
    refresh_git_index(cwd)?;

    let mut hasher = Sha256::new();
    hasher.update(b"sinex-tree-fingerprint-v2\x00");
    hash_git_output(
        cwd,
        &mut hasher,
        "head",
        &["rev-parse", "HEAD"],
        "rev-parse HEAD for whole-tree fingerprint",
    )?;
    hash_dirty_content(cwd, &mut hasher, &[])?;
    hash_labeled_bytes(&mut hasher, "substrate-seal", substrate_seal()?.as_bytes());
    Ok(format!("{:x}", hasher.finalize()))
}

fn tree_fingerprint() -> Result<String> {
    tree_fingerprint_in(Path::new("."))
}

/// R1: Map a package name to its source directory path for git diff scoping.
///
/// Used by `scoped_tree_fingerprint` to limit `git diff` to relevant directories.
/// Over-inclusion (returning a broader path) is safe — it causes unnecessary cache
/// misses but never incorrect freshness. Under-inclusion would be incorrect.
fn package_to_path(pkg: &str) -> String {
    match pkg {
        "sinexctl" => "crate/sinexctl/".to_string(),
        "xtask" => "xtask/".to_string(),
        "sinex-e2e-tests" => "tests/e2e/".to_string(),
        "sinex-workspace-tests" => "tests/workspace/".to_string(),
        "sinex-vm-test-suite" => "tests/vm-suite/".to_string(),
        _ => {
            let name_underscore = pkg.replace('-', "_");
            let path_hyphen = format!("crate/{pkg}/");
            if std::path::Path::new(&path_hyphen).exists() {
                return path_hyphen;
            }
            let path_under = format!("crate/{name_underscore}/");
            if std::path::Path::new(&path_under).exists() {
                return path_under;
            }
            // Unknown package — include crate/ broadly (over-includes, never misses)
            "crate/".to_string()
        }
    }
}

/// R1: Extract package names from -p/--package flags in command args.
fn extract_explicit_packages(command: &str, args: &[String]) -> Vec<String> {
    if !matches!(command, "check" | "build" | "fix" | "test") {
        return vec![];
    }

    let mut packages = Vec::new();
    if let Some(marker) = args.iter().find(|arg| arg.starts_with("--scope=")) {
        let raw = marker.trim_start_matches("--scope=");
        if let Some(marker_packages) = raw.strip_prefix("packages:") {
            marker_packages
                .split(',')
                .filter(|entry| !entry.is_empty())
                .map(ToOwned::to_owned)
                .for_each(|package| packages.push(package));
        }
        // Unknown --scope= format: fall through and parse -p/--package flags
        // instead of silently dropping them.  A future scope variant will get
        // package resolution for free without a separate special-case here.
    }

    let mut take_next = false;

    for arg in args {
        if command == "test" && arg == "--" {
            break;
        }
        if take_next {
            packages.push(arg.clone());
            take_next = false;
            continue;
        }
        if arg == "-p" || arg == "--package" || arg == "--packages" {
            take_next = true;
        } else if let Some(pkg) = arg.strip_prefix("--packages=") {
            packages.push(pkg.to_string());
        } else if let Some(pkg) = arg.strip_prefix("--package=") {
            packages.push(pkg.to_string());
        } else if let Some(pkg) = arg.strip_prefix("-p").filter(|s| !s.is_empty()) {
            packages.push(pkg.to_string());
        } else if let Some(runtime) = arg.strip_prefix("--runtime-binary=") {
            let package = runtime
                .split_once(':')
                .map_or(runtime, |(package, _)| package);
            if !package.is_empty() {
                packages.push(package.to_string());
            }
        }
    }

    packages.sort();
    packages.dedup();
    packages
}

fn extract_cargo_features(command: &str, args: &[String]) -> Vec<String> {
    if !matches!(command, "check" | "build" | "fix" | "test") {
        return vec![];
    }

    let mut features = Vec::new();
    let mut take_next = false;

    for arg in args {
        if command == "test" && arg == "--" {
            break;
        }
        if take_next {
            features.extend(split_cargo_features(arg));
            take_next = false;
            continue;
        }
        if arg == "--features" {
            take_next = true;
        } else if let Some(value) = arg.strip_prefix("--features=") {
            features.extend(split_cargo_features(value));
        }
    }

    features.sort();
    features.dedup();
    features
}

fn split_cargo_features(value: &str) -> impl Iterator<Item = String> + '_ {
    value
        .split(',')
        .map(str::trim)
        .filter(|feature| !feature.is_empty())
        .map(ToOwned::to_owned)
}

fn scoped_fingerprint_packages_in(
    cwd: &Path,
    packages: &[String],
    features: &[String],
) -> Result<Vec<String>> {
    if features.is_empty() {
        crate::affected::active_package_dependency_closure_in(cwd, packages, &[])
    } else {
        crate::affected::active_package_dependency_closure_in(cwd, packages, features)
    }
}

/// R1: Compute a scoped tree fingerprint for the given command and args.
///
/// If the command targets explicit packages (via `-p`), hashes only the git diff
/// for those package directories rather than the entire workspace. This means
/// changing `nixos/README.md` no longer invalidates `check -p sinex-db`.
///
/// Falls back to the whole-workspace `tree_fingerprint()` when no explicit
/// packages are specified (affected-mode and workspace-wide invocations).
fn scoped_tree_fingerprint_in(cwd: &Path, command: &str, args: &[String]) -> Result<String> {
    let packages = extract_explicit_packages(command, args);
    let features = extract_cargo_features(command, args);

    if packages.is_empty() {
        // No -p flag: use whole-workspace fingerprint (safe, over-inclusive)
        return tree_fingerprint_in(cwd);
    }
    let fingerprint_packages = match scoped_fingerprint_packages_in(cwd, &packages, &features) {
        Ok(closure) => closure,
        Err(error) => {
            tracing::warn!(
                error = %error,
                "failed to resolve package dependency closure for scoped freshness; falling back to whole-tree fingerprint"
            );
            return tree_fingerprint_in(cwd);
        }
    };

    // Refresh git index (same as tree_fingerprint)
    refresh_git_index(cwd)?;

    let mut hasher = Sha256::new();

    // Seed the hasher so a clean working tree (no diff, no untracked) still
    // produces a fingerprint that's distinct per (HEAD, package-set). Before
    // this seeding, every clean per-package run hashed zero bytes and
    // collided on SHA256("") — 117 such collisions in 7d (#1212).
    //
    // Domain separator + version is intentional: changing the seeding format
    // later should bump the version to invalidate old cache entries.
    hasher.update(b"sinex-tree-fingerprint-v2\x00");
    hash_git_output(
        cwd,
        &mut hasher,
        "head",
        &["rev-parse", "HEAD"],
        "rev-parse HEAD for fingerprint seeding",
    )?;
    // fingerprint_packages includes the requested packages plus their transitive
    // workspace dependencies. Sort for deterministic fingerprint regardless of
    // -p order or metadata order.
    let mut sorted_packages: Vec<&String> = fingerprint_packages.iter().collect();
    sorted_packages.sort_unstable();
    for pkg in &sorted_packages {
        hasher.update(pkg.as_bytes());
        hasher.update(b"\x00");
    }

    for pkg in &sorted_packages {
        let prefix = package_to_path(pkg);
        hash_dirty_content(cwd, &mut hasher, &[&prefix])?;
    }
    hash_dirty_content(cwd, &mut hasher, SHARED_FINGERPRINT_INPUTS)?;
    hash_labeled_bytes(&mut hasher, "substrate-seal", substrate_seal()?.as_bytes());

    Ok(format!("{:x}", hasher.finalize()))
}

fn scoped_tree_fingerprint(command: &str, args: &[String]) -> Result<String> {
    scoped_tree_fingerprint_in(Path::new("."), command, args)
}

/// Explain the current coordinator freshness key without mutating state.
///
/// This is the auditable counterpart to `scoped_tree_fingerprint`: consumers can
/// see the command/scope inputs before trusting a fresh-hit decision.
pub fn explain_freshness(command: &str, args: &[String]) -> Result<FreshnessExplanation> {
    let packages = extract_explicit_packages(command, args);
    let features = extract_cargo_features(command, args);
    let scope = if packages.is_empty() {
        FreshnessScopeExplanation::Workspace
    } else {
        let fingerprint_packages =
            scoped_fingerprint_packages_in(Path::new("."), &packages, &features)
                .unwrap_or(packages);
        let mut packages = fingerprint_packages
            .into_iter()
            .map(|package| PackageScopeInput {
                path: package_to_path(&package),
                package,
            })
            .collect::<Vec<_>>();
        packages.sort_unstable_by(|left, right| left.package.cmp(&right.package));
        FreshnessScopeExplanation::Packages { packages }
    };

    Ok(FreshnessExplanation {
        command: command.to_string(),
        args: args.to_vec(),
        should_coordinate: false,
        fresh_reuse_enabled: command == "test" && test_scope_has_exact_proof(args),
        proof_kind: proof_kind(command, args),
        scope_key: scope_key(command, args),
        tree_fingerprint: scoped_tree_fingerprint(command, args)?,
        substrate_seal: substrate_seal()?,
        scope,
        shared_inputs: SHARED_FINGERPRINT_INPUTS
            .iter()
            .map(|input| (*input).to_string())
            .collect(),
    })
}

/// Compute scope key: hash of command-specific parameters that define
/// what work is being done.
///
/// Handles both `--flag=value` and `--flag value` (two separate args) forms.
/// For flags like `-p sinex-db`, captures both the flag AND the following value.
fn scope_key(command: &str, args: &[String]) -> String {
    let relevant = extract_scope_args(command, args);

    let mut sorted: Vec<&str> = relevant.iter().map(String::as_str).collect();
    sorted.sort_unstable(); // Deterministic order

    let mut hasher = Sha256::new();
    for arg in &sorted {
        hasher.update(arg.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

/// Human-readable proof unit class for a foreground or host-managed command.
#[must_use]
pub fn proof_kind(command: &str, args: &[String]) -> String {
    match command {
        "check" => {
            let mut modes = Vec::new();
            for flag in [
                "--all",
                "--fix",
                "--full",
                "--lint",
                "--fmt",
                "--forbidden",
                "--nix",
                "--skip-tests",
                "--changed-strict",
            ] {
                if args.iter().any(|arg| arg == flag) {
                    modes.push(flag.trim_start_matches('-').replace('-', "_"));
                }
            }
            if modes.is_empty() {
                "check.default".to_string()
            } else {
                modes.sort_unstable();
                format!("check.{}", modes.join("+"))
            }
        }
        "fix" => {
            if args.iter().any(|arg| arg == "--check") {
                "fix.check".to_string()
            } else {
                "fix.apply".to_string()
            }
        }
        "build" => {
            if args.iter().any(|arg| arg == "--dry-run") {
                "build.dry_run".to_string()
            } else {
                "build.default".to_string()
            }
        }
        "test" => {
            if test_scope_has_exact_proof(args) {
                "test.nextest.exact".to_string()
            } else {
                "test.nextest.plan".to_string()
            }
        }
        other => format!("{other}.default"),
    }
}

fn test_scope_has_exact_proof(args: &[String]) -> bool {
    !args.iter().any(|arg| {
        matches!(
            arg.as_str(),
            "--heavy"
                | "--include-ignored"
                | "--debug"
                | "--fuzz"
                | "--mutants"
                | "--coverage"
                | "--bench"
                | "--list"
                | "--dry-run"
                | "-l"
                | "--prime"
                | "--update-snapshots"
                | "--no-reuse"
        )
    })
}

/// Extract scope-relevant arguments for a command.
///
/// Handles the tricky case where `-p sinex-db` is two separate args:
/// the flag `-p` and its value `sinex-db` are both captured.
fn extract_scope_args(command: &str, args: &[String]) -> Vec<String> {
    let marker = args
        .iter()
        .find(|arg| arg.starts_with("--scope="))
        .cloned()
        .or_else(|| canonical_package_scope_marker(command, args));

    fn is_package_value_flag(command: &str, arg: &str) -> bool {
        matches!(command, "build" | "check" | "fix" | "test")
            && matches!(arg, "-p" | "--package" | "--packages")
    }

    fn is_package_combined_flag(command: &str, arg: &str) -> bool {
        matches!(command, "build" | "check" | "fix" | "test")
            && ((arg.starts_with("-p") && arg.len() > 2)
                || arg.starts_with("--package=")
                || arg.starts_with("--packages="))
    }

    fn value_flag_prefix(command: &str, arg: &str) -> Option<&'static str> {
        // Flags that take a separate next-arg value
        match command {
            "check" => match arg {
                "--changed-strict" => Some("--changed-strict="),
                _ => None,
            },
            "test" => match arg {
                "-E" | "--filter" => Some("--filter="),
                "--test" => Some("--test="),
                "--exclude" => Some("--exclude="),
                "--features" => Some("--features="),
                "--runtime-binary" => Some("--runtime-binary="),
                "--threads" => Some("--threads="),
                "--retries" => Some("--retries="),
                "--timeout" => Some("--timeout="),
                _ => None,
            },
            _ => None,
        }
    }

    fn is_standalone_flag(command: &str, arg: &str) -> bool {
        // Flags that are scope-relevant on their own (no value)
        match command {
            "build" => arg == "--release" || arg.starts_with("--all") || arg == "--dry-run",
            "test" => matches!(
                arg,
                "--debug"
                    | "--heavy"
                    | "--include-ignored"
                    | "--all"
                    | "--lib"
                    | "--list"
                    | "--prime"
                    | "--update-snapshots"
                    | "--no-reuse"
            ),
            "check" | "fix" => {
                matches!(
                    arg,
                    "--all"
                        | "--fix"
                        | "--full"
                        | "--lint"
                        | "--fmt"
                        | "--forbidden"
                        | "--nix"
                        | "--heavy"
                        | "--skip-tests"
                ) || arg.starts_with("--changed-strict=")
            }
            _ => false,
        }
    }

    fn canonical_combined_flag(command: &str, arg: &str) -> Option<String> {
        // Flags with value attached: --package=foo, -p=foo, -Etest(name)
        match command {
            "test" => {
                if let Some(filter) = arg.strip_prefix("-E").filter(|value| !value.is_empty()) {
                    Some(format!("--filter={filter}"))
                } else if arg.starts_with("--filter=")
                    || arg.starts_with("--test=")
                    || arg.starts_with("--exclude=")
                    || arg.starts_with("--features=")
                    || arg.starts_with("--runtime-binary=")
                    || arg.starts_with("--threads=")
                    || arg.starts_with("--retries=")
                    || arg.starts_with("--timeout=")
                    || arg.starts_with("--db-pool-size-env=")
                    || arg.starts_with("--impact-mode=")
                    || arg.starts_with("--impact-planner-version=")
                    || arg.starts_with("--impact-coverage-schema=")
                {
                    Some(arg.to_string())
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    let mut relevant = Vec::new();
    if let Some(marker) = marker {
        relevant.push(marker);
    }
    let mut take_next: Option<&'static str> = None;
    let mut test_arg_index = 0usize;
    let mut iter = args.iter();

    while let Some(arg) = iter.next() {
        if command == "test" && arg == "--" {
            for test_arg in iter {
                relevant.push(format!("--test-arg[{test_arg_index:04}]={test_arg}"));
                test_arg_index += 1;
            }
            break;
        }
        if arg.starts_with("--scope=") {
            continue;
        }
        if is_package_value_flag(command, arg) {
            let _ = iter.next();
            continue;
        }
        if is_package_combined_flag(command, arg) {
            continue;
        }
        if let Some(prefix) = take_next.take() {
            relevant.push(format!("{prefix}{arg}"));
            continue;
        }
        if let Some(prefix) = value_flag_prefix(command, arg) {
            take_next = Some(prefix);
        } else if is_standalone_flag(command, arg) {
            relevant.push(arg.clone());
        } else if command == "test"
            && let Some(test_arg) = arg.strip_prefix("--test-arg=")
        {
            relevant.push(format!("--test-arg[{test_arg_index:04}]={test_arg}"));
            test_arg_index += 1;
        } else if let Some(canonical) = canonical_combined_flag(command, arg) {
            relevant.push(canonical);
        }
    }

    relevant
}

fn canonical_package_scope_marker(command: &str, args: &[String]) -> Option<String> {
    let mut packages = extract_explicit_packages(command, args);
    if packages.is_empty() {
        return None;
    }
    packages.sort();
    packages.dedup();
    Some(format!("--scope=packages:{}", packages.join(",")))
}

/// Describe the command's workload scope using only scope-relevant arguments.
///
/// Unlike `scope_key`, this preserves argument order for human-facing output.
#[must_use]
pub fn describe_scope(command: &str, args: &[String]) -> Option<String> {
    let relevant = extract_scope_args(command, args);
    (!relevant.is_empty()).then(|| relevant.join(" "))
}

/// Tree fingerprint exposed for callers that need it (e.g., recording in history DB).
pub fn current_tree_fingerprint() -> Result<String> {
    tree_fingerprint()
}

/// Scoped tree fingerprint exposed for foreground command recording.
///
/// This records the same scoped input identity that foreground proof reuse
/// uses when it writes HistoryDB rows.
pub fn current_scoped_tree_fingerprint(command: &str, args: &[String]) -> Result<String> {
    scoped_tree_fingerprint(command, args)
}

/// Scope key exposed for callers (e.g., recording in history DB).
#[must_use]
pub fn compute_scope_key(command: &str, args: &[String]) -> String {
    scope_key(command, args)
}

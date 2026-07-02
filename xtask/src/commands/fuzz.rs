//! Fuzzing infrastructure for security testing

use crate::process::ProcessBuilder;
use color_eyre::eyre::{Result, WrapErr, bail};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::output::StructuredError;

/// Fuzz command configuration
#[derive(Debug, Clone, clap::Args)]
pub struct FuzzCommand {
    #[command(subcommand)]
    pub subcommand: FuzzSubcommand,
}

/// Fuzz subcommands
#[derive(Debug, Clone, clap::Subcommand)]
pub enum FuzzSubcommand {
    /// Initialize fuzzing infrastructure for a crate
    Init { package: String },
    /// List available fuzz targets
    List,
    /// Run a specific fuzz target
    Run {
        target: String,
        max_time: u64,
        jobs: Option<usize>,
    },
    /// Show fuzzing corpus for a target
    Corpus { target: String },
}

impl XtaskCommand for FuzzCommand {
    fn name(&self) -> &'static str {
        "fuzz"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            FuzzSubcommand::Init { package } => execute_init(package, ctx),
            FuzzSubcommand::List => execute_list(ctx),
            FuzzSubcommand::Run {
                target,
                max_time,
                jobs,
            } => execute_run(target, *max_time, *jobs, ctx),
            FuzzSubcommand::Corpus { target } => execute_corpus(target, ctx),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            category: Some("security"),
            timeout: Some(std::time::Duration::from_mins(10)), // 10 minutes default
            modifies_state: matches!(self.subcommand, FuzzSubcommand::Init { .. }),
            track_in_history: true,
            history_access: crate::command::HistoryAccessMode::ReadWrite,
        }
    }
}

fn execute_init(package: &str, ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading(&format!("initialize fuzzing for {package}"));

    // Find the crate directory
    let crate_dir = find_crate_dir(package)?;
    let fuzz_dir = crate_dir.join("fuzz");

    if fuzz_dir.exists() {
        if ctx.is_human() {
            println!("Fuzz directory already exists at {}", fuzz_dir.display());
        }
        return Ok(CommandResult::success()
            .with_message(format!("Fuzz directory exists at {}", fuzz_dir.display()))
            .with_warning("Fuzz infrastructure already initialized".to_string())
            .with_duration(ctx.elapsed()));
    }

    // Create fuzz directory structure
    fs::create_dir_all(fuzz_dir.join("fuzz_targets"))?;
    fs::create_dir_all(fuzz_dir.join("corpus"))?;

    // Create Cargo.toml for fuzz crate
    let fuzz_cargo = format!(
        r#"[package]
name = "{package}-fuzz"
version = "0.0.0"
authors = ["Automatically generated"]
publish = false
edition = "2021"

[package.metadata]
cargo-fuzz = true

[dependencies]
libfuzzer-sys = "0.4"
arbitrary = {{ version = "1", features = ["derive"] }}

[dependencies.{package}]
path = ".."

[[bin]]
name = "fuzz_input_validation"
path = "fuzz_targets/fuzz_input_validation.rs"
test = false
doc = false
bench = false

[workspace]
members = ["."]
"#
    );

    fs::write(fuzz_dir.join("Cargo.toml"), fuzz_cargo)?;

    // Create example fuzz target
    let fuzz_target = r"#![no_main]

use libfuzzer_sys::fuzz_target;
use arbitrary::Arbitrary;

// Example fuzz target - customize for your crate
fuzz_target!(|data: &[u8]| {
    // Add fuzzing logic here
    // Example: parse input, validate, etc.
    let _ = std::hint::black_box(data);
});
";

    fs::write(
        fuzz_dir.join("fuzz_targets/fuzz_input_validation.rs"),
        fuzz_target,
    )?;

    // Create .gitignore for fuzz artifacts
    let gitignore = "target/\ncorpus/\nartifacts/\n";
    fs::write(fuzz_dir.join(".gitignore"), gitignore)?;

    if ctx.is_human() {
        println!(
            "Initialized fuzzing infrastructure at {}",
            fuzz_dir.display()
        );
        println!("\nNext steps:");
        println!(
            "  1. Edit {}/fuzz_targets/fuzz_input_validation.rs",
            fuzz_dir.display()
        );
        println!("  2. Run: xtask test --fuzz");
    }

    Ok(CommandResult::success()
        .with_message(format!("Initialized fuzzing for {package}"))
        .with_detail(format!("Fuzz directory: {}", fuzz_dir.display()))
        .with_detail("Created fuzz_input_validation target".to_string())
        .with_duration(ctx.elapsed()))
}

fn execute_list(ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading("available fuzz targets");

    let mut targets = Vec::new();
    for manifest in discover_fuzz_manifests()? {
        targets.extend(parse_fuzz_manifest(&manifest)?);
    }

    if targets.is_empty() {
        if ctx.is_human() {
            println!("No fuzz targets found.");
            println!("\nTo add fuzzing to a crate, run:");
            println!("  Add crate/<name>/fuzz with fuzz_targets/*, then rerun xtask test --fuzz");
        }
        return Ok(CommandResult::success()
            .with_message("No fuzz targets found")
            .with_data(serde_json::json!({
                "target_count": 0u64,
                "targets": []
            }))
            .with_duration(ctx.elapsed()));
    }

    if ctx.is_human() {
        // Group by package
        let mut current_pkg = "";
        for (pkg, target) in &targets {
            if pkg != current_pkg {
                println!("Package: {pkg}");
                current_pkg = pkg;
            }
            println!("  - {target}");
        }
    }

    let mut result = CommandResult::success()
        .with_message(format!("Found {} fuzz targets", targets.len()))
        .with_data(serde_json::json!({
            "target_count": targets.len(),
            "targets": targets
                .iter()
                .map(|(package, target)| serde_json::json!({ "package": package, "target": target }))
                .collect::<Vec<_>>()
        }))
        .with_duration(ctx.elapsed());

    for (pkg, target) in targets {
        result = result.with_detail(format!("{pkg}::{target}"));
    }

    Ok(result)
}

fn execute_run(
    target: &str,
    max_time: u64,
    jobs: Option<usize>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading(&format!("fuzzing {target}"));

    // Validate format before checking tool availability — fail fast on bad input.
    let parts: Vec<&str> = target.split("::").collect();
    if parts.len() != 2 {
        return Ok(CommandResult::failure(StructuredError {
            code: "INVALID_TARGET_FORMAT".to_string(),
            message: format!("Invalid target format: {target}"),
            location: Some("fuzz::run".to_string()),
            suggestion: Some(
                "Use format 'crate::target_name' (e.g., sinex-db::fuzz_input_validation)"
                    .to_string(),
            ),
        }));
    }

    if !ProcessBuilder::cargo()
        .args(["fuzz", "--help"])
        .run_success()?
    {
        return Ok(CommandResult::failure(StructuredError {
            code: "CARGO_FUZZ_MISSING".to_string(),
            message: "cargo-fuzz is not available in PATH".to_string(),
            location: Some("fuzz::run".to_string()),
            suggestion: Some("Add cargo-fuzz to this repo's devshell/flake".to_string()),
        }));
    }

    let package_name = parts[0];
    let target_name = parts[1];

    let fuzz_dir = match find_fuzz_dir_for_package(package_name) {
        Ok(dir) => dir,
        Err(_e) => {
            return Ok(CommandResult::failure(StructuredError {
                code: "FUZZ_PACKAGE_NOT_FOUND".to_string(),
                message: format!("Could not find fuzz package: {package_name}"),
                location: Some("fuzz::run".to_string()),
                suggestion: Some(
                    "Run `xtask test fuzz --list` and use one of the listed package::target pairs"
                        .to_string(),
                ),
            }));
        }
    };

    let mut builder = ProcessBuilder::cargo()
        .current_dir(&fuzz_dir)
        .args(["fuzz", "run"])
        .arg(target_name);
    if let Some(ld_library_path) = fuzz_ld_library_path_from_env() {
        builder = builder.env("LD_LIBRARY_PATH", ld_library_path);
    }

    if max_time > 0 {
        builder = builder.with_timeout(Duration::from_secs(max_time.saturating_add(300)));
        builder = builder.arg("--").arg(format!("-max_total_time={max_time}"));
    }

    if let Some(j) = jobs {
        builder = builder.arg(format!("-jobs={j}"));
    }

    if ctx.is_human() {
        println!("Running in: {}", fuzz_dir.display());
        println!("Target: {target_name}");
        if max_time > 0 {
            println!("Max time: {max_time}s");
        }
        if let Some(j) = jobs {
            println!("Jobs: {j}");
        }
        println!();
    }

    let output = builder
        .with_description(format!("cargo fuzz run {target_name}"))
        .run_capture()
        .with_context(|| "Failed to execute cargo fuzz run")?;

    if !output.success() {
        return Ok(CommandResult::failure(StructuredError {
            code: "FUZZ_RUN_FAILED".to_string(),
            message: format!("Fuzzing failed for {target}"),
            location: Some("fuzz::run".to_string()),
            suggestion: Some(
                "Inspect target output and ensure cargo-fuzz + test dependencies are available"
                    .to_string(),
            ),
        })
        .with_detail(output.stderr)
        .with_duration(ctx.elapsed()));
    }

    if ctx.is_human() && !output.stdout.is_empty() {
        print!("{}", output.stdout);
    }

    Ok(CommandResult::success()
        .with_message(format!("Completed fuzzing {target}"))
        .with_detail(format!("Package: {package_name}"))
        .with_detail(format!("Target: {target_name}"))
        .with_duration(ctx.elapsed()))
}

fn execute_corpus(target: &str, ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading(&format!("corpus for {target}"));

    let parts: Vec<&str> = target.split("::").collect();
    if parts.len() != 2 {
        return Ok(CommandResult::failure(StructuredError {
            code: "INVALID_TARGET_FORMAT".to_string(),
            message: format!("Invalid target format: {target}"),
            location: Some("fuzz::corpus".to_string()),
            suggestion: Some(
                "Use format 'crate::target_name' (e.g., sinex-db::fuzz_validator)".to_string(),
            ),
        }));
    }

    let package_name = parts[0];
    let target_name = parts[1];

    let fuzz_dir = find_fuzz_dir_for_package(package_name)?;
    let corpus_dir = fuzz_dir.join("corpus").join(target_name);

    if !corpus_dir.exists() {
        if ctx.is_human() {
            println!("No corpus found at {}", corpus_dir.display());
            println!("Run the fuzzer first to generate corpus entries.");
        }
        return Ok(CommandResult::success()
            .with_message("No corpus found")
            .with_detail(format!("Expected location: {}", corpus_dir.display()))
            .with_duration(ctx.elapsed()));
    }

    let entries = collect_dir_entry_names(
        &corpus_dir,
        fs::read_dir(&corpus_dir)?.map(|entry| entry.map(|entry| entry.file_name())),
    )?;

    if ctx.is_human() {
        println!("Corpus directory: {}", corpus_dir.display());
        println!("Entries: {}", entries.len());

        for entry in entries.iter().take(10) {
            println!("  - {entry}");
        }

        if entries.len() > 10 {
            println!("  ... and {} more", entries.len() - 10);
        }
    }

    let mut result = CommandResult::success()
        .with_message(format!("Corpus: {} entries", entries.len()))
        .with_detail(format!("Location: {}", corpus_dir.display()))
        .with_duration(ctx.elapsed());

    // Add first 10 entries as details
    for entry in entries.iter().take(10) {
        result = result.with_detail(entry.clone());
    }

    if entries.len() > 10 {
        result = result.with_detail(format!("... and {} more entries", entries.len() - 10));
    }

    Ok(result)
}

fn discover_fuzz_manifests() -> Result<Vec<PathBuf>> {
    let mut manifests = Vec::new();
    let crate_root = crate::config::workspace_root().join("crate");
    for entry in walkdir::WalkDir::new(&crate_root).max_depth(4) {
        let entry =
            entry.wrap_err("failed to walk crate tree while searching for fuzz manifests")?;
        if entry.path().ends_with("fuzz/Cargo.toml") {
            manifests.push(entry.into_path());
        }
    }
    Ok(manifests)
}

fn find_fuzz_dir_for_package(package_name: &str) -> Result<PathBuf> {
    find_fuzz_dir_for_package_in_manifests(package_name, discover_fuzz_manifests()?)
}

fn find_fuzz_dir_for_package_in_manifests(
    package_name: &str,
    manifests: impl IntoIterator<Item = PathBuf>,
) -> Result<PathBuf> {
    for manifest in manifests {
        let content = fs::read_to_string(&manifest)
            .with_context(|| format!("failed to read fuzz manifest {}", manifest.display()))?;
        let fuzz_manifest: FuzzManifest = toml::from_str(&content)
            .with_context(|| format!("failed to parse fuzz manifest {}", manifest.display()))?;
        if fuzz_manifest.package.name == package_name {
            let fuzz_dir = manifest.parent().ok_or_else(|| {
                color_eyre::eyre::eyre!(
                    "fuzz manifest {} has no parent directory",
                    manifest.display()
                )
            })?;
            return Ok(fuzz_dir.to_path_buf());
        }
    }

    bail!("Could not find fuzz package '{package_name}'")
}

fn fuzz_ld_library_path_from_env() -> Option<String> {
    fuzz_ld_library_path(
        std::env::var("NIX_LDFLAGS").ok().as_deref(),
        std::env::var("LD_LIBRARY_PATH").ok().as_deref(),
        libstdcxx_dir_from_cxx().as_deref(),
    )
}

fn libstdcxx_dir_from_cxx() -> Option<String> {
    let output = ProcessBuilder::new("g++")
        .args(["-print-file-name=libstdc++.so.6"])
        .run_capture()
        .ok()?;
    if !output.success() {
        return None;
    }
    let path = output.stdout.trim();
    if path.is_empty() || path == "libstdc++.so.6" {
        return None;
    }
    Path::new(path)
        .parent()
        .map(|parent| parent.to_string_lossy().into_owned())
}

fn fuzz_ld_library_path(
    nix_ldflags: Option<&str>,
    existing: Option<&str>,
    libstdcxx_dir: Option<&str>,
) -> Option<String> {
    let mut paths = Vec::new();
    if let Some(nix_ldflags) = nix_ldflags {
        for part in nix_ldflags.split_whitespace() {
            if let Some(path) = part.strip_prefix("-L")
                && !path.is_empty()
                && !paths.iter().any(|existing| existing == path)
            {
                paths.push(path.to_string());
            }
        }
    }

    if let Some(path) = libstdcxx_dir
        && !path.is_empty()
        && !paths.iter().any(|known| known == path)
    {
        paths.push(path.to_string());
    }

    if let Some(existing) = existing {
        for path in existing.split(':').filter(|path| !path.is_empty()) {
            if !paths.iter().any(|known| known == path) {
                paths.push(path.to_string());
            }
        }
    }

    if paths.is_empty() {
        None
    } else {
        Some(paths.join(":"))
    }
}

fn parse_fuzz_manifest(path: &Path) -> Result<Vec<(String, String)>> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read fuzz manifest {}", path.display()))?;
    let manifest: FuzzManifest = toml::from_str(&content)
        .with_context(|| format!("failed to parse fuzz manifest {}", path.display()))?;

    Ok(manifest
        .bin
        .into_iter()
        .filter(|bin| bin.name.starts_with("fuzz_"))
        .map(|bin| (manifest.package.name.clone(), bin.name))
        .collect())
}

fn collect_dir_entry_names<I>(dir: &Path, entries: I) -> Result<Vec<String>>
where
    I: IntoIterator<Item = std::io::Result<std::ffi::OsString>>,
{
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry
            .with_context(|| format!("failed to read directory entry in {}", dir.display()))?;
        names.push(entry.to_string_lossy().into_owned());
    }
    Ok(names)
}

#[derive(Debug, Deserialize)]
struct FuzzManifest {
    package: FuzzManifestPackage,
    #[serde(default)]
    bin: Vec<FuzzManifestBin>,
}

#[derive(Debug, Deserialize)]
struct FuzzManifestPackage {
    name: String,
}

#[derive(Debug, Deserialize)]
struct FuzzManifestBin {
    name: String,
}

/// Find the directory for a given crate name
fn find_crate_dir(crate_name: &str) -> Result<PathBuf> {
    let workspace_root = crate::config::workspace_root();

    // Try common workspace package locations.
    let mut locations = vec![
        workspace_root.join(format!("crate/{crate_name}")),
        workspace_root.join(format!("tests/{crate_name}")),
    ];
    if crate_name == "xtask" {
        locations.push(workspace_root.join("xtask"));
    }

    for loc in &locations {
        if loc.join("Cargo.toml").exists() {
            return Ok(loc.clone());
        }
    }

    bail!("Could not find crate directory for '{crate_name}'")
}

#[cfg(test)]
#[path = "fuzz_test.rs"]
mod tests;

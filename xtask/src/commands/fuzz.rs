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

    let crate_name = parts[0];
    let target_name = parts[1];

    let crate_dir = match find_crate_dir(crate_name) {
        Ok(dir) => dir,
        Err(_e) => {
            return Ok(CommandResult::failure(StructuredError {
                code: "CRATE_NOT_FOUND".to_string(),
                message: format!("Could not find crate: {crate_name}"),
                location: Some("fuzz::run".to_string()),
                suggestion: Some(
                    "Available locations checked: crate/lib, crate/core, crate/nodes, cli"
                        .to_string(),
                ),
            }));
        }
    };

    let fuzz_dir = crate_dir.join("fuzz");

    if !fuzz_dir.exists() {
        return Ok(CommandResult::failure(StructuredError {
            code: "FUZZ_NOT_INITIALIZED".to_string(),
            message: format!("Fuzz directory not found for {crate_name}"),
            location: Some(format!("fuzz::run({crate_name})")),
            suggestion: Some(format!(
                "Create {crate_name}/fuzz target layout and rerun `xtask test --fuzz`"
            )),
        }));
    }

    let mut builder = ProcessBuilder::cargo()
        .current_dir(&fuzz_dir)
        .args(["fuzz", "run"])
        .arg(target_name);

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
        .with_detail(format!("Crate: {crate_name}"))
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

    let crate_name = parts[0];
    let target_name = parts[1];

    let crate_dir = find_crate_dir(crate_name)?;
    let corpus_dir = crate_dir.join("fuzz").join("corpus").join(target_name);

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

    // Try common locations
    let locations = [
        workspace_root.join(format!("crate/lib/{crate_name}")),
        workspace_root.join(format!("crate/core/{crate_name}")),
        workspace_root.join(format!("crate/nodes/{crate_name}")),
        workspace_root.join(format!("cli/{crate_name}")),
    ];

    for loc in &locations {
        if loc.join("Cargo.toml").exists() {
            return Ok(loc.clone());
        }
    }

    bail!("Could not find crate directory for '{crate_name}'")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;
    use std::ffi::OsString;

    #[sinex_test]
    async fn test_command_name() -> ::xtask::sandbox::TestResult<()> {
        let cmd = FuzzCommand {
            subcommand: FuzzSubcommand::List,
        };
        assert_eq!(cmd.name(), "fuzz");
        Ok(())
    }

    #[sinex_test]
    async fn test_command_metadata() -> ::xtask::sandbox::TestResult<()> {
        let cmd = FuzzCommand {
            subcommand: FuzzSubcommand::Run {
                target: "test::target".to_string(),
                max_time: 60,
                jobs: None,
            },
        };
        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("security"));
        assert!(metadata.timeout.is_some());
        assert!(!metadata.modifies_state);
        Ok(())
    }

    #[sinex_test]
    async fn test_init_modifies_state() -> ::xtask::sandbox::TestResult<()> {
        let cmd = FuzzCommand {
            subcommand: FuzzSubcommand::Init {
                package: "test".to_string(),
            },
        };
        let metadata = cmd.metadata();
        assert!(metadata.modifies_state);
        Ok(())
    }

    #[sinex_test]
    async fn test_list_command() -> ::xtask::sandbox::TestResult<()> {
        let cmd = FuzzCommand {
            subcommand: FuzzSubcommand::List,
        };
        let ctx = crate::command::CommandContext::new(
            crate::output::OutputWriter::new(crate::output::OutputFormat::Silent),
            false,
            None,
            "fuzz",
        );

        // Should not panic even if no fuzz targets exist
        let result = cmd.execute(&ctx).await;
        assert!(result.is_ok());
        Ok(())
    }

    #[sinex_test]
    async fn test_invalid_target_format() -> ::xtask::sandbox::TestResult<()> {
        let cmd = FuzzCommand {
            subcommand: FuzzSubcommand::Run {
                target: "invalid_format".to_string(),
                max_time: 60,
                jobs: None,
            },
        };
        let ctx = crate::command::CommandContext::new(
            crate::output::OutputWriter::new(crate::output::OutputFormat::Silent),
            false,
            None,
            "fuzz",
        );

        let result = cmd.execute(&ctx).await?;
        assert!(result.is_failure());
        assert_eq!(result.errors[0].code, "INVALID_TARGET_FORMAT");
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_fuzz_manifest_extracts_fuzz_bins() -> ::xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let manifest = dir.path().join("Cargo.toml");
        fs::write(
            &manifest,
            r#"[package]
name = "demo-fuzz"

[[bin]]
name = "fuzz_input_validation"

[[bin]]
name = "helper"
"#,
        )?;

        let targets = parse_fuzz_manifest(&manifest)?;
        assert_eq!(
            targets,
            vec![("demo-fuzz".to_string(), "fuzz_input_validation".to_string())]
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_fuzz_manifest_reports_malformed_toml() -> ::xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let manifest = dir.path().join("Cargo.toml");
        fs::write(&manifest, "[package\nname = \"broken\"")?;

        let error = parse_fuzz_manifest(&manifest).expect_err("malformed manifest should surface");
        assert!(error.to_string().contains("failed to parse fuzz manifest"));
        Ok(())
    }

    #[sinex_test]
    async fn test_collect_dir_entry_names_reports_entry_failures()
    -> ::xtask::sandbox::TestResult<()> {
        let error = collect_dir_entry_names(
            Path::new("/tmp/corpus"),
            [
                Ok(OsString::from("seed-1")),
                Err(std::io::Error::other("entry read failed")),
            ],
        )
        .expect_err("entry failure should surface");

        assert!(error.to_string().contains("failed to read directory entry"));
        Ok(())
    }
}

//! Build command for SimpleProcessor crates
//!
//! This module implements `sx build` which compiles a processor crate,
//! optionally in release mode.

use color_eyre::eyre::{eyre, Context, Result};
use std::path::Path;
use std::process::Stdio;
use tracing::{info, warn};

/// Arguments for the build command
#[derive(Debug, Clone)]
pub struct BuildArgs {
    /// Path to the processor crate (defaults to current directory)
    pub path: String,
    /// Build in release mode
    pub release: bool,
}

/// Run the build command
pub async fn run(args: BuildArgs) -> Result<()> {
    let crate_path = Path::new(&args.path);

    // Verify Cargo.toml exists
    let cargo_toml = crate_path.join("Cargo.toml");
    if !cargo_toml.exists() {
        return Err(eyre!(
            "No Cargo.toml found at {}. Is this a Rust crate directory?",
            crate_path.display()
        ));
    }

    // Check if this looks like a SimpleProcessor crate
    let cargo_content =
        std::fs::read_to_string(&cargo_toml).wrap_err("Failed to read Cargo.toml")?;

    if !cargo_content.contains("sinex-node-sdk") && !cargo_content.contains("sinex_node_sdk") {
        warn!(
            "Cargo.toml doesn't reference sinex-node-sdk - this may not be a SimpleProcessor crate"
        );
    }

    // Build the command
    let mut cmd = tokio::process::Command::new("cargo");
    cmd.arg("build");

    if args.release {
        cmd.arg("--release");
    }

    // Set working directory
    cmd.current_dir(crate_path);

    // Inherit stdio for real-time output
    cmd.stdout(Stdio::inherit());
    cmd.stderr(Stdio::inherit());

    info!(
        path = %crate_path.display(),
        release = args.release,
        "Building processor"
    );

    let status = cmd
        .status()
        .await
        .wrap_err("Failed to execute cargo build")?;

    if !status.success() {
        return Err(eyre!(
            "cargo build failed with exit code: {:?}",
            status.code()
        ));
    }

    // Report success
    let binary_dir = if args.release { "release" } else { "debug" };
    let target_dir = crate_path.join("target").join(binary_dir);

    info!(
        path = %crate_path.display(),
        target_dir = %target_dir.display(),
        "Build completed successfully"
    );

    // Try to find the binary name
    if let Ok(cargo_content) = std::fs::read_to_string(&cargo_toml) {
        if let Some(name) = extract_crate_name(&cargo_content) {
            let binary_path = target_dir.join(&name);
            if binary_path.exists() {
                println!("\nBinary: {}", binary_path.display());
                println!("Run with: sx dev {}", crate_path.display());
            }
        }
    }

    Ok(())
}

/// Extract crate name from Cargo.toml content
fn extract_crate_name(cargo_content: &str) -> Option<String> {
    // Simple parser - look for name = "..." in [package] section
    let mut in_package = false;

    for line in cargo_content.lines() {
        let trimmed = line.trim();

        if trimmed == "[package]" {
            in_package = true;
            continue;
        }

        if trimmed.starts_with('[') && trimmed != "[package]" {
            in_package = false;
            continue;
        }

        if in_package && trimmed.starts_with("name") {
            // Parse name = "value"
            if let Some(value) = trimmed.split('=').nth(1) {
                let name = value
                    .trim()
                    .trim_matches('"')
                    .trim_matches('\'')
                    .to_string();
                return Some(name);
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_crate_name() {
        let cargo = r#"
[package]
name = "sinex-my-processor"
version = "0.1.0"

[dependencies]
sinex-node-sdk = { path = "../../lib/sinex-node-sdk" }
"#;
        assert_eq!(
            extract_crate_name(cargo),
            Some("sinex-my-processor".to_string())
        );
    }

    #[test]
    fn test_extract_crate_name_no_package() {
        let cargo = "[lib]\nname = \"something\"";
        assert_eq!(extract_crate_name(cargo), None);
    }
}

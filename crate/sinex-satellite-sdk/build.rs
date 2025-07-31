use std::env;
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile protobuf files first
    tonic_build::compile_protos("proto/ingest.proto")?;

    // Generate version information
    generate_version_info();

    Ok(())
}

fn generate_version_info() {
    // Don't rebuild on every git operation - only on actual version changes
    // println!("cargo:rerun-if-changed=.git/HEAD");
    // println!("cargo:rerun-if-changed=.git/index");

    // Get commit count as patch version (monotonically increasing)
    let commit_count = get_commit_count().unwrap_or(0);

    // Get short commit hash for build metadata
    let commit_hash = get_commit_hash().unwrap_or_else(|| "unknown".into());

    // Get current branch
    let branch = get_current_branch().unwrap_or_else(|| "unknown".into());

    // Check if working directory is dirty
    let is_dirty = is_working_directory_dirty();

    // Manual major.minor, auto patch version
    let major = env::var("SATELLITE_MAJOR_VERSION").unwrap_or_else(|_| "1".to_string());
    let minor = env::var("SATELLITE_MINOR_VERSION").unwrap_or_else(|_| "0".to_string());

    // Create semantic version
    let version = format!("{}.{}.{}", major, minor, commit_count);
    let full_version = if is_dirty {
        format!("{}+{}.dirty", version, commit_hash)
    } else {
        format!("{}+{}", version, commit_hash)
    };

    // Build timestamp
    let build_timestamp = chrono::Utc::now().to_rfc3339();

    // Set environment variables for use in code
    println!("cargo:rustc-env=SATELLITE_VERSION={}", version);
    println!("cargo:rustc-env=SATELLITE_FULL_VERSION={}", full_version);
    println!("cargo:rustc-env=SATELLITE_COMMIT_HASH={}", commit_hash);
    println!("cargo:rustc-env=SATELLITE_COMMIT_COUNT={}", commit_count);
    println!("cargo:rustc-env=SATELLITE_BRANCH={}", branch);
    println!(
        "cargo:rustc-env=SATELLITE_BUILD_TIMESTAMP={}",
        build_timestamp
    );
    println!("cargo:rustc-env=SATELLITE_IS_DIRTY={}", is_dirty);

    // Print version info for build logs
    println!("cargo:warning=Building satellite version: {}", full_version);
}

fn get_commit_count() -> Option<u32> {
    let output = Command::new("git")
        .args(&["rev-list", "--count", "HEAD"])
        .output()
        .ok()?;

    if output.status.success() {
        String::from_utf8(output.stdout).ok()?.trim().parse().ok()
    } else {
        None
    }
}

fn get_commit_hash() -> Option<String> {
    let output = Command::new("git")
        .args(&["rev-parse", "--short=8", "HEAD"])
        .output()
        .ok()?;

    if output.status.success() {
        Some(String::from_utf8(output.stdout).ok()?.trim().to_string())
    } else {
        None
    }
}

fn get_current_branch() -> Option<String> {
    let output = Command::new("git")
        .args(&["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .ok()?;

    if output.status.success() {
        Some(String::from_utf8(output.stdout).ok()?.trim().to_string())
    } else {
        None
    }
}

fn is_working_directory_dirty() -> bool {
    let output = Command::new("git")
        .args(&["status", "--porcelain"])
        .output();

    match output {
        Ok(output) if output.status.success() => !output.stdout.is_empty(),
        _ => false,
    }
}

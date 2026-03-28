//! Build script for xtask.
//!
//! Uses shadow-rs to embed build-time metadata (git hash, build time, etc.)
//! into the binary for version info and debugging.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

fn main() -> shadow_rs::SdResult<()> {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../crate/lib/sinex-primitives/src/events/payloads");

    let contracts_hash = hash_contracts_dir(Path::new(
        "../crate/lib/sinex-primitives/src/events/payloads",
    ))?;
    println!("cargo:rustc-env=SINEX_XTASK_BUILD_CONTRACTS_HASH={contracts_hash}");

    shadow_rs::new()
}

fn hash_contracts_dir(payloads_dir: &Path) -> std::io::Result<String> {
    if !payloads_dir.exists() {
        return Ok("empty".to_string());
    }

    let mut file_contents = BTreeMap::new();
    collect_rust_sources_from_dir(payloads_dir, "", &mut file_contents)?;
    if file_contents.is_empty() {
        return Ok("empty".to_string());
    }

    let mut hasher = blake3::Hasher::new();
    for (name, contents) in file_contents {
        hasher.update(name.as_bytes());
        hasher.update(b"\0");
        hasher.update(&contents);
        hasher.update(b"\0");
    }

    Ok(hasher.finalize().to_hex()[..16].to_string())
}

fn collect_rust_sources_from_dir(
    dir: &Path,
    prefix: &str,
    file_contents: &mut BTreeMap<String, Vec<u8>>,
) -> std::io::Result<()> {
    let entries = fs::read_dir(dir)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();

        if path.is_dir() {
            let child_prefix = if prefix.is_empty() {
                name
            } else {
                format!("{prefix}/{name}")
            };
            collect_rust_sources_from_dir(&path, &child_prefix, file_contents)?;
            continue;
        }

        if !name.ends_with(".rs") {
            continue;
        }

        let key = if prefix.is_empty() {
            name
        } else {
            format!("{prefix}/{name}")
        };
        file_contents.insert(key, fs::read(&path)?);
    }

    Ok(())
}

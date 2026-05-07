//! Dedicated workspace-level integration test crate.
//!
//! Keep repo-wide integration tests in a named workspace member instead of a
//! misleading top-level `src/lib.rs` with no product code behind it.

use std::ffi::OsString;
use std::path::PathBuf;

/// Repository root for tests that need built artifacts or fixture paths.
#[must_use]
pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace-tests manifest should live under the repo root")
}

/// Path to a debug-built workspace binary under the shared target dir.
#[must_use]
pub fn built_binary(name: &str) -> PathBuf {
    let root = repo_root();
    let target_dir = runtime_or_compile_env("CARGO_TARGET_DIR", option_env!("CARGO_TARGET_DIR"))
        .map(PathBuf::from)
        .or_else(|| {
            runtime_or_compile_env("SINEX_DEV_CACHE_ROOT", option_env!("SINEX_DEV_CACHE_ROOT"))
                .map(|cache_root| PathBuf::from(cache_root).join("target"))
        })
        .unwrap_or_else(|| root.join(".sinex/cache/target"));
    let target_dir = if target_dir.is_absolute() {
        target_dir
    } else {
        root.join(target_dir)
    };

    target_dir.join("debug").join(name)
}

fn runtime_or_compile_env(name: &str, compile_value: Option<&'static str>) -> Option<OsString> {
    std::env::var_os(name).or_else(|| compile_value.map(OsString::from))
}

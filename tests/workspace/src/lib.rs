//! Dedicated workspace-level integration test crate.
//!
//! Keep repo-wide integration tests in a named workspace member instead of a
//! misleading top-level `src/lib.rs` with no product code behind it.

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
    repo_root().join(format!(".sinex/target/debug/{name}"))
}

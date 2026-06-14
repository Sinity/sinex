//! Drift gate for the generated source catalog (#1727).
//!
//! The committed `nixos/modules/source-catalog.generated.json` artifact is the
//! Rust→Nix generation seam: the NixOS deployment layer reads it via
//! `builtins.fromJSON`. `xtask` cannot enumerate the source inventory (it does
//! not link the source registrations), so the drift gate lives here — this test
//! re-renders the catalog from the link-time inventory and asserts it matches
//! the committed artifact. If it fails, run `sinexd export-source-catalog`.

use std::path::PathBuf;

use sinexd::sources::catalog_export::{CATALOG_ARTIFACT_PATH, render_catalog};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn source_catalog_artifact_matches_inventory() -> TestResult<()> {
    // crate/sinexd → workspace root is two levels up.
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..");
    let artifact = workspace_root.join(CATALOG_ARTIFACT_PATH);

    let rendered = render_catalog().expect("render source catalog from inventory");
    let committed = std::fs::read_to_string(&artifact).unwrap_or_else(|e| {
        panic!(
            "failed to read committed source catalog at {}: {e}\n\
             run `sinexd export-source-catalog` to generate it",
            artifact.display()
        )
    });

    assert_eq!(
        committed,
        rendered,
        "source catalog artifact is stale — run `sinexd export-source-catalog` to regenerate ({})",
        artifact.display()
    );
    Ok(())
}

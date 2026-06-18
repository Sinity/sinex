//! Source-catalog export — the Rust→Nix generation seam (#1727).
//!
//! The typed source-declaration inventory (`SourceContract` +
//! `SourceRuntimeBinding`, populated at link time by the source registrations
//! this binary hosts) is the authoring source of truth. This module serializes
//! that inventory into a committed JSON artifact that the NixOS deployment layer
//! consumes via `builtins.fromJSON` to derive per-source `services.sinex.sources`
//! defaults and systemd unit limits — so deployment shape is generated from the
//! Rust catalog rather than hand-maintained twice.
//!
//! `xtask` cannot enumerate this inventory (it does not link the source
//! registrations), so generation lives here in `sinexd` and the drift gate is a
//! `sinexd` test that compares the rendered catalog against the committed file.

use std::path::Path;

use serde::Serialize;
use sinex_primitives::source_contracts::{
    ResourceBudgetSpec, ResourceLimits, SourceContract, SourceRuntimeBinding, all_source_contracts,
    source_runtime_bindings,
};

/// Repo-relative path of the committed catalog artifact consumed by Nix.
pub const CATALOG_ARTIFACT_PATH: &str = "nixos/modules/source-catalog.generated.json";

/// Bumped when the catalog *shape* changes (not its contents).
const CATALOG_SCHEMA_VERSION: u32 = 2;

/// One source's full typed declaration: semantic contract + deployment binding,
/// the concrete resource ceiling, and the richer package budget derived from
/// the binding's `ResourceProfile`.
#[derive(Debug, Serialize)]
struct CatalogEntry<'a> {
    contract: &'a SourceContract,
    binding: Option<&'a SourceRuntimeBinding>,
    /// `binding.resource_profile.limits()` lifted into the artifact so Nix does
    /// not need to re-encode the profile→limits mapping.
    resource_limits: Option<ResourceLimits>,
    /// `binding.resource_budget()` lifted into the artifact so package
    /// completeness and runtime-pressure tooling can consume the same typed
    /// budget contract as the Rust runtime.
    resource_budget: Option<ResourceBudgetSpec>,
}

#[derive(Debug, Serialize)]
struct SourceCatalog<'a> {
    schema_version: u32,
    entries: Vec<CatalogEntry<'a>>,
}

/// Build the catalog from the link-time inventory, joined by `source_id` and
/// ordered by contract id for deterministic output.
fn build_catalog() -> SourceCatalog<'static> {
    let bindings: Vec<&'static SourceRuntimeBinding> = source_runtime_bindings().collect();

    let mut contracts: Vec<&'static SourceContract> = all_source_contracts().collect();
    contracts.sort_by(|a, b| a.id.cmp(b.id));

    let entries = contracts
        .into_iter()
        .map(|contract| {
            let binding = bindings
                .iter()
                .copied()
                .find(|b| b.source_id == contract.id);
            CatalogEntry {
                contract,
                binding,
                resource_limits: binding.map(|b| b.resource_profile.limits()),
                resource_budget: binding.map(|b| b.resource_budget()),
            }
        })
        .collect();

    SourceCatalog {
        schema_version: CATALOG_SCHEMA_VERSION,
        entries,
    }
}

/// Render the catalog to deterministic pretty JSON with a trailing newline.
pub fn render_catalog() -> serde_json::Result<String> {
    let catalog = build_catalog();
    Ok(serde_json::to_string_pretty(&catalog)? + "\n")
}

/// Write (or, with `check_only`, compare) the catalog artifact at `output`.
///
/// Returns `Ok(true)` when the on-disk artifact differs from the freshly
/// rendered catalog (i.e. it was rewritten, or — under `check_only` — is stale).
pub fn export_catalog(output: &Path, check_only: bool) -> std::io::Result<bool> {
    let rendered =
        render_catalog().map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let current = std::fs::read_to_string(output).ok();
    let changed = current.as_deref() != Some(rendered.as_str());

    if changed && !check_only {
        if let Some(parent) = output.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(output, &rendered)?;
    }

    Ok(changed)
}

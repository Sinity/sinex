//! Source-unit descriptor rendering and validation.
//!
//! Source units are logical capture leaves owned by a runner pack.  This command
//! renders the descriptor manifest and checks the physical-build contract that a
//! new source unit can be added without a new crate, binary, Nix output, or SQLx
//! validation surface unless the descriptor records an explicit exception.

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::workspace_root;
use crate::output::StructuredError;
use color_eyre::eyre::{Context, Result};
use serde::Serialize;
use sinex_primitives::proof::{self, ProofObligation, SourceUnitDescriptor};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// Source-unit manifest operations.
#[derive(Debug, Clone, clap::Args)]
pub struct SourceUnitsCommand {
    #[command(subcommand)]
    pub subcommand: SourceUnitsSubcommand,
}

#[derive(Debug, Clone, clap::Subcommand)]
pub enum SourceUnitsSubcommand {
    /// Render the source-unit manifest.
    Render {
        /// Output file (default: docs/source-units.json)
        #[arg(long)]
        output: Option<PathBuf>,

        /// Print to stdout instead of writing a file.
        #[arg(long)]
        stdout: bool,
    },

    /// Validate source-unit descriptors and the generated manifest.
    Check {
        /// Manifest path to check (default: docs/source-units.json)
        #[arg(long)]
        output: Option<PathBuf>,
    },
}

impl XtaskCommand for SourceUnitsCommand {
    fn name(&self) -> &'static str {
        "source-units"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            SourceUnitsSubcommand::Render { output, stdout } => {
                execute_render(output.as_deref(), *stdout, ctx)
            }
            SourceUnitsSubcommand::Check { output } => execute_check(output.as_deref(), ctx),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        match &self.subcommand {
            SourceUnitsSubcommand::Render { stdout: true, .. } => CommandMetadata::analysis(),
            SourceUnitsSubcommand::Render { .. } => {
                CommandMetadata::analysis().with_state_mutation(true)
            }
            SourceUnitsSubcommand::Check { .. } => CommandMetadata::check(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct SourceUnitManifest {
    schema_version: u32,
    issue_refs: Vec<&'static str>,
    source_units: Vec<SourceUnitDescriptor>,
    runner_packs: Vec<RunnerPackManifest>,
    services: Vec<SourceUnitServiceManifest>,
    package_impact: SourceUnitImpactReport,
    proof_obligations: Vec<ProofObligation>,
    descriptor_contract: DescriptorContract,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct RunnerPackManifest {
    id: String,
    source_unit_ids: Vec<String>,
    shared_binary: String,
    source_unit_count: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct SourceUnitServiceManifest {
    source_unit_id: String,
    service_name: String,
    runner_pack: String,
    binary: String,
    checkpoint_identity: String,
    control_identity: String,
    host_identity: &'static str,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct SourceUnitImpactReport {
    total_source_units: usize,
    zero_crate_impact: usize,
    zero_binary_impact: usize,
    zero_nix_output_impact: usize,
    zero_derivation_impact: usize,
    zero_sqlx_validation_impact: usize,
    dedicated_build_rationales: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct DescriptorContract {
    registration: &'static str,
    runner_pack_selection: &'static str,
    runtime_identity_axes: &'static [&'static str],
    non_rust_source_unit_mode: &'static str,
    normal_source_unit_physical_impact: &'static str,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct SourceUnitValidation {
    source_unit_count: usize,
    runner_pack_count: usize,
    duplicate_subjects: Vec<String>,
    duplicate_ids: Vec<String>,
    empty_fields: Vec<String>,
    invalid_physical_impact: Vec<String>,
    runner_packs_with_multiple_units: Vec<String>,
    stale_manifest: bool,
}

fn execute_render(
    output: Option<&Path>,
    to_stdout: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let workspace = workspace_root();
    let manifest = build_source_unit_manifest();
    let content = render_source_unit_manifest_json(&manifest)?;
    let dest = output.map_or_else(|| default_manifest_path(&workspace), Path::to_path_buf);

    if to_stdout {
        print!("{content}");
        return Ok(CommandResult::success()
            .with_message("source-unit manifest printed to stdout")
            .with_data(serde_json::to_value(manifest)?)
            .with_duration(ctx.elapsed()));
    }

    let changed = write_if_changed(&dest, &content)?;
    let message = if changed {
        "source-unit manifest generated"
    } else {
        "source-unit manifest already up to date"
    };

    Ok(CommandResult::success()
        .with_message(message)
        .with_data(serde_json::json!({
            "path": dest,
            "changed": changed,
            "manifest": manifest,
        }))
        .with_duration(ctx.elapsed()))
}

fn execute_check(output: Option<&Path>, ctx: &CommandContext) -> Result<CommandResult> {
    let workspace = workspace_root();
    let dest = output.map_or_else(|| default_manifest_path(&workspace), Path::to_path_buf);
    let manifest = build_source_unit_manifest();
    let rendered = render_source_unit_manifest_json(&manifest)?;
    let stale_manifest = std::fs::read_to_string(&dest).ok().as_deref() != Some(rendered.as_str());
    let validation = validate_source_units(&manifest.source_units, stale_manifest);
    let has_failures = !validation.duplicate_subjects.is_empty()
        || !validation.duplicate_ids.is_empty()
        || !validation.empty_fields.is_empty()
        || !validation.invalid_physical_impact.is_empty()
        || validation.runner_packs_with_multiple_units.is_empty()
        || validation.stale_manifest;

    let result = if has_failures {
        CommandResult::failure(StructuredError {
            code: "SOURCE_UNIT_MANIFEST_INVALID".to_string(),
            message: "source-unit descriptors or generated manifest are invalid".to_string(),
            location: Some(dest.display().to_string()),
            suggestion: Some(
                "Run `xtask source-units render` after fixing descriptors".to_string(),
            ),
        })
        .with_message("source-unit manifest check failed")
    } else {
        CommandResult::success().with_message("source-unit manifest check passed")
    };

    Ok(result
        .with_data(serde_json::json!({
            "validation": validation,
            "manifest": manifest,
        }))
        .with_duration(ctx.elapsed()))
}

fn build_source_unit_manifest() -> SourceUnitManifest {
    let mut source_units = proof::source_unit_descriptors()
        .copied()
        .collect::<Vec<_>>();
    source_units.sort_by(|left, right| left.subject.as_str().cmp(right.subject.as_str()));

    let mut obligations = proof::obligations()
        .copied()
        .filter(|obligation| obligation.subject.as_str().starts_with("source_unit:"))
        .collect::<Vec<_>>();
    obligations.sort_by(|left, right| left.id.cmp(right.id));

    SourceUnitManifest {
        schema_version: proof::PROOF_CATALOG_SCHEMA_VERSION,
        issue_refs: vec!["issue:518", "issue:486", "issue:369"],
        runner_packs: runner_pack_manifests(&source_units),
        services: service_manifests(&source_units),
        package_impact: package_impact_report(&source_units),
        proof_obligations: obligations,
        descriptor_contract: DescriptorContract {
            registration: "Rust descriptors register with inventory::submit!; no new Cargo member is required for a normal source unit.",
            runner_pack_selection: "Runner packs select source units by stable source_unit_id through --source-unit and share one binary per pack.",
            runtime_identity_axes: &[
                "source_unit",
                "service_instance",
                "runner_pack",
                "host",
                "run_id",
            ],
            non_rust_source_unit_mode: "implementation_mode records external, off-host, or non-Rust units; dedicated build surfaces require rationale.",
            normal_source_unit_physical_impact: "crate_impact=0, binary_impact=0, nix_output_impact=0, derivation_impact=0, sqlx_validation_impact=0",
        },
        source_units,
    }
}

fn runner_pack_manifests(source_units: &[SourceUnitDescriptor]) -> Vec<RunnerPackManifest> {
    let mut by_pack = BTreeMap::<String, Vec<String>>::new();
    for unit in source_units {
        by_pack
            .entry(unit.runner_pack.to_string())
            .or_default()
            .push(unit.id.to_string());
    }

    by_pack
        .into_iter()
        .map(|(id, mut source_unit_ids)| {
            source_unit_ids.sort();
            RunnerPackManifest {
                shared_binary: runner_pack_binary(&id).to_string(),
                source_unit_count: source_unit_ids.len(),
                id,
                source_unit_ids,
            }
        })
        .collect()
}

fn service_manifests(source_units: &[SourceUnitDescriptor]) -> Vec<SourceUnitServiceManifest> {
    source_units
        .iter()
        .map(|unit| SourceUnitServiceManifest {
            source_unit_id: unit.id.to_string(),
            service_name: source_unit_service_name(unit.id),
            runner_pack: unit.runner_pack.to_string(),
            binary: runner_pack_binary(unit.runner_pack).to_string(),
            checkpoint_identity: unit.id.to_string(),
            control_identity: unit.id.to_string(),
            host_identity: "runtime_hostname",
        })
        .collect()
}

fn package_impact_report(source_units: &[SourceUnitDescriptor]) -> SourceUnitImpactReport {
    SourceUnitImpactReport {
        total_source_units: source_units.len(),
        zero_crate_impact: source_units
            .iter()
            .filter(|unit| unit.crate_impact == "0")
            .count(),
        zero_binary_impact: source_units
            .iter()
            .filter(|unit| unit.binary_impact == "0")
            .count(),
        zero_nix_output_impact: source_units
            .iter()
            .filter(|unit| unit.nix_output_impact == "0")
            .count(),
        zero_derivation_impact: source_units
            .iter()
            .filter(|unit| unit.derivation_impact == "0")
            .count(),
        zero_sqlx_validation_impact: source_units
            .iter()
            .filter(|unit| unit.sqlx_validation_impact == "0")
            .count(),
        dedicated_build_rationales: source_units
            .iter()
            .filter_map(|unit| {
                unit.dedicated_build_rationale
                    .map(|rationale| format!("{}: {rationale}", unit.id))
            })
            .collect(),
    }
}

fn validate_source_units(
    source_units: &[SourceUnitDescriptor],
    stale_manifest: bool,
) -> SourceUnitValidation {
    let duplicate_subjects = duplicates(source_units.iter().map(|unit| unit.subject.as_str()));
    let duplicate_ids = duplicates(source_units.iter().map(|unit| unit.id));
    let empty_fields = source_units
        .iter()
        .flat_map(required_empty_fields)
        .collect::<Vec<_>>();
    let invalid_physical_impact = source_units
        .iter()
        .filter(|unit| {
            (unit.crate_impact != "0"
                || unit.binary_impact != "0"
                || unit.nix_output_impact != "0"
                || unit.derivation_impact != "0"
                || unit.sqlx_validation_impact != "0")
                && unit.dedicated_build_rationale.is_none()
        })
        .map(|unit| unit.id.to_string())
        .collect::<Vec<_>>();
    let runner_packs_with_multiple_units = runner_pack_manifests(source_units)
        .into_iter()
        .filter(|pack| pack.source_unit_count >= 2)
        .map(|pack| pack.id)
        .collect::<Vec<_>>();

    SourceUnitValidation {
        source_unit_count: source_units.len(),
        runner_pack_count: source_units
            .iter()
            .map(|unit| unit.runner_pack)
            .collect::<BTreeSet<_>>()
            .len(),
        duplicate_subjects,
        duplicate_ids,
        empty_fields,
        invalid_physical_impact,
        runner_packs_with_multiple_units,
        stale_manifest,
    }
}

fn duplicates<'a>(values: impl Iterator<Item = &'a str>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut duplicates = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            duplicates.insert(value);
        }
    }
    duplicates.into_iter().map(str::to_string).collect()
}

fn required_empty_fields(unit: &SourceUnitDescriptor) -> Vec<String> {
    [
        ("id", unit.id),
        ("domain", unit.domain),
        ("role", unit.role),
        ("acquisition_shape", unit.acquisition_shape),
        ("material_policy", unit.material_policy),
        ("checkpoint_policy", unit.checkpoint_policy),
        ("occurrence_policy", unit.occurrence_policy),
        ("output_event_type", unit.output_event_type),
        ("privacy_context", unit.privacy_context),
        ("resource_profile", unit.resource_profile),
        ("access_policy", unit.access_policy),
        ("service_policy", unit.service_policy),
        ("runner_pack", unit.runner_pack),
        ("package_impact", unit.package_impact),
        ("implementation_mode", unit.implementation_mode),
        ("crate_impact", unit.crate_impact),
        ("binary_impact", unit.binary_impact),
        ("nix_output_impact", unit.nix_output_impact),
        ("derivation_impact", unit.derivation_impact),
        ("sqlx_validation_impact", unit.sqlx_validation_impact),
    ]
    .into_iter()
    .filter(|(_, value)| value.is_empty())
    .map(|(field, _)| format!("{}:{field}", unit.subject.as_str()))
    .collect()
}

fn runner_pack_binary(runner_pack: &str) -> &str {
    match runner_pack {
        "terminal" => "sinex-terminal-ingestor",
        other => other,
    }
}

fn source_unit_service_name(source_unit_id: &str) -> String {
    format!("sinex-source@{source_unit_id}")
}

fn default_manifest_path(workspace: &Path) -> PathBuf {
    workspace.join("docs/source-units.json")
}

fn render_source_unit_manifest_json(manifest: &SourceUnitManifest) -> Result<String> {
    let mut rendered = serde_json::to_string_pretty(manifest)
        .context("failed to serialize source-unit manifest")?;
    rendered.push('\n');
    Ok(rendered)
}

fn write_if_changed(path: &Path, content: &str) -> Result<bool> {
    let changed = std::fs::read_to_string(path).ok().as_deref() != Some(content);
    if changed {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        std::fs::write(path, content)
            .with_context(|| format!("failed to write {}", path.display()))?;
    }
    Ok(changed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::prelude::*;

    #[sinex_test]
    async fn source_unit_manifest_groups_terminal_runner_pack() -> TestResult<()> {
        let manifest = build_source_unit_manifest();
        let terminal = manifest
            .runner_packs
            .iter()
            .find(|pack| pack.id == "terminal")
            .expect("terminal runner pack is registered");

        assert!(terminal.source_unit_count >= 2);
        assert_eq!(terminal.shared_binary, "sinex-terminal-ingestor");
        assert!(
            terminal
                .source_unit_ids
                .contains(&"terminal.atuin-history".to_string())
        );
        Ok(())
    }

    #[sinex_test]
    async fn source_unit_validation_requires_zero_impact_or_rationale() -> TestResult<()> {
        let manifest = build_source_unit_manifest();
        let validation = validate_source_units(&manifest.source_units, false);

        assert!(validation.duplicate_subjects.is_empty());
        assert!(validation.duplicate_ids.is_empty());
        assert!(validation.empty_fields.is_empty());
        assert!(validation.invalid_physical_impact.is_empty());
        assert!(
            validation
                .runner_packs_with_multiple_units
                .contains(&"terminal".to_string())
        );
        Ok(())
    }
}

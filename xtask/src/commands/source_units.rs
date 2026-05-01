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
use sinex_primitives::proof::{self, ProofObligation};
use sinex_primitives::source_unit::{
    self, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
};
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
struct SourceUnitDescriptor {
    subject: String,
    id: String,
    domain: String,
    role: String,
    modes: Vec<String>,
    acquisition_shape: String,
    material_policy: String,
    checkpoint_policy: String,
    occurrence_policy: String,
    output_event_type: String,
    output_event_types: Vec<SourceUnitEventType>,
    privacy_context: String,
    retention_policy: String,
    resource_profile: String,
    access_policy: String,
    service_policy: String,
    runner_pack: String,
    package_impact: String,
    implementation_mode: String,
    proof_obligations: Vec<String>,
    crate_impact: String,
    binary_impact: String,
    nix_output_impact: String,
    derivation_impact: String,
    sqlx_validation_impact: String,
    dedicated_build_rationale: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct SourceUnitEventType {
    source: String,
    event_type: String,
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
    crate::source_unit_inventory::link_source_unit_inventories();

    let mut source_units = source_unit::all_source_units()
        .map(canonical_source_unit_descriptor)
        .collect::<Vec<_>>();
    source_units.extend(
        proof::source_unit_descriptors()
            .copied()
            .map(legacy_proof_source_unit_descriptor),
    );
    source_units.sort_by(|left, right| left.subject.cmp(&right.subject));

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

fn canonical_source_unit_descriptor(
    unit: &'static source_unit::SourceUnitDescriptor,
) -> SourceUnitDescriptor {
    SourceUnitDescriptor {
        subject: format!("source_unit:{}", unit.id),
        id: unit.id.to_string(),
        domain: unit.namespace.to_string(),
        role: role_for(unit).to_string(),
        modes: unit
            .horizons
            .iter()
            .map(|horizon| horizon_name(*horizon).to_string())
            .collect(),
        acquisition_shape: checkpoint_family_name(unit.checkpoint_family).to_string(),
        material_policy: material_policy_for(unit).to_string(),
        checkpoint_policy: checkpoint_policy(unit.checkpoint_family),
        occurrence_policy: occurrence_policy(unit.occurrence_identity),
        output_event_type: unit
            .event_types
            .iter()
            .map(|(source, event_type)| format!("{source}/{event_type}"))
            .collect::<Vec<_>>()
            .join(","),
        output_event_types: unit
            .event_types
            .iter()
            .map(|(source, event_type)| SourceUnitEventType {
                source: (*source).to_string(),
                event_type: (*event_type).to_string(),
            })
            .collect(),
        privacy_context: privacy_tier_name(unit.privacy_tier).to_string(),
        retention_policy: retention_policy(unit.retention),
        resource_profile: runtime_shape_name(unit.runtime_shape).to_string(),
        access_policy: unit.access_policy.to_string(),
        service_policy: service_policy(unit.runtime_shape).to_string(),
        runner_pack: unit.runner_pack.to_string(),
        package_impact: unit.package_impact.to_string(),
        implementation_mode: unit.implementation_mode.to_string(),
        proof_obligations: unit
            .proof_obligations
            .iter()
            .map(|obligation| (*obligation).to_string())
            .collect(),
        crate_impact: unit.build_impact.crate_impact.to_string(),
        binary_impact: unit.build_impact.binary_impact.to_string(),
        nix_output_impact: unit.build_impact.nix_output_impact.to_string(),
        derivation_impact: unit.build_impact.derivation_impact.to_string(),
        sqlx_validation_impact: unit.build_impact.sqlx_validation_impact.to_string(),
        dedicated_build_rationale: unit
            .build_impact
            .dedicated_build_rationale
            .map(str::to_string),
    }
}

fn legacy_proof_source_unit_descriptor(unit: proof::SourceUnitDescriptor) -> SourceUnitDescriptor {
    SourceUnitDescriptor {
        subject: unit.subject.as_str().to_string(),
        id: unit.id.to_string(),
        domain: unit.domain.to_string(),
        role: unit.role.to_string(),
        modes: unit.modes.iter().map(|mode| (*mode).to_string()).collect(),
        acquisition_shape: unit.acquisition_shape.to_string(),
        material_policy: unit.material_policy.to_string(),
        checkpoint_policy: unit.checkpoint_policy.to_string(),
        occurrence_policy: unit.occurrence_policy.to_string(),
        output_event_type: unit.output_event_type.to_string(),
        output_event_types: output_event_types_from_legacy(unit.output_event_type),
        privacy_context: unit.privacy_context.to_string(),
        retention_policy: "not_declared_in_legacy_proof_descriptor".to_string(),
        resource_profile: unit.resource_profile.to_string(),
        access_policy: unit.access_policy.to_string(),
        service_policy: unit.service_policy.to_string(),
        runner_pack: unit.runner_pack.to_string(),
        package_impact: unit.package_impact.to_string(),
        implementation_mode: unit.implementation_mode.to_string(),
        proof_obligations: unit
            .proof_obligations
            .iter()
            .map(|obligation| (*obligation).to_string())
            .collect(),
        crate_impact: unit.crate_impact.to_string(),
        binary_impact: unit.binary_impact.to_string(),
        nix_output_impact: unit.nix_output_impact.to_string(),
        derivation_impact: unit.derivation_impact.to_string(),
        sqlx_validation_impact: unit.sqlx_validation_impact.to_string(),
        dedicated_build_rationale: unit.dedicated_build_rationale.map(str::to_string),
    }
}

fn output_event_types_from_legacy(output_event_type: &str) -> Vec<SourceUnitEventType> {
    output_event_type
        .split(',')
        .filter_map(|pair| pair.split_once('/'))
        .map(|(source, event_type)| SourceUnitEventType {
            source: source.to_string(),
            event_type: event_type.to_string(),
        })
        .collect()
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
            service_name: source_unit_service_name(&unit.id),
            runner_pack: unit.runner_pack.to_string(),
            binary: runner_pack_binary(&unit.runner_pack).to_string(),
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
                    .as_ref()
                    .map(|rationale| format!("{}: {rationale}", unit.id))
            })
            .collect(),
    }
}

fn role_for(unit: &source_unit::SourceUnitDescriptor) -> &'static str {
    if unit.namespace == "derived" {
        "derived_node"
    } else {
        "source_adapter"
    }
}

fn material_policy_for(unit: &source_unit::SourceUnitDescriptor) -> &'static str {
    if unit.namespace == "derived" {
        "synthesis_provenance"
    } else {
        "source_material_provenance"
    }
}

fn checkpoint_family_name(family: CheckpointFamily) -> &'static str {
    match family {
        CheckpointFamily::AppendStream => "append_stream",
        CheckpointFamily::MutableSnapshot { .. } => "mutable_snapshot",
        CheckpointFamily::Journal => "journal",
        CheckpointFamily::Polling => "polling",
        CheckpointFamily::LiveObservation => "live_observation",
    }
}

fn checkpoint_policy(family: CheckpointFamily) -> String {
    match family {
        CheckpointFamily::AppendStream => "append_stream_cursor".to_string(),
        CheckpointFamily::MutableSnapshot {
            backing_store_kind,
            occurrence_anchor,
        } => format!("mutable_snapshot:{backing_store_kind}:{occurrence_anchor}"),
        CheckpointFamily::Journal => "journal_cursor".to_string(),
        CheckpointFamily::Polling => "polling_diff".to_string(),
        CheckpointFamily::LiveObservation => "live_observation_state".to_string(),
    }
}

fn occurrence_policy(identity: OccurrenceIdentity) -> String {
    match identity {
        OccurrenceIdentity::Uuid5From(source) => format!("uuid5:{source}"),
        OccurrenceIdentity::Natural => "natural_key".to_string(),
        OccurrenceIdentity::Anchor => "material_anchor".to_string(),
    }
}

fn horizon_name(horizon: Horizon) -> &'static str {
    match horizon {
        Horizon::Continuous => "continuous",
        Horizon::Historical => "historical",
    }
}

fn privacy_tier_name(tier: PrivacyTier) -> &'static str {
    match tier {
        PrivacyTier::Public => "public",
        PrivacyTier::Sensitive => "sensitive",
        PrivacyTier::Secret => "secret",
    }
}

fn retention_policy(policy: RetentionPolicy) -> String {
    match policy {
        RetentionPolicy::Forever => "forever".to_string(),
        RetentionPolicy::Days { days } => format!("days:{days}"),
        RetentionPolicy::Tiered {
            hot_days,
            warm_days,
        } => format!("tiered:hot={hot_days}:warm={warm_days}"),
    }
}

fn runtime_shape_name(shape: RuntimeShape) -> &'static str {
    match shape {
        RuntimeShape::Continuous => "continuous",
        RuntimeShape::OnDemand => "on_demand",
        RuntimeShape::Scheduled => "scheduled",
    }
}

fn service_policy(shape: RuntimeShape) -> &'static str {
    match shape {
        RuntimeShape::Continuous => "dedicated_instance:on-failure",
        RuntimeShape::OnDemand => "invoked_on_demand",
        RuntimeShape::Scheduled => "scheduled_runner",
    }
}

fn validate_source_units(
    source_units: &[SourceUnitDescriptor],
    stale_manifest: bool,
) -> SourceUnitValidation {
    let duplicate_subjects = duplicates(source_units.iter().map(|unit| unit.subject.as_str()));
    let duplicate_ids = duplicates(source_units.iter().map(|unit| unit.id.as_str()));
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
            .map(|unit| unit.runner_pack.as_str())
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
        ("id", unit.id.as_str()),
        ("domain", unit.domain.as_str()),
        ("role", unit.role.as_str()),
        ("acquisition_shape", unit.acquisition_shape.as_str()),
        ("material_policy", unit.material_policy.as_str()),
        ("checkpoint_policy", unit.checkpoint_policy.as_str()),
        ("occurrence_policy", unit.occurrence_policy.as_str()),
        ("output_event_type", unit.output_event_type.as_str()),
        ("privacy_context", unit.privacy_context.as_str()),
        ("retention_policy", unit.retention_policy.as_str()),
        ("resource_profile", unit.resource_profile.as_str()),
        ("access_policy", unit.access_policy.as_str()),
        ("service_policy", unit.service_policy.as_str()),
        ("runner_pack", unit.runner_pack.as_str()),
        ("package_impact", unit.package_impact.as_str()),
        ("implementation_mode", unit.implementation_mode.as_str()),
        ("crate_impact", unit.crate_impact.as_str()),
        ("binary_impact", unit.binary_impact.as_str()),
        ("nix_output_impact", unit.nix_output_impact.as_str()),
        ("derivation_impact", unit.derivation_impact.as_str()),
        (
            "sqlx_validation_impact",
            unit.sqlx_validation_impact.as_str(),
        ),
    ]
    .into_iter()
    .filter(|(_, value)| value.is_empty())
    .map(|(field, _)| format!("{}:{field}", unit.subject.as_str()))
    .collect()
}

fn runner_pack_binary(runner_pack: &str) -> &str {
    match runner_pack {
        "analytics" => "sinex-analytics-automaton",
        "browser" => "sinex-browser-ingestor",
        "daily" => "sinex-daily-summarizer",
        "desktop" => "sinex-desktop-ingestor",
        "document" => "sinex-document-ingestor",
        "fs" => "sinex-fs-ingestor",
        "health" => "sinex-health-automaton",
        "hourly" => "sinex-hourly-summarizer",
        "session" => "sinex-session-detector",
        "system" => "sinex-system-ingestor",
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
    async fn source_unit_manifest_includes_node_crate_descriptors() -> TestResult<()> {
        let manifest = build_source_unit_manifest();
        let source_unit_ids = manifest
            .source_units
            .iter()
            .map(|unit| unit.id.as_str())
            .collect::<BTreeSet<_>>();

        for expected in [
            "fs",
            "desktop",
            "browser",
            "system",
            "document",
            "terminal",
            "terminal-canonicalizer",
            "session-detector",
            "analytics",
            "health",
        ] {
            assert!(
                source_unit_ids.contains(expected),
                "source-unit manifest should include descriptor registered by {expected}"
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn canonical_source_unit_fields_drive_manifest_contract() -> TestResult<()> {
        let manifest = build_source_unit_manifest();
        let desktop = manifest
            .source_units
            .iter()
            .find(|unit| unit.id == "desktop")
            .expect("desktop descriptor should be rendered from canonical registry");
        assert_eq!(desktop.runner_pack, "desktop");
        assert_eq!(desktop.access_policy, "target_runtime_bridge:desktop");
        assert_eq!(desktop.package_impact, "no_new_output");
        assert_eq!(desktop.implementation_mode, "rust_in_pack:desktop");
        assert_eq!(desktop.crate_impact, "0");
        assert_eq!(desktop.binary_impact, "0");
        assert_eq!(desktop.nix_output_impact, "0");
        assert_eq!(desktop.derivation_impact, "0");
        assert_eq!(desktop.sqlx_validation_impact, "0");

        let terminal_canonicalizer = manifest
            .source_units
            .iter()
            .find(|unit| unit.id == "terminal-canonicalizer")
            .expect("terminal canonicalizer descriptor should be present");
        assert_eq!(terminal_canonicalizer.runner_pack, "terminal");
        assert_eq!(
            terminal_canonicalizer.implementation_mode,
            "rust_in_pack:terminal"
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

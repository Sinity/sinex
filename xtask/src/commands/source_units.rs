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
use sinex_primitives::events::schema_registry::get_all_payloads;
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
    unmapped_runner_packs: Vec<String>,
    invalid_physical_impact: Vec<String>,
    invalid_output_event_pairs: Vec<String>,
    unbacked_output_event_pairs: Vec<String>,
    missing_output_event_pair_backing: Vec<String>,
    exempted_output_event_pair_backing: Vec<String>,
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
        || !validation.unmapped_runner_packs.is_empty()
        || !validation.invalid_physical_impact.is_empty()
        || !validation.invalid_output_event_pairs.is_empty()
        || !validation.unbacked_output_event_pairs.is_empty()
        || !validation.missing_output_event_pair_backing.is_empty()
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
                shared_binary: runner_pack_binary(&id).unwrap_or(&id).to_string(),
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
            binary: runner_pack_binary(&unit.runner_pack)
                .unwrap_or(&unit.runner_pack)
                .to_string(),
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
    let unmapped_runner_packs = source_units
        .iter()
        .map(|unit| unit.runner_pack.as_str())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|runner_pack| runner_pack_binary(runner_pack).is_none())
        .map(ToString::to_string)
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
    let payload_pairs = get_all_payloads()
        .map(|payload| (payload.source, payload.event_type))
        .collect::<BTreeSet<_>>();
    let invalid_output_event_pairs = source_units
        .iter()
        .flat_map(|unit| {
            unit.output_event_types.iter().filter_map(|event_pair| {
                let pair = (event_pair.source.as_str(), event_pair.event_type.as_str());
                (!payload_pairs.contains(&pair)).then(|| {
                    format!(
                        "{}:{}/{}",
                        unit.subject, event_pair.source, event_pair.event_type
                    )
                })
            })
        })
        .collect::<Vec<_>>();
    let mut unbacked_output_event_pairs = Vec::new();
    let mut missing_output_event_pair_backing = Vec::new();
    let mut exempted_output_event_pair_backing = Vec::new();
    for unit in source_units {
        let backed_pairs = static_emitter_event_pairs(&unit.id);
        let exemption = emitter_backing_exemption_reason(&unit.id);
        for event_pair in &unit.output_event_types {
            let formatted_pair = format!(
                "{}:{}/{}",
                unit.subject, event_pair.source, event_pair.event_type
            );
            let pair = (event_pair.source.as_str(), event_pair.event_type.as_str());
            match (&backed_pairs, exemption) {
                (Some(backed_pairs), _) if !backed_pairs.contains(&pair) => {
                    unbacked_output_event_pairs.push(formatted_pair);
                }
                (Some(_), _) => {}
                (None, Some(reason)) => {
                    exempted_output_event_pair_backing.push(format!("{formatted_pair}: {reason}"));
                }
                (None, None) => {
                    missing_output_event_pair_backing.push(formatted_pair);
                }
            }
        }
    }
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
        unmapped_runner_packs,
        invalid_physical_impact,
        invalid_output_event_pairs,
        unbacked_output_event_pairs,
        missing_output_event_pair_backing,
        exempted_output_event_pair_backing,
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
    let mut empty_fields = [
        ("id", unit.id.as_str()),
        ("domain", unit.domain.as_str()),
        ("role", unit.role.as_str()),
        ("acquisition_shape", unit.acquisition_shape.as_str()),
        ("material_policy", unit.material_policy.as_str()),
        ("checkpoint_policy", unit.checkpoint_policy.as_str()),
        ("occurrence_policy", unit.occurrence_policy.as_str()),
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
    .collect::<Vec<_>>();

    if unit.output_event_types.is_empty() {
        empty_fields.push(format!("{}:output_event_types", unit.subject.as_str()));
    }

    empty_fields
}

fn runner_pack_binary(runner_pack: &str) -> Option<&'static str> {
    match runner_pack {
        "analytics" => Some("sinex-analytics-automaton"),
        "browser" => Some("sinex-browser-ingestor"),
        "daily" => Some("sinex-daily-summarizer"),
        "desktop" => Some("sinex-desktop-ingestor"),
        "document" => Some("sinex-document-ingestor"),
        "fs" => Some("sinex-fs-ingestor"),
        "health" => Some("sinex-health-automaton"),
        "hourly" => Some("sinex-hourly-summarizer"),
        "session" => Some("sinex-session-detector"),
        "system" => Some("sinex-system-ingestor"),
        "terminal" => Some("sinex-terminal-ingestor"),
        "terminal-canonicalizer" => Some("sinex-terminal-command-canonicalizer"),
        _ => None,
    }
}

fn static_emitter_event_pairs(
    source_unit_id: &str,
) -> Option<BTreeSet<(&'static str, &'static str)>> {
    let pairs = match source_unit_id {
        "system.monitor" => &[("system", "monitoring.started")][..],
        "system.systemd" => &[
            ("systemd", "unit.started"),
            ("systemd", "unit.stopped"),
            ("systemd", "unit.failed"),
            ("systemd", "unit.reloaded"),
            ("systemd", "timer.triggered"),
        ][..],
        "system.journald" => &[
            ("journald", "entry.written"),
            ("journald", "sync.completed"),
        ][..],
        "system.dbus" => &[
            ("dbus", "signal.received"),
            ("dbus", "method.called"),
            ("dbus", "power.state_changed"),
            ("dbus", "bluetooth.device_changed"),
            ("dbus", "network.state_changed"),
            ("dbus", "device.connected"),
            ("dbus", "media.state_changed"),
            ("dbus", "mount.event"),
            ("dbus", "notification.sent"),
        ][..],
        "system.udev" => &[
            ("udev", "device.connected"),
            ("udev", "device.disconnected"),
            ("udev", "device.changed"),
            ("udev", "device.driver_changed"),
            ("udev", "device.other"),
        ][..],
        "terminal.monitor" => &[("terminal", "shell.terminal_monitoring_started")][..],
        "terminal.text-history"
        | "terminal.bash-history"
        | "terminal.zsh-history"
        | "terminal.fish-history" => &[("shell.history", "command.imported")][..],
        "terminal.atuin-history" => &[("shell.atuin", "command.executed")][..],
        "terminal-canonicalizer" => &[("canonical.terminal", "command.canonical")][..],
        _ => return None,
    };
    Some(pairs.iter().copied().collect())
}

fn emitter_backing_exemption_reason(source_unit_id: &str) -> Option<&'static str> {
    match source_unit_id {
        "analytics"
        | "daily-summarizer"
        | "health"
        | "hourly-summarizer"
        | "session-detector" => Some(
            "derived-node output is declared through node trait constants; static emitter catalog pending",
        ),
        "browser.history" => {
            Some("browser history emitter path is parser/importer-driven; static catalog pending")
        }
        "desktop.activitywatch" | "desktop.clipboard" | "desktop.monitor" | "desktop.window-manager" => Some(
            "desktop emitter path spans live bridge and historical import adapters; static catalog pending",
        ),
        "document.staging" => {
            Some("document staging emitter path is scan/importer-driven; static catalog pending")
        }
        "fs" => {
            Some("filesystem emitter path is watcher/importer-driven; static catalog pending")
        }
        _ => None,
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
            "desktop.monitor",
            "desktop.clipboard",
            "desktop.window-manager",
            "desktop.activitywatch",
            "browser.history",
            "system.monitor",
            "system.systemd",
            "system.journald",
            "system.dbus",
            "system.udev",
            "document.staging",
            "terminal.monitor",
            "terminal.text-history",
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
            .find(|unit| unit.id == "desktop.clipboard")
            .expect("desktop descriptor should be rendered from canonical registry");
        assert_eq!(desktop.runner_pack, "desktop");
        assert_eq!(desktop.access_policy, "target_runtime_bridge:clipboard");
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
        assert_eq!(terminal_canonicalizer.runner_pack, "terminal-canonicalizer");
        assert_eq!(
            terminal_canonicalizer.implementation_mode,
            "rust_in_pack:terminal-canonicalizer"
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
        assert!(validation.unmapped_runner_packs.is_empty());
        assert!(validation.invalid_physical_impact.is_empty());
        assert!(validation.invalid_output_event_pairs.is_empty());
        assert!(validation.unbacked_output_event_pairs.is_empty());
        assert!(validation.missing_output_event_pair_backing.is_empty());
        assert!(
            !validation.exempted_output_event_pair_backing.is_empty(),
            "source units without static emitter catalogs must be explicit exemptions"
        );
        assert!(
            validation
                .runner_packs_with_multiple_units
                .contains(&"terminal".to_string())
        );
        Ok(())
    }

    #[sinex_test]
    async fn source_unit_validation_requires_backing_or_exemption() -> TestResult<()> {
        let manifest = build_source_unit_manifest();
        let mut unit = manifest
            .source_units
            .iter()
            .find(|unit| unit.id == "fs")
            .expect("fs descriptor should be present")
            .clone();
        unit.id = "uncataloged".to_string();
        unit.subject = "source_unit:uncataloged".to_string();

        let validation = validate_source_units(&[unit], false);
        assert_eq!(
            validation.missing_output_event_pair_backing,
            vec![
                "source_unit:uncataloged:fs-watcher/file.created".to_string(),
                "source_unit:uncataloged:fs-watcher/file.modified".to_string(),
                "source_unit:uncataloged:fs-watcher/file.deleted".to_string(),
                "source_unit:uncataloged:fs-watcher/file.moved".to_string(),
                "source_unit:uncataloged:fs-watcher/file.discovered".to_string(),
                "source_unit:uncataloged:fs-watcher/dir.created".to_string(),
                "source_unit:uncataloged:fs-watcher/dir.deleted".to_string(),
                "source_unit:uncataloged:fs-watcher/dir.discovered".to_string(),
            ]
        );
        Ok(())
    }

    #[sinex_test]
    async fn source_unit_validation_rejects_unregistered_output_pairs() -> TestResult<()> {
        let manifest = build_source_unit_manifest();
        let mut unit = manifest
            .source_units
            .iter()
            .find(|unit| unit.id == "fs")
            .expect("fs descriptor should be present")
            .clone();
        unit.output_event_types.push(SourceUnitEventType {
            source: "missing".to_string(),
            event_type: "source.event".to_string(),
        });

        let validation = validate_source_units(&[unit], false);
        assert_eq!(
            validation.invalid_output_event_pairs,
            vec!["source_unit:fs:missing/source.event".to_string()]
        );
        Ok(())
    }

    #[sinex_test]
    async fn source_unit_validation_rejects_registered_but_unbacked_terminal_pairs()
    -> TestResult<()> {
        let manifest = build_source_unit_manifest();
        let mut unit = manifest
            .source_units
            .iter()
            .find(|unit| unit.id == "terminal.fish-history")
            .expect("fish descriptor should be present")
            .clone();
        unit.output_event_types = vec![SourceUnitEventType {
            source: "shell.history.fish".to_string(),
            event_type: "command.executed".to_string(),
        }];

        let validation = validate_source_units(&[unit], false);
        assert!(
            validation.invalid_output_event_pairs.is_empty(),
            "fish command.executed is a registered payload pair, so this must be caught by emitter validation"
        );
        assert_eq!(
            validation.unbacked_output_event_pairs,
            vec![
                "source_unit:terminal.fish-history:shell.history.fish/command.executed".to_string()
            ]
        );
        Ok(())
    }

    #[sinex_test]
    async fn source_unit_validation_rejects_registered_but_unbacked_system_pairs()
    -> TestResult<()> {
        let manifest = build_source_unit_manifest();
        let mut unit = manifest
            .source_units
            .iter()
            .find(|unit| unit.id == "system.udev")
            .expect("udev descriptor should be present")
            .clone();
        unit.output_event_types = vec![SourceUnitEventType {
            source: "udev".to_string(),
            event_type: "device.added".to_string(),
        }];

        let validation = validate_source_units(&[unit], false);
        assert!(
            validation.invalid_output_event_pairs.is_empty(),
            "udev device.added is a registered payload pair, so this must be caught by emitter validation"
        );
        assert_eq!(
            validation.unbacked_output_event_pairs,
            vec!["source_unit:system.udev:udev/device.added".to_string()]
        );
        Ok(())
    }

    #[sinex_test]
    async fn source_unit_validation_requires_output_event_pairs() -> TestResult<()> {
        let manifest = build_source_unit_manifest();
        let mut unit = manifest
            .source_units
            .iter()
            .find(|unit| unit.id == "fs")
            .expect("fs descriptor should be present")
            .clone();
        unit.output_event_types.clear();

        let validation = validate_source_units(&[unit], false);
        assert_eq!(
            validation.empty_fields,
            vec!["source_unit:fs:output_event_types".to_string()]
        );
        Ok(())
    }

    #[sinex_test]
    async fn source_unit_validation_requires_runner_pack_binary_mapping() -> TestResult<()> {
        let manifest = build_source_unit_manifest();
        let mut unit = manifest
            .source_units
            .iter()
            .find(|unit| unit.id == "fs")
            .expect("fs descriptor should be present")
            .clone();
        unit.runner_pack = "missing-pack".to_string();

        let validation = validate_source_units(&[unit], false);
        assert_eq!(
            validation.unmapped_runner_packs,
            vec!["missing-pack".to_string()]
        );
        Ok(())
    }
}

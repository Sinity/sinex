//! Proof catalog projection.
//!
//! The catalog is a generated developer surface: it joins proof inventory from
//! `sinex-primitives`, EventPayload inventory, xtask command metadata, and
//! discovered scenario annotations. Runtime semantics stay in Rust tests and
//! SDK descriptors; this module only projects them into one inspectable graph.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;

use color_eyre::eyre::Result;
use serde::Serialize;
use sinex_primitives::events::schema_registry::get_all_payloads;
use sinex_primitives::proof::{
    self, CheckpointFamily, Claim, Exemption, Horizon, OccurrenceIdentity,
    PROOF_CATALOG_SCHEMA_VERSION, PrivacyTier, ProofObligation, RetentionPolicy, RunnerBinding,
    RuntimeShape, SourceUnitBinding,
};

use crate::command_catalog::{CommandInfo, collect_command_catalog};
use crate::commands::test::{ScenarioCatalogEntry, discover_scenario_catalog};

#[derive(Debug, Clone, Serialize)]
pub struct ProofCatalog {
    pub schema_version: u32,
    pub runtime_units: Vec<SourceUnitBinding>,
    pub source_units: Vec<SourceUnitSubject>,
    pub claims: Vec<Claim>,
    pub runner_bindings: Vec<RunnerBinding>,
    pub obligations: Vec<ProofObligation>,
    pub exemptions: Vec<Exemption>,
    pub event_payloads: Vec<EventPayloadSubject>,
    pub xtask_commands: Vec<XtaskCommandSubject>,
    pub scenarios: Vec<ScenarioSubject>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct EventPayloadSubject {
    pub subject: String,
    pub source: String,
    pub event_type: String,
    pub version: String,
    pub type_name: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SourceUnitEventPair {
    pub source: String,
    pub event_type: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SourceUnitSubject {
    pub subject: String,
    pub id: String,
    pub namespace: String,
    pub runner_pack: String,
    pub modes: Vec<String>,
    pub output_event_types: Vec<SourceUnitEventPair>,
    pub privacy_tier: String,
    pub runtime_shape: String,
    pub retention_policy: String,
    pub checkpoint_family: String,
    pub occurrence_identity: String,
    pub access_policy: String,
    pub package_impact: String,
    pub implementation_mode: String,
    pub proof_obligations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct XtaskCommandSubject {
    pub subject: String,
    pub path: String,
    pub about: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ScenarioSubject {
    pub subject: String,
    pub id: String,
    pub test_name: String,
    pub package: Option<String>,
    pub path: String,
    pub category: String,
    pub lane: String,
    pub cost_tier: String,
    pub tags: Vec<String>,
    pub fixtures: Vec<String>,
    pub subject_refs: Vec<String>,
    pub claim_ids: Vec<String>,
    pub assertion_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProofCatalogValidation {
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

impl ProofCatalogValidation {
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.errors.is_empty()
    }

    pub fn into_result(self) -> Result<Self> {
        if self.is_valid() {
            Ok(self)
        } else {
            color_eyre::eyre::bail!(
                "proof catalog semantic validation failed:\n{}",
                self.errors.join("\n")
            )
        }
    }
}

pub fn build_proof_catalog(workspace_root: &Path) -> Result<ProofCatalog> {
    crate::source_unit_inventory::link_source_unit_inventories();

    let mut runtime_units_by_subject: BTreeMap<&'static str, SourceUnitBinding> = BTreeMap::new();
    for binding in proof::source_unit_bindings() {
        match runtime_units_by_subject.get(binding.subject.as_str()) {
            Some(existing) if !existing.proposed && binding.proposed => {}
            _ => {
                runtime_units_by_subject.insert(binding.subject.as_str(), *binding);
            }
        }
    }
    let runtime_units = runtime_units_by_subject.into_values().collect::<Vec<_>>();

    // Build a binding lookup keyed by source_unit_id so source_unit_subject
    // can read deployment-shape fields (`runner_pack`, `runtime_shape`,
    // `checkpoint_family`, `package_impact`, `implementation_mode`) from the
    // binding (#1175 split). Live bindings beat proposed bindings when both
    // exist; descriptors with no binding fall back to inert defaults.
    let mut binding_lookup: HashMap<&'static str, &'static SourceUnitBinding> = HashMap::new();
    for binding in proof::source_unit_bindings() {
        if binding.source_unit_id.is_empty() {
            continue;
        }
        match binding_lookup.get(binding.source_unit_id) {
            Some(existing) if !existing.proposed && binding.proposed => {}
            _ => {
                binding_lookup.insert(binding.source_unit_id, binding);
            }
        }
    }

    let mut source_units = proof::all_source_units()
        .map(|unit| source_unit_subject(unit, &binding_lookup))
        .collect::<Vec<_>>();
    source_units.sort_by(|left, right| left.subject.cmp(&right.subject));

    let mut claims = proof::claims().copied().collect::<Vec<_>>();
    claims.sort_by(|left, right| left.id.cmp(right.id));

    let mut runner_bindings = proof::runner_bindings().copied().collect::<Vec<_>>();
    runner_bindings.sort_by(|left, right| left.id.cmp(right.id));

    let mut obligations = proof::obligations().copied().collect::<Vec<_>>();
    obligations.sort_by(|left, right| left.id.cmp(right.id));

    let mut exemptions = proof::exemptions().copied().collect::<Vec<_>>();
    exemptions.sort_by(|left, right| left.id.cmp(right.id));

    Ok(ProofCatalog {
        schema_version: PROOF_CATALOG_SCHEMA_VERSION,
        runtime_units,
        source_units,
        claims,
        runner_bindings,
        obligations,
        exemptions,
        event_payloads: collect_event_payload_subjects(),
        xtask_commands: collect_xtask_command_subjects(),
        scenarios: collect_scenario_subjects(workspace_root)?,
    })
}

pub fn render_proof_catalog_json(catalog: &ProofCatalog) -> Result<String> {
    let mut rendered = serde_json::to_string_pretty(catalog)?;
    rendered.push('\n');
    Ok(rendered)
}

#[must_use]
pub fn validate_proof_catalog(catalog: &ProofCatalog) -> ProofCatalogValidation {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    check_unique(
        &mut errors,
        "claim id",
        catalog.claims.iter().map(|claim| claim.id),
    );
    check_unique(
        &mut errors,
        "runner binding id",
        catalog.runner_bindings.iter().map(|binding| binding.id),
    );
    check_unique(
        &mut errors,
        "proof obligation id",
        catalog.obligations.iter().map(|obligation| obligation.id),
    );
    check_unique(
        &mut errors,
        "proof exemption id",
        catalog.exemptions.iter().map(|exemption| exemption.id),
    );
    check_unique(
        &mut errors,
        "runtime unit subject",
        catalog
            .runtime_units
            .iter()
            .map(|unit| unit.subject.as_str()),
    );
    check_unique(
        &mut errors,
        "source unit subject",
        catalog
            .source_units
            .iter()
            .map(|unit| unit.subject.as_str()),
    );
    check_unique(
        &mut errors,
        "event payload subject",
        catalog
            .event_payloads
            .iter()
            .map(|payload| payload.subject.as_str()),
    );
    check_unique(
        &mut errors,
        "xtask command subject",
        catalog
            .xtask_commands
            .iter()
            .map(|command| command.subject.as_str()),
    );
    check_unique(
        &mut errors,
        "scenario id",
        catalog
            .scenarios
            .iter()
            .map(|scenario| scenario.id.as_str()),
    );

    let claim_ids = catalog
        .claims
        .iter()
        .map(|claim| claim.id)
        .collect::<BTreeSet<_>>();
    let runner_bindings = catalog
        .runner_bindings
        .iter()
        .map(|binding| (binding.id, binding))
        .collect::<BTreeMap<_, _>>();
    let obligations = catalog
        .obligations
        .iter()
        .map(|obligation| (obligation.id, obligation))
        .collect::<BTreeMap<_, _>>();
    let xtask_commands = catalog
        .xtask_commands
        .iter()
        .map(|command| command.subject.as_str())
        .collect::<BTreeSet<_>>();

    for binding in &catalog.runner_bindings {
        for claim_id in binding.claims {
            if !claim_ids.contains(claim_id) {
                errors.push(format!(
                    "{} references unknown catalog claim {claim_id}",
                    binding.id
                ));
            }
        }
        if let Some(command_subject) = xtask_command_subject_for_runner(binding.command)
            && !xtask_commands.contains(command_subject.as_str())
        {
            errors.push(format!(
                "{} references undocumented xtask command `{}` ({command_subject})",
                binding.id, binding.command
            ));
        }
    }

    let claims_by_id = catalog
        .claims
        .iter()
        .map(|claim| (claim.id, claim))
        .collect::<BTreeMap<_, _>>();
    for obligation in &catalog.obligations {
        let claim = claims_by_id.get(obligation.claim_id);
        let binding = runner_bindings.get(obligation.runner_binding_id);
        if claim.is_none() {
            errors.push(format!(
                "{} references unknown claim {}",
                obligation.id, obligation.claim_id
            ));
        }
        let Some(binding) = binding else {
            errors.push(format!(
                "{} references unknown runner binding {}",
                obligation.id, obligation.runner_binding_id
            ));
            continue;
        };
        if !binding.claims.contains(&obligation.claim_id) {
            errors.push(format!(
                "{} uses runner {} which does not list claim {}",
                obligation.id, binding.id, obligation.claim_id
            ));
        }
        if matches!(obligation.level, proof::ProofObligationLevel::Required)
            && binding.subject != obligation.subject
        {
            errors.push(format!(
                "{} is required for subject `{}` but runner {} is declared for `{}`",
                obligation.id,
                obligation.subject.as_str(),
                binding.id,
                binding.subject.as_str()
            ));
        }
        if let Some(claim) = claim
            && matches!(obligation.level, proof::ProofObligationLevel::Required)
            && claim.subject != obligation.subject
        {
            errors.push(format!(
                "{} is required for subject `{}` but claim {} is declared for `{}`",
                obligation.id,
                obligation.subject.as_str(),
                claim.id,
                claim.subject.as_str()
            ));
        }
    }

    let mut local_source_unit_tags = BTreeMap::<&str, Vec<&str>>::new();
    for unit in &catalog.source_units {
        for obligation_id in &unit.proof_obligations {
            if obligation_id.starts_with("obligation:")
                && !obligations.contains_key(obligation_id.as_str())
            {
                errors.push(format!(
                    "{} references unknown proof obligation {obligation_id}",
                    unit.subject
                ));
            } else if !obligation_id.starts_with("obligation:") {
                local_source_unit_tags
                    .entry(unit.subject.as_str())
                    .or_default()
                    .push(obligation_id);
            }
        }
    }
    for (subject, mut tags) in local_source_unit_tags {
        tags.sort_unstable();
        warnings.push(format!(
            "{subject} carries {} local source-unit proof tag(s): {}",
            tags.len(),
            tags.join(", ")
        ));
    }

    for exemption in &catalog.exemptions {
        if !obligations.contains_key(exemption.obligation_id) {
            errors.push(format!(
                "{} references unknown proof obligation {}",
                exemption.id, exemption.obligation_id
            ));
        }
    }

    for scenario in &catalog.scenarios {
        if scenario.subject_refs.is_empty() {
            errors.push(format!("scenario:{} has no subject refs", scenario.id));
        }
        if scenario.claim_ids.is_empty() && scenario.assertion_ids.is_empty() {
            errors.push(format!(
                "scenario:{} has neither catalog claim ids nor assertion ids",
                scenario.id
            ));
        }
        for claim_id in &scenario.claim_ids {
            if !claim_ids.contains(claim_id.as_str()) {
                errors.push(format!(
                    "scenario:{} references unknown catalog claim {claim_id}",
                    scenario.id
                ));
            }
        }
    }

    let required_subjects = catalog
        .obligations
        .iter()
        .filter(|obligation| matches!(obligation.level, proof::ProofObligationLevel::Required))
        .map(|obligation| obligation.subject.as_str())
        .collect::<BTreeSet<_>>();
    for subject in required_subjects {
        if !catalog
            .runner_bindings
            .iter()
            .any(|binding| binding.subject.as_str() == subject)
        {
            warnings.push(format!(
                "required proof subject `{subject}` has no runner binding"
            ));
        }
    }

    ProofCatalogValidation { errors, warnings }
}

fn check_unique<'a>(errors: &mut Vec<String>, label: &str, values: impl Iterator<Item = &'a str>) {
    let mut seen = BTreeSet::new();
    let mut duplicates = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            duplicates.insert(value);
        }
    }
    for duplicate in duplicates {
        errors.push(format!("duplicate {label}: {duplicate}"));
    }
}

fn xtask_command_subject_for_runner(command: &str) -> Option<String> {
    let mut parts = command.split_whitespace();
    if parts.next()? != "xtask" {
        return None;
    }
    let mut path = Vec::new();
    for part in parts {
        if part.starts_with('-') || part.starts_with('<') {
            break;
        }
        path.push(part);
    }
    if path.is_empty() {
        None
    } else {
        Some(format!("xtask_command:{}", path.join(".")))
    }
}

fn collect_event_payload_subjects() -> Vec<EventPayloadSubject> {
    let mut subjects = get_all_payloads()
        .map(|payload| EventPayloadSubject {
            subject: format!(
                "event_payload:{}/{}/{}",
                payload.source, payload.event_type, payload.version
            ),
            source: payload.source.to_string(),
            event_type: payload.event_type.to_string(),
            version: payload.version.to_string(),
            type_name: payload.type_name.to_string(),
        })
        .collect::<Vec<_>>();
    subjects.sort_by(|left, right| left.subject.cmp(&right.subject));
    subjects
}

fn source_unit_subject(
    unit: &'static proof::SourceUnitDescriptor,
    binding_lookup: &HashMap<&'static str, &'static SourceUnitBinding>,
) -> SourceUnitSubject {
    // Deployment-shape fields live on the binding only (#1175). Descriptor with
    // no binding falls back to inert defaults — the manifest validator surfaces
    // those via `unmapped_runner_packs` / `unresolved_binding_source_unit_ids`.
    let binding = binding_lookup.get(unit.id).copied();
    let runner_pack = binding.map_or("", |b| b.runner_pack);
    let runtime_shape = binding.map_or(RuntimeShape::Continuous, |b| b.runtime_shape);
    let checkpoint_family = binding.map_or(CheckpointFamily::AppendStream, |b| b.checkpoint_family);
    let package_impact = binding.map_or("", |b| b.package_impact);
    let implementation_mode = binding.map_or("", |b| b.implementation_mode);

    SourceUnitSubject {
        subject: format!("source_unit:{}", unit.id),
        id: unit.id.to_string(),
        namespace: unit.namespace.to_string(),
        runner_pack: runner_pack.to_string(),
        modes: unit
            .horizons
            .iter()
            .map(|horizon| horizon_name(*horizon).to_string())
            .collect(),
        output_event_types: unit
            .event_types
            .iter()
            .map(|(source, event_type)| SourceUnitEventPair {
                source: (*source).to_string(),
                event_type: (*event_type).to_string(),
            })
            .collect(),
        privacy_tier: privacy_tier_name(unit.privacy_tier).to_string(),
        runtime_shape: runtime_shape_name(runtime_shape).to_string(),
        retention_policy: retention_policy(unit.retention),
        checkpoint_family: checkpoint_family_name(checkpoint_family).to_string(),
        occurrence_identity: occurrence_identity(unit.occurrence_identity),
        access_policy: unit.access_policy.to_string(),
        package_impact: package_impact.to_string(),
        implementation_mode: implementation_mode.to_string(),
        proof_obligations: unit
            .proof_obligations
            .iter()
            .map(|obligation| (*obligation).to_string())
            .collect(),
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

fn runtime_shape_name(shape: RuntimeShape) -> &'static str {
    match shape {
        RuntimeShape::Continuous => "continuous",
        RuntimeShape::OnDemand => "on_demand",
        RuntimeShape::Scheduled => "scheduled",
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

fn checkpoint_family_name(family: proof::CheckpointFamily) -> &'static str {
    match family {
        proof::CheckpointFamily::AppendStream => "append_stream",
        proof::CheckpointFamily::MutableSnapshot { .. } => "mutable_snapshot",
        proof::CheckpointFamily::Journal => "journal",
        proof::CheckpointFamily::Polling => "polling",
        proof::CheckpointFamily::LiveObservation => "live_observation",
    }
}

fn occurrence_identity(identity: OccurrenceIdentity) -> String {
    match identity {
        OccurrenceIdentity::Uuid5From(source) => format!("uuid5:{source}"),
        OccurrenceIdentity::Natural => "natural_key".to_string(),
        OccurrenceIdentity::Anchor => "material_anchor".to_string(),
    }
}

fn collect_xtask_command_subjects() -> Vec<XtaskCommandSubject> {
    let mut subjects = Vec::new();
    for command in collect_command_catalog() {
        collect_command_subject(&command, String::new(), &mut subjects);
    }
    subjects.sort_by(|left, right| left.subject.cmp(&right.subject));
    subjects
}

fn collect_command_subject(
    command: &CommandInfo,
    parent_path: String,
    subjects: &mut Vec<XtaskCommandSubject>,
) {
    let path = if parent_path.is_empty() {
        command.name.clone()
    } else {
        format!("{}.{}", parent_path, command.name)
    };
    subjects.push(XtaskCommandSubject {
        subject: format!("xtask_command:{path}"),
        path: path.clone(),
        about: command.about.clone(),
    });
    for subcommand in &command.subcommands {
        collect_command_subject(subcommand, path.clone(), subjects);
    }
}

fn collect_scenario_subjects(workspace_root: &Path) -> Result<Vec<ScenarioSubject>> {
    let mut subjects = discover_scenario_catalog(workspace_root)?
        .into_iter()
        .map(scenario_subject)
        .collect::<Vec<_>>();
    subjects.sort_by(|left, right| left.subject.cmp(&right.subject));
    Ok(subjects)
}

fn scenario_subject(entry: ScenarioCatalogEntry) -> ScenarioSubject {
    ScenarioSubject {
        subject: format!("scenario:{}", entry.id),
        id: entry.id,
        test_name: entry.test_name,
        package: entry.package,
        path: entry.path,
        category: entry.category,
        lane: entry.lane,
        cost_tier: entry.cost_tier,
        tags: entry.tags,
        fixtures: entry.fixtures,
        subject_refs: entry.subject_refs,
        claim_ids: entry.claim_ids,
        assertion_ids: entry.assertion_ids,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::prelude::*;

    #[sinex_test]
    async fn proof_catalog_contains_inventory_and_command_subjects() -> TestResult<()> {
        let workspace = crate::sandbox::orchestrator::find_workspace_root()?;
        let catalog = build_proof_catalog(&workspace)?;

        assert!(
            catalog
                .runtime_units
                .iter()
                .any(|unit| unit.subject.as_str() == "source_unit:terminal.atuin-history")
        );
        let runtime_subject_count = catalog
            .runtime_units
            .iter()
            .filter(|unit| unit.subject.as_str() == "source_unit:terminal.atuin-history")
            .count();
        assert_eq!(runtime_subject_count, 1);
        assert!(
            catalog
                .source_units
                .iter()
                .any(|unit| unit.subject.as_str() == "source_unit:terminal.monitor")
        );
        assert!(
            catalog
                .source_units
                .iter()
                .any(|unit| unit.subject.as_str() == "source_unit:fs")
        );
        assert!(
            catalog
                .source_units
                .iter()
                .any(|unit| unit.subject.as_str() == "source_unit:desktop.clipboard")
        );
        assert!(
            catalog
                .source_units
                .iter()
                .any(|unit| unit.subject.as_str() == "source_unit:system.udev")
        );
        assert!(
            catalog
                .claims
                .iter()
                .any(|claim| claim.id == "claim:source_material.material_provenance")
        );
        assert!(
            catalog
                .xtask_commands
                .iter()
                .any(|command| command.subject == "xtask_command:test")
        );
        assert!(!catalog.event_payloads.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn proof_catalog_validation_demotes_local_source_unit_tags() -> TestResult<()> {
        let workspace = crate::sandbox::orchestrator::find_workspace_root()?;
        let mut catalog = build_proof_catalog(&workspace)?;
        catalog.source_units[0].proof_obligations = vec!["timestamp_intrinsic".to_string()];

        let validation = validate_proof_catalog(&catalog);

        assert!(
            validation.errors.is_empty(),
            "local source-unit proof tags should not be catalog obligation errors: {:?}",
            validation.errors
        );
        assert!(
            validation
                .warnings
                .iter()
                .any(|warning| warning.contains("timestamp_intrinsic")),
            "expected local proof tag warning, got {:?}",
            validation.warnings
        );
        Ok(())
    }

    #[sinex_test]
    async fn proof_catalog_validation_rejects_unknown_catalog_obligations() -> TestResult<()> {
        let workspace = crate::sandbox::orchestrator::find_workspace_root()?;
        let mut catalog = build_proof_catalog(&workspace)?;
        catalog.source_units[0].proof_obligations =
            vec!["obligation:source_unit.missing".to_string()];

        let validation = validate_proof_catalog(&catalog);

        assert!(
            validation
                .errors
                .iter()
                .any(|error| error.contains("obligation:source_unit.missing")),
            "expected unknown catalog obligation validation error, got {:?}",
            validation.errors
        );
        Ok(())
    }

    #[sinex_test]
    async fn proof_catalog_semantic_validation_passes_live_catalog() -> TestResult<()> {
        let workspace = crate::sandbox::orchestrator::find_workspace_root()?;
        let catalog = build_proof_catalog(&workspace)?;
        let validation = validate_proof_catalog(&catalog);

        assert!(
            validation.errors.is_empty(),
            "proof catalog validation errors: {:?}",
            validation.errors
        );
        Ok(())
    }

    #[sinex_test]
    async fn proof_catalog_json_is_stable_object_shape() -> TestResult<()> {
        let workspace = crate::sandbox::orchestrator::find_workspace_root()?;
        let catalog = build_proof_catalog(&workspace)?;
        let rendered = render_proof_catalog_json(&catalog)?;
        let json: serde_json::Value = serde_json::from_str(&rendered)?;

        assert_eq!(json["schema_version"], PROOF_CATALOG_SCHEMA_VERSION);
        assert!(json["runtime_units"].is_array());
        assert!(json["source_units"].is_array());
        assert!(json["event_payloads"].is_array());
        assert!(json["xtask_commands"].is_array());
        assert!(json["scenarios"].is_array());
        Ok(())
    }

    #[sinex_test]
    async fn proof_catalog_validation_rejects_dangling_catalog_claims() -> TestResult<()> {
        let workspace = crate::sandbox::orchestrator::find_workspace_root()?;
        let mut catalog = build_proof_catalog(&workspace)?;
        catalog.scenarios.push(ScenarioSubject {
            subject: "scenario:demo".to_string(),
            id: "demo".to_string(),
            test_name: "demo_test".to_string(),
            package: Some("xtask".to_string()),
            path: "xtask/src/demo.rs".to_string(),
            category: "command_contract".to_string(),
            lane: "fast".to_string(),
            cost_tier: "fast".to_string(),
            tags: Vec::new(),
            fixtures: Vec::new(),
            subject_refs: vec!["xtask_command:test".to_string()],
            claim_ids: vec!["claim:missing".to_string()],
            assertion_ids: Vec::new(),
        });

        let validation = validate_proof_catalog(&catalog);
        assert!(
            validation
                .errors
                .iter()
                .any(|error| error.contains("claim:missing")),
            "expected dangling claim validation error, got {:?}",
            validation.errors
        );
        Ok(())
    }
}

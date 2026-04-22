//! Proof catalog projection.
//!
//! The catalog is a generated developer surface: it joins proof inventory from
//! `sinex-primitives`, EventPayload inventory, xtask command metadata, and
//! discovered scenario annotations. Runtime semantics stay in Rust tests and
//! SDK descriptors; this module only projects them into one inspectable graph.

use std::path::Path;

use color_eyre::eyre::Result;
use serde::Serialize;
use sinex_primitives::events::schema_registry::get_all_payloads;
use sinex_primitives::proof::{
    self, Claim, Exemption, PROOF_CATALOG_SCHEMA_VERSION, ProofObligation, RunnerBinding,
    RuntimeUnitDescriptor, SourceUnitDescriptor,
};

use crate::command_catalog::{CommandInfo, collect_command_catalog};
use crate::commands::test::{ScenarioCatalogEntry, discover_scenario_catalog};

#[derive(Debug, Clone, Serialize)]
pub struct ProofCatalog {
    pub schema_version: u32,
    pub runtime_units: Vec<RuntimeUnitDescriptor>,
    pub source_units: Vec<SourceUnitDescriptor>,
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
}

pub fn build_proof_catalog(workspace_root: &Path) -> Result<ProofCatalog> {
    let mut runtime_units = proof::runtime_unit_descriptors()
        .copied()
        .collect::<Vec<_>>();
    runtime_units.sort_by(|left, right| left.subject.as_str().cmp(right.subject.as_str()));

    let mut source_units = proof::source_unit_descriptors()
        .copied()
        .collect::<Vec<_>>();
    source_units.sort_by(|left, right| left.subject.as_str().cmp(right.subject.as_str()));

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
                .any(|unit| unit.subject.as_str() == "runtime_unit:terminal.atuin")
        );
        assert!(
            catalog
                .source_units
                .iter()
                .any(|unit| unit.subject.as_str() == "source_unit:terminal.atuin-history")
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
}

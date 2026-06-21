//! Source/Input package completeness report (#1792).
//!
//! This is deliberately a compiler over existing Rust inventories and generated
//! artifacts, not a detached proof ledger. The authoring truth remains
//! `SourceContract`, `SourceRuntimeBinding`, parser/source factory inventory,
//! payload schema inventory, the generated source catalog, and the generated
//! privacy coverage matrix.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::Serialize;
use serde_json::{Value, json};
use sinex_primitives::events::schema_registry::get_all_payloads;
use sinex_primitives::source_contracts::{
    RunnerPack, SourceCapabilityKind, SourceCapabilityRef, SourceContract, SourceRuntimeBinding,
    all_source_contracts, source_runtime_bindings,
};
use sinex_primitives::{AdmissionPolicy, EventContract, admission_policies, event_contracts};

use crate::sources::catalog_export::render_catalog;
use crate::sources::dispatch::parser_inventory_records;
use crate::sources::privacy_coverage::render_privacy_coverage_matrix;
use crate::sources::source_factory::registered_source_factory_ids;

/// Bumped when the report JSON shape changes.
pub const PACKAGE_COMPLETENESS_SCHEMA_VERSION: u32 = 2;

/// Repo-relative path for a future committed report artifact, if the project
/// chooses to check it in. The first slice exposes rendering + tests; it does
/// not make this a new source of truth.
pub const PACKAGE_COMPLETENESS_ARTIFACT_PATH: &str =
    "crate/sinexd/docs/sources/package-completeness.generated.json";

#[derive(Debug, Clone, Serialize)]
pub struct PackageCompletenessReport {
    pub schema_version: u32,
    pub generated_from: GeneratedFrom,
    pub summary: PackageCompletenessSummary,
    pub packages: BTreeMap<String, PackageCompletenessPackage>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GeneratedFrom {
    pub source_contract_inventory: &'static str,
    pub runtime_binding_inventory: &'static str,
    pub parser_factory_inventory: &'static str,
    pub source_factory_inventory: &'static str,
    pub event_payload_inventory: &'static str,
    pub source_catalog_projection: &'static str,
    pub privacy_coverage_projection: &'static str,
    pub report_authority: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct PackageCompletenessSummary {
    pub package_count: usize,
    pub mode_count: usize,
    pub accepted_mode_count: usize,
    pub proposed_mode_count: usize,
    pub manual_mode_count: usize,
    pub incomplete_mode_count: usize,
    pub blocking_missing_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct PackageCompletenessPackage {
    pub package_id: String,
    pub family: String,
    pub display_namespace: String,
    pub modes: BTreeMap<String, PackageCompletenessMode>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PackageCompletenessMode {
    pub mode_id: String,
    pub package_id: String,
    pub mode_state: PackageModeState,
    pub completeness: PackageCompleteness,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manual_reason: Option<&'static str>,
    pub subject: Option<String>,
    pub acquisition_kind: &'static str,
    pub operator_enablement: &'static str,
    pub event_pairs: Vec<EventPairReport>,
    pub event_contract_refs: Vec<String>,
    pub admission_policy_refs: Vec<String>,
    pub coverage_debt_refs: Vec<String>,
    pub operation_refs: Vec<String>,
    pub sources: ModeSourceRefs,
    pub requirements: Vec<RequirementDiagnostic>,
    pub missing: Vec<String>,
    pub caveats: Vec<String>,
}

#[derive(Debug)]
pub enum PackageCompletenessFilterError {
    PackageNotFound(String),
    ModeRequiresPackage,
    ModeNotFound { package_id: String, mode_id: String },
    Serialize(serde_json::Error),
}

impl fmt::Display for PackageCompletenessFilterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PackageNotFound(package_id) => {
                write!(
                    f,
                    "package `{package_id}` not found in package completeness report"
                )
            }
            Self::ModeRequiresPackage => write!(
                f,
                "package completeness mode filtering requires --package-id because mode ids are package-local"
            ),
            Self::ModeNotFound {
                package_id,
                mode_id,
            } => write!(f, "mode `{mode_id}` not found for package `{package_id}`"),
            Self::Serialize(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for PackageCompletenessFilterError {}

impl From<serde_json::Error> for PackageCompletenessFilterError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialize(error)
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PackageModeState {
    Accepted,
    Proposed,
    Manual,
    Incomplete,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PackageCompleteness {
    Complete,
    Incomplete,
}

#[derive(Debug, Clone, Serialize)]
pub struct EventPairReport {
    pub source: String,
    pub event_type: String,
    pub payload_schema_registered: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_schema_version: Option<String>,
    pub event_contract_ref: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ModeSourceRefs {
    pub source_contract: SourceContractRef,
    pub runtime_binding: Option<RuntimeBindingRef>,
    pub parser_manifest: Option<ParserManifestRef>,
    pub source_factory_registered: bool,
    pub parser_factory_registered: bool,
    pub catalog_projection_registered: bool,
    pub privacy_coverage_registered: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceContractRef {
    pub id: String,
    pub namespace: String,
    pub privacy_tier: Value,
    pub horizons: Value,
    pub retention: Value,
    pub occurrence_identity: Value,
    pub access_scope: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeBindingRef {
    pub id: String,
    pub subject: String,
    pub domain: String,
    pub implementation: String,
    pub adapter: String,
    pub output_event_type: String,
    pub privacy_context: Value,
    pub resource_profile: Value,
    pub capabilities: Vec<String>,
    pub resource_limits: Value,
    pub resource_budget: Value,
    pub proposed: bool,
    pub runner_pack: Value,
    pub checkpoint_family: Value,
    pub runtime_shape: Value,
    pub build_impact: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct ParserManifestRef {
    pub parser_id: String,
    pub parser_version: String,
    pub source_id: String,
    pub accepted_input_shapes: Value,
    pub declared_event_types: Vec<EventPairReport>,
    pub field_privacy_metadata_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct RequirementDiagnostic {
    pub id: &'static str,
    pub status: RequirementStatus,
    pub blocking: bool,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RequirementStatus {
    Present,
    Missing,
    Caveat,
    NotApplicable,
}

/// Build the package/mode completeness report from compiled inventories.
#[must_use]
pub fn build_package_completeness_report() -> PackageCompletenessReport {
    let bindings_by_source = collect_bindings_by_source();
    let parser_records = parser_inventory_records()
        .into_iter()
        .map(|record| (record.source_id.clone(), record))
        .collect::<BTreeMap<_, _>>();
    let source_factory_ids = registered_source_factory_ids()
        .into_iter()
        .map(|id| id.as_str().to_string())
        .collect::<BTreeSet<_>>();
    let payload_pairs = payload_schema_pairs();
    let catalog_projection = rendered_json(render_catalog()).unwrap_or(Value::Null);
    let privacy_projection = rendered_json(render_privacy_coverage_matrix()).unwrap_or(Value::Null);
    let privacy_entries = privacy_entry_ids(&privacy_projection);

    let mut contracts: Vec<&'static SourceContract> = all_source_contracts().collect();
    contracts.sort_by(|a, b| a.id.cmp(b.id));

    let mut packages = BTreeMap::new();

    for contract in contracts {
        let bindings = bindings_by_source
            .get(contract.id)
            .cloned()
            .unwrap_or_default();
        let modes = if bindings.is_empty() {
            vec![build_unbound_mode(
                contract,
                &parser_records,
                &source_factory_ids,
                &payload_pairs,
                &catalog_projection,
                &privacy_entries,
            )]
        } else {
            bindings
                .into_iter()
                .map(|binding| {
                    build_bound_mode(
                        contract,
                        binding,
                        &parser_records,
                        &source_factory_ids,
                        &payload_pairs,
                        &catalog_projection,
                        &privacy_entries,
                    )
                })
                .collect()
        };

        let mut mode_map = BTreeMap::new();
        for mode in modes {
            mode_map.insert(mode.mode_id.clone(), mode);
        }

        packages.insert(
            contract.id.to_string(),
            PackageCompletenessPackage {
                package_id: contract.id.to_string(),
                family: contract.namespace.to_string(),
                display_namespace: contract.namespace.to_string(),
                modes: mode_map,
            },
        );
    }

    PackageCompletenessReport {
        schema_version: PACKAGE_COMPLETENESS_SCHEMA_VERSION,
        generated_from: GeneratedFrom {
            source_contract_inventory: "sinex_primitives::source_contracts::all_source_contracts",
            runtime_binding_inventory: "sinex_primitives::source_contracts::source_runtime_bindings",
            parser_factory_inventory: "sinexd::sources::dispatch::parser_inventory_records",
            source_factory_inventory: "sinexd::sources::source_factory::registered_source_factory_ids",
            event_payload_inventory: "sinex_primitives::events::schema_registry::get_all_payloads",
            source_catalog_projection: "sinexd::sources::catalog_export::render_catalog",
            privacy_coverage_projection: "sinexd::sources::privacy_coverage::render_privacy_coverage_matrix",
            report_authority: "compiled inventories plus generated projections; not a detached proof ledger",
        },
        summary: summarize_packages(&packages),
        packages,
    }
}

/// Render deterministic pretty JSON with a trailing newline.
pub fn render_package_completeness_report() -> serde_json::Result<String> {
    Ok(serde_json::to_string_pretty(&build_package_completeness_report())? + "\n")
}

/// Render a package/mode-scoped completeness report for authoring loops.
///
/// Mode ids are package-local, so callers must provide `package_id` when they
/// provide `mode_id`.
pub fn render_filtered_package_completeness_report(
    package_id: Option<&str>,
    mode_id: Option<&str>,
) -> Result<String, PackageCompletenessFilterError> {
    let report = filter_package_completeness_report(
        build_package_completeness_report(),
        package_id,
        mode_id,
    )?;
    Ok(serde_json::to_string_pretty(&report)? + "\n")
}

fn filter_package_completeness_report(
    mut report: PackageCompletenessReport,
    package_id: Option<&str>,
    mode_id: Option<&str>,
) -> Result<PackageCompletenessReport, PackageCompletenessFilterError> {
    if mode_id.is_some() && package_id.is_none() {
        return Err(PackageCompletenessFilterError::ModeRequiresPackage);
    }

    if let Some(package_id) = package_id {
        let package = report
            .packages
            .remove(package_id)
            .ok_or_else(|| PackageCompletenessFilterError::PackageNotFound(package_id.into()))?;
        report.packages.clear();
        report.packages.insert(package_id.into(), package);
    }

    if let Some(mode_id) = mode_id {
        let package_id = package_id.expect("mode_id requires package_id");
        let package = report
            .packages
            .get_mut(package_id)
            .expect("package filter already validated package id");
        let mode = package.modes.remove(mode_id).ok_or_else(|| {
            PackageCompletenessFilterError::ModeNotFound {
                package_id: package_id.into(),
                mode_id: mode_id.into(),
            }
        })?;
        package.modes.clear();
        package.modes.insert(mode_id.into(), mode);
    }

    report.summary = summarize_packages(&report.packages);
    Ok(report)
}

fn summarize_packages(
    packages: &BTreeMap<String, PackageCompletenessPackage>,
) -> PackageCompletenessSummary {
    let mut summary = PackageCompletenessSummary {
        package_count: packages.len(),
        mode_count: 0,
        accepted_mode_count: 0,
        proposed_mode_count: 0,
        manual_mode_count: 0,
        incomplete_mode_count: 0,
        blocking_missing_count: 0,
    };

    for mode in packages.values().flat_map(|package| package.modes.values()) {
        summary.mode_count += 1;
        match mode.mode_state {
            PackageModeState::Accepted => summary.accepted_mode_count += 1,
            PackageModeState::Proposed => summary.proposed_mode_count += 1,
            PackageModeState::Manual => summary.manual_mode_count += 1,
            PackageModeState::Incomplete => {}
        }
        if mode.completeness == PackageCompleteness::Incomplete {
            summary.incomplete_mode_count += 1;
        }
        summary.blocking_missing_count += mode.requirements.iter().filter(|r| r.blocking).count();
    }

    summary
}

fn build_unbound_mode(
    contract: &'static SourceContract,
    parser_records: &BTreeMap<String, crate::sources::dispatch::ParserInventoryRecord>,
    source_factory_ids: &BTreeSet<String>,
    payload_pairs: &BTreeMap<(String, String), String>,
    catalog_projection: &Value,
    privacy_entries: &BTreeSet<String>,
) -> PackageCompletenessMode {
    let mode_id = "unbound".to_string();
    let mut diagnostics = BaseDiagnostics::new(PackageModeState::Incomplete, None);
    diagnostics.require(
        "producer_runtime_binding",
        RequirementStatus::Missing,
        true,
        "no SourceRuntimeBinding is registered for this SourceContract",
    );
    finalize_mode(
        contract,
        None,
        mode_id,
        PackageModeState::Incomplete,
        None,
        parser_records,
        source_factory_ids,
        payload_pairs,
        catalog_projection,
        privacy_entries,
        diagnostics,
    )
}

fn build_bound_mode(
    contract: &'static SourceContract,
    binding: &'static SourceRuntimeBinding,
    parser_records: &BTreeMap<String, crate::sources::dispatch::ParserInventoryRecord>,
    source_factory_ids: &BTreeSet<String>,
    payload_pairs: &BTreeMap<(String, String), String>,
    catalog_projection: &Value,
    privacy_entries: &BTreeSet<String>,
) -> PackageCompletenessMode {
    let manual_reason = manual_reason(binding, parser_records, source_factory_ids);
    let mut mode_state = if binding.proposed {
        PackageModeState::Proposed
    } else if manual_reason.is_some() {
        PackageModeState::Manual
    } else {
        PackageModeState::Accepted
    };
    let mode_id = binding_mode_id(binding);
    let diagnostics = BaseDiagnostics::new(mode_state, manual_reason);

    let mut mode = finalize_mode(
        contract,
        Some(binding),
        mode_id,
        mode_state,
        manual_reason,
        parser_records,
        source_factory_ids,
        payload_pairs,
        catalog_projection,
        privacy_entries,
        diagnostics,
    );

    if mode.completeness == PackageCompleteness::Incomplete
        && matches!(mode_state, PackageModeState::Accepted)
    {
        // Accepted intent with missing blocking requirements is the executable
        // package-gate definition of incomplete.
        mode_state = PackageModeState::Incomplete;
        mode.mode_state = mode_state;
    }

    mode
}

#[allow(clippy::too_many_arguments)]
fn finalize_mode(
    contract: &'static SourceContract,
    binding: Option<&'static SourceRuntimeBinding>,
    mode_id: String,
    mode_state: PackageModeState,
    manual_reason: Option<&'static str>,
    parser_records: &BTreeMap<String, crate::sources::dispatch::ParserInventoryRecord>,
    source_factory_ids: &BTreeSet<String>,
    payload_pairs: &BTreeMap<(String, String), String>,
    catalog_projection: &Value,
    privacy_entries: &BTreeSet<String>,
    mut diagnostics: BaseDiagnostics,
) -> PackageCompletenessMode {
    let source_factory_registered = source_factory_ids.contains(contract.id);
    let parser_record = parser_records.get(contract.id);
    let parser_factory_registered = parser_record.is_some();
    let privacy_coverage_registered = privacy_entries.contains(contract.id);
    let subject = binding.map(|binding| binding.subject.as_str().to_string());
    let catalog_projection_registered =
        catalog_has_mode(catalog_projection, contract.id, subject.as_deref());
    let package_event_contracts = event_contracts_for_package(contract.id);
    let event_contract_refs = package_event_contracts
        .iter()
        .map(|contract| contract.id.to_string())
        .collect::<Vec<_>>();
    let admission_policy_refs = admission_policy_refs_for_event_contracts(&package_event_contracts);
    let coverage_debt_refs = capability_refs(
        binding,
        &[SourceCapabilityKind::Coverage, SourceCapabilityKind::Debt],
    );
    let operation_refs = capability_refs(binding, &[SourceCapabilityKind::Operation]);
    let event_pairs = event_pair_reports(contract, payload_pairs, &package_event_contracts);

    diagnostics.require(
        "package_identity",
        RequirementStatus::Present,
        false,
        format!(
            "SourceContract `{}` in namespace `{}`",
            contract.id, contract.namespace
        ),
    );
    diagnostics.require(
        "mode_identity",
        if binding.is_some() {
            RequirementStatus::Present
        } else {
            RequirementStatus::Missing
        },
        binding.is_none(),
        binding.map_or_else(
            || "no runtime binding id/subject".to_string(),
            |binding| {
                format!(
                    "binding `{}` subject `{}`",
                    binding.id,
                    binding.subject.as_str()
                )
            },
        ),
    );
    diagnostics.require(
        "material_class",
        if binding.is_some() {
            RequirementStatus::Present
        } else {
            RequirementStatus::Caveat
        },
        false,
        binding.map_or_else(
            || {
                format!(
                    "access scope {}; no resource profile without binding",
                    to_json_value(contract.access_scope)
                )
            },
            |binding| {
                format!(
                    "access scope {}; resource profile {}",
                    to_json_value(contract.access_scope),
                    to_json_value(binding.resource_profile)
                )
            },
        ),
    );
    if binding.is_some() {
        diagnostics.require(
            "producer_runtime_binding",
            RequirementStatus::Present,
            false,
            "SourceRuntimeBinding registered".to_string(),
        );
    }

    let parser_required = parser_required_for_mode(binding, manual_reason);
    diagnostics.require(
        "parser_binding",
        match (parser_required, parser_factory_registered) {
            (true, true) => RequirementStatus::Present,
            (true, false) => RequirementStatus::Missing,
            (false, true) => RequirementStatus::Present,
            (false, false) => RequirementStatus::NotApplicable,
        },
        parser_required && !parser_factory_registered && mode_state == PackageModeState::Accepted,
        match (parser_record, manual_reason) {
            (Some(record), _) => format!(
                "parser `{}` version `{}` registered",
                record.manifest.parser_id, record.manifest.parser_version
            ),
            (None, Some(reason)) => {
                format!("local parser factory not required for manual mode: {reason}")
            }
            (None, None) => "no parser factory registered".to_string(),
        },
    );
    diagnostics.require(
        "source_factory",
        if source_factory_required_for_mode(binding, manual_reason) {
            if source_factory_registered {
                RequirementStatus::Present
            } else {
                RequirementStatus::Missing
            }
        } else {
            RequirementStatus::NotApplicable
        },
        source_factory_required_for_mode(binding, manual_reason)
            && !source_factory_registered
            && mode_state == PackageModeState::Accepted,
        if source_factory_registered {
            "source factory registered".to_string()
        } else {
            "source factory not registered or not required by mode kind".to_string()
        },
    );

    let missing_payloads = event_pairs
        .iter()
        .filter(|pair| !pair.payload_schema_registered)
        .map(|pair| format!("{}/{}", pair.source, pair.event_type))
        .collect::<Vec<_>>();
    diagnostics.require(
        "payload_schema",
        if missing_payloads.is_empty() {
            RequirementStatus::Present
        } else {
            RequirementStatus::Missing
        },
        !missing_payloads.is_empty() && mode_state == PackageModeState::Accepted,
        if missing_payloads.is_empty() {
            "every SourceContract event pair has an EventPayload schema".to_string()
        } else {
            format!(
                "missing EventPayload schema(s): {}",
                missing_payloads.join(", ")
            )
        },
    );
    diagnostics.require(
        "event_contract_refs",
        if event_contract_refs.is_empty() {
            RequirementStatus::Missing
        } else {
            RequirementStatus::Present
        },
        event_contract_refs.is_empty() && mode_state == PackageModeState::Accepted,
        if event_contract_refs.is_empty() {
            "no EventContract registry refs name this package/mode yet".to_string()
        } else {
            format!("EventContract refs: {}", event_contract_refs.join(", "))
        },
    );
    diagnostics.require(
        "admission_policy_ref",
        if admission_policy_refs.is_empty() {
            RequirementStatus::Missing
        } else {
            RequirementStatus::Present
        },
        admission_policy_refs.is_empty() && mode_state == PackageModeState::Accepted,
        if admission_policy_refs.is_empty() {
            "no AdmissionPolicy accepts this package/mode's EventContract refs yet".to_string()
        } else {
            format!("AdmissionPolicy refs: {}", admission_policy_refs.join(", "))
        },
    );
    diagnostics.require(
        "identity_policy",
        RequirementStatus::Present,
        false,
        format!(
            "occurrence identity {}",
            to_json_value(contract.occurrence_identity)
        ),
    );
    diagnostics.require(
        "privacy_disclosure_policy",
        if privacy_coverage_registered {
            RequirementStatus::Present
        } else {
            RequirementStatus::Missing
        },
        !privacy_coverage_registered && mode_state == PackageModeState::Accepted,
        if privacy_coverage_registered {
            "privacy coverage matrix has an entry for this package".to_string()
        } else {
            "privacy coverage matrix is missing this package".to_string()
        },
    );
    diagnostics.require(
        "storage_material_lifecycle_policy",
        RequirementStatus::Caveat,
        false,
        format!(
            "SourceContract retention is {}; explicit material lifecycle policy is not separated yet",
            to_json_value(contract.retention)
        ),
    );
    diagnostics.require(
        "resource_budget_spec",
        if binding.is_some() {
            RequirementStatus::Present
        } else {
            RequirementStatus::Missing
        },
        binding.is_none() && mode_state == PackageModeState::Accepted,
        binding.map_or_else(
            || "no binding/resource profile; ResourceBudgetSpec cannot be derived".to_string(),
            |binding| {
                format!(
                    "ResourceProfile `{}` has limits {} and budget {}",
                    to_json_value(binding.resource_profile),
                    to_json_value(binding.resource_profile.limits()),
                    to_json_value(binding.resource_budget())
                )
            },
        ),
    );
    diagnostics.require(
        "transport_semantics",
        RequirementStatus::Caveat,
        false,
        binding.map_or_else(
            || "transport cannot be inferred without a runtime binding".to_string(),
            |binding| {
                format!(
                    "runner {}, runtime {}; ack/replay/DLQ policy not explicit in package metadata",
                    to_json_value(binding.runner_pack),
                    to_json_value(binding.runtime_shape)
                )
            },
        ),
    );
    diagnostics.require(
        "runtime_profile",
        if binding.is_some() {
            RequirementStatus::Present
        } else {
            RequirementStatus::Missing
        },
        binding.is_none() && mode_state == PackageModeState::Accepted,
        binding.map_or_else(
            || "checkpoint/runtime shape missing".to_string(),
            |binding| {
                format!(
                    "checkpoint {}, runtime {}",
                    to_json_value(binding.checkpoint_family),
                    to_json_value(binding.runtime_shape)
                )
            },
        ),
    );
    diagnostics.require(
        "fixtures_and_tests",
        RequirementStatus::Caveat,
        false,
        "first slice observes registry/parser/catalog/privacy tests but does not yet map fixture tests per package mode".to_string(),
    );
    diagnostics.require(
        "coverage_and_debt_views",
        if has_capability_ref(&coverage_debt_refs, SourceCapabilityKind::Coverage)
            && has_capability_ref(&coverage_debt_refs, SourceCapabilityKind::Debt)
        {
            RequirementStatus::Present
        } else {
            RequirementStatus::Missing
        },
        mode_state == PackageModeState::Accepted
            && (!has_capability_ref(&coverage_debt_refs, SourceCapabilityKind::Coverage)
                || !has_capability_ref(&coverage_debt_refs, SourceCapabilityKind::Debt)),
        if coverage_debt_refs.is_empty() {
            "SourceCoverage exists; unified DebtListView provider refs are not declared per package mode yet".to_string()
        } else {
            format!(
                "coverage/debt refs: {}",
                coverage_debt_refs.join(", ")
            )
        },
    );
    diagnostics.require(
        "operations",
        if operation_refs.is_empty() {
            RequirementStatus::Missing
        } else {
            RequirementStatus::Present
        },
        mode_state == PackageModeState::Accepted && operation_refs.is_empty(),
        if operation_refs.is_empty() {
            "OperationView/action refs from #1691 are not declared per package mode yet".to_string()
        } else {
            format!("operation refs: {}", operation_refs.join(", "))
        },
    );
    diagnostics.require(
        "deployment_catalog_projection",
        if catalog_projection_registered {
            RequirementStatus::Present
        } else {
            RequirementStatus::Missing
        },
        !catalog_projection_registered && mode_state == PackageModeState::Accepted,
        if catalog_projection_registered {
            "generated source catalog has a matching package/mode projection".to_string()
        } else {
            "generated source catalog is missing this binding projection; multi-binding modes should not collapse to one row".to_string()
        },
    );
    diagnostics.require(
        "review_output",
        RequirementStatus::Present,
        false,
        "this package-completeness JSON row is emitted from code inventories".to_string(),
    );

    let (requirements, missing, caveats) = diagnostics.finish();
    let completeness = if requirements.iter().any(|r| r.blocking) {
        PackageCompleteness::Incomplete
    } else {
        PackageCompleteness::Complete
    };

    PackageCompletenessMode {
        mode_id,
        package_id: contract.id.to_string(),
        mode_state,
        completeness,
        manual_reason,
        subject,
        acquisition_kind: acquisition_kind(binding),
        operator_enablement: operator_enablement(binding, manual_reason),
        event_pairs,
        event_contract_refs,
        admission_policy_refs,
        coverage_debt_refs,
        operation_refs,
        sources: ModeSourceRefs {
            source_contract: source_contract_ref(contract),
            runtime_binding: binding.map(runtime_binding_ref),
            parser_manifest: parser_record.map(parser_manifest_ref),
            source_factory_registered,
            parser_factory_registered,
            catalog_projection_registered,
            privacy_coverage_registered,
        },
        requirements,
        missing,
        caveats,
    }
}

struct BaseDiagnostics {
    requirements: Vec<RequirementDiagnostic>,
    missing: Vec<String>,
    caveats: Vec<String>,
}

impl BaseDiagnostics {
    fn new(mode_state: PackageModeState, manual_reason: Option<&'static str>) -> Self {
        let mut caveats = Vec::new();
        match mode_state {
            PackageModeState::Proposed => caveats.push(
                "proposed mode: metadata may be incomplete, but owner/action caveats must stay visible"
                    .to_string(),
            ),
            PackageModeState::Manual => caveats.push(format!(
                "manual mode: {}",
                manual_reason.unwrap_or("manual reason not declared")
            )),
            PackageModeState::Accepted | PackageModeState::Incomplete => {}
        }
        Self {
            requirements: Vec::new(),
            missing: Vec::new(),
            caveats,
        }
    }

    fn require(
        &mut self,
        id: &'static str,
        status: RequirementStatus,
        blocking: bool,
        detail: impl Into<String>,
    ) {
        let detail = detail.into();
        if matches!(status, RequirementStatus::Missing) {
            self.missing.push(id.to_string());
        }
        if matches!(status, RequirementStatus::Caveat) {
            self.caveats.push(format!("{id}: {detail}"));
        }
        self.requirements.push(RequirementDiagnostic {
            id,
            status,
            blocking,
            detail,
        });
    }

    fn finish(self) -> (Vec<RequirementDiagnostic>, Vec<String>, Vec<String>) {
        (self.requirements, self.missing, self.caveats)
    }
}

fn collect_bindings_by_source() -> BTreeMap<&'static str, Vec<&'static SourceRuntimeBinding>> {
    let mut map: BTreeMap<&'static str, Vec<&'static SourceRuntimeBinding>> = BTreeMap::new();
    for binding in source_runtime_bindings() {
        map.entry(binding.source_id).or_default().push(binding);
    }
    for bindings in map.values_mut() {
        bindings.sort_by(|a, b| a.subject.as_str().cmp(b.subject.as_str()));
    }
    map
}

fn payload_schema_pairs() -> BTreeMap<(String, String), String> {
    get_all_payloads()
        .map(|payload| {
            (
                (payload.source.to_string(), payload.event_type.to_string()),
                payload.version.to_string(),
            )
        })
        .collect()
}

fn privacy_entry_ids(value: &Value) -> BTreeSet<String> {
    value
        .get("entries")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.get("source_id").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect()
}

fn rendered_json(rendered: serde_json::Result<String>) -> Option<Value> {
    rendered
        .ok()
        .and_then(|text| serde_json::from_str::<Value>(&text).ok())
}

fn binding_mode_id(binding: &SourceRuntimeBinding) -> String {
    binding
        .subject
        .as_str()
        .strip_prefix("source:")
        .unwrap_or(binding.id)
        .to_string()
}

fn catalog_has_mode(catalog: &Value, contract_id: &str, subject: Option<&str>) -> bool {
    catalog
        .get("entries")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .any(|entry| {
            let contract_matches = entry
                .get("contract")
                .and_then(|contract| contract.get("id"))
                .and_then(Value::as_str)
                == Some(contract_id);
            if !contract_matches {
                return false;
            }
            match subject {
                Some(subject) => catalog_entry_has_subject(entry, subject),
                None => true,
            }
        })
}

fn catalog_entry_has_subject(entry: &Value, subject: &str) -> bool {
    entry
        .get("runtime_bindings")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .any(|binding| binding.get("subject").and_then(Value::as_str) == Some(subject))
        || entry
            .get("binding")
            .and_then(|binding| binding.get("subject"))
            .and_then(Value::as_str)
            == Some(subject)
}

fn manual_reason(
    binding: &SourceRuntimeBinding,
    parser_records: &BTreeMap<String, crate::sources::dispatch::ParserInventoryRecord>,
    source_factory_ids: &BTreeSet<String>,
) -> Option<&'static str> {
    if binding.proposed {
        return None;
    }
    match binding.runner_pack {
        RunnerPack::External => Some("external_producer_no_local_runtime"),
        RunnerPack::InProcess => Some("in_process_emitter_or_projection"),
        RunnerPack::SinexdSource | RunnerPack::Live | RunnerPack::Staged => {
            let has_parser = parser_records.contains_key(binding.source_id);
            let has_source_factory = source_factory_ids.contains(binding.source_id);
            if has_parser && !has_source_factory {
                Some("parser_only_dispatch_no_source_factory")
            } else {
                None
            }
        }
    }
}

fn parser_required_for_mode(
    binding: Option<&SourceRuntimeBinding>,
    manual_reason: Option<&'static str>,
) -> bool {
    let Some(binding) = binding else {
        return true;
    };
    if binding.proposed {
        return false;
    }
    if matches!(
        manual_reason,
        Some("external_producer_no_local_runtime" | "in_process_emitter_or_projection")
    ) {
        return false;
    }
    matches!(
        binding.runner_pack,
        RunnerPack::SinexdSource | RunnerPack::Live | RunnerPack::Staged
    )
}

fn source_factory_required_for_mode(
    binding: Option<&SourceRuntimeBinding>,
    manual_reason: Option<&'static str>,
) -> bool {
    let Some(binding) = binding else {
        return true;
    };
    if binding.proposed || manual_reason.is_some() {
        return false;
    }
    matches!(
        binding.runner_pack,
        RunnerPack::SinexdSource | RunnerPack::Live | RunnerPack::Staged
    )
}

fn event_pair_reports(
    contract: &SourceContract,
    payload_pairs: &BTreeMap<(String, String), String>,
    package_event_contracts: &[&EventContract],
) -> Vec<EventPairReport> {
    contract
        .event_types
        .iter()
        .map(|(source, event_type)| {
            let key = ((*source).to_string(), (*event_type).to_string());
            EventPairReport {
                source: key.0.clone(),
                event_type: key.1.clone(),
                payload_schema_registered: payload_pairs.contains_key(&key),
                payload_schema_version: payload_pairs.get(&key).cloned(),
                event_contract_ref: package_event_contracts
                    .iter()
                    .find(|contract| contract.event_source == key.0 && contract.event_type == key.1)
                    .map(|contract| contract.id.to_string()),
            }
        })
        .collect()
}

fn event_contracts_for_package(package_id: &str) -> Vec<&'static EventContract> {
    let mut contracts = event_contracts()
        .filter(|contract| contract.package_refs.contains(&package_id))
        .collect::<Vec<_>>();
    contracts.sort_by(|a, b| a.id.cmp(b.id));
    contracts
}

fn admission_policy_refs_for_event_contracts(contracts: &[&'static EventContract]) -> Vec<String> {
    let contract_ids = contracts
        .iter()
        .map(|contract| contract.id)
        .collect::<BTreeSet<_>>();
    let mut policies = admission_policies()
        .filter(|policy| {
            policy
                .accepted_event_contracts
                .iter()
                .any(|contract_id| contract_ids.contains(contract_id))
        })
        .map(|policy: &AdmissionPolicy| policy.id.to_string())
        .collect::<Vec<_>>();
    policies.sort();
    policies.dedup();
    policies
}

fn source_contract_ref(contract: &SourceContract) -> SourceContractRef {
    SourceContractRef {
        id: contract.id.to_string(),
        namespace: contract.namespace.to_string(),
        privacy_tier: to_json_value(contract.privacy_tier),
        horizons: to_json_value(contract.horizons),
        retention: to_json_value(contract.retention),
        occurrence_identity: to_json_value(contract.occurrence_identity),
        access_scope: to_json_value(contract.access_scope),
    }
}

fn runtime_binding_ref(binding: &SourceRuntimeBinding) -> RuntimeBindingRef {
    RuntimeBindingRef {
        id: binding.id.to_string(),
        subject: binding.subject.as_str().to_string(),
        domain: binding.domain.to_string(),
        implementation: binding.implementation.to_string(),
        adapter: binding.adapter.to_string(),
        output_event_type: binding.output_event_type.to_string(),
        privacy_context: to_json_value(binding.privacy_context),
        resource_profile: to_json_value(binding.resource_profile),
        capabilities: binding
            .capabilities
            .iter()
            .map(|capability| (*capability).to_string())
            .collect(),
        resource_limits: to_json_value(binding.resource_profile.limits()),
        resource_budget: to_json_value(binding.resource_budget()),
        proposed: binding.proposed,
        runner_pack: to_json_value(binding.runner_pack),
        checkpoint_family: to_json_value(binding.checkpoint_family),
        runtime_shape: to_json_value(binding.runtime_shape),
        build_impact: to_json_value(binding.build_impact),
    }
}

fn parser_manifest_ref(
    record: &crate::sources::dispatch::ParserInventoryRecord,
) -> ParserManifestRef {
    ParserManifestRef {
        parser_id: record.manifest.parser_id.as_str().to_string(),
        parser_version: record.manifest.parser_version.clone(),
        source_id: record.manifest.source_id.as_str().to_string(),
        accepted_input_shapes: to_json_value(&record.manifest.accepted_input_shapes),
        declared_event_types: record
            .manifest
            .declared_event_types
            .iter()
            .map(|(source, event_type)| EventPairReport {
                source: source.as_str().to_string(),
                event_type: event_type.as_str().to_string(),
                payload_schema_registered: true,
                payload_schema_version: None,
                event_contract_ref: None,
            })
            .collect(),
        field_privacy_metadata_count: record.field_privacy_metadata.len(),
    }
}

fn capability_refs(
    binding: Option<&SourceRuntimeBinding>,
    kinds: &[SourceCapabilityKind],
) -> Vec<String> {
    let mut refs = binding
        .into_iter()
        .flat_map(SourceRuntimeBinding::capability_refs)
        .filter(|capability| kinds.contains(&capability.kind))
        .map(|capability| capability.raw.to_string())
        .collect::<Vec<_>>();
    refs.sort();
    refs.dedup();
    refs
}

fn has_capability_ref(refs: &[String], kind: SourceCapabilityKind) -> bool {
    refs.iter()
        .filter_map(|capability| SourceCapabilityRef::parse(capability))
        .any(|capability| capability.is_kind(kind))
}

fn acquisition_kind(binding: Option<&SourceRuntimeBinding>) -> &'static str {
    let Some(binding) = binding else {
        return "unbound";
    };
    if binding.proposed {
        return "proposed";
    }
    match binding.runner_pack {
        RunnerPack::External => "external_producer",
        RunnerPack::InProcess => "in_process",
        RunnerPack::Live => "live",
        RunnerPack::Staged => "staged",
        RunnerPack::SinexdSource => match binding.runtime_shape {
            sinex_primitives::source_contracts::RuntimeShape::Continuous => "continuous_source",
            sinex_primitives::source_contracts::RuntimeShape::OnDemand => "on_demand_source",
            sinex_primitives::source_contracts::RuntimeShape::Scheduled => "scheduled_source",
        },
    }
}

fn operator_enablement(
    binding: Option<&SourceRuntimeBinding>,
    manual_reason: Option<&'static str>,
) -> &'static str {
    let Some(binding) = binding else {
        return "unavailable_no_runtime_binding";
    };
    if binding.proposed {
        "not_runnable_proposed"
    } else if manual_reason.is_some() {
        "manual_or_external_enablement_required"
    } else {
        "runnable_when_deployment_enables_binding"
    }
}

fn to_json_value<T: Serialize>(value: T) -> Value {
    match serde_json::to_value(value) {
        Ok(value) => value,
        Err(error) => json!({
            "error": "package_completeness_metadata_serialization_failed",
            "message": error.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::privacy::ProcessingContext;
    use sinex_primitives::source_contracts::{
        CheckpointFamily, ResourceProfile, RuntimeShape, SourceBuildImpact, SubjectRef,
    };

    #[test]
    fn filtered_report_recomputes_package_summary() {
        let rendered =
            render_filtered_package_completeness_report(Some("terminal.kitty-osc-live"), None)
                .unwrap();
        let report: Value = serde_json::from_str(&rendered).unwrap();

        assert_eq!(report["summary"]["package_count"], 1);
        let packages = report["packages"].as_object().unwrap();
        assert_eq!(packages.len(), 1);
        let package = packages.get("terminal.kitty-osc-live").unwrap();
        let mode_count = package["modes"].as_object().unwrap().len();
        assert_eq!(report["summary"]["mode_count"], mode_count);
    }

    #[test]
    fn filtered_report_recomputes_package_mode_summary() {
        let rendered = render_filtered_package_completeness_report(
            Some("terminal.kitty-osc-live"),
            Some("terminal.kitty-osc-live"),
        )
        .unwrap();
        let report: Value = serde_json::from_str(&rendered).unwrap();

        assert_eq!(report["summary"]["package_count"], 1);
        assert_eq!(report["summary"]["mode_count"], 1);
        let package = &report["packages"]["terminal.kitty-osc-live"];
        let modes = package["modes"].as_object().unwrap();
        assert_eq!(modes.len(), 1);
        assert!(modes.contains_key("terminal.kitty-osc-live"));
    }

    #[test]
    fn mode_filter_requires_package_id() {
        let err =
            render_filtered_package_completeness_report(None, Some("terminal.kitty-osc-live"))
                .unwrap_err();

        assert!(matches!(
            err,
            PackageCompletenessFilterError::ModeRequiresPackage
        ));
    }

    #[test]
    fn capability_report_refs_are_filtered_through_typed_parser() {
        static CAPABILITIES: &[&str] = &[
            "coverage:source-coverage",
            "debt:unified-debt-view",
            "operation:fixture.source.check",
            "operation:",
            "package:fixture.source",
        ];
        let binding = SourceRuntimeBinding::builder(
            SubjectRef::from_static("source:fixture.source"),
            "fixture.source",
            "fixture",
        )
        .implementation("test")
        .adapter("static")
        .output_event_type("fixture.event")
        .privacy_context(ProcessingContext::Metadata)
        .resource_profile(ResourceProfile::EmbeddedEmitter)
        .capabilities(CAPABILITIES)
        .checkpoint_family(CheckpointFamily::AppendStream)
        .runtime_shape(RuntimeShape::OnDemand)
        .build_impact(SourceBuildImpact::ZERO)
        .build();

        assert_eq!(
            capability_refs(
                Some(&binding),
                &[SourceCapabilityKind::Coverage, SourceCapabilityKind::Debt]
            ),
            vec![
                "coverage:source-coverage".to_string(),
                "debt:unified-debt-view".to_string()
            ]
        );
        assert_eq!(
            capability_refs(Some(&binding), &[SourceCapabilityKind::Operation]),
            vec!["operation:fixture.source.check".to_string()]
        );
    }
}

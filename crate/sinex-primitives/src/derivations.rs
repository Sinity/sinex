//! Derivation contracts for durable derived outputs.
//!
//! This is metadata, not a generic projection runtime. It connects existing
//! projection/artifact declarations to output-kind, freshness, invalidation,
//! and operation-planning vocabulary so replay/archive/redaction work can
//! report which derived outputs are affected.

use schemars::JsonSchema;
use serde::Serialize;

use crate::output_kind::OutputKind;
use crate::task_domain::{TASK_REDUCER_DOMAIN_ID, TASK_REDUCER_INPUT_EVENT_TYPES};

pub type DerivationSpecId = &'static str;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DerivationInputScope {
    EventTypes {
        domain_id: &'static str,
        event_types: &'static [&'static str],
    },
    MaterialClass {
        material_class: &'static str,
    },
    QueryScope {
        scope: &'static str,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FreshnessPolicy {
    RebuildOnInputChange,
    RefreshOnRead,
    EphemeralQuery,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InvalidationTrigger {
    Replay,
    Archive,
    Redaction,
    SourceMaterialChange,
    ParserSemanticsChange,
    DisclosurePolicyChange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DerivationOperationHook {
    Rebuild,
    Refresh,
    Explain,
    Redact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
pub struct DerivationSpec {
    pub id: DerivationSpecId,
    pub input_scope: DerivationInputScope,
    pub output_id: &'static str,
    pub output_kind: OutputKind,
    pub freshness_policy: FreshnessPolicy,
    pub invalidates_on: &'static [InvalidationTrigger],
    pub rebuild_resource_policy_ref: Option<&'static str>,
    pub disclosure_policy_ref: Option<&'static str>,
    pub operation_hooks: &'static [DerivationOperationHook],
}

impl DerivationSpec {
    #[must_use]
    pub fn invalidates_on(&self, trigger: InvalidationTrigger) -> bool {
        self.invalidates_on.contains(&trigger)
    }
}

pub const TASK_CURRENT_OBJECTS_DERIVATION_ID: DerivationSpecId =
    "derivation:tasks.current/domain.current_objects@v1";

pub const TASK_CURRENT_OBJECTS_DERIVATION: DerivationSpec = DerivationSpec {
    id: TASK_CURRENT_OBJECTS_DERIVATION_ID,
    input_scope: DerivationInputScope::EventTypes {
        domain_id: TASK_REDUCER_DOMAIN_ID,
        event_types: TASK_REDUCER_INPUT_EVENT_TYPES,
    },
    output_id: "domain.current_objects",
    output_kind: OutputKind::ProjectionRow,
    freshness_policy: FreshnessPolicy::RebuildOnInputChange,
    invalidates_on: &[
        InvalidationTrigger::Replay,
        InvalidationTrigger::Archive,
        InvalidationTrigger::Redaction,
        InvalidationTrigger::ParserSemanticsChange,
        InvalidationTrigger::DisclosurePolicyChange,
    ],
    rebuild_resource_policy_ref: Some("resource-policy:projection.rebuild.standard"),
    disclosure_policy_ref: Some("operator.default-disclosure"),
    operation_hooks: &[
        DerivationOperationHook::Rebuild,
        DerivationOperationHook::Explain,
    ],
};

pub const DERIVATION_SPECS: &[DerivationSpec] = &[TASK_CURRENT_OBJECTS_DERIVATION];

pub fn derivation_specs() -> impl Iterator<Item = &'static DerivationSpec> {
    DERIVATION_SPECS.iter()
}

#[must_use]
pub fn find_derivation_spec(id: &str) -> Option<&'static DerivationSpec> {
    derivation_specs().find(|spec| spec.id == id)
}

pub fn derivations_for_output(output_id: &str) -> impl Iterator<Item = &'static DerivationSpec> {
    derivation_specs().filter(move |spec| spec.output_id == output_id)
}

pub fn affected_derivations(
    trigger: InvalidationTrigger,
) -> impl Iterator<Item = &'static DerivationSpec> {
    derivation_specs().filter(move |spec| spec.invalidates_on(trigger))
}

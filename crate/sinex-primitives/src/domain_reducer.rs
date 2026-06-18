//! Shared vocabulary for event-native current-state reducers.
//!
//! NOTE: metadata only. No generic spec-driven reducer runtime consumes
//! `DomainProjectionSpec` yet; concrete reducers (e.g. `reduce_task_event`) are
//! invoked directly. A spec-driven reducer engine is tracked by #1120.

use schemars::JsonSchema;
use serde::Serialize;

use crate::output_kind::OutputKind;

/// Declarative identity and contract metadata for one reducer projection family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct DomainProjectionSpec {
    pub domain_id: &'static str,
    pub semantics_version: &'static str,
    pub object_kind: &'static str,
    pub input_event_types: &'static [&'static str],
    pub object_key_policy: &'static str,
    pub ordering_policy: ProjectionOrderingPolicy,
    pub settlement_policy: ProjectionSettlementPolicy,
    pub conflict_policy: ProjectionConflictPolicy,
    pub output_kind: OutputKind,
    pub output_shape: ProjectionOutputShape,
}

/// Event ordering rule used before reducer application.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionOrderingPolicy {
    TsOrigThenEventId,
}

/// Late-arrival posture for the projected current state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionSettlementPolicy {
    RebuildOnInputChange,
}

/// Conflict handling rule exposed by the reducer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionConflictPolicy {
    RejectInvalidTransition,
}

/// Shape of the reducer output stored or returned by read surfaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionOutputShape {
    TypedState,
}

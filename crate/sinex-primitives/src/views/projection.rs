//! `ProjectionReadinessView` (sinex-68c.1): renders readiness caveats for
//! rebuildable read-model ("projection") state, sourced from
//! `derivation.projection_registry` rows.
//!
//! This is deliberately a pure rendering layer: it consumes
//! [`ProjectionReadinessInput`] values (a DB-shape-agnostic projection of a
//! registry row, or a read-time-computed stand-in for a projection not yet
//! migrated into the registry — see the module doc on
//! `sinex_primitives::derivation::ProjectionStatus`) and turns each row's
//! `status` into the shared [`CaveatView`]/[`ReadinessCaveatId`] vocabulary.
//! It does not itself decide whether a projection IS stale — that judgment
//! lives with whichever writer transitions the registry row (or, for the
//! first slice, with the read-time freshness comparison in the caller) —
//! this view only renders the judgment that already landed in `status`.

use super::{CaveatView, ReadinessCaveatId};
use crate::derivation::{ProjectionFreshnessClass, ProjectionStatus};
use crate::temporal::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const PROJECTION_READINESS_VIEW_SCHEMA_VERSION: &str = "sinex.projection-readiness/v1";

/// DB-shape-agnostic input row: either a `derivation.projection_registry`
/// record, or (first slice) a read-time-computed stand-in for a projection
/// that has not yet been migrated to write registry rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionReadinessInput {
    pub projection_kind: String,
    pub scope_key: String,
    pub semantics_version: String,
    pub status: ProjectionStatus,
    pub freshness_class: ProjectionFreshnessClass,
    pub built_at: Option<Timestamp>,
    pub stale_reason: Option<String>,
    pub last_error: Option<String>,
    pub verification_command: String,
    /// `true` when this row was computed at read time from the projection's
    /// own backing table rather than read from `derivation.projection_registry`
    /// (sinex-68c.1 first slice — see AC: "first slice may hardcode current
    /// read-time projections").
    #[allow(clippy::struct_excessive_bools)]
    pub read_time_computed: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ProjectionReadinessEntry {
    pub projection_kind: String,
    pub scope_key: String,
    pub semantics_version: String,
    pub status: ProjectionStatus,
    pub freshness_class: ProjectionFreshnessClass,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub built_at: Option<Timestamp>,
    pub verification_command: String,
    #[serde(default)]
    pub read_time_computed: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<CaveatView>,
}

impl From<ProjectionReadinessInput> for ProjectionReadinessEntry {
    fn from(input: ProjectionReadinessInput) -> Self {
        let caveats = readiness_caveats(&input);
        Self {
            projection_kind: input.projection_kind,
            scope_key: input.scope_key,
            semantics_version: input.semantics_version,
            status: input.status,
            freshness_class: input.freshness_class,
            built_at: input.built_at,
            verification_command: input.verification_command,
            read_time_computed: input.read_time_computed,
            caveats,
        }
    }
}

fn readiness_caveats(input: &ProjectionReadinessInput) -> Vec<CaveatView> {
    let id = match input.status {
        ProjectionStatus::Ready => return Vec::new(),
        ProjectionStatus::Absent => ReadinessCaveatId::ReadmodelAbsent,
        ProjectionStatus::Building => ReadinessCaveatId::ReadmodelBuilding,
        ProjectionStatus::Stale => ReadinessCaveatId::ReadmodelStaleBy,
        ProjectionStatus::Failed => ReadinessCaveatId::ReadmodelFailed,
        ProjectionStatus::Partial => ReadinessCaveatId::ReadmodelPartial,
    };
    let detail = input
        .last_error
        .as_deref()
        .or(input.stale_reason.as_deref());
    let message = match detail {
        Some(detail) => format!(
            "{} ({}/{}): {detail}",
            input.status, input.projection_kind, input.scope_key
        ),
        None => format!(
            "{} ({}/{})",
            input.status, input.projection_kind, input.scope_key
        ),
    };
    vec![CaveatView {
        id: id.as_str().to_string(),
        message,
        ref_: None,
    }]
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ProjectionReadinessView {
    pub schema_version: String,
    pub count: usize,
    pub ready_count: usize,
    pub degraded_count: usize,
    pub projections: Vec<ProjectionReadinessEntry>,
}

impl ProjectionReadinessView {
    #[must_use]
    pub fn from_rows(rows: impl IntoIterator<Item = ProjectionReadinessInput>) -> Self {
        let projections: Vec<ProjectionReadinessEntry> = rows
            .into_iter()
            .map(ProjectionReadinessEntry::from)
            .collect();
        let ready_count = projections
            .iter()
            .filter(|entry| entry.status.is_read_ready())
            .count();
        let degraded_count = projections.len() - ready_count;
        Self {
            schema_version: PROJECTION_READINESS_VIEW_SCHEMA_VERSION.to_string(),
            count: projections.len(),
            ready_count,
            degraded_count,
            projections,
        }
    }
}

#[cfg(test)]
#[path = "projection_test.rs"]
mod tests;

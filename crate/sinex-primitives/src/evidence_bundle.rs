//! Evidence bundle read-profile DTOs.
//!
//! An evidence bundle is a finite view over existing Sinex observability
//! surfaces. It is not an incident model and not a new source of truth.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::public_ref::ResolvedObjectView;
use crate::temporal::Timestamp;
use crate::views::{
    ActionAvailability, CaveatView, DebtRowView, OperationView, SinexObjectRef, SourceCoverageView,
};

pub const EVIDENCE_BUNDLE_SCHEMA_VERSION: &str = "sinex.evidence-bundle/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceBundleSeedKind {
    PublicRef,
    DebtQuery,
    Operation,
    SourceDriver,
    OperatorNote,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EvidenceBundleSeedView {
    pub kind: EvidenceBundleSeedKind,
    pub value: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ref_: Option<SinexObjectRef>,
}

impl EvidenceBundleSeedView {
    #[must_use]
    pub fn public_ref(ref_: SinexObjectRef) -> Self {
        Self {
            kind: EvidenceBundleSeedKind::PublicRef,
            value: ref_.to_string(),
            ref_: Some(ref_),
        }
    }

    #[must_use]
    pub fn debt_query(value: impl Into<String>) -> Self {
        Self {
            kind: EvidenceBundleSeedKind::DebtQuery,
            value: value.into(),
            ref_: None,
        }
    }

    #[must_use]
    pub fn operation(operation_id: impl Into<String>) -> Self {
        let operation_id = operation_id.into();
        Self {
            kind: EvidenceBundleSeedKind::Operation,
            value: operation_id.clone(),
            ref_: Some(SinexObjectRef::new(
                crate::views::SinexObjectKind::Operation,
                operation_id,
            )),
        }
    }

    #[must_use]
    pub fn source_driver(source_id: impl Into<String>) -> Self {
        let source_id = source_id.into();
        Self {
            kind: EvidenceBundleSeedKind::SourceDriver,
            value: source_id.clone(),
            ref_: Some(SinexObjectRef::new(
                crate::views::SinexObjectKind::SourceDriver,
                source_id,
            )),
        }
    }

    #[must_use]
    pub fn operator_note(note: impl Into<String>) -> Self {
        Self {
            kind: EvidenceBundleSeedKind::OperatorNote,
            value: note.into(),
            ref_: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EvidenceBundleOmissionView {
    pub section: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<CaveatView>,
}

impl EvidenceBundleOmissionView {
    #[must_use]
    pub fn new(section: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            section: section.into(),
            reason: reason.into(),
            caveats: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EvidenceBundleView {
    pub schema_version: String,
    pub generated_at: Timestamp,
    pub source_surface: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_context: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub seeds: Vec<EvidenceBundleSeedView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolved_objects: Vec<ResolvedObjectView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_coverage: Vec<SourceCoverageView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub debt_rows: Vec<DebtRowView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub operations: Vec<OperationView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub omitted_sections: Vec<EvidenceBundleOmissionView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<CaveatView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ActionAvailability>,
}

impl EvidenceBundleView {
    #[must_use]
    pub fn new(source_surface: impl Into<String>) -> Self {
        Self {
            schema_version: EVIDENCE_BUNDLE_SCHEMA_VERSION.to_string(),
            generated_at: Timestamp::now(),
            source_surface: source_surface.into(),
            target_context: None,
            seeds: Vec::new(),
            resolved_objects: Vec::new(),
            source_coverage: Vec::new(),
            debt_rows: Vec::new(),
            operations: Vec::new(),
            omitted_sections: Vec::new(),
            caveats: Vec::new(),
            actions: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_target_context(mut self, target_context: Option<String>) -> Self {
        self.target_context = target_context;
        self
    }

    #[must_use]
    pub fn section_count(&self) -> usize {
        [
            !self.resolved_objects.is_empty(),
            !self.source_coverage.is_empty(),
            !self.debt_rows.is_empty(),
            !self.operations.is_empty(),
        ]
        .into_iter()
        .filter(|included| *included)
        .count()
    }

    #[must_use]
    pub fn evidence_row_count(&self) -> usize {
        self.resolved_objects.len()
            + self.source_coverage.len()
            + self.debt_rows.len()
            + self.operations.len()
    }
}

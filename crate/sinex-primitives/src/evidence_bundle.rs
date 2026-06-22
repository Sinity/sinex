//! Evidence bundle read-profile DTOs.
//!
//! An evidence bundle is a finite view over existing Sinex observability
//! surfaces. It is not an incident model and not a new source of truth.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::public_ref::ResolvedObjectView;
use crate::rpc::sources::SourcePackageCompletenessPackageView;
use crate::temporal::Timestamp;
use crate::views::{
    ActionAvailability, CaveatView, DebtRowView, OperationView, SinexObjectRef, SourceCoverageView,
};

pub const EVIDENCE_BUNDLE_SCHEMA_VERSION: &str = "sinex.evidence-bundle/v2";
pub const EVIDENCE_BUNDLE_SPEC_SCHEMA_VERSION: &str = "sinex.evidence-bundle-spec/v2";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceBundleSeedKind {
    PublicRef,
    DebtQuery,
    Operation,
    SourceDriver,
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
pub struct EvidenceBundleSpec {
    pub schema_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_context: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub seeds: Vec<EvidenceBundleSeedView>,
    #[serde(default)]
    pub include_debt: bool,
    #[serde(default)]
    pub include_capture: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection_trigger: Option<String>,
    #[serde(default)]
    pub include_runtime_health: bool,
    #[serde(default)]
    pub include_package_completeness: bool,
    #[serde(default)]
    pub save_artifact: bool,
}

impl EvidenceBundleSpec {
    #[must_use]
    pub fn new() -> Self {
        Self {
            schema_version: EVIDENCE_BUNDLE_SPEC_SCHEMA_VERSION.to_string(),
            target_context: None,
            seeds: Vec::new(),
            include_debt: false,
            include_capture: false,
            projection_trigger: None,
            include_runtime_health: false,
            include_package_completeness: false,
            save_artifact: false,
        }
    }
}

impl Default for EvidenceBundleSpec {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EvidenceBundleRuntimeHealthView {
    pub stale_after_secs: u64,
    pub active_count: i64,
    pub inactive_count: i64,
    pub unique_modules: i64,
    pub active_run_count: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oldest_heartbeat: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EvidenceBundleSavedArtifactView {
    #[serde(rename = "ref")]
    pub ref_: SinexObjectRef,
    pub content_key: String,
    pub content_type: String,
    pub size: u64,
    pub blake3_hash: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EvidenceBundleDiagnosticExcerptView {
    pub section: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ref: Option<SinexObjectRef>,
    pub excerpt: String,
    pub max_chars: usize,
    pub truncated: bool,
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
    pub target_refs: Vec<SinexObjectRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolved_objects: Vec<ResolvedObjectView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_coverage: Vec<SourceCoverageView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub debt_rows: Vec<DebtRowView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub operations: Vec<OperationView>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_health: Option<EvidenceBundleRuntimeHealthView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub package_completeness: Vec<SourcePackageCompletenessPackageView>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub saved_artifact: Option<EvidenceBundleSavedArtifactView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostic_excerpts: Vec<EvidenceBundleDiagnosticExcerptView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub omitted_sections: Vec<EvidenceBundleOmissionView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<CaveatView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disclosure_caveats: Vec<CaveatView>,
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
            target_refs: Vec::new(),
            resolved_objects: Vec::new(),
            source_coverage: Vec::new(),
            debt_rows: Vec::new(),
            operations: Vec::new(),
            runtime_health: None,
            package_completeness: Vec::new(),
            saved_artifact: None,
            diagnostic_excerpts: Vec::new(),
            omitted_sections: Vec::new(),
            caveats: Vec::new(),
            disclosure_caveats: Vec::new(),
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
            !self.target_refs.is_empty(),
            !self.resolved_objects.is_empty(),
            !self.source_coverage.is_empty(),
            !self.debt_rows.is_empty(),
            !self.operations.is_empty(),
            self.runtime_health.is_some(),
            !self.package_completeness.is_empty(),
            !self.diagnostic_excerpts.is_empty(),
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
            + usize::from(self.runtime_health.is_some())
            + self.package_completeness.len()
            + self.diagnostic_excerpts.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::sinex_test;

    #[sinex_test]
    async fn evidence_bundle_view_schema_exposes_stable_context_fields()
    -> xtask::sandbox::TestResult<()> {
        let schema = serde_json::to_value(schemars::schema_for!(EvidenceBundleView))
            .expect("EvidenceBundleView schema serializes");
        let schema_text = serde_json::to_string(&schema).expect("schema renders as JSON text");

        for field in [
            "target_refs",
            "diagnostic_excerpts",
            "omitted_sections",
            "caveats",
            "disclosure_caveats",
            "actions",
            "package_completeness",
        ] {
            assert!(
                schema_text.contains(field),
                "EvidenceBundleView schema should expose `{field}`"
            );
        }
        Ok(())
    }
}

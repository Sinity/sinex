use super::{ActionAvailability, CaveatView};
use crate::rpc::sources::{SourceReadiness, SourceShapeDriftObservation};
use crate::sources::continuity::{SourceContinuityReport, SourcesExplainGapResponse};
use crate::temporal::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const SOURCE_CONTINUITY_DETAIL_SCHEMA_VERSION: &str = "sinex.source-continuity-detail/v1";
pub const SOURCE_CONTINUITY_GAP_SCHEMA_VERSION: &str = "sinex.source-continuity-gap/v1";
pub const SOURCE_CONTINUITY_LIST_SCHEMA_VERSION: &str = "sinex.source-continuity-list/v1";
pub const SOURCE_COVERAGE_LIST_SCHEMA_VERSION: &str = "sinex.source-coverage-list/v1";
pub const SOURCE_DRIFT_LIST_SCHEMA_VERSION: &str = "sinex.source-drift-list/v1";
pub const SOURCE_READINESS_DETAIL_SCHEMA_VERSION: &str = "sinex.source-readiness-detail/v1";
pub const SOURCE_READINESS_LIST_SCHEMA_VERSION: &str = "sinex.source-readiness-list/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceCoverageReadiness {
    Ready,
    Proposed,
    MissingMaterial,
    MissingEvents,
    MissingBinding,
}

impl SourceCoverageReadiness {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Proposed => "proposed",
            Self::MissingMaterial => "missing_material",
            Self::MissingEvents => "missing_events",
            Self::MissingBinding => "missing_binding",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceCoverageContinuity {
    Active,
    MaterialOnly,
    EventOnly,
    Gapped,
    Unknown,
}

impl SourceCoverageContinuity {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::MaterialOnly => "material_only",
            Self::EventOnly => "event_only",
            Self::Gapped => "gapped",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SourcePrivacyPosture {
    pub tier: String,
    pub context: String,
    #[serde(default)]
    pub proposed: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SourceResourceBudgetView {
    pub resource_profile: String,
    pub work_class: String,
    pub steady_memory_mib: u32,
    pub burst_memory_mib: u32,
    pub cpu_weight: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_input_bytes_per_sec: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_input_events_per_sec: Option<u32>,
    pub max_pending_material_bytes: u64,
    pub max_pending_candidates: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_unacked_transport_messages: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flush_interval_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_interval_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pressure_actions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SourceModeStatusView {
    pub mode_id: String,
    pub binding_id: String,
    pub implementation: String,
    pub adapter: String,
    pub output_event_type: String,
    pub proposed: bool,
    pub runner_pack: String,
    pub runtime_shape: String,
    pub checkpoint_family: String,
    pub material_lifecycle: String,
    pub transport: String,
    pub delivery: String,
    pub ordering: String,
    pub replayable: bool,
    pub dlq: bool,
    pub backpressure: bool,
    pub privacy_context: String,
    pub resource_budget: SourceResourceBudgetView,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_observed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_live: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_output_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recent_output_count: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_operation_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_auth_state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_network_state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_sync_state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_rate_limit_state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_failure_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_required_action: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_retry_after_secs: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_reconnect_state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_operation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_coverage_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_debt_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mailbox_projection_message_count: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mailbox_projection_thread_count: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mailbox_projection_body_bytes: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mailbox_projection_attachment_count: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mailbox_projection_attachment_observed_count: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mailbox_projection_last_observed_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ActionAvailability>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct CoverageGapView {
    pub kind: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SourceCoverageView {
    pub source_id: String,
    pub namespace: String,
    pub event_types: Vec<String>,
    pub readiness: SourceCoverageReadiness,
    pub continuity: SourceCoverageContinuity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_material_at: Option<Timestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_event_at: Option<Timestamp>,
    pub material_count: i64,
    pub event_count: i64,
    pub binding_count: usize,
    pub accepted_binding_count: usize,
    pub proposed_binding_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub gaps: Vec<CoverageGapView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<CaveatView>,
    pub privacy: SourcePrivacyPosture,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_budget: Option<SourceResourceBudgetView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub modes: Vec<SourceModeStatusView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ActionAvailability>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SourceCoverageListView {
    pub schema_version: String,
    pub count: usize,
    pub summary: SourceCoverageSummaryView,
    pub sources: Vec<SourceCoverageView>,
}

impl SourceCoverageListView {
    #[must_use]
    pub fn new(sources: Vec<SourceCoverageView>) -> Self {
        let count = sources.len();
        let summary = SourceCoverageSummaryView::from_sources(&sources);
        Self {
            schema_version: SOURCE_COVERAGE_LIST_SCHEMA_VERSION.to_string(),
            count,
            summary,
            sources,
        }
    }

    pub fn refresh_summary(&mut self) {
        self.count = self.sources.len();
        self.summary = SourceCoverageSummaryView::from_sources(&self.sources);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SourceCoverageSummaryView {
    pub total_sources: usize,
    pub readiness: BTreeMap<String, usize>,
    pub continuity: BTreeMap<String, usize>,
    #[serde(default)]
    pub coverage_error_sources: usize,
    #[serde(default)]
    pub coverage_error_basis_points: u32,
    #[serde(default)]
    pub coverage_error_kinds: BTreeMap<String, usize>,
    pub accepted_bindings: usize,
    pub proposed_bindings: usize,
    pub eventful_sources: usize,
    pub materialized_sources: usize,
    pub total_events: i64,
    pub total_materials: i64,
}

impl SourceCoverageSummaryView {
    #[must_use]
    pub fn from_sources(sources: &[SourceCoverageView]) -> Self {
        let mut readiness = BTreeMap::new();
        let mut continuity = BTreeMap::new();
        let mut coverage_error_sources = 0_usize;
        let mut coverage_error_kinds = BTreeMap::new();
        let mut accepted_bindings = 0_usize;
        let mut proposed_bindings = 0_usize;
        let mut eventful_sources = 0_usize;
        let mut materialized_sources = 0_usize;
        let mut total_events = 0_i64;
        let mut total_materials = 0_i64;

        for source in sources {
            *readiness
                .entry(source.readiness.as_str().to_string())
                .or_insert(0) += 1;
            *continuity
                .entry(source.continuity.as_str().to_string())
                .or_insert(0) += 1;
            let mut has_coverage_error = false;
            if source.readiness != SourceCoverageReadiness::Ready {
                has_coverage_error = true;
                *coverage_error_kinds
                    .entry(format!("readiness.{}", source.readiness.as_str()))
                    .or_insert(0) += 1;
            }
            if source.continuity != SourceCoverageContinuity::Active {
                has_coverage_error = true;
                *coverage_error_kinds
                    .entry(format!("continuity.{}", source.continuity.as_str()))
                    .or_insert(0) += 1;
            }
            for gap in &source.gaps {
                has_coverage_error = true;
                *coverage_error_kinds
                    .entry(format!("gap.{}", gap.kind))
                    .or_insert(0) += 1;
            }
            if has_coverage_error {
                coverage_error_sources += 1;
            }
            accepted_bindings += source.accepted_binding_count;
            proposed_bindings += source.proposed_binding_count;
            if source.event_count > 0 {
                eventful_sources += 1;
            }
            if source.material_count > 0 {
                materialized_sources += 1;
            }
            total_events += source.event_count;
            total_materials += source.material_count;
        }

        let coverage_error_basis_points = if sources.is_empty() {
            0
        } else {
            ((coverage_error_sources as u128) * 10_000 / (sources.len() as u128)) as u32
        };

        Self {
            total_sources: sources.len(),
            readiness,
            continuity,
            coverage_error_sources,
            coverage_error_basis_points,
            coverage_error_kinds,
            accepted_bindings,
            proposed_bindings,
            eventful_sources,
            materialized_sources,
            total_events,
            total_materials,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceReadinessListView {
    pub schema_version: String,
    pub count: usize,
    pub sources: Vec<SourceReadiness>,
}

impl SourceReadinessListView {
    #[must_use]
    pub fn new(sources: Vec<SourceReadiness>) -> Self {
        let count = sources.len();
        Self {
            schema_version: SOURCE_READINESS_LIST_SCHEMA_VERSION.to_string(),
            count,
            sources,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceReadinessDetailView {
    pub schema_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceReadiness>,
}

impl SourceReadinessDetailView {
    #[must_use]
    pub fn new(source: Option<SourceReadiness>) -> Self {
        Self {
            schema_version: SOURCE_READINESS_DETAIL_SCHEMA_VERSION.to_string(),
            source,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceDriftListView {
    pub schema_version: String,
    pub count: usize,
    pub drifts: Vec<SourceShapeDriftObservation>,
}

impl SourceDriftListView {
    #[must_use]
    pub fn new(drifts: Vec<SourceShapeDriftObservation>) -> Self {
        let count = drifts.len();
        Self {
            schema_version: SOURCE_DRIFT_LIST_SCHEMA_VERSION.to_string(),
            count,
            drifts,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceContinuityListView {
    pub schema_version: String,
    pub count: usize,
    pub reports: Vec<SourceContinuityReport>,
}

impl SourceContinuityListView {
    #[must_use]
    pub fn new(reports: Vec<SourceContinuityReport>) -> Self {
        let count = reports.len();
        Self {
            schema_version: SOURCE_CONTINUITY_LIST_SCHEMA_VERSION.to_string(),
            count,
            reports,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceContinuityDetailView {
    pub schema_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report: Option<SourceContinuityReport>,
}

impl SourceContinuityDetailView {
    #[must_use]
    pub fn new(report: Option<SourceContinuityReport>) -> Self {
        Self {
            schema_version: SOURCE_CONTINUITY_DETAIL_SCHEMA_VERSION.to_string(),
            report,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceContinuityGapView {
    pub schema_version: String,
    pub explanation: SourcesExplainGapResponse,
}

impl SourceContinuityGapView {
    #[must_use]
    pub fn new(explanation: SourcesExplainGapResponse) -> Self {
        Self {
            schema_version: SOURCE_CONTINUITY_GAP_SCHEMA_VERSION.to_string(),
            explanation,
        }
    }
}

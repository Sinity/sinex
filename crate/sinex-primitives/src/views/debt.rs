use super::{ActionAvailability, CaveatView, FreshnessView, SinexObjectRef};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const DEBT_LIST_SCHEMA_VERSION: &str = "sinex.debt-list/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DebtKind {
    Capture,
    Admission,
    Projection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DebtStage {
    Capturing,
    MaterialReady,
    CandidateRejected,
    CandidateQuarantined,
    CandidateDeferred,
    ProjectionStale,
    ArtifactInvalidated,
    OperationPending,
    OperationFailed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DebtOwnerView {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub package_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_ref: Option<SinexObjectRef>,
}

impl DebtOwnerView {
    #[must_use]
    pub fn admission_policy(policy_ref: impl Into<String>) -> Self {
        Self {
            package_ref: None,
            mode_ref: None,
            policy_ref: Some(policy_ref.into()),
            operation_ref: None,
        }
    }

    #[must_use]
    pub fn operation(operation_ref: SinexObjectRef) -> Self {
        Self {
            package_ref: None,
            mode_ref: None,
            policy_ref: None,
            operation_ref: Some(operation_ref),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DebtRowView {
    pub id: String,
    pub kind: DebtKind,
    pub stage: DebtStage,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub refs: Vec<SinexObjectRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<DebtOwnerView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub freshness: Option<FreshnessView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<CaveatView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ActionAvailability>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DebtListView {
    pub schema_version: String,
    pub count: usize,
    pub rows: Vec<DebtRowView>,
}

impl DebtListView {
    #[must_use]
    pub fn new(rows: Vec<DebtRowView>) -> Self {
        let count = rows.len();
        Self {
            schema_version: DEBT_LIST_SCHEMA_VERSION.to_string(),
            count,
            rows,
        }
    }
}

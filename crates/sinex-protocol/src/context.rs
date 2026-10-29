//! Owner-provided public temporal and attention policy snapshots.
use serde::{Deserialize, Serialize};
use sinex_model::Timestamp;

#[derive(Clone, Debug, Default, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PublicContextRequest {
    #[serde(default)]
    pub coverage: Option<CoverageRequest>,
}

#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CoverageRequest {
    pub cache: String,
    pub completed_position: i64,
}

/// The signed stamp is opaque. Its claims are validated by the owner on new writes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CoverageProof {
    pub stamp: String,
    pub position: i64,
    pub scope: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum CoverageStatus {
    Complete { proof: CoverageProof },
    Incomplete { reason: String },
    NotRequested,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PublicInterruptBudget {
    pub per_hour: u64,
    pub used: u64,
    pub remaining: u64,
    pub window_ms: i64,
}

/// One observed committed policy revision, with no private configuration or keys.
/// Clients resolve the supplied named zone explicitly at their operation boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum PublicContext {
    Available {
        context_token: String,
        settings_identity: String,
        owner_revision: u64,
        day: query_language::time::DaySettingWire,
        quiet_hours: Option<signals::QuietHours>,
        interrupt_budget: PublicInterruptBudget,
        observed_at: Timestamp,
        coverage: CoverageStatus,
    },
    Pending {
        reason: String,
    },
    Unavailable {
        reason: String,
    },
    Refused {
        reason: String,
    },
}

#[cfg(test)]
mod tests;

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
    #[serde(default)]
    pub basis: Option<CoverageProof>,
    pub cache: String,
    pub completed_position: i64,
    #[serde(default)]
    pub completion: Option<DeliveredCompletion>,
}

/// A delivered native completion marker. This proves an eligible delivery
/// window; it does not assert that the consumer committed its local changes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliveredCompletion {
    pub stream_instance: String,
    pub stream_sequence: u64,
}

/// The signed stamp is opaque. Its claims are validated by the owner on new writes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CoverageProof {
    pub stamp: String,
    pub position: i64,
    pub policy_epoch: i64,
    pub scope: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum CoverageStatus {
    Complete {
        proof: CoverageProof,
    },
    Incomplete {
        reason: String,
        basis: crate::ReplayBasisStatus,
    },
    NotRequested,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PublicInterruptBudget {
    pub per_hour: u64,
    pub used: u64,
    pub remaining: u64,
    pub window_ms: i64,
}

/// A purpose-specific permission cached before the one compiled local outage alert.
/// Its lease does not change the lifetime of ordinary temporal settings.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OfflineEmergency {
    pub producer: String,
    pub host: String,
    pub settings_identity: String,
    pub rule: crate::signals::ShippedRule,
    pub route: OfflineEmergencyRoute,
    pub issued_at: Timestamp,
    pub expires_at: Timestamp,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OfflineEmergencyRoute {
    Desktop,
}

impl OfflineEmergency {
    /// The host calls this with its trusted actual Invocation time, before the
    /// effect. A replay or evaluation clock cannot authorize an offline effect.
    pub fn permits(
        &self,
        producer: &str,
        host: &str,
        settings: &str,
        invoked_at: Timestamp,
    ) -> bool {
        self.producer == producer
            && self.host == host
            && self.settings_identity == settings
            && self.rule == crate::signals::emergency_rule()
            && self.route == OfflineEmergencyRoute::Desktop
            && self.issued_at <= invoked_at
            && invoked_at < self.expires_at
            && self.issued_at < self.expires_at
    }
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
        #[serde(default)]
        offline_emergency: Option<OfflineEmergency>,
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

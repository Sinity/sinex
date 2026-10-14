//! Typed evidence about whether a source acquisition scope is complete.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How strongly the expected opportunity set is known for an acquisition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OpportunityCompleteness {
    /// The selected scope is known to be complete, including a legitimate zero.
    Complete,
    /// The scope is known to be incomplete, but the missing amount is unknown.
    Incomplete,
    /// Available evidence cannot determine whether the scope is complete.
    Unknown,
}

/// A source-scoped acquisition observation that must not be confused with
/// downstream persistence or parser success.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AcquisitionOpportunity {
    /// Stable logical source identifier supplied by the source runtime.
    pub source_id: String,
    /// State of the acquisition scope, independent of downstream processing.
    pub completeness: OpportunityCompleteness,
    /// Human-readable reason, suitable for diagnostics and status surfaces.
    pub reason: String,
    /// Concrete filesystem paths implicated by watcher overflow, when known.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub affected_paths: Vec<String>,
    /// Event observation time in Unix nanoseconds, when supplied by the adapter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at_unix_nanos: Option<u64>,
    /// Lower cursor bound when known; opaque because source cursor formats vary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor_before: Option<String>,
    /// First cursor observed after recovery, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor_after: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_opportunity_round_trips_without_a_missing_count() {
        let opportunity = AcquisitionOpportunity {
            source_id: "system.journald".into(),
            completeness: OpportunityCompleteness::Unknown,
            reason: "subscriber lagged on shared fan-out".into(),
            affected_paths: Vec::new(),
            observed_at_unix_nanos: None,
            cursor_before: Some("cursor-a".into()),
            cursor_after: Some("cursor-z".into()),
        };

        let encoded = serde_json::to_value(&opportunity).expect("serialize");
        let decoded: AcquisitionOpportunity =
            serde_json::from_value(encoded.clone()).expect("deserialize");
        assert_eq!(decoded, opportunity);
        assert!(encoded.get("missing_count").is_none());
    }
}

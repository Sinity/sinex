//! Sinex-provided public temporal and attention policy snapshots.
use serde::{Deserialize, Serialize};
use sinex_model::Timestamp;

/// The producer is the authenticated subject's; the body names nothing.
#[derive(Clone, Debug, Default, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PublicContextRequest {}

/// The confirmed-stream completion marker a consumer applied last: the stream
/// instance, the marker's sequence and its fingerprint. Sinex reads that
/// message back from the broker to resume delivery after it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliveredCompletion {
    pub stream_instance: String,
    pub stream_sequence: u64,
    pub fingerprint: String,
}
impl DeliveredCompletion {
    /// Identify one stored stream message, including its original broker
    /// timestamp, so a new append of an old body is not mistaken for it.
    pub fn fingerprint(
        sequence: u64,
        published_nanos: i128,
        subject: &str,
        payload: &[u8],
    ) -> String {
        let mut hash = blake3::Hasher::new();
        hash.update(b"sinex.native_record.v1\0");
        hash.update(&sequence.to_le_bytes());
        hash.update(&published_nanos.to_le_bytes());
        hash.update(&(subject.len() as u64).to_le_bytes());
        hash.update(subject.as_bytes());
        hash.update(payload);
        hash.finalize().to_hex().to_string()
    }
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
        settings_identity: String,
        settings_revision: u64,
        day: query_language::time::DaySettingWire,
        quiet_hours: Option<signals::QuietHours>,
        interrupt_budget: PublicInterruptBudget,
        observed_at: Timestamp,
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

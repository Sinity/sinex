//! SJSON1 identifies a projected payload, independently of its source record.
//! It is compact serde_json value serialization with recursively sorted UTF-8
//! object keys. Number formatting follows serde_json's finite JSON numbers.
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectionIdentity {
    pub policy_epoch: i64,
    /// Lowercase hex BLAKE3 of the SJSON1 payload bytes.
    pub payload_hash: String,
}
impl ProjectionIdentity {
    pub fn for_payload(policy_epoch: i64, payload: &Value) -> Self {
        Self {
            policy_epoch,
            payload_hash: Self::hash(payload),
        }
    }
    pub fn hash(payload: &Value) -> String {
        sinex_model::json_identity::hash(payload)
    }
    pub fn verifies(&self, payload: &Value) -> bool {
        self.payload_hash == Self::hash(payload)
    }
}
#[cfg(test)]
mod tests;

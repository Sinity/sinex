//! A projected payload is identified by the hash of its canonical JSON
//! (common's `canonical-json`), independently of its source record.
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectionIdentity {
    /// Lowercase hex BLAKE3 of the payload's canonical JSON.
    pub payload_hash: String,
}
impl ProjectionIdentity {
    pub fn for_payload(payload: &Value) -> Self {
        Self {
            payload_hash: Self::hash(payload),
        }
    }
    pub fn hash(payload: &Value) -> String {
        let canonical = canonical_json::canonical_json(payload).expect("JSON values serialize");
        blake3::hash(&canonical).to_hex().to_string()
    }
    pub fn verifies(&self, payload: &Value) -> bool {
        self.payload_hash == Self::hash(payload)
    }
}
#[cfg(test)]
mod tests;

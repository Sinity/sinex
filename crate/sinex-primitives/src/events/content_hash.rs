//! Canonical content hashing for event payloads (sinex-n9a).
//!
//! Admission's `SupersedeOnChange` revision policy must decide whether a fresh
//! interpretation carrying an occurrence's `equivalence_key` is *identical* to
//! the live row it would replace or a genuine *revision*. A naive
//! `serde_json::to_vec` byte comparison is wrong here: the live payload is read
//! back from Postgres `jsonb`, which normalizes object key order (and numeric
//! representation), so two semantically identical objects that differ only in
//! serialized key order would falsely register as "changed" and trigger an
//! endless archive/re-admit churn.
//!
//! [`payload_content_hash`] canonicalizes first — recursively sorting object
//! keys — then hashes the canonical bytes with BLAKE3, matching the hashing
//! primitive already used across this crate (`builder::with_anchor_payload_*`,
//! `schema_registry::calculate_schema_content_hash`). Equal *content* always
//! yields an equal hash regardless of key order on either side.

use serde_json::{Map, Value};

/// Recursively rewrite `value` into a canonical form: every object's keys are
/// sorted (`serde_json::Map` preserves insertion order under the crate's
/// `preserve_order` feature, so sorting is required for a stable serialization).
/// Arrays keep their order (array order is semantically significant); scalars
/// pass through unchanged.
fn canonicalize(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut sorted: Vec<(&String, &Value)> = map.iter().collect();
            sorted.sort_by_key(|(key, _)| *key);
            let mut out = Map::with_capacity(map.len());
            for (key, val) in sorted {
                out.insert(key.clone(), canonicalize(val));
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonicalize).collect()),
        other => other.clone(),
    }
}

/// Compute the canonical 32-byte BLAKE3 content hash of an event payload.
///
/// Two payloads with the same logical content hash-equal even if their object
/// keys are serialized in a different order (the jsonb round-trip case). Used
/// by admission to distinguish an idempotent re-emit (identical → suppress)
/// from a real revision (changed → supersede).
#[must_use]
pub fn payload_content_hash(payload: &Value) -> [u8; 32] {
    let canonical = canonicalize(payload);
    // Serializing a canonicalized Value is infallible in practice (no custom
    // Serialize impls, no non-string map keys), but avoid unwrap: fall back to
    // hashing the Debug form so a pathological value still yields a stable,
    // self-consistent hash rather than panicking in the admission hot path.
    let bytes = serde_json::to_vec(&canonical)
        .unwrap_or_else(|_| format!("{canonical:?}").into_bytes());
    *blake3::hash(&bytes).as_bytes()
}

#[cfg(test)]
#[path = "content_hash_test.rs"]
mod tests;

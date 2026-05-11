//! Schema-drift detection via structural fingerprinting.
//!
//! This module provides schema-drift detection for source records by computing
//! structural fingerprints of record shapes and detecting when the structure
//! changes (added/removed/retyped fields). Drift is rate-limited per source
//! unit to avoid spam in high-volume environments.
//!
//! # Fingerprint Model
//!
//! A `SourceRecordFingerprint` captures:
//! - **format**: record encoding (`"json"`, `"csv"`, etc.)
//! - **keys**: field names / JSON pointers / column names (sorted, deduped)
//! - **type_map**: inferred type for each key (`"string"`, `"integer"`, etc.)
//! - **blake3_hash**: BLAKE3 of canonical representation (stability across sessions)
//!
//! Two fingerprints are equal iff their BLAKE3 hashes match.
//!
//! # Drift Detection
//!
//! `DriftAccumulator` tracks the last-seen fingerprint per source unit and emits
//! `DriftEvent` when:
//! 1. First observation (always returns `None`)
//! 2. Identical fingerprint (returns `None`)
//! 3. Different fingerprint (returns `Some(DriftEvent)`) if not rate-limited
//!
//! Rate limiting applies two gates:
//! - **emit_every_n_records**: minimum records between drift events for the same hash
//! - **cooldown_secs**: minimum seconds between drift events
//!
//! Either gate can suppress a drift event; both must clear for emission.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

// Note: blake3 is used directly (not via the Digest trait) — `blake3::Hasher`
// has its own update/finalize methods that don't require importing a trait.
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use sinex_primitives::parser::SourceUnitId;
use sinex_primitives::temporal::Timestamp;

// =============================================================================
// SourceRecordFingerprint
// =============================================================================

/// A structural fingerprint of a source record's shape.
///
/// Captures the keys and types present in a record, stable across
/// different orderings and representations of the same logical data.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceRecordFingerprint {
    /// Record format (e.g., "json", "csv", "sqlite_row").
    pub format: String,

    /// Sorted, deduplicated list of field/column names or JSON Pointers.
    pub keys: Vec<String>,

    /// Inferred type for each key (e.g., "string", "integer", "object").
    pub type_map: BTreeMap<String, String>,

    /// BLAKE3 hash of canonical (format, keys, type_map) representation.
    /// Serves as the structural identity — two fingerprints with the same
    /// hash are structurally identical.
    blake3_hash: String,
}

impl SourceRecordFingerprint {
    /// Creates a fingerprint from a JSON value.
    ///
    /// Recursively infers types from the JSON structure. The result is stable
    /// across different orderings of the same keys.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let fp = SourceRecordFingerprint::from_json(&json!({"name": "foo", "id": 42}));
    /// assert_eq!(fp.keys, vec!["id", "name"]);  // sorted
    /// assert_eq!(fp.type_map["name"], "string");
    /// assert_eq!(fp.type_map["id"], "integer");
    /// ```
    pub fn from_json(value: &JsonValue) -> Self {
        let (mut keys, type_map) = Self::extract_types(value);

        // Normalize: ensure keys are sorted and deduplicated.
        keys.sort();
        keys.dedup();

        let fp = Self {
            format: "json".to_string(),
            keys: keys.clone(),
            type_map: type_map.clone(),
            blake3_hash: String::new(),
        };

        let mut fingerprint = fp;
        fingerprint.blake3_hash = fingerprint.compute_hash();
        fingerprint
    }

    /// Creates a fingerprint from a SourceRecord.
    ///
    /// Dispatches based on record format (currently only JSON is fully supported).
    pub fn from_record(record: &crate::parser::SourceRecord) -> Self {
        // Try to parse as JSON first.
        if let Ok(json) = serde_json::from_slice::<JsonValue>(&record.bytes) {
            return Self::from_json(&json);
        }

        // Fallback: treat as opaque format.
        Self {
            format: "binary".to_string(),
            keys: vec![],
            type_map: BTreeMap::new(),
            blake3_hash: Self::compute_opaque_hash(&record.bytes),
        }
    }

    /// Extracts field names and their inferred types from a JSON value.
    fn extract_types(value: &JsonValue) -> (Vec<String>, BTreeMap<String, String>) {
        let mut keys = Vec::new();
        let mut type_map = BTreeMap::new();

        match value {
            JsonValue::Object(map) => {
                for (key, val) in map.iter() {
                    keys.push(key.clone());
                    type_map.insert(key.clone(), Self::infer_type(val));
                }
            }
            JsonValue::Array(_) => {
                // For arrays, we don't index individual elements;
                // we just note it's an array.
            }
            _ => {
                // Scalar values: no keys to extract.
            }
        }

        (keys, type_map)
    }

    /// Infers the JSON type of a value.
    fn infer_type(value: &JsonValue) -> String {
        match value {
            JsonValue::Null => "null".to_string(),
            JsonValue::Bool(_) => "boolean".to_string(),
            JsonValue::Number(_) => {
                // Distinguish between integer and float if possible.
                if value.is_i64() || value.is_u64() {
                    "integer".to_string()
                } else {
                    "number".to_string()
                }
            }
            JsonValue::String(_) => "string".to_string(),
            JsonValue::Array(_) => "array".to_string(),
            JsonValue::Object(_) => "object".to_string(),
        }
    }

    /// Computes the canonical BLAKE3 hash.
    fn compute_hash(&self) -> String {
        // Canonical form: format | keys (sorted) | type_map (sorted)
        let mut hasher = blake3::Hasher::new();

        hasher.update(self.format.as_bytes());
        hasher.update(b"|");

        for key in &self.keys {
            hasher.update(key.as_bytes());
            hasher.update(b",");
        }
        hasher.update(b"|");

        for (key, typ) in &self.type_map {
            hasher.update(key.as_bytes());
            hasher.update(b":");
            hasher.update(typ.as_bytes());
            hasher.update(b";");
        }

        hasher.finalize().to_hex().to_string()
    }

    /// Computes a hash for opaque (non-JSON) binary data.
    fn compute_opaque_hash(bytes: &[u8]) -> String {
        blake3::hash(bytes).to_hex().to_string()
    }

    /// Returns the BLAKE3 hash of this fingerprint.
    pub fn hash(&self) -> &str {
        &self.blake3_hash
    }
}

// =============================================================================
// DriftAccumulator
// =============================================================================

/// Rate-limited drift detector for a source unit.
///
/// Tracks the last-seen fingerprint and emits `DriftEvent` when the
/// structure changes, subject to configurable rate limits.
#[derive(Debug, Clone)]
pub struct DriftAccumulator {
    source_unit_id: SourceUnitId,

    /// Last fingerprint hash observed.
    last_hash: Option<String>,

    /// Records observed since the last drift emission.
    record_count_since_last_emit: usize,

    /// Minimum records between drift emissions (rate limit).
    emit_every_n_records: usize,

    /// Minimum seconds between drift emissions (cooldown).
    cooldown_secs: u64,

    /// Unix timestamp of the last drift emission (or None).
    last_emit_ts: Option<u64>,

    /// Type and key state for comparison.
    last_fingerprint: Option<(Vec<String>, BTreeMap<String, String>)>,
}

impl DriftAccumulator {
    /// Creates a new drift accumulator for a source unit.
    pub fn new(source_unit_id: SourceUnitId) -> Self {
        Self {
            source_unit_id,
            last_hash: None,
            record_count_since_last_emit: 0,
            emit_every_n_records: 10_000,
            cooldown_secs: 3600,
            last_emit_ts: None,
            last_fingerprint: None,
        }
    }

    /// Sets the minimum records between drift emissions.
    #[must_use]
    pub fn with_emit_every_n_records(mut self, n: usize) -> Self {
        self.emit_every_n_records = n;
        self
    }

    /// Sets the minimum seconds between drift emissions.
    #[must_use]
    pub fn with_cooldown_secs(mut self, secs: u64) -> Self {
        self.cooldown_secs = secs;
        self
    }

    /// Observes a fingerprint and returns a drift event if drift is detected
    /// and rate limits are satisfied.
    pub fn observe(&mut self, fingerprint: &SourceRecordFingerprint) -> Option<DriftEvent> {
        self.record_count_since_last_emit += 1;

        let hash = fingerprint.hash();

        // First observation: remember it but don't emit.
        if self.last_hash.is_none() {
            self.last_hash = Some(hash.to_string());
            self.last_fingerprint = Some((fingerprint.keys.clone(), fingerprint.type_map.clone()));
            return None;
        }

        // Same hash: no drift.
        if let Some(ref last) = self.last_hash {
            if last == hash {
                return None;
            }
        }

        // Drift detected: check rate limits.
        if !self.is_emit_allowed() {
            return None;
        }

        // Rate limits clear: emit drift event.
        let event = self.build_drift_event(fingerprint);
        self.last_hash = Some(hash.to_string());
        self.last_fingerprint = Some((fingerprint.keys.clone(), fingerprint.type_map.clone()));
        self.record_count_since_last_emit = 0;
        self.last_emit_ts = Some(current_unix_timestamp());

        Some(event)
    }

    /// Returns the last-observed fingerprint hash.
    pub fn last_seen_hash(&self) -> Option<&str> {
        self.last_hash.as_deref()
    }

    /// Checks if emission is allowed by rate limits.
    fn is_emit_allowed(&self) -> bool {
        // Check record count limit.
        if self.record_count_since_last_emit < self.emit_every_n_records {
            return false;
        }

        // Check cooldown limit.
        if let Some(last_ts) = self.last_emit_ts {
            let now = current_unix_timestamp();
            if now.saturating_sub(last_ts) < self.cooldown_secs {
                return false;
            }
        }

        true
    }

    /// Builds a drift event from the new fingerprint and the last one.
    fn build_drift_event(&self, current: &SourceRecordFingerprint) -> DriftEvent {
        let (previous_keys, previous_types) = self
            .last_fingerprint
            .clone()
            .unwrap_or_else(|| (vec![], BTreeMap::new()));

        let previous_hash = self.last_hash.clone().unwrap_or_default();

        // Compute key deltas.
        let current_key_set: std::collections::HashSet<_> =
            current.keys.iter().cloned().collect();
        let previous_key_set: std::collections::HashSet<_> =
            previous_keys.iter().cloned().collect();

        let added_keys: Vec<_> = current_key_set
            .difference(&previous_key_set)
            .cloned()
            .collect();

        let removed_keys: Vec<_> = previous_key_set
            .difference(&current_key_set)
            .cloned()
            .collect();

        // Compute type changes for keys that exist in both.
        let mut type_changes = vec![];
        for key in previous_keys.iter() {
            if current_key_set.contains(key) {
                let prev_type = previous_types
                    .get(key)
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());
                let curr_type = current
                    .type_map
                    .get(key)
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());
                if prev_type != curr_type {
                    type_changes.push((key.clone(), prev_type, curr_type));
                }
            }
        }

        DriftEvent {
            source_unit_id: self.source_unit_id.clone(),
            previous_hash,
            current_hash: current.hash().to_string(),
            format: current.format.clone(),
            previous_keys,
            current_keys: current.keys.clone(),
            added_keys,
            removed_keys,
            type_changes,
            observed_at: Timestamp::now(),
        }
    }
}

// =============================================================================
// DriftEvent
// =============================================================================

/// Emitted when a source record structure changes.
///
/// This event carries sufficient information for operators to understand
/// what changed and when.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftEvent {
    /// The source unit that drifted.
    pub source_unit_id: SourceUnitId,

    /// The previous structural hash.
    pub previous_hash: String,

    /// The current structural hash.
    pub current_hash: String,

    /// The record format (e.g., "json").
    pub format: String,

    /// Keys present in the previous observation.
    pub previous_keys: Vec<String>,

    /// Keys present in the current observation.
    pub current_keys: Vec<String>,

    /// Keys that appeared in the current but not the previous.
    pub added_keys: Vec<String>,

    /// Keys that disappeared from the previous but are absent in current.
    pub removed_keys: Vec<String>,

    /// Type changes for keys that exist in both: (key, old_type, new_type).
    pub type_changes: Vec<(String, String, String)>,

    /// When this drift was observed.
    pub observed_at: Timestamp,
}

impl DriftEvent {
    /// Serializes this event as a JSON payload suitable for a parser-emitted event.
    pub fn to_payload(&self) -> serde_json::Value {
        serde_json::json!({
            "source_unit_id": self.source_unit_id.as_str(),
            "previous_hash": self.previous_hash,
            "current_hash": self.current_hash,
            "format": self.format,
            "previous_keys": self.previous_keys,
            "current_keys": self.current_keys,
            "added_keys": self.added_keys,
            "removed_keys": self.removed_keys,
            "type_changes": self.type_changes.iter()
                .map(|(k, old, new)| serde_json::json!({
                    "key": k,
                    "previous_type": old,
                    "current_type": new,
                }))
                .collect::<Vec<_>>(),
            "observed_at": self.observed_at.format_rfc3339(),
        })
    }
}

// =============================================================================
// Helpers
// =============================================================================

/// Returns the current Unix timestamp in seconds.
fn current_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::sinex_test;
    use serde_json::json;

    #[sinex_test]
    async fn test_from_json_simple() -> xtask::sandbox::TestResult<()> {
        let value = json!({"name": "Alice", "age": 30});
        let fp = SourceRecordFingerprint::from_json(&value);

        assert_eq!(fp.format, "json");
        assert_eq!(fp.keys, vec!["age", "name"]); // sorted
        assert_eq!(fp.type_map["name"], "string");
        assert_eq!(fp.type_map["age"], "integer");
        Ok(())
    }

    #[sinex_test]
    async fn test_from_json_with_nulls() -> xtask::sandbox::TestResult<()> {
        let value = json!({"name": "Bob", "email": null});
        let fp = SourceRecordFingerprint::from_json(&value);

        assert_eq!(fp.keys, vec!["email", "name"]);
        assert_eq!(fp.type_map["email"], "null");
        Ok(())
    }

    #[sinex_test]
    async fn test_from_json_with_mixed_types() -> xtask::sandbox::TestResult<()> {
        let value = json!({
            "text": "hello",
            "count": 42,
            "active": true,
            "nested": { "key": "value" },
            "items": [1, 2, 3],
            "nullable": null
        });
        let fp = SourceRecordFingerprint::from_json(&value);

        assert_eq!(fp.type_map["text"], "string");
        assert_eq!(fp.type_map["count"], "integer");
        assert_eq!(fp.type_map["active"], "boolean");
        assert_eq!(fp.type_map["nested"], "object");
        assert_eq!(fp.type_map["items"], "array");
        assert_eq!(fp.type_map["nullable"], "null");
        Ok(())
    }

    #[sinex_test]
    async fn test_fingerprint_stability() -> xtask::sandbox::TestResult<()> {
        let value1 = json!({"z": 1, "a": "x", "m": 3.14});
        let value2 = json!({"a": "x", "m": 3.14, "z": 1});

        let fp1 = SourceRecordFingerprint::from_json(&value1);
        let fp2 = SourceRecordFingerprint::from_json(&value2);

        assert_eq!(fp1.keys, fp2.keys);
        assert_eq!(fp1.type_map, fp2.type_map);
        assert_eq!(fp1.hash(), fp2.hash());
        Ok(())
    }

    #[sinex_test]
    async fn test_fingerprint_different_when_keys_change() -> xtask::sandbox::TestResult<()> {
        let value1 = json!({"name": "Alice", "age": 30});
        let value2 = json!({"name": "Alice", "age": 30, "city": "NYC"});

        let fp1 = SourceRecordFingerprint::from_json(&value1);
        let fp2 = SourceRecordFingerprint::from_json(&value2);

        assert_ne!(fp1.hash(), fp2.hash());
        Ok(())
    }

    #[sinex_test]
    async fn test_fingerprint_different_when_types_change() -> xtask::sandbox::TestResult<()> {
        let value1 = json!({"count": 42});
        let value2 = json!({"count": "42"});

        let fp1 = SourceRecordFingerprint::from_json(&value1);
        let fp2 = SourceRecordFingerprint::from_json(&value2);

        assert_ne!(fp1.hash(), fp2.hash());
        Ok(())
    }

    #[sinex_test]
    async fn test_drift_accumulator_first_observation() -> xtask::sandbox::TestResult<()> {
        let source_unit = SourceUnitId::from_static("test.unit");
        let mut acc = DriftAccumulator::new(source_unit);

        let fp = SourceRecordFingerprint::from_json(&json!({"id": 1, "name": "test"}));
        let event = acc.observe(&fp);

        assert!(event.is_none());
        assert_eq!(acc.last_seen_hash(), Some(fp.hash()));
        Ok(())
    }

    #[sinex_test]
    async fn test_drift_accumulator_same_fingerprint() -> xtask::sandbox::TestResult<()> {
        let source_unit = SourceUnitId::from_static("test.unit");
        let mut acc = DriftAccumulator::new(source_unit);

        let fp = SourceRecordFingerprint::from_json(&json!({"id": 1, "name": "test"}));

        // First observation.
        acc.observe(&fp);

        // Second observation: identical fingerprint.
        let event = acc.observe(&fp);
        assert!(event.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_drift_accumulator_detects_drift() -> xtask::sandbox::TestResult<()> {
        let source_unit = SourceUnitId::from_static("test.unit");
        let mut acc = DriftAccumulator::new(source_unit)
            .with_emit_every_n_records(2) // Low threshold for testing.
            .with_cooldown_secs(0);

        let fp1 = SourceRecordFingerprint::from_json(&json!({"id": 1}));
        let fp2 = SourceRecordFingerprint::from_json(&json!({"id": 1, "name": "test"}));

        // Observe fp1.
        acc.observe(&fp1);
        assert_eq!(acc.record_count_since_last_emit, 1);

        // Observe fp1 again (identical).
        acc.observe(&fp1);
        assert_eq!(acc.record_count_since_last_emit, 2);

        // Observe fp2 (drift, and rate limit clears).
        let event = acc.observe(&fp2);
        assert!(event.is_some());

        let drift = event.unwrap();
        assert_eq!(drift.added_keys, vec!["name".to_string()]);
        assert!(drift.removed_keys.is_empty());
        assert!(drift.type_changes.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_drift_accumulator_respects_record_count_limit() -> xtask::sandbox::TestResult<()> {
        let source_unit = SourceUnitId::from_static("test.unit");
        let mut acc = DriftAccumulator::new(source_unit)
            .with_emit_every_n_records(100)
            .with_cooldown_secs(0);

        let fp1 = SourceRecordFingerprint::from_json(&json!({"id": 1}));
        let fp2 = SourceRecordFingerprint::from_json(&json!({"id": 1, "name": "test"}));

        acc.observe(&fp1);

        // Only 1 record observed; need 100 before emitting drift.
        let event = acc.observe(&fp2);
        assert!(event.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_drift_accumulator_respects_cooldown() -> xtask::sandbox::TestResult<()> {
        let source_unit = SourceUnitId::from_static("test.unit");
        let mut acc = DriftAccumulator::new(source_unit)
            .with_emit_every_n_records(1)
            .with_cooldown_secs(1000); // 1000 seconds between emissions.

        let fp1 = SourceRecordFingerprint::from_json(&json!({"id": 1}));
        let fp2 = SourceRecordFingerprint::from_json(&json!({"id": 1, "name": "test"}));
        let fp3 = SourceRecordFingerprint::from_json(&json!({"id": 1})); // back to fp1 shape

        acc.observe(&fp1);

        // First drift: emitted (no prior emit).
        let event1 = acc.observe(&fp2);
        assert!(event1.is_some());

        // Second drift (back to fp1 shape): not emitted due to cooldown.
        acc.record_count_since_last_emit = 0; // Reset counter.
        let event2 = acc.observe(&fp3);
        assert!(event2.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_drift_event_construction() -> xtask::sandbox::TestResult<()> {
        let source_unit = SourceUnitId::from_static("test.unit");
        let mut acc = DriftAccumulator::new(source_unit.clone())
            .with_emit_every_n_records(1)
            .with_cooldown_secs(0);

        let fp1 = SourceRecordFingerprint::from_json(&json!({"a": 1, "b": "x"}));
        let fp2 = SourceRecordFingerprint::from_json(&json!({"a": 1, "c": true}));

        acc.observe(&fp1);
        let event = acc.observe(&fp2).unwrap();

        assert_eq!(event.source_unit_id, source_unit);
        assert_eq!(event.added_keys, vec!["c"]);
        assert_eq!(event.removed_keys, vec!["b"]);
        // "a" should have no type change (integer -> integer).
        assert!(event.type_changes.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_drift_event_type_changes() -> xtask::sandbox::TestResult<()> {
        let source_unit = SourceUnitId::from_static("test.unit");
        let mut acc = DriftAccumulator::new(source_unit)
            .with_emit_every_n_records(1)
            .with_cooldown_secs(0);

        let fp1 = SourceRecordFingerprint::from_json(&json!({"count": 42}));
        let fp2 = SourceRecordFingerprint::from_json(&json!({"count": "42"}));

        acc.observe(&fp1);
        let event = acc.observe(&fp2).unwrap();

        assert!(event.added_keys.is_empty());
        assert!(event.removed_keys.is_empty());
        assert_eq!(event.type_changes.len(), 1);
        assert_eq!(event.type_changes[0], ("count".to_string(), "integer".to_string(), "string".to_string()));
        Ok(())
    }

    #[sinex_test]
    async fn test_drift_event_to_payload() -> xtask::sandbox::TestResult<()> {
        let source_unit = SourceUnitId::from_static("test.unit");
        let mut acc = DriftAccumulator::new(source_unit)
            .with_emit_every_n_records(1)
            .with_cooldown_secs(0);

        let fp1 = SourceRecordFingerprint::from_json(&json!({"x": 1}));
        let fp2 = SourceRecordFingerprint::from_json(&json!({"x": 1, "y": 2}));

        acc.observe(&fp1);
        let event = acc.observe(&fp2).unwrap();

        let payload = event.to_payload();
        assert!(payload.is_object());
        assert_eq!(payload["format"], "json");
        assert_eq!(payload["added_keys"], serde_json::json!(["y"]));
        Ok(())
    }

    #[sinex_test]
    async fn test_empty_record() -> xtask::sandbox::TestResult<()> {
        let value = json!({});
        let fp = SourceRecordFingerprint::from_json(&value);

        assert_eq!(fp.format, "json");
        assert!(fp.keys.is_empty());
        assert!(fp.type_map.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_array_top_level() -> xtask::sandbox::TestResult<()> {
        let value = json!([1, 2, 3]);
        let fp = SourceRecordFingerprint::from_json(&value);

        assert_eq!(fp.format, "json");
        assert!(fp.keys.is_empty());
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Coverage gaps filled (#1100 substrate hardening)
    // -----------------------------------------------------------------------

    #[sinex_test]
    async fn from_record_falls_back_to_binary_for_non_json() -> xtask::sandbox::TestResult<()> {
        use sinex_primitives::parser::{MaterialAnchor, SourceRecord};
        use sinex_primitives::Id;
        let record = SourceRecord {
            material_id: Id::from_uuid(uuid::Uuid::nil()),
            anchor: MaterialAnchor::ByteRange { start: 0, len: 8 },
            bytes: b"not-json".to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        };
        let fp = SourceRecordFingerprint::from_record(&record);
        assert_eq!(fp.format, "binary");
        assert!(fp.keys.is_empty());
        assert!(fp.type_map.is_empty());
        // Non-empty hash computed over the opaque bytes.
        assert!(!fp.hash().is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn drift_record_count_resets_after_emission() -> xtask::sandbox::TestResult<()> {
        // After a drift event fires, record_count_since_last_emit must reset
        // so that a third schema requires another emit_every_n_records before
        // emitting again. Without this contract, every record after a drift
        // would re-emit.
        use sinex_primitives::parser::SourceUnitId;
        let mut acc = DriftAccumulator::new(SourceUnitId::from_static("test.unit"))
            .with_emit_every_n_records(1)
            .with_cooldown_secs(0);
        let fp1 = SourceRecordFingerprint::from_json(&json!({"a": 1}));
        let fp2 = SourceRecordFingerprint::from_json(&json!({"a": 1, "b": 2}));
        let fp3 =
            SourceRecordFingerprint::from_json(&json!({"a": 1, "b": 2, "c": 3}));
        let _ = acc.observe(&fp1); // baseline
        let drift = acc.observe(&fp2);
        assert!(drift.is_some(), "first drift after baseline should emit");
        // After emit, next observation with a NEW schema should re-emit only
        // when count threshold is met again. With emit_every_n_records=1,
        // observing fp3 (a different schema) should fire a new event.
        let drift_again = acc.observe(&fp3);
        assert!(
            drift_again.is_some(),
            "subsequent drift past emit_every_n_records should fire"
        );
        Ok(())
    }

    #[sinex_test]
    async fn drift_hash_stable_for_same_schema_under_value_changes() -> xtask::sandbox::TestResult<()> {
        // Two records with the same field set + types but different values
        // must produce the same fingerprint hash, so DriftAccumulator does
        // not flap on every record.
        let fp1 =
            SourceRecordFingerprint::from_json(&json!({"a": 1, "b": "x"}));
        let fp2 =
            SourceRecordFingerprint::from_json(&json!({"a": 999, "b": "y"}));
        assert_eq!(fp1.hash(), fp2.hash());
        assert_eq!(fp1.keys, fp2.keys);
        Ok(())
    }
}

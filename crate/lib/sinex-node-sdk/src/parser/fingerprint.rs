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
//! - **`type_map`**: inferred type for each key (`"string"`, `"integer"`, etc.)
//! - **`blake3_hash`**: BLAKE3 of canonical representation (stability across sessions)
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
//! - **`emit_every_n_records`**: minimum records between drift events for the same hash
//! - **`cooldown_secs`**: minimum seconds between drift events
//!
//! Either gate can suppress a drift event; both must clear for emission.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

// Note: blake3 is used directly (not via the Digest trait) — `blake3::Hasher`
// has its own update/finalize methods that don't require importing a trait.
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use sinex_primitives::parser::SourceUnitId;
use sinex_primitives::rpc::sources::{
    SourceCaveat, source_shape_drift_readiness_caveats_with_required_fields,
};
use sinex_primitives::temporal::Timestamp;

use crate::parser::DeclarativeParserSpec;

const MAX_JSON_FINGERPRINT_DEPTH: usize = 8;
const MAX_JSON_FINGERPRINT_FIELDS: usize = 512;
const MAX_DELIMITED_FINGERPRINT_FIELDS: usize = 512;
const MAX_DIRECTORY_MANIFEST_FIELDS: usize = 512;

// =============================================================================
// SourceRecordFingerprint
// =============================================================================

/// A structural fingerprint of a source record's shape.
///
/// Captures the keys and types present in a record, stable across
/// different orderings and representations of the same logical data.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceRecordFingerprint {
    /// Record format (e.g., "json", "csv", "`sqlite_row`").
    pub format: String,

    /// Sorted, deduplicated list of field/column names or JSON Pointers.
    pub keys: Vec<String>,

    /// Inferred type for each key (e.g., "string", "integer", "object").
    pub type_map: BTreeMap<String, String>,

    /// BLAKE3 hash of canonical (format, keys, `type_map`) representation.
    /// Serves as the structural identity — two fingerprints with the same
    /// hash are structurally identical.
    blake3_hash: String,
}

impl SourceRecordFingerprint {
    /// Creates a fingerprint from a JSON value.
    ///
    /// Recursively infers types from the JSON structure. The result is stable
    /// across different orderings of the same keys. Object paths use JSON
    /// Pointer syntax (`/message/content`) so nested drift is visible without
    /// storing raw sample values.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let fp = SourceRecordFingerprint::from_json(&json!({"name": "foo", "id": 42}));
    /// assert_eq!(fp.keys, vec!["/id", "/name"]);  // sorted
    /// assert_eq!(fp.type_map["/name"], "string");
    /// assert_eq!(fp.type_map["/id"], "integer");
    /// ```
    #[must_use]
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

    /// Creates a fingerprint from a CSV record.
    ///
    /// Header names are the shape keys. Value samples are only used for coarse
    /// type inference and are not retained in the fingerprint.
    #[must_use]
    pub fn from_csv_record(headers: &[String], fields: &[String]) -> Self {
        Self::from_delimited_record("csv", headers, fields)
    }

    /// Creates a fingerprint from a TSV record.
    ///
    /// Header names are the shape keys. Value samples are only used for coarse
    /// type inference and are not retained in the fingerprint.
    #[must_use]
    pub fn from_tsv_record(headers: &[String], fields: &[String]) -> Self {
        Self::from_delimited_record("tsv", headers, fields)
    }

    /// Creates a fingerprint from CSV bytes by reading the header row and first
    /// data row. Empty inputs and header-only inputs still produce a stable
    /// structural fingerprint of the visible columns.
    pub fn from_csv_bytes(bytes: &[u8]) -> Result<Self, csv::Error> {
        Self::from_delimited_bytes("csv", b',', bytes)
    }

    /// Creates a fingerprint from TSV bytes by reading the header row and first
    /// data row. Empty inputs and header-only inputs still produce a stable
    /// structural fingerprint of the visible columns.
    pub fn from_tsv_bytes(bytes: &[u8]) -> Result<Self, csv::Error> {
        Self::from_delimited_bytes("tsv", b'\t', bytes)
    }

    /// Creates a fingerprint from the declared `SQLite` table/column shape.
    ///
    /// The fingerprint records table names, column names, declared types,
    /// not-null flags, and primary-key positions. It never reads row values.
    pub fn from_sqlite_connection(conn: &rusqlite::Connection) -> rusqlite::Result<Self> {
        let mut table_stmt = conn.prepare(
            r"
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            ",
        )?;
        let table_names = table_stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        let mut keys = Vec::new();
        let mut type_map = BTreeMap::new();
        for table_name in table_names {
            let table_key = format!("table:{table_name}");
            keys.push(table_key.clone());
            type_map.insert(table_key, "table".to_string());

            let quoted = quote_sqlite_identifier(&table_name);
            let mut column_stmt = conn.prepare(&format!("PRAGMA table_info({quoted})"))?;
            let columns = column_stmt
                .query_map([], |row| {
                    Ok(SqliteColumnShape {
                        name: row.get(1)?,
                        declared_type: row.get::<_, String>(2)?,
                        not_null: row.get::<_, i64>(3)? != 0,
                        primary_key_position: row.get(5)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;

            for column in columns {
                let key = format!("{table_name}.{}", column.name);
                keys.push(key.clone());
                type_map.insert(
                    key,
                    format!(
                        "{};not_null={};pk={}",
                        normalize_sqlite_declared_type(&column.declared_type),
                        column.not_null,
                        column.primary_key_position
                    ),
                );
            }
        }

        keys.sort();
        keys.dedup();

        let fp = Self {
            format: "sqlite_schema".to_string(),
            keys: keys.clone(),
            type_map: type_map.clone(),
            blake3_hash: String::new(),
        };

        let mut fingerprint = fp;
        fingerprint.blake3_hash = fingerprint.compute_hash();
        Ok(fingerprint)
    }

    /// Creates a fingerprint from a directory manifest.
    ///
    /// The manifest stores relative file paths as keys and coarse file kinds
    /// as types. It intentionally records filesystem shape only: path presence
    /// and extension class, not file contents.
    #[must_use]
    pub fn from_directory_manifest<I, P, T>(entries: I) -> Self
    where
        I: IntoIterator<Item = (P, T)>,
        P: Into<String>,
        T: Into<String>,
    {
        let mut keys = Vec::new();
        let mut type_map = BTreeMap::new();

        for (path, file_kind) in entries.into_iter().take(MAX_DIRECTORY_MANIFEST_FIELDS) {
            let key = normalize_directory_manifest_path(path.into());
            keys.push(key.clone());
            type_map.insert(key, file_kind.into());
        }

        keys.sort();
        keys.dedup();

        let fp = Self {
            format: "directory_manifest".to_string(),
            keys: keys.clone(),
            type_map: type_map.clone(),
            blake3_hash: String::new(),
        };

        let mut fingerprint = fp;
        fingerprint.blake3_hash = fingerprint.compute_hash();
        fingerprint
    }

    /// Creates a fingerprint from a `SourceRecord`.
    ///
    /// Dispatches based on record format (currently only JSON is fully supported).
    #[must_use]
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

    fn from_delimited_bytes(format: &str, delimiter: u8, bytes: &[u8]) -> Result<Self, csv::Error> {
        if bytes.iter().all(u8::is_ascii_whitespace) {
            return Ok(Self::from_delimited_record(format, &[], &[]));
        }

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .flexible(true)
            .from_reader(bytes);
        let headers = reader
            .headers()?
            .iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let mut records = reader.records();
        let fields = match records.next().transpose()? {
            Some(record) => record.iter().map(str::to_string).collect::<Vec<_>>(),
            None => Vec::new(),
        };
        Ok(Self::from_delimited_record(format, &headers, &fields))
    }

    fn from_delimited_record(format: &str, headers: &[String], fields: &[String]) -> Self {
        let mut type_map = BTreeMap::new();
        let mut keys = Vec::new();

        for (idx, header) in headers
            .iter()
            .take(MAX_DELIMITED_FINGERPRINT_FIELDS)
            .enumerate()
        {
            let key = normalize_delimited_header(idx, header);
            keys.push(key.clone());
            let inferred = fields
                .get(idx)
                .map_or_else(|| "missing".to_string(), |value| infer_text_type(value));
            type_map.insert(key, inferred);
        }

        let remaining_capacity = MAX_DELIMITED_FINGERPRINT_FIELDS.saturating_sub(keys.len());
        for idx in headers.len()..headers.len().saturating_add(remaining_capacity) {
            if let Some(value) = fields.get(idx) {
                let key = format!("__extra_{idx}");
                keys.push(key.clone());
                type_map.insert(key, infer_text_type(value));
            }
        }

        keys.sort();
        keys.dedup();

        let fp = Self {
            format: format.to_string(),
            keys: keys.clone(),
            type_map: type_map.clone(),
            blake3_hash: String::new(),
        };

        let mut fingerprint = fp;
        fingerprint.blake3_hash = fingerprint.compute_hash();
        fingerprint
    }

    /// Extracts JSON Pointer paths and their inferred types from a JSON value.
    fn extract_types(value: &JsonValue) -> (Vec<String>, BTreeMap<String, String>) {
        let mut keys = Vec::new();
        let mut type_map = BTreeMap::new();
        Self::extract_types_at(
            value,
            "",
            0,
            &mut keys,
            &mut type_map,
            MAX_JSON_FINGERPRINT_FIELDS,
        );
        (keys, type_map)
    }

    fn extract_types_at(
        value: &JsonValue,
        path: &str,
        depth: usize,
        keys: &mut Vec<String>,
        type_map: &mut BTreeMap<String, String>,
        max_fields: usize,
    ) {
        if depth >= MAX_JSON_FINGERPRINT_DEPTH || keys.len() >= max_fields {
            return;
        }
        match value {
            JsonValue::Object(map) => {
                for (key, val) in map {
                    if keys.len() >= max_fields {
                        break;
                    }
                    let child_path = join_json_pointer(path, key);
                    keys.push(child_path.clone());
                    type_map.insert(child_path.clone(), Self::infer_type(val));
                    if val.is_object() {
                        Self::extract_types_at(
                            val,
                            &child_path,
                            depth + 1,
                            keys,
                            type_map,
                            max_fields,
                        );
                    }
                }
            }
            JsonValue::Array(_) => {
                // Arrays are represented by the field that owns them. We do
                // not index elements because array cardinality is data, not
                // source shape.
            }
            _ => {
                // Scalar values: no keys to extract.
            }
        }
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
    #[must_use]
    pub fn hash(&self) -> &str {
        &self.blake3_hash
    }

    /// Builds a drift event between two already-observed fingerprints.
    ///
    /// This is useful for runtimes that persist the previous fingerprint in
    /// checkpoint state rather than keeping a live [`DriftAccumulator`].
    #[must_use]
    pub fn diff(
        source_unit_id: SourceUnitId,
        previous: &SourceRecordFingerprint,
        current: &SourceRecordFingerprint,
    ) -> Option<DriftEvent> {
        if previous.hash() == current.hash() {
            return None;
        }

        Some(build_drift_event_from_parts(
            source_unit_id,
            previous.hash().to_string(),
            previous.keys.clone(),
            previous.type_map.clone(),
            current,
        ))
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
    #[must_use]
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
        if let Some(ref last) = self.last_hash
            && last == hash
        {
            return None;
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
    #[must_use]
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

        build_drift_event_from_parts(
            self.source_unit_id.clone(),
            previous_hash,
            previous_keys,
            previous_types,
            current,
        )
    }
}

fn build_drift_event_from_parts(
    source_unit_id: SourceUnitId,
    previous_hash: String,
    previous_keys: Vec<String>,
    previous_types: BTreeMap<String, String>,
    current: &SourceRecordFingerprint,
) -> DriftEvent {
    let current_key_set: std::collections::HashSet<_> = current.keys.iter().cloned().collect();
    let previous_key_set: std::collections::HashSet<_> = previous_keys.iter().cloned().collect();

    let mut added_keys: Vec<_> = current_key_set
        .difference(&previous_key_set)
        .cloned()
        .collect();
    added_keys.sort();

    let mut removed_keys: Vec<_> = previous_key_set
        .difference(&current_key_set)
        .cloned()
        .collect();
    removed_keys.sort();

    let mut type_changes = vec![];
    for key in &previous_keys {
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
        source_unit_id,
        previous_hash,
        current_hash: current.hash().to_string(),
        format: current.format.clone(),
        previous_keys,
        current_keys: current.keys.clone(),
        added_keys,
        removed_keys,
        type_changes,
        required_input_keys: Vec::new(),
        observed_at: Timestamp::now(),
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

    /// Type changes for keys that exist in both: (key, `old_type`, `new_type`).
    pub type_changes: Vec<(String, String, String)>,

    /// Parser-declared input keys required by the producer, when known.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_input_keys: Vec<String>,

    /// When this drift was observed.
    pub observed_at: Timestamp,
}

impl DriftEvent {
    /// Convert this drift observation into readiness caveats.
    ///
    /// Added fields are advisory because existing parser mappings can usually
    /// ignore them. Removed fields and type changes are degraded because they
    /// are the shapes most likely to produce missing/defaulted parsed values.
    #[must_use]
    pub fn readiness_caveats(&self) -> Vec<SourceCaveat> {
        self.readiness_caveats_with_required_fields(&self.required_input_keys)
    }

    /// Convert this drift observation into readiness caveats while honoring
    /// parser-declared required input keys.
    #[must_use]
    pub fn readiness_caveats_with_required_fields(
        &self,
        required_input_keys: &[String],
    ) -> Vec<SourceCaveat> {
        source_shape_drift_readiness_caveats_with_required_fields(
            &self.source_unit_id,
            &self.current_hash,
            self.added_keys.len(),
            &self.removed_keys,
            self.type_changes.len(),
            required_input_keys,
        )
    }

    /// Convert this drift observation into readiness caveats using the
    /// required input keys declared by a declarative parser spec.
    #[must_use]
    pub fn readiness_caveats_for_declarative_parser(
        &self,
        spec: &DeclarativeParserSpec,
    ) -> Vec<SourceCaveat> {
        self.readiness_caveats_with_required_fields(&spec.required_input_keys())
    }

    /// Serializes this event as a JSON payload suitable for a parser-emitted event.
    #[must_use]
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
            "required_input_keys": self.required_input_keys,
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
        .map_or(0, |d| d.as_secs())
}

fn join_json_pointer(parent: &str, key: &str) -> String {
    let escaped = key.replace('~', "~0").replace('/', "~1");
    if parent.is_empty() {
        format!("/{escaped}")
    } else {
        format!("{parent}/{escaped}")
    }
}

fn normalize_delimited_header(idx: usize, header: &str) -> String {
    let trimmed = header.trim();
    if trimmed.is_empty() {
        format!("column_{idx}")
    } else {
        trimmed.to_string()
    }
}

fn normalize_directory_manifest_path(path: String) -> String {
    path.trim_start_matches("./").replace('\\', "/")
}

fn infer_text_type(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return "empty".to_string();
    }
    if trimmed.eq_ignore_ascii_case("true") || trimmed.eq_ignore_ascii_case("false") {
        return "boolean".to_string();
    }
    if trimmed.parse::<i64>().is_ok() || trimmed.parse::<u64>().is_ok() {
        return "integer".to_string();
    }
    if trimmed.parse::<f64>().is_ok() {
        return "number".to_string();
    }
    "string".to_string()
}

struct SqliteColumnShape {
    name: String,
    declared_type: String,
    not_null: bool,
    primary_key_position: i64,
}

fn quote_sqlite_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn normalize_sqlite_declared_type(declared_type: &str) -> String {
    let trimmed = declared_type.trim();
    if trimmed.is_empty() {
        "untyped".to_string()
    } else {
        trimmed.to_ascii_lowercase()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{FieldSource, FieldSpec, FieldType, InputFormat};
    use serde_json::json;
    use sinex_primitives::domain::{EventSource, EventType};
    use sinex_primitives::parser::{ParserId, SourceUnitId};
    use sinex_primitives::privacy::ProcessingContext;
    use sinex_primitives::rpc::sources::{CaveatSeverity, caveat_codes};
    use xtask::sandbox::prelude::sinex_test;

    #[sinex_test]
    async fn test_from_json_simple() -> xtask::sandbox::TestResult<()> {
        let value = json!({"name": "Alice", "age": 30});
        let fp = SourceRecordFingerprint::from_json(&value);

        assert_eq!(fp.format, "json");
        assert_eq!(fp.keys, vec!["/age", "/name"]); // sorted
        assert_eq!(fp.type_map["/name"], "string");
        assert_eq!(fp.type_map["/age"], "integer");
        Ok(())
    }

    #[sinex_test]
    async fn test_from_json_with_nulls() -> xtask::sandbox::TestResult<()> {
        let value = json!({"name": "Bob", "email": null});
        let fp = SourceRecordFingerprint::from_json(&value);

        assert_eq!(fp.keys, vec!["/email", "/name"]);
        assert_eq!(fp.type_map["/email"], "null");
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

        assert_eq!(fp.type_map["/text"], "string");
        assert_eq!(fp.type_map["/count"], "integer");
        assert_eq!(fp.type_map["/active"], "boolean");
        assert_eq!(fp.type_map["/nested"], "object");
        assert_eq!(fp.type_map["/nested/key"], "string");
        assert_eq!(fp.type_map["/items"], "array");
        assert_eq!(fp.type_map["/nullable"], "null");
        Ok(())
    }

    #[sinex_test]
    async fn test_from_json_uses_nested_json_pointer_paths() -> xtask::sandbox::TestResult<()> {
        let value = json!({
            "message": {
                "content": "hello",
                "meta/with~escape": {
                    "tokens": 12
                }
            },
            "session_id": "abc"
        });
        let fp = SourceRecordFingerprint::from_json(&value);

        assert!(fp.keys.contains(&"/message/content".to_string()));
        assert!(fp.keys.contains(&"/message/meta~1with~0escape".to_string()));
        assert!(
            fp.keys
                .contains(&"/message/meta~1with~0escape/tokens".to_string())
        );
        assert_eq!(fp.type_map["/message/content"], "string");
        assert_eq!(fp.type_map["/message/meta~1with~0escape/tokens"], "integer");
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
    async fn test_from_csv_bytes_infers_header_shape() -> xtask::sandbox::TestResult<()> {
        let fp =
            SourceRecordFingerprint::from_csv_bytes(b"id,name,active,score\n42,Alice,true,98.5\n")?;

        assert_eq!(fp.format, "csv");
        assert_eq!(fp.keys, vec!["active", "id", "name", "score"]);
        assert_eq!(fp.type_map["id"], "integer");
        assert_eq!(fp.type_map["name"], "string");
        assert_eq!(fp.type_map["active"], "boolean");
        assert_eq!(fp.type_map["score"], "number");
        Ok(())
    }

    #[sinex_test]
    async fn test_from_tsv_bytes_detects_missing_and_extra_columns()
    -> xtask::sandbox::TestResult<()> {
        let fp = SourceRecordFingerprint::from_tsv_bytes(b"id\tname\n42\tAlice\tunexpected\n")?;

        assert_eq!(fp.format, "tsv");
        assert_eq!(fp.keys, vec!["__extra_2", "id", "name"]);
        assert_eq!(fp.type_map["__extra_2"], "string");
        Ok(())
    }

    #[sinex_test]
    async fn test_from_csv_bytes_handles_empty_input() -> xtask::sandbox::TestResult<()> {
        let fp = SourceRecordFingerprint::from_csv_bytes(b"  \n\t")?;

        assert_eq!(fp.format, "csv");
        assert!(fp.keys.is_empty());
        assert!(fp.type_map.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_csv_drift_reports_header_and_type_changes() -> xtask::sandbox::TestResult<()> {
        let source_unit = SourceUnitId::from_static("test.csv");
        let mut acc = DriftAccumulator::new(source_unit)
            .with_emit_every_n_records(1)
            .with_cooldown_secs(0);
        let fp1 = SourceRecordFingerprint::from_csv_bytes(b"id,name,score\n42,Alice,98.5\n")?;
        let fp2 = SourceRecordFingerprint::from_csv_bytes(b"id,full_name,score\n42,Alice,high\n")?;

        acc.observe(&fp1);
        let drift = acc.observe(&fp2).expect("csv shape drift should emit");

        assert_eq!(drift.format, "csv");
        assert_eq!(drift.added_keys, vec!["full_name"]);
        assert_eq!(drift.removed_keys, vec!["name"]);
        assert_eq!(
            drift.type_changes,
            vec![(
                "score".to_string(),
                "number".to_string(),
                "string".to_string()
            )]
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_from_sqlite_connection_fingerprints_table_columns()
    -> xtask::sandbox::TestResult<()> {
        let conn = rusqlite::Connection::open_in_memory()?;
        conn.execute(
            "CREATE TABLE history (
                id INTEGER PRIMARY KEY,
                command TEXT NOT NULL,
                ts_ms INTEGER
            )",
            [],
        )?;

        let fp = SourceRecordFingerprint::from_sqlite_connection(&conn)?;

        assert_eq!(fp.format, "sqlite_schema");
        assert!(fp.keys.contains(&"table:history".to_string()));
        assert!(fp.keys.contains(&"history.command".to_string()));
        assert_eq!(fp.type_map["history.command"], "text;not_null=true;pk=0");
        assert_eq!(fp.type_map["history.id"], "integer;not_null=false;pk=1");
        Ok(())
    }

    #[sinex_test]
    async fn test_sqlite_schema_drift_reports_column_change() -> xtask::sandbox::TestResult<()> {
        let source_unit = SourceUnitId::from_static("test.sqlite");
        let mut acc = DriftAccumulator::new(source_unit)
            .with_emit_every_n_records(1)
            .with_cooldown_secs(0);

        let conn1 = rusqlite::Connection::open_in_memory()?;
        conn1.execute(
            "CREATE TABLE history (id INTEGER PRIMARY KEY, command TEXT)",
            [],
        )?;
        let conn2 = rusqlite::Connection::open_in_memory()?;
        conn2.execute(
            "CREATE TABLE history (
                id INTEGER PRIMARY KEY,
                command BLOB,
                exit_code INTEGER
            )",
            [],
        )?;

        let fp1 = SourceRecordFingerprint::from_sqlite_connection(&conn1)?;
        let fp2 = SourceRecordFingerprint::from_sqlite_connection(&conn2)?;
        acc.observe(&fp1);
        let drift = acc
            .observe(&fp2)
            .expect("sqlite schema shape drift should emit");

        assert_eq!(drift.format, "sqlite_schema");
        assert_eq!(drift.added_keys, vec!["history.exit_code"]);
        assert_eq!(
            drift.type_changes,
            vec![(
                "history.command".to_string(),
                "text;not_null=false;pk=0".to_string(),
                "blob;not_null=false;pk=0".to_string()
            )]
        );
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
        assert_eq!(drift.added_keys, vec!["/name".to_string()]);
        assert!(drift.removed_keys.is_empty());
        assert!(drift.type_changes.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_drift_accumulator_respects_record_count_limit() -> xtask::sandbox::TestResult<()>
    {
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
        assert_eq!(event.added_keys, vec!["/c"]);
        assert_eq!(event.removed_keys, vec!["/b"]);
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
        assert_eq!(
            event.type_changes[0],
            (
                "/count".to_string(),
                "integer".to_string(),
                "string".to_string()
            )
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_fingerprint_diff_matches_drift_payload() -> xtask::sandbox::TestResult<()> {
        let source_unit = SourceUnitId::from_static("test.unit");
        let fp1 = SourceRecordFingerprint::from_json(&json!({"count": 42, "name": "old"}));
        let fp2 = SourceRecordFingerprint::from_json(&json!({"count": "42", "enabled": true}));

        let drift = SourceRecordFingerprint::diff(source_unit.clone(), &fp1, &fp2)
            .expect("different fingerprints should report drift");

        assert_eq!(drift.source_unit_id, source_unit);
        assert_eq!(drift.previous_hash, fp1.hash());
        assert_eq!(drift.current_hash, fp2.hash());
        assert_eq!(drift.added_keys, vec!["/enabled"]);
        assert_eq!(drift.removed_keys, vec!["/name"]);
        assert_eq!(
            drift.type_changes,
            vec![(
                "/count".to_string(),
                "integer".to_string(),
                "string".to_string()
            )]
        );
        assert!(
            SourceRecordFingerprint::diff(SourceUnitId::from_static("test.unit"), &fp1, &fp1)
                .is_none()
        );
        Ok(())
    }

    #[sinex_test]
    async fn drift_readiness_caveats_classify_advisory_and_degraded_shapes()
    -> xtask::sandbox::TestResult<()> {
        let source_unit = SourceUnitId::from_static("test.unit");

        let additive = SourceRecordFingerprint::diff(
            source_unit.clone(),
            &SourceRecordFingerprint::from_json(&json!({"id": 1})),
            &SourceRecordFingerprint::from_json(&json!({"id": 1, "optional": true})),
        )
        .ok_or_else(|| color_eyre::eyre::eyre!("additive drift expected"))?;
        let additive_caveats = additive.readiness_caveats();
        assert_eq!(additive_caveats.len(), 1);
        assert_eq!(additive_caveats[0].code, caveat_codes::SOURCE_SHAPE_CHANGED);
        assert_eq!(additive_caveats[0].severity, CaveatSeverity::Info);
        assert!(
            additive_caveats[0]
                .evidence_ref
                .as_deref()
                .is_some_and(|reference| reference.starts_with("drift:"))
        );

        let degraded = SourceRecordFingerprint::diff(
            source_unit,
            &SourceRecordFingerprint::from_json(&json!({"id": 1, "name": "old"})),
            &SourceRecordFingerprint::from_json(&json!({"id": "1"})),
        )
        .ok_or_else(|| color_eyre::eyre::eyre!("degraded drift expected"))?;
        let degraded_caveats = degraded.readiness_caveats();
        let codes = degraded_caveats
            .iter()
            .map(|caveat| caveat.code.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            codes,
            vec![
                caveat_codes::PARSER_FIELD_TYPE_CHANGED,
                caveat_codes::PARSER_REQUIRED_FIELD_MISSING
            ]
        );
        assert!(
            degraded_caveats
                .iter()
                .all(|caveat| caveat.severity == CaveatSeverity::Degraded)
        );

        let required_caveats =
            degraded.readiness_caveats_with_required_fields(&["/name".to_string()]);
        assert!(
            required_caveats.iter().any(|caveat| {
                caveat.code == caveat_codes::PARSER_REQUIRED_FIELD_MISSING
                    && caveat.severity == CaveatSeverity::Blocking
            }),
            "required input removal should block readiness: {required_caveats:?}"
        );

        let spec = DeclarativeParserSpec {
            parser_id: ParserId::from_static("test-parser"),
            parser_version: "1.0.0".to_string(),
            source_unit_id: SourceUnitId::from_static("test.unit"),
            event_source: EventSource::from_static("test"),
            event_type: EventType::from_static("test.event"),
            default_privacy_context: ProcessingContext::Metadata,
            input_format: InputFormat::Json,
            fields: vec![FieldSpec {
                name: "name".to_string(),
                source: FieldSource::JsonPointer {
                    pointer: "/name".to_string(),
                },
                field_type: FieldType::String,
                required: true,
                default: None,
                skip_payload: false,
                privacy_context: None,
                occurrence_key: false,
                timestamp: None,
                suppress_if: None,
                carry: None,
            }],
            discriminator: None,
        };
        let spec_caveats = degraded.readiness_caveats_for_declarative_parser(&spec);
        assert!(
            spec_caveats.iter().any(|caveat| {
                caveat.code == caveat_codes::PARSER_REQUIRED_FIELD_MISSING
                    && caveat.severity == CaveatSeverity::Blocking
            }),
            "declarative required input removal should block readiness: {spec_caveats:?}"
        );
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
        assert_eq!(payload["added_keys"], serde_json::json!(["/y"]));
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
        use sinex_primitives::Id;
        use sinex_primitives::parser::{MaterialAnchor, SourceRecord};
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
        let fp3 = SourceRecordFingerprint::from_json(&json!({"a": 1, "b": 2, "c": 3}));
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
    async fn drift_hash_stable_for_same_schema_under_value_changes()
    -> xtask::sandbox::TestResult<()> {
        // Two records with the same field set + types but different values
        // must produce the same fingerprint hash, so DriftAccumulator does
        // not flap on every record.
        let fp1 = SourceRecordFingerprint::from_json(&json!({"a": 1, "b": "x"}));
        let fp2 = SourceRecordFingerprint::from_json(&json!({"a": 999, "b": "y"}));
        assert_eq!(fp1.hash(), fp2.hash());
        assert_eq!(fp1.keys, fp2.keys);
        Ok(())
    }
}

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
//! `DriftAccumulator` tracks the last-seen fingerprint per source and emits
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

use crate::parser::SourceId;
use crate::rpc::sources::{
    SourceCaveat, source_shape_drift_readiness_caveats_with_required_fields,
};
use crate::temporal::Timestamp;

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

    /// Creates a fingerprint from JSON Lines bytes.
    ///
    /// Each non-empty line is parsed as one JSON value. Object field paths are
    /// recorded under `/[]/...`, matching top-level JSON array exports while
    /// preserving the fact that this source was JSONL in the fingerprint hash.
    pub fn from_jsonl_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        let mut keys = Vec::new();
        let mut type_map = BTreeMap::new();

        for line in bytes.split(|byte| *byte == b'\n') {
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let value: JsonValue = serde_json::from_slice(line)?;
            Self::extract_top_level_array_object_types(
                &[value],
                0,
                &mut keys,
                &mut type_map,
                MAX_JSON_FINGERPRINT_FIELDS,
            );
            if keys.len() >= MAX_JSON_FINGERPRINT_FIELDS {
                break;
            }
        }

        keys.sort();
        keys.dedup();

        let fp = Self {
            format: "jsonl".to_string(),
            keys: keys.clone(),
            type_map: type_map.clone(),
            blake3_hash: String::new(),
        };

        let mut fingerprint = fp;
        fingerprint.blake3_hash = fingerprint.compute_hash();
        Ok(fingerprint)
    }

    /// Creates a fingerprint from the declared `SQLite` table/column shape.
    ///
    /// The fingerprint records table names, column names, declared types,
    /// not-null flags, and primary-key positions. It never reads row values.
    #[cfg(feature = "rusqlite")]
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
            let key = normalize_directory_manifest_path(&path.into());
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
            JsonValue::Array(items) => {
                // Top-level export files are often arrays of homogeneous row
                // objects. Record their element object keys under /[] so drift
                // can detect a field disappearing without making array length
                // or element index part of the shape.
                if path.is_empty() {
                    Self::extract_top_level_array_object_types(
                        items, depth, keys, type_map, max_fields,
                    );
                } else {
                    // Nested arrays are represented by the field that owns
                    // them. We do not index elements because array cardinality
                    // is data, not source shape.
                }
            }
            _ => {
                // Scalar values: no keys to extract.
            }
        }
    }

    fn extract_top_level_array_object_types(
        items: &[JsonValue],
        depth: usize,
        keys: &mut Vec<String>,
        type_map: &mut BTreeMap<String, String>,
        max_fields: usize,
    ) {
        let element_path = join_json_pointer("", "[]");
        for item in items {
            let JsonValue::Object(map) = item else {
                continue;
            };
            for (key, val) in map {
                if keys.len() >= max_fields {
                    return;
                }
                let child_path = join_json_pointer(&element_path, key);
                keys.push(child_path.clone());
                merge_inferred_type(type_map, child_path.clone(), Self::infer_type(val));
                if val.is_object() {
                    Self::extract_types_at(val, &child_path, depth + 1, keys, type_map, max_fields);
                }
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
        source_id: SourceId,
        previous: &SourceRecordFingerprint,
        current: &SourceRecordFingerprint,
    ) -> Option<DriftEvent> {
        if previous.hash() == current.hash() {
            return None;
        }

        Some(build_drift_event_from_parts(
            source_id,
            previous.hash().to_string(),
            previous.keys.clone(),
            &previous.type_map,
            current,
        ))
    }
}

// =============================================================================
// DriftAccumulator
// =============================================================================

/// Rate-limited drift detector for a source.
///
/// Tracks the last-seen fingerprint and emits `DriftEvent` when the
/// structure changes, subject to configurable rate limits.
#[derive(Debug, Clone)]
pub struct DriftAccumulator {
    source_id: SourceId,

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
    /// Creates a new drift accumulator for a source.
    #[must_use]
    pub fn new(source_id: SourceId) -> Self {
        Self {
            source_id,
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
            self.source_id.clone(),
            previous_hash,
            previous_keys,
            &previous_types,
            current,
        )
    }
}

fn build_drift_event_from_parts(
    source_id: SourceId,
    previous_hash: String,
    previous_keys: Vec<String>,
    previous_types: &BTreeMap<String, String>,
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
        source_id,
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
    /// The source that drifted.
    pub source_id: SourceId,

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
            &self.source_id,
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
            "source_id": self.source_id.as_str(),
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

fn merge_inferred_type(type_map: &mut BTreeMap<String, String>, key: String, inferred: String) {
    match type_map.get_mut(&key) {
        Some(existing) if *existing != inferred => {
            *existing = "mixed".to_string();
        }
        Some(_) => {}
        None => {
            type_map.insert(key, inferred);
        }
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

fn normalize_directory_manifest_path(path: &str) -> String {
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

#[cfg(feature = "rusqlite")]
struct SqliteColumnShape {
    name: String,
    declared_type: String,
    not_null: bool,
    primary_key_position: i64,
}

#[cfg(feature = "rusqlite")]
fn quote_sqlite_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

#[cfg(feature = "rusqlite")]
fn normalize_sqlite_declared_type(declared_type: &str) -> String {
    let trimmed = declared_type.trim();
    if trimmed.is_empty() {
        "untyped".to_string()
    } else {
        trimmed.to_ascii_lowercase()
    }
}

#[cfg(test)]
#[path = "fingerprint_test.rs"]
mod tests;

//! Declarative parser substrate (#1100).
//!
//! Both `#[derive(SourceRecord)]` (compile-time, in `sinex-macros`) and the
//! YAML loader (runtime, see [`yaml_loader`](super::yaml_loader)) compile into
//! a [`DeclarativeParserSpec`]. The [`DeclarativeParser::evaluate`] method
//! takes a spec + a [`SourceRecord`] and produces zero or more
//! [`ParsedEventIntent`] values — the same code path regardless of how the
//! spec was authored.
//!
//! See `crate/lib/sinex-node-sdk/docs/declarative_parser.md` for the locked
//! design and the macro attribute catalog.
//!
//! # v1 scope (this file)
//!
//! - JSON / tab-separated / SQLite-row / CSV-row / raw-line input formats
//! - Field extraction via JSON Pointer, column index, column name, raw line
//! - Type coercion for string, integer, number, boolean, JSON
//! - Per-field privacy via `privacy::process()` (records FieldPrivacyDecision)
//! - `#[suppress_if]` predicate (per-field or whole-event)
//! - `#[required]` / `#[default]` / `#[skip]` semantics
//! - `#[occurrence_key]` composite key construction
//! - `#[timestamp]` derivation with rfc3339 / unix-seconds / unix-millis /
//!   unix-micros / unix-nanos formats and material-time fallback
//!
//! # Deferred (Phase 1A v2)
//!
//! - `#[anchor(kind = "...")]` override — for v1 we pass through the
//!   adapter's anchor on the source record. Adapters that need a derived
//!   anchor must compute it themselves before yielding the SourceRecord.
//! - Regex captures for line logs (use raw_line + a thin imperative
//!   wrapper for now)
//! - `#[redact_if(rule = "...")]` named rule references

use serde::{Deserialize, Serialize};
use sinex_primitives::events::{EventSource, EventType};
use sinex_primitives::parser::{
    MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId, SourceRecord,
    TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::{
    self, FieldPrivacyDecision, ProcessingContext,
};
use sinex_primitives::sources::SourceUnitId;
use sinex_primitives::Timestamp;
use std::borrow::Cow;
use std::collections::BTreeMap;

use crate::parser::ParserError;

// =============================================================================
// Spec types — the data the macro / YAML loader produces
// =============================================================================

/// Static description of a declarative parser.
///
/// Built once per parser at compile time (via macro) or load time (via YAML).
/// Consumed by [`DeclarativeParser::evaluate`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeclarativeParserSpec {
    pub parser_id: ParserId,
    pub parser_version: String,
    pub source_unit_id: SourceUnitId,
    pub event_source: EventSource,
    pub event_type: EventType,
    pub default_privacy_context: ProcessingContext,
    pub input_format: InputFormat,
    pub fields: Vec<FieldSpec>,
}

/// What kind of record bytes the parser consumes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputFormat {
    /// JSON object — extract via JSON Pointer.
    Json,
    /// Tab-delimited line — extract via column index (0-based).
    TabSeparated,
    /// CSV row already deserialized into a JSON object — extract via column name.
    CsvRow,
    /// SQLite row already deserialized into a JSON object — extract via column name.
    SqliteRow,
    /// Single line, no field structure — extract via `RawLine`.
    RawLine,
}

/// Per-field declaration on a declarative parser.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSpec {
    /// Field name in the emitted payload (e.g. `"command"`).
    pub name: String,

    /// Where to read the value from in the input record.
    pub source: FieldSource,

    /// How to interpret the raw value.
    pub field_type: FieldType,

    /// Whether the field is required. Missing required fields fail the record.
    #[serde(default)]
    pub required: bool,

    /// Default value if missing (only meaningful when `required = false`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,

    /// Excluded from emitted payload but may still contribute to occurrence/timestamp.
    #[serde(default)]
    pub skip_payload: bool,

    /// Privacy processing context. If set, the field's value runs through
    /// `privacy::process()` before being placed in the payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub privacy_context: Option<ProcessingContext>,

    /// Include this field as part of the parser's `OccurrenceKey`.
    #[serde(default)]
    pub occurrence_key: bool,

    /// If set, the field's value derives the event's `ts_orig`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<TimestampSpec>,

    /// If set, the field is suppressed when the named binding-config flag is true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suppress_if: Option<SuppressPredicate>,
}

/// Where to read a field value from in the source record.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FieldSource {
    /// JSON Pointer (RFC 6901), e.g. `"/command"`.
    JsonPointer { pointer: String },
    /// Tab/CSV column by 0-based index.
    ColumnIndex { index: usize },
    /// Column by name (CSV header or SQLite column).
    ColumnName { name: String },
    /// The entire record bytes interpreted as a string.
    RawLine,
}

/// How to interpret the raw extracted value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    String,
    Integer,
    Number,
    Boolean,
    /// Pass through as a JSON subtree.
    Json,
}

/// How a field's value derives the event timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimestampSpec {
    pub format: TimestampFormat,
    #[serde(default)]
    pub fallback: TimestampFallback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampFormat {
    UnixSeconds,
    UnixSecondsNanos,
    UnixMillis,
    UnixMicros,
    Rfc3339,
    /// ISO 8601 — alias for Rfc3339 in this implementation.
    Iso8601,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampFallback {
    /// Fall back to the material's acquisition time (default).
    MaterialTiming,
    /// Fail the record.
    Error,
}

impl Default for TimestampFallback {
    fn default() -> Self {
        Self::MaterialTiming
    }
}

/// Predicate for `#[suppress_if(field = "...")]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuppressPredicate {
    /// Name of the binding-config field to check.
    pub binding_field: String,
    /// If true, suppressing this field drops the entire event. Otherwise just
    /// the field is dropped.
    #[serde(default)]
    pub whole_event: bool,
}

// =============================================================================
// Binding config — runtime values that suppress predicates check against
// =============================================================================

/// Runtime configuration values that `#[suppress_if]` predicates and other
/// binding-aware fields read at parse time. Supplied by the source-worker host
/// from the active source-binding.
#[derive(Debug, Clone, Default)]
pub struct BindingConfig {
    flags: BTreeMap<String, bool>,
}

impl BindingConfig {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_flag(mut self, name: impl Into<String>, value: bool) -> Self {
        self.flags.insert(name.into(), value);
        self
    }

    #[must_use]
    pub fn is_truthy(&self, name: &str) -> bool {
        self.flags.get(name).copied().unwrap_or(false)
    }
}

// =============================================================================
// Evaluator
// =============================================================================

/// Stateless evaluator. The same code path runs whether the spec was authored
/// via the derive macro or the YAML loader.
pub struct DeclarativeParser;

impl DeclarativeParser {
    /// Evaluate a record against a spec, producing zero or more event intents.
    ///
    /// Returns `Ok(vec![])` (zero events) if the entire event was suppressed
    /// by a `whole_event` predicate. Returns `Err` if a required field was
    /// missing or a type conversion failed.
    pub fn evaluate(
        spec: &DeclarativeParserSpec,
        record: SourceRecord,
        ctx: &ParserContext,
        binding: &BindingConfig,
    ) -> Result<Vec<ParsedEventIntent>, ParserError> {
        let decoded = decode_record(spec.input_format, &record)?;

        let mut payload = serde_json::Map::new();
        let mut field_privacy_log = Vec::new();
        let mut occurrence_fields: Vec<(String, String)> = Vec::new();
        let mut ts_override: Option<(Timestamp, String)> = None;
        let mut whole_event_suppressed = false;

        for field in &spec.fields {
            let raw_value = extract_field(&decoded, &field.source, spec.input_format)?;

            let value = match raw_value {
                Some(v) => v,
                None => {
                    if let Some(default) = field.default.clone() {
                        default
                    } else if field.required {
                        return Err(ParserError::Field(format!(
                            "required field '{}' missing from record",
                            field.name
                        )));
                    } else {
                        continue;
                    }
                }
            };

            let coerced = coerce_field(&value, field.field_type, &field.name)?;

            let suppressed_by_predicate = match &field.suppress_if {
                Some(pred) => binding.is_truthy(&pred.binding_field),
                None => false,
            };

            // Privacy processing for fields with a declared context.
            let final_value = if let Some(ctx_priv) = field.privacy_context {
                if suppressed_by_predicate {
                    let mut decision = FieldPrivacyDecision::suppressed_by_predicate(
                        &field.name,
                        ctx_priv,
                    );
                    if let Some(pred) = &field.suppress_if {
                        if pred.whole_event {
                            decision = decision.into_whole_event_suppressor();
                            whole_event_suppressed = true;
                        }
                    }
                    field_privacy_log.push(decision);
                    None
                } else {
                    let value_str = value_as_string(&coerced);
                    let processed = privacy::process(&value_str, ctx_priv)
                        .map_err(|e| ParserError::Privacy(e.to_string()))?;
                    let decision = FieldPrivacyDecision::from_processed(
                        &field.name,
                        ctx_priv,
                        &processed,
                    );
                    field_privacy_log.push(decision);
                    if processed.suppressed {
                        None
                    } else {
                        Some(serde_json::Value::String(match processed.text {
                            Cow::Borrowed(s) => s.to_string(),
                            Cow::Owned(s) => s,
                        }))
                    }
                }
            } else if suppressed_by_predicate {
                if let Some(pred) = &field.suppress_if {
                    if pred.whole_event {
                        whole_event_suppressed = true;
                    }
                }
                None
            } else {
                Some(coerced.clone())
            };

            // Timestamp derivation.
            if let Some(ts_spec) = &field.timestamp {
                if let Some(ts) = parse_timestamp(&coerced, ts_spec, &field.name, ctx)? {
                    ts_override = Some((ts, field.name.clone()));
                }
            }

            // Occurrence key contribution.
            if field.occurrence_key {
                occurrence_fields.push((field.name.clone(), value_as_string(&coerced)));
            }

            // Add to payload unless skipped or suppressed.
            if !field.skip_payload {
                if let Some(v) = final_value {
                    payload.insert(field.name.clone(), v);
                }
            }
        }

        if whole_event_suppressed {
            return Ok(vec![]);
        }

        let (ts_orig, timing) = match ts_override {
            Some((ts, field_name)) => (
                ts,
                TimingEvidence::Intrinsic {
                    field: field_name,
                    confidence: TimingConfidence::Intrinsic,
                },
            ),
            None => (ctx.acquisition_time, TimingEvidence::StagedAtFallback),
        };

        let occurrence_key = if occurrence_fields.is_empty() {
            None
        } else {
            Some(OccurrenceKey {
                source_unit_id: ctx.source_unit_id.clone(),
                fields: occurrence_fields,
            })
        };

        Ok(vec![ParsedEventIntent {
            source_unit_id: ctx.source_unit_id.clone(),
            parser_id: spec.parser_id.clone(),
            parser_version: spec.parser_version.clone(),
            event_type: spec.event_type.clone(),
            event_source: spec.event_source.clone(),
            payload: serde_json::Value::Object(payload),
            ts_orig,
            timing,
            anchor: record.anchor.clone(),
            occurrence_key,
            privacy_context: spec.default_privacy_context,
            field_privacy_log: Some(field_privacy_log),
        }])
    }
}

// =============================================================================
// Internal helpers
// =============================================================================

enum DecodedRecord {
    Json(serde_json::Value),
    TabFields(Vec<String>),
    Line(String),
}

fn decode_record(
    format: InputFormat,
    record: &SourceRecord,
) -> Result<DecodedRecord, ParserError> {
    let text = std::str::from_utf8(&record.bytes)
        .map_err(|e| ParserError::Decode(format!("record bytes not valid UTF-8: {e}")))?;
    match format {
        InputFormat::Json | InputFormat::CsvRow | InputFormat::SqliteRow => {
            let v: serde_json::Value = serde_json::from_str(text).map_err(|e| {
                ParserError::Decode(format!(
                    "record is not valid JSON: {e} (csv_row/sqlite_row adapters must \
                     yield row as JSON object)"
                ))
            })?;
            Ok(DecodedRecord::Json(v))
        }
        InputFormat::TabSeparated => Ok(DecodedRecord::TabFields(
            text.split('\t').map(|s| s.to_string()).collect(),
        )),
        InputFormat::RawLine => Ok(DecodedRecord::Line(text.to_string())),
    }
}

fn extract_field(
    decoded: &DecodedRecord,
    source: &FieldSource,
    format: InputFormat,
) -> Result<Option<serde_json::Value>, ParserError> {
    match (decoded, source) {
        (DecodedRecord::Json(value), FieldSource::JsonPointer { pointer }) => {
            Ok(value.pointer(pointer).cloned())
        }
        (DecodedRecord::Json(value), FieldSource::ColumnName { name }) => {
            Ok(value.as_object().and_then(|o| o.get(name)).cloned())
        }
        (DecodedRecord::TabFields(fields), FieldSource::ColumnIndex { index }) => Ok(fields
            .get(*index)
            .map(|s| serde_json::Value::String(s.clone()))),
        (DecodedRecord::Line(line), FieldSource::RawLine) => {
            Ok(Some(serde_json::Value::String(line.clone())))
        }
        (decoded, source) => Err(ParserError::Field(format!(
            "field source incompatible with input format {format:?}: source={source:?}, decoded={}",
            match decoded {
                DecodedRecord::Json(_) => "json",
                DecodedRecord::TabFields(_) => "tab_fields",
                DecodedRecord::Line(_) => "line",
            }
        ))),
    }
}

fn coerce_field(
    value: &serde_json::Value,
    target: FieldType,
    field_name: &str,
) -> Result<serde_json::Value, ParserError> {
    match (target, value) {
        (FieldType::String, serde_json::Value::String(_)) => Ok(value.clone()),
        (FieldType::String, _) => Ok(serde_json::Value::String(value_as_string(value))),
        (FieldType::Integer, serde_json::Value::Number(n)) if n.is_i64() => Ok(value.clone()),
        (FieldType::Integer, serde_json::Value::String(s)) => s
            .parse::<i64>()
            .map(|n| serde_json::Value::Number(n.into()))
            .map_err(|_| {
                ParserError::Field(format!("'{field_name}' = {s:?} is not an integer"))
            }),
        (FieldType::Integer, serde_json::Value::Number(n)) if n.is_f64() => n
            .as_f64()
            .filter(|f| f.fract() == 0.0)
            .and_then(|f| {
                let i = f as i64;
                serde_json::Number::from_f64(i as f64).map(|_| {
                    serde_json::Value::Number(serde_json::Number::from(i))
                })
            })
            .ok_or_else(|| {
                ParserError::Field(format!("'{field_name}' = {n:?} is not an integer"))
            }),
        (FieldType::Number, serde_json::Value::Number(_)) => Ok(value.clone()),
        (FieldType::Number, serde_json::Value::String(s)) => s
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(serde_json::Value::Number)
            .ok_or_else(|| {
                ParserError::Field(format!("'{field_name}' = {s:?} is not a number"))
            }),
        (FieldType::Boolean, serde_json::Value::Bool(_)) => Ok(value.clone()),
        (FieldType::Boolean, serde_json::Value::String(s)) => match s.to_lowercase().as_str() {
            "true" | "1" | "yes" => Ok(serde_json::Value::Bool(true)),
            "false" | "0" | "no" => Ok(serde_json::Value::Bool(false)),
            _ => Err(ParserError::Field(format!(
                "'{field_name}' = {s:?} is not a boolean"
            ))),
        },
        (FieldType::Json, _) => Ok(value.clone()),
        (target, value) => Err(ParserError::Field(format!(
            "cannot coerce '{field_name}' from {value:?} to {target:?}"
        ))),
    }
}

fn parse_timestamp(
    value: &serde_json::Value,
    spec: &TimestampSpec,
    field_name: &str,
    ctx: &ParserContext,
) -> Result<Option<Timestamp>, ParserError> {
    let result = match spec.format {
        TimestampFormat::UnixSeconds => value.as_i64().and_then(Timestamp::from_unix_timestamp),
        TimestampFormat::UnixSecondsNanos => {
            // Treat as i64 nanoseconds — the serde-side disambiguation is the field's name
            value
                .as_i64()
                .and_then(|n| Timestamp::from_unix_timestamp_nanos(i128::from(n)))
        }
        TimestampFormat::UnixMillis => value
            .as_i64()
            .and_then(Timestamp::from_unix_timestamp_millis),
        TimestampFormat::UnixMicros => value
            .as_i64()
            .and_then(|us| Timestamp::from_unix_timestamp_nanos(i128::from(us) * 1_000)),
        TimestampFormat::Rfc3339 | TimestampFormat::Iso8601 => value
            .as_str()
            .and_then(|s| Timestamp::parse_rfc3339(s).ok()),
    };

    match result {
        Some(ts) => Ok(Some(ts)),
        None => match spec.fallback {
            TimestampFallback::MaterialTiming => Ok(Some(ctx.acquisition_time)),
            TimestampFallback::Error => Err(ParserError::Field(format!(
                "timestamp field '{field_name}' = {value:?} could not be parsed as {:?}",
                spec.format
            ))),
        },
    }
}

fn value_as_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => String::new(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::Id;

    fn test_ctx() -> ParserContext {
        ParserContext {
            source_unit_id: SourceUnitId::from_static("test.unit"),
            source_material_id: Id::from_uuid(uuid::Uuid::nil()),
            record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
            operation_id: uuid::Uuid::nil(),
            job_id: uuid::Uuid::nil(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    fn json_record(json: &str) -> SourceRecord {
        SourceRecord {
            material_id: Id::from_uuid(uuid::Uuid::nil()),
            anchor: MaterialAnchor::ByteRange {
                start: 0,
                len: json.len() as u64,
            },
            bytes: json.as_bytes().to_vec(),
            logical_path: None,
        }
    }

    fn minimal_spec() -> DeclarativeParserSpec {
        DeclarativeParserSpec {
            parser_id: ParserId::from_static("test-parser"),
            parser_version: "1.0.0".into(),
            source_unit_id: SourceUnitId::from_static("test.unit"),
            event_source: EventSource::from_static("test"),
            event_type: EventType::from_static("test.event"),
            default_privacy_context: ProcessingContext::Metadata,
            input_format: InputFormat::Json,
            fields: vec![],
        }
    }

    #[test]
    fn empty_spec_emits_one_event_with_empty_payload() {
        let intents = DeclarativeParser::evaluate(
            &minimal_spec(),
            json_record("{}"),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].payload, serde_json::json!({}));
        assert_eq!(intents[0].field_privacy_log, Some(vec![]));
    }

    #[test]
    fn json_pointer_extracts_string_field() {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "command".into(),
            source: FieldSource::JsonPointer {
                pointer: "/cmd".into(),
            },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            json_record(r#"{"cmd": "ls -la"}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert_eq!(intents[0].payload["command"], "ls -la");
    }

    #[test]
    fn missing_required_field_errors() {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "cmd".into(),
            source: FieldSource::JsonPointer {
                pointer: "/cmd".into(),
            },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
        });
        let result = DeclarativeParser::evaluate(
            &spec,
            json_record(r#"{}"#),
            &test_ctx(),
            &BindingConfig::default(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn missing_optional_field_uses_default() {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "exit".into(),
            source: FieldSource::JsonPointer {
                pointer: "/exit".into(),
            },
            field_type: FieldType::Integer,
            required: false,
            default: Some(serde_json::json!(0)),
            skip_payload: false,
            privacy_context: None,
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            json_record(r#"{}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert_eq!(intents[0].payload["exit"], 0);
    }

    #[test]
    fn missing_optional_no_default_omits_field() {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "exit".into(),
            source: FieldSource::JsonPointer {
                pointer: "/exit".into(),
            },
            field_type: FieldType::Integer,
            required: false,
            default: None,
            skip_payload: false,
            privacy_context: None,
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            json_record(r#"{}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert!(intents[0].payload.get("exit").is_none());
    }

    #[test]
    fn skip_payload_excludes_from_output() {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "internal".into(),
            source: FieldSource::JsonPointer {
                pointer: "/internal".into(),
            },
            field_type: FieldType::Integer,
            required: true,
            default: None,
            skip_payload: true,
            privacy_context: None,
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            json_record(r#"{"internal": 42}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert!(intents[0].payload.get("internal").is_none());
    }

    #[test]
    fn occurrence_key_concatenates_fields_in_declared_order() {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "session".into(),
            source: FieldSource::JsonPointer {
                pointer: "/session".into(),
            },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            occurrence_key: true,
            timestamp: None,
            suppress_if: None,
        });
        spec.fields.push(FieldSpec {
            name: "id".into(),
            source: FieldSource::JsonPointer { pointer: "/id".into() },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            occurrence_key: true,
            timestamp: None,
            suppress_if: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            json_record(r#"{"session": "abc", "id": "evt-1"}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        let key = intents[0].occurrence_key.as_ref().unwrap();
        assert_eq!(
            key.fields,
            vec![
                ("session".into(), "abc".into()),
                ("id".into(), "evt-1".into())
            ]
        );
    }

    #[test]
    fn suppress_if_field_drops_field_only() {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "command".into(),
            source: FieldSource::JsonPointer { pointer: "/cmd".into() },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: Some(ProcessingContext::Command),
            occurrence_key: false,
            timestamp: None,
            suppress_if: Some(SuppressPredicate {
                binding_field: "private_mode_active".into(),
                whole_event: false,
            }),
        });
        let binding = BindingConfig::new().with_flag("private_mode_active", true);
        let intents = DeclarativeParser::evaluate(
            &spec,
            json_record(r#"{"cmd": "secret"}"#),
            &test_ctx(),
            &binding,
        )
        .unwrap();
        assert!(intents[0].payload.get("command").is_none());
        let log = intents[0].field_privacy_log.as_ref().unwrap();
        assert_eq!(log.len(), 1);
        assert!(log[0].suppressed);
        assert!(!log[0].whole_event_suppressed);
    }

    #[test]
    fn suppress_if_whole_event_drops_event_entirely() {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "command".into(),
            source: FieldSource::JsonPointer { pointer: "/cmd".into() },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: Some(ProcessingContext::Command),
            occurrence_key: false,
            timestamp: None,
            suppress_if: Some(SuppressPredicate {
                binding_field: "private_mode_active".into(),
                whole_event: true,
            }),
        });
        let binding = BindingConfig::new().with_flag("private_mode_active", true);
        let intents = DeclarativeParser::evaluate(
            &spec,
            json_record(r#"{"cmd": "secret"}"#),
            &test_ctx(),
            &binding,
        )
        .unwrap();
        assert_eq!(intents.len(), 0);
    }

    #[test]
    fn suppress_if_inactive_passes_through() {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "command".into(),
            source: FieldSource::JsonPointer { pointer: "/cmd".into() },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: Some(ProcessingContext::Command),
            occurrence_key: false,
            timestamp: None,
            suppress_if: Some(SuppressPredicate {
                binding_field: "private_mode_active".into(),
                whole_event: false,
            }),
        });
        let binding = BindingConfig::new().with_flag("private_mode_active", false);
        let intents = DeclarativeParser::evaluate(
            &spec,
            json_record(r#"{"cmd": "ls"}"#),
            &test_ctx(),
            &binding,
        )
        .unwrap();
        assert_eq!(intents[0].payload["command"], "ls");
    }

    #[test]
    fn type_coercion_string_to_integer_works() {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "exit".into(),
            source: FieldSource::JsonPointer {
                pointer: "/exit".into(),
            },
            field_type: FieldType::Integer,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            json_record(r#"{"exit": "42"}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert_eq!(intents[0].payload["exit"], 42);
    }

    #[test]
    fn type_coercion_string_to_boolean_works() {
        for (input, expected) in [("true", true), ("false", false), ("1", true), ("yes", true)] {
            let mut spec = minimal_spec();
            spec.fields.push(FieldSpec {
                name: "flag".into(),
                source: FieldSource::JsonPointer {
                    pointer: "/flag".into(),
                },
                field_type: FieldType::Boolean,
                required: true,
                default: None,
                skip_payload: false,
                privacy_context: None,
                occurrence_key: false,
                timestamp: None,
                suppress_if: None,
            });
            let json = format!(r#"{{"flag": {input:?}}}"#);
            let intents = DeclarativeParser::evaluate(
                &spec,
                json_record(&json),
                &test_ctx(),
                &BindingConfig::default(),
            )
            .unwrap();
            assert_eq!(intents[0].payload["flag"], expected, "input was {input:?}");
        }
    }

    #[test]
    fn timestamp_rfc3339_parses() {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "ts".into(),
            source: FieldSource::JsonPointer { pointer: "/ts".into() },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            occurrence_key: false,
            timestamp: Some(TimestampSpec {
                format: TimestampFormat::Rfc3339,
                fallback: TimestampFallback::Error,
            }),
            suppress_if: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            json_record(r#"{"ts": "2024-01-15T12:34:56Z"}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert!(matches!(
            &intents[0].timing,
            TimingEvidence::Intrinsic { field, .. } if field == "ts"
        ));
    }

    #[test]
    fn timestamp_unix_seconds_parses() {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "ts".into(),
            source: FieldSource::JsonPointer { pointer: "/ts".into() },
            field_type: FieldType::Integer,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            occurrence_key: false,
            timestamp: Some(TimestampSpec {
                format: TimestampFormat::UnixSeconds,
                fallback: TimestampFallback::Error,
            }),
            suppress_if: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            json_record(r#"{"ts": 1705320896}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert!(matches!(
            &intents[0].timing,
            TimingEvidence::Intrinsic { .. }
        ));
    }

    #[test]
    fn timestamp_invalid_falls_back_to_material_time() {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "ts".into(),
            source: FieldSource::JsonPointer { pointer: "/ts".into() },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            occurrence_key: false,
            timestamp: Some(TimestampSpec {
                format: TimestampFormat::Rfc3339,
                fallback: TimestampFallback::MaterialTiming,
            }),
            suppress_if: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            json_record(r#"{"ts": "not a timestamp"}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert!(matches!(
            &intents[0].timing,
            TimingEvidence::Intrinsic { .. }
        ));
    }

    #[test]
    fn no_timestamp_uses_acquisition_time_with_staged_fallback_evidence() {
        let intents = DeclarativeParser::evaluate(
            &minimal_spec(),
            json_record("{}"),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert!(matches!(intents[0].timing, TimingEvidence::StagedAtFallback));
    }

    #[test]
    fn tab_separated_extracts_by_index() {
        let mut spec = minimal_spec();
        spec.input_format = InputFormat::TabSeparated;
        spec.fields.push(FieldSpec {
            name: "first".into(),
            source: FieldSource::ColumnIndex { index: 0 },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
        });
        spec.fields.push(FieldSpec {
            name: "third".into(),
            source: FieldSource::ColumnIndex { index: 2 },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
        });
        let record = SourceRecord {
            material_id: Id::from_uuid(uuid::Uuid::nil()),
            anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
            bytes: b"alpha\tbeta\tgamma".to_vec(),
            logical_path: None,
        };
        let intents =
            DeclarativeParser::evaluate(&spec, record, &test_ctx(), &BindingConfig::default())
                .unwrap();
        assert_eq!(intents[0].payload["first"], "alpha");
        assert_eq!(intents[0].payload["third"], "gamma");
    }

    #[test]
    fn binding_config_default_is_falsy() {
        let b = BindingConfig::default();
        assert!(!b.is_truthy("anything"));
    }

    #[test]
    fn binding_config_with_flag_is_truthy() {
        let b = BindingConfig::new().with_flag("on", true);
        assert!(b.is_truthy("on"));
        assert!(!b.is_truthy("off"));
    }

    #[test]
    fn record_anchor_passes_through_to_intent() {
        let record = SourceRecord {
            material_id: Id::from_uuid(uuid::Uuid::nil()),
            anchor: MaterialAnchor::SqliteRow {
                table: "history".into(),
                rowid: 42,
            },
            bytes: b"{}".to_vec(),
            logical_path: None,
        };
        let intents = DeclarativeParser::evaluate(
            &minimal_spec(),
            record,
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert!(matches!(
            &intents[0].anchor,
            MaterialAnchor::SqliteRow { table, rowid: 42 } if table == "history"
        ));
    }
}

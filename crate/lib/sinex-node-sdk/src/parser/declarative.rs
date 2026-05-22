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
//! - Per-field privacy via `privacy::process()` (records `FieldPrivacyDecision`)
//! - `#[suppress_if]` predicate (per-field or whole-event)
//! - `#[required]` / `#[default]` / `#[skip]` semantics
//! - `#[occurrence_key]` composite key construction
//! - `#[timestamp]` derivation with rfc3339 / unix-seconds / unix-millis /
//!   unix-micros / unix-nanos formats and material-time fallback
//!
//! # Extension A — discriminator / multi-event-type
//!
//! `DeclarativeParserSpec.discriminator` names one field whose extracted value
//! selects the emitted `(event_source, event_type)` at parse time.  Declared
//! via `#[event_dispatch("value" => "event.type", ...)]` on the discriminator
//! field; `on_unknown` controls what happens when no case matches.
//!
//! Collapses these previously-imperative parsers into declarative:
//! - `fs` (file.created / file.modified / file.deleted / file.moved)
//! - `desktop.activitywatch` (window.active / afk.changed / browser.tab.active)
//! - `system.dbus` — partially (per-interface dispatch still needs a thin wrapper
//!   for multi-field key; the discriminator handles the common cases)
//! - `desktop.window-manager` (via `type>>data` prefix dispatch on the `kind`
//!   field)
//!
//! # Extension F — `carry_across_records` / stateful continuation
//!
//! `StatefulCarryPolicy` lets one field "carry" a value from one record into the
//! next.  Used for zsh extended history (`": timestamp:elapsed;cmd"` prefix
//! line carries its timestamp to the following command line).
//!
//! The `DeclarativeParser` is now a stateful object (`StatefulDeclarativeParser`)
//! when carry fields are present.  For purely stateless specs the
//! `DeclarativeParser::evaluate` free function still works.
//!
//! # Deferred (Phase 1A v2)
//!
//! - `#[anchor(kind = "...")]` override — for v1 we pass through the
//!   adapter's anchor on the source record. Adapters that need a derived
//!   anchor must compute it themselves before yielding the `SourceRecord`.
//! - Regex captures for line logs (use `raw_line` + a thin imperative
//!   wrapper for now)
//! - `#[redact_if(rule = "...")]` named rule references
//! - Multi-line record continuation (backslash-continuation in zsh history)
//!   is NOT handled by `carry_across_records` — that requires an adapter-level
//!   record assembler. `carry_across_records` handles only cross-record state
//!   propagation for records that are individually complete lines.

use serde::{Deserialize, Serialize};
use sinex_primitives::Timestamp;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    OccurrenceKey, ParsedEventIntent, ParserContext, ParserId, SourceRecord, SourceUnitId,
    TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::{self, FieldPrivacyDecision, ProcessingContext};
use std::borrow::Cow;
use std::collections::BTreeMap;

use crate::parser::ParserError;

// =============================================================================
// Spec types — the data the macro / YAML loader produces
// =============================================================================

/// Static description of a declarative parser.
///
/// Built once per parser at compile time (via macro) or load time (via YAML).
/// Consumed by [`DeclarativeParser::evaluate`] and [`StatefulDeclarativeParser`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeclarativeParserSpec {
    pub parser_id: ParserId,
    pub parser_version: String,
    pub source_unit_id: SourceUnitId,
    pub event_source: EventSource,
    /// Default event type — used when no discriminator matches (or no discriminator is present).
    pub event_type: EventType,
    pub default_privacy_context: ProcessingContext,
    pub input_format: InputFormat,
    pub fields: Vec<FieldSpec>,

    // --- Extension A: discriminator / multi-event-type dispatch ---
    /// If `Some`, one field's extracted value selects the emitted
    /// `(event_source, event_type)` at parse time.  See [`Discriminator`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discriminator: Option<Discriminator>,
}

impl DeclarativeParserSpec {
    /// Return the input-shape keys that this parser requires to be present.
    ///
    /// The returned keys use the same vocabulary as
    /// [`SourceRecordFingerprint`](crate::parser::SourceRecordFingerprint):
    /// JSON Pointer paths for JSON records, column names for named tabular
    /// records, and `column_N` for positional columns without a declared
    /// header. Raw-line fields do not produce a structural key because the
    /// fingerprint for opaque line material has no removable field shape.
    #[must_use]
    pub fn required_input_keys(&self) -> Vec<String> {
        let mut keys = self
            .fields
            .iter()
            .filter(|field| field.required)
            .filter_map(FieldSpec::input_shape_key)
            .collect::<Vec<_>>();
        keys.sort();
        keys.dedup();
        keys
    }
}

// =============================================================================
// Extension A — Discriminator spec
// =============================================================================

/// Discriminator dispatch: read a field value, look it up in `cases`,
/// override the emitted event type (and optionally event source).
///
/// Built by `#[event_dispatch("value" => "event.type", ...)]` on the
/// field that holds the discriminator value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Discriminator {
    /// Which field holds the discriminator value.  Must match a field name in
    /// the parent spec's `fields` vec.  The field is extracted before dispatch.
    pub field: String,

    /// Ordered mapping from discriminator value to event-type override.
    /// First matching case wins.
    pub cases: Vec<DiscriminatorCase>,

    /// What to do when no case matches.
    #[serde(default)]
    pub on_unknown: DiscriminatorFallback,
}

/// One entry in the discriminator case table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscriminatorCase {
    /// String value extracted from the discriminator field.
    pub value: String,
    /// Event type to emit for this case.
    pub event_type: EventType,
    /// Optional event-source override.  When `None`, the spec's `event_source`
    /// is used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_source: Option<EventSource>,
}

/// What [`DeclarativeParser`] does when the discriminator field value does not
/// match any [`DiscriminatorCase`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscriminatorFallback {
    /// Skip the record silently (emit no events).
    SkipRecord,
    /// Fail the record with a [`ParserError::Field`].
    Error,
    /// Use the spec's top-level `event_type` / `event_source` (the default).
    #[default]
    Default,
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
    /// `SQLite` row already deserialized into a JSON object — extract via column name.
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

    // --- Extension F: stateful carry across records ---
    /// If `Some`, this field participates in stateful carry-across-records.
    /// The semantics depend on [`StatefulCarryPolicy`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub carry: Option<CarrySpec>,
}

impl FieldSpec {
    /// Return this field's source-record structural key, if it has one.
    #[must_use]
    pub fn input_shape_key(&self) -> Option<String> {
        match &self.source {
            FieldSource::JsonPointer { pointer } => Some(pointer.clone()),
            FieldSource::ColumnIndex { index } => Some(format!("column_{index}")),
            FieldSource::ColumnName { name } => Some(name.clone()),
            FieldSource::RawLine => None,
        }
    }
}

// =============================================================================
// Extension F — Carry-across-records spec
// =============================================================================

/// Specifies how a field participates in stateful carry-across-records parsing.
///
/// Two roles:
/// - **Producer** (`policy = SetThenConsume | SetThenRetain`): when this field
///   is present in a record, its value is stored in the parser's carry-state map
///   under `self.name`.  The value is then available to consumer fields in
///   subsequent records.
/// - **Consumer** (`policy = ConsumeCarried`): on each record, if the named
///   `from_carry` field exists in carry-state, inject its value as this field's
///   value.  If `clear_on_use = true`, the carry-state entry is cleared after
///   injection (single-use).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CarrySpec {
    pub policy: StatefulCarryPolicy,
    /// For `ConsumeCarried`: which carry-state key to pull from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_carry: Option<String>,
    /// For `ConsumeCarried`: clear the carry-state entry after use.
    #[serde(default)]
    pub clear_on_use: bool,
}

/// Policy for `carry_across_records` participation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatefulCarryPolicy {
    /// When this field is found in the current record, store its value in
    /// carry-state and clear it on the next record that consumes it.
    SetThenConsume,
    /// Store value in carry-state; keep it alive across multiple records until
    /// explicitly overwritten.
    SetThenRetain,
    /// Pull value from carry-state (named by `from_carry`), not from the record
    /// bytes.  If `clear_on_use`, remove from state after injection.
    ConsumeCarried,
}

/// Where to read a field value from in the source record.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FieldSource {
    /// JSON Pointer (RFC 6901), e.g. `"/command"`.
    JsonPointer { pointer: String },
    /// Tab/CSV column by 0-based index.
    ColumnIndex { index: usize },
    /// Column by name (CSV header or `SQLite` column).
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
#[derive(Default)]
pub enum TimestampFallback {
    /// Fall back to the material's acquisition time (default).
    #[default]
    MaterialTiming,
    /// Fail the record.
    Error,
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
///
/// For specs that use `carry_across_records` (Extension F), use
/// [`StatefulDeclarativeParser`] instead — it maintains carry-state across
/// `evaluate_stateful()` calls.
pub struct DeclarativeParser;

impl DeclarativeParser {
    /// Evaluate a record against a spec, producing zero or more event intents.
    ///
    /// Returns `Ok(vec![])` (zero events) if the entire event was suppressed
    /// by a `whole_event` predicate or by a discriminator `skip_record` case.
    /// Returns `Err` if a required field was missing or a type conversion failed.
    ///
    /// For specs with carry fields, pass `carry_state = &mut BTreeMap::new()`
    /// and hold the map across calls, or use [`StatefulDeclarativeParser`].
    pub fn evaluate(
        spec: &DeclarativeParserSpec,
        record: SourceRecord,
        ctx: &ParserContext,
        binding: &BindingConfig,
    ) -> Result<Vec<ParsedEventIntent>, ParserError> {
        let mut carry_state = BTreeMap::new();
        evaluate_inner(spec, record, ctx, binding, &mut carry_state)
    }
}

// =============================================================================
// Stateful evaluator (Extension F — carry_across_records)
// =============================================================================

/// Stateful wrapper around [`DeclarativeParser::evaluate`] that persists
/// carry-state between records.
///
/// Use this when the spec contains fields with `carry` policies
/// ([`StatefulCarryPolicy`]).  For purely stateless specs, the free function
/// [`DeclarativeParser::evaluate`] is equivalent and cheaper.
///
/// # Example
///
/// ```rust,ignore
/// let mut parser = StatefulDeclarativeParser::new(spec.clone());
/// for record in records {
///     let intents = parser.evaluate(record, &ctx, &binding)?;
///     // handle intents
/// }
/// ```
pub struct StatefulDeclarativeParser {
    spec: DeclarativeParserSpec,
    /// Carry-state: field name → last produced value.
    carry_state: BTreeMap<String, serde_json::Value>,
}

impl StatefulDeclarativeParser {
    #[must_use]
    pub fn new(spec: DeclarativeParserSpec) -> Self {
        Self {
            spec,
            carry_state: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn spec(&self) -> &DeclarativeParserSpec {
        &self.spec
    }

    /// Evaluate one record, threading carry-state.
    pub fn evaluate(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
        binding: &BindingConfig,
    ) -> Result<Vec<ParsedEventIntent>, ParserError> {
        evaluate_inner(&self.spec, record, ctx, binding, &mut self.carry_state)
    }

    /// Reset carry-state (e.g. after a checkpoint restore).
    pub fn reset_carry_state(&mut self) {
        self.carry_state.clear();
    }
}

// =============================================================================
// Shared inner evaluator
// =============================================================================

fn evaluate_inner(
    spec: &DeclarativeParserSpec,
    record: SourceRecord,
    ctx: &ParserContext,
    binding: &BindingConfig,
    carry_state: &mut BTreeMap<String, serde_json::Value>,
) -> Result<Vec<ParsedEventIntent>, ParserError> {
    let decoded = decode_record(spec.input_format, &record)?;

    let mut payload = serde_json::Map::new();
    let mut field_privacy_log = Vec::new();
    let mut occurrence_fields: Vec<(String, String)> = Vec::new();
    let mut ts_override: Option<(Timestamp, String)> = None;
    let mut whole_event_suppressed = false;
    // Value of the discriminator field, collected during field iteration.
    let mut discriminator_value: Option<String> = None;

    for field in &spec.fields {
        // --- Extension F: ConsumeCarried — inject from carry-state instead of record ---
        let raw_value = if let Some(carry) = &field.carry {
            if carry.policy == StatefulCarryPolicy::ConsumeCarried {
                let key = carry.from_carry.as_deref().unwrap_or(&field.name);
                let carried = carry_state.get(key).cloned();
                if carry.clear_on_use {
                    carry_state.remove(key);
                }
                carried
            } else {
                extract_field(&decoded, &field.source, spec.input_format)?
            }
        } else {
            extract_field(&decoded, &field.source, spec.input_format)?
        };

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

        // --- Extension F: producer — store in carry-state ---
        if let Some(carry) = &field.carry {
            match carry.policy {
                StatefulCarryPolicy::SetThenConsume | StatefulCarryPolicy::SetThenRetain => {
                    carry_state.insert(field.name.clone(), coerced.clone());
                }
                StatefulCarryPolicy::ConsumeCarried => {}
            }
        }

        // --- Extension A: collect discriminator value ---
        if let Some(disc) = &spec.discriminator
            && disc.field == field.name
        {
            discriminator_value = Some(value_as_string(&coerced));
        }

        let suppressed_by_predicate = match &field.suppress_if {
            Some(pred) => binding.is_truthy(&pred.binding_field),
            None => false,
        };

        // Privacy processing for fields with a declared context.
        let final_value = if let Some(ctx_priv) = field.privacy_context {
            if suppressed_by_predicate {
                let mut decision =
                    FieldPrivacyDecision::suppressed_by_predicate(&field.name, ctx_priv);
                if let Some(pred) = &field.suppress_if
                    && pred.whole_event
                {
                    decision = decision.into_whole_event_suppressor();
                    whole_event_suppressed = true;
                }
                field_privacy_log.push(decision);
                None
            } else {
                let value_str = value_as_string(&coerced);
                let processed = privacy::process(&value_str, ctx_priv)
                    .map_err(|e| ParserError::Privacy(e.to_string()))?;
                let decision =
                    FieldPrivacyDecision::from_processed(&field.name, ctx_priv, &processed);
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
            if let Some(pred) = &field.suppress_if
                && pred.whole_event
            {
                whole_event_suppressed = true;
            }
            None
        } else {
            Some(coerced.clone())
        };

        // Timestamp derivation.
        if let Some(ts_spec) = &field.timestamp
            && let Some(ts) = parse_timestamp(&coerced, ts_spec, &field.name, ctx)?
        {
            ts_override = Some((ts, field.name.clone()));
        }

        // Occurrence key contribution.
        if field.occurrence_key {
            occurrence_fields.push((field.name.clone(), value_as_string(&coerced)));
        }

        // Add to payload unless skipped or suppressed.
        if !field.skip_payload
            && let Some(v) = final_value
        {
            payload.insert(field.name.clone(), v);
        }
    }

    if whole_event_suppressed {
        return Ok(vec![]);
    }

    // --- Extension A: discriminator dispatch ---
    let (resolved_event_type, resolved_event_source) = if let Some(disc) = &spec.discriminator {
        match discriminator_value {
            Some(ref val) => match disc.cases.iter().find(|c| &c.value == val) {
                Some(case) => (
                    case.event_type.clone(),
                    case.event_source
                        .clone()
                        .unwrap_or_else(|| spec.event_source.clone()),
                ),
                None => match disc.on_unknown {
                    DiscriminatorFallback::SkipRecord => return Ok(vec![]),
                    DiscriminatorFallback::Error => {
                        return Err(ParserError::Field(format!(
                            "discriminator field '{}' = {:?} matched no case",
                            disc.field, val
                        )));
                    }
                    DiscriminatorFallback::Default => {
                        (spec.event_type.clone(), spec.event_source.clone())
                    }
                },
            },
            None => {
                // Discriminator field was absent.
                match disc.on_unknown {
                    DiscriminatorFallback::SkipRecord => return Ok(vec![]),
                    DiscriminatorFallback::Error => {
                        return Err(ParserError::Field(format!(
                            "discriminator field '{}' was missing from record",
                            disc.field
                        )));
                    }
                    DiscriminatorFallback::Default => {
                        (spec.event_type.clone(), spec.event_source.clone())
                    }
                }
            }
        }
    } else {
        (spec.event_type.clone(), spec.event_source.clone())
    };

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

    Ok(vec![
        ParsedEventIntent::builder()
            .source_unit_id(ctx.source_unit_id.clone())
            .parser_id(spec.parser_id.clone())
            .parser_version(spec.parser_version.clone())
            .event_type(resolved_event_type)
            .event_source(resolved_event_source)
            .payload(serde_json::Value::Object(payload))
            .ts_orig(ts_orig)
            .timing(timing)
            .anchor(record.anchor.clone())
            .maybe_occurrence_key(occurrence_key)
            .privacy_context(spec.default_privacy_context)
            .field_privacy_log(field_privacy_log)
            .build(),
    ])
}

// =============================================================================
// Internal helpers
// =============================================================================

enum DecodedRecord {
    Json(serde_json::Value),
    TabFields(Vec<String>),
    Line(String),
}

fn decode_record(format: InputFormat, record: &SourceRecord) -> Result<DecodedRecord, ParserError> {
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
            text.split('\t')
                .map(std::string::ToString::to_string)
                .collect(),
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
            .map_err(|_| ParserError::Field(format!("'{field_name}' = {s:?} is not an integer"))),
        (FieldType::Integer, serde_json::Value::Number(n)) if n.is_f64() => n
            .as_f64()
            .filter(|f| f.fract() == 0.0)
            .and_then(|f| {
                let i = f as i64;
                serde_json::Number::from_f64(i as f64)
                    .map(|_| serde_json::Value::Number(serde_json::Number::from(i)))
            })
            .ok_or_else(|| ParserError::Field(format!("'{field_name}' = {n:?} is not an integer"))),
        (FieldType::Number, serde_json::Value::Number(_)) => Ok(value.clone()),
        (FieldType::Number, serde_json::Value::String(s)) => s
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(serde_json::Value::Number)
            .ok_or_else(|| ParserError::Field(format!("'{field_name}' = {s:?} is not a number"))),
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
    use sinex_primitives::parser::MaterialAnchor;
    use xtask::sandbox::prelude::sinex_test;

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
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
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
            discriminator: None,
        }
    }

    #[sinex_test]
    async fn required_input_keys_follow_declared_field_sources() -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields = vec![
            FieldSpec {
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
                carry: None,
            },
            FieldSpec {
                name: "optional".into(),
                source: FieldSource::JsonPointer {
                    pointer: "/optional".into(),
                },
                field_type: FieldType::String,
                required: false,
                default: None,
                skip_payload: false,
                privacy_context: None,
                occurrence_key: false,
                timestamp: None,
                suppress_if: None,
                carry: None,
            },
            FieldSpec {
                name: "line".into(),
                source: FieldSource::RawLine,
                field_type: FieldType::String,
                required: true,
                default: None,
                skip_payload: false,
                privacy_context: None,
                occurrence_key: false,
                timestamp: None,
                suppress_if: None,
                carry: None,
            },
        ];

        assert_eq!(spec.required_input_keys(), vec!["/cmd"]);
        Ok(())
    }

    #[sinex_test]
    async fn positional_required_input_key_uses_fingerprint_column_name()
    -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.input_format = InputFormat::TabSeparated;
        spec.fields.push(FieldSpec {
            name: "command".into(),
            source: FieldSource::ColumnIndex { index: 2 },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
            carry: None,
        });

        assert_eq!(spec.required_input_keys(), vec!["column_2"]);
        Ok(())
    }

    #[sinex_test]
    async fn empty_spec_emits_one_event_with_empty_payload() -> xtask::sandbox::TestResult<()> {
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
        Ok(())
    }

    #[sinex_test]
    async fn json_pointer_extracts_string_field() -> xtask::sandbox::TestResult<()> {
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
            carry: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            json_record(r#"{"cmd": "ls -la"}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert_eq!(intents[0].payload["command"], "ls -la");
        Ok(())
    }

    #[sinex_test]
    async fn missing_required_field_errors() -> xtask::sandbox::TestResult<()> {
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
            carry: None,
        });
        let result = DeclarativeParser::evaluate(
            &spec,
            json_record(r"{}"),
            &test_ctx(),
            &BindingConfig::default(),
        );
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    async fn missing_optional_field_uses_default() -> xtask::sandbox::TestResult<()> {
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
            carry: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            json_record(r"{}"),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert_eq!(intents[0].payload["exit"], 0);
        Ok(())
    }

    #[sinex_test]
    async fn missing_optional_no_default_omits_field() -> xtask::sandbox::TestResult<()> {
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
            carry: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            json_record(r"{}"),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert!(intents[0].payload.get("exit").is_none());
        Ok(())
    }

    #[sinex_test]
    async fn skip_payload_excludes_from_output() -> xtask::sandbox::TestResult<()> {
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
            carry: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            json_record(r#"{"internal": 42}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert!(intents[0].payload.get("internal").is_none());
        Ok(())
    }

    #[sinex_test]
    async fn occurrence_key_concatenates_fields_in_declared_order() -> xtask::sandbox::TestResult<()>
    {
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
            carry: None,
        });
        spec.fields.push(FieldSpec {
            name: "id".into(),
            source: FieldSource::JsonPointer {
                pointer: "/id".into(),
            },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            occurrence_key: true,
            timestamp: None,
            suppress_if: None,
            carry: None,
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
        Ok(())
    }

    #[sinex_test]
    async fn suppress_if_field_drops_field_only() -> xtask::sandbox::TestResult<()> {
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
            privacy_context: Some(ProcessingContext::Command),
            occurrence_key: false,
            timestamp: None,
            suppress_if: Some(SuppressPredicate {
                binding_field: "private_mode_active".into(),
                whole_event: false,
            }),
            carry: None,
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
        Ok(())
    }

    #[sinex_test]
    async fn suppress_if_whole_event_drops_event_entirely() -> xtask::sandbox::TestResult<()> {
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
            privacy_context: Some(ProcessingContext::Command),
            occurrence_key: false,
            timestamp: None,
            suppress_if: Some(SuppressPredicate {
                binding_field: "private_mode_active".into(),
                whole_event: true,
            }),
            carry: None,
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
        Ok(())
    }

    #[sinex_test]
    async fn suppress_if_inactive_passes_through() -> xtask::sandbox::TestResult<()> {
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
            privacy_context: Some(ProcessingContext::Command),
            occurrence_key: false,
            timestamp: None,
            suppress_if: Some(SuppressPredicate {
                binding_field: "private_mode_active".into(),
                whole_event: false,
            }),
            carry: None,
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
        Ok(())
    }

    #[sinex_test]
    async fn type_coercion_string_to_integer_works() -> xtask::sandbox::TestResult<()> {
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
            carry: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            json_record(r#"{"exit": "42"}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert_eq!(intents[0].payload["exit"], 42);
        Ok(())
    }

    #[sinex_test]
    async fn type_coercion_string_to_boolean_works() -> xtask::sandbox::TestResult<()> {
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
                carry: None,
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
        Ok(())
    }

    #[sinex_test]
    async fn timestamp_rfc3339_parses() -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "ts".into(),
            source: FieldSource::JsonPointer {
                pointer: "/ts".into(),
            },
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
            carry: None,
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
        Ok(())
    }

    #[sinex_test]
    async fn timestamp_unix_seconds_parses() -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "ts".into(),
            source: FieldSource::JsonPointer {
                pointer: "/ts".into(),
            },
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
            carry: None,
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
        Ok(())
    }

    #[sinex_test]
    async fn timestamp_invalid_falls_back_to_material_time() -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "ts".into(),
            source: FieldSource::JsonPointer {
                pointer: "/ts".into(),
            },
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
            carry: None,
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
        Ok(())
    }

    #[sinex_test]
    async fn no_timestamp_uses_acquisition_time_with_staged_fallback_evidence()
    -> xtask::sandbox::TestResult<()> {
        let intents = DeclarativeParser::evaluate(
            &minimal_spec(),
            json_record("{}"),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert!(matches!(
            intents[0].timing,
            TimingEvidence::StagedAtFallback
        ));
        Ok(())
    }

    #[sinex_test]
    async fn tab_separated_extracts_by_index() -> xtask::sandbox::TestResult<()> {
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
            carry: None,
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
            carry: None,
        });
        let record = SourceRecord {
            material_id: Id::from_uuid(uuid::Uuid::nil()),
            anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
            bytes: b"alpha\tbeta\tgamma".to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        };
        let intents =
            DeclarativeParser::evaluate(&spec, record, &test_ctx(), &BindingConfig::default())
                .unwrap();
        assert_eq!(intents[0].payload["first"], "alpha");
        assert_eq!(intents[0].payload["third"], "gamma");
        Ok(())
    }

    #[sinex_test]
    async fn binding_config_default_is_falsy() -> xtask::sandbox::TestResult<()> {
        let b = BindingConfig::default();
        assert!(!b.is_truthy("anything"));
        Ok(())
    }

    #[sinex_test]
    async fn binding_config_with_flag_is_truthy() -> xtask::sandbox::TestResult<()> {
        let b = BindingConfig::new().with_flag("on", true);
        assert!(b.is_truthy("on"));
        assert!(!b.is_truthy("off"));
        Ok(())
    }

    #[sinex_test]
    async fn record_anchor_passes_through_to_intent() -> xtask::sandbox::TestResult<()> {
        let record = SourceRecord {
            material_id: Id::from_uuid(uuid::Uuid::nil()),
            anchor: MaterialAnchor::SqliteRow {
                table: "history".into(),
                rowid: 42,
            },
            bytes: b"{}".to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
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
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Coverage gaps filled (#1100 substrate hardening)
    // -----------------------------------------------------------------------

    #[sinex_test]
    async fn timestamp_invalid_with_error_fallback_rejects_record() -> xtask::sandbox::TestResult<()>
    {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "ts".into(),
            source: FieldSource::JsonPointer {
                pointer: "/ts".into(),
            },
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
            carry: None,
        });
        let result = DeclarativeParser::evaluate(
            &spec,
            json_record(r#"{"ts": "not-a-real-date"}"#),
            &test_ctx(),
            &BindingConfig::default(),
        );
        assert!(matches!(result, Err(ParserError::Field(_))));
        Ok(())
    }

    #[sinex_test]
    async fn timestamp_unix_millis_distinguishable_from_seconds() -> xtask::sandbox::TestResult<()>
    {
        // Same numeric input under millis vs seconds yields different timestamps.
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "ts".into(),
            source: FieldSource::JsonPointer {
                pointer: "/ts".into(),
            },
            field_type: FieldType::Integer,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            occurrence_key: false,
            timestamp: Some(TimestampSpec {
                format: TimestampFormat::UnixMillis,
                fallback: TimestampFallback::Error,
            }),
            suppress_if: None,
            carry: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            // 1_700_000_000_000 ms = 2023-11-14T22:13:20Z
            json_record(r#"{"ts": 1700000000000}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert!(matches!(
            &intents[0].timing,
            TimingEvidence::Intrinsic { .. }
        ));
        let expected = Timestamp::from_unix_timestamp_millis(1_700_000_000_000).unwrap();
        assert_eq!(intents[0].ts_orig, expected);
        Ok(())
    }

    #[sinex_test]
    async fn timestamp_unix_micros_parses() -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "ts".into(),
            source: FieldSource::JsonPointer {
                pointer: "/ts".into(),
            },
            field_type: FieldType::Integer,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            occurrence_key: false,
            timestamp: Some(TimestampSpec {
                format: TimestampFormat::UnixMicros,
                fallback: TimestampFallback::Error,
            }),
            suppress_if: None,
            carry: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            json_record(r#"{"ts": 1700000000000000}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert!(matches!(
            intents[0].timing,
            TimingEvidence::Intrinsic { .. }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn coerce_non_integer_string_errors() -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "count".into(),
            source: FieldSource::JsonPointer {
                pointer: "/count".into(),
            },
            field_type: FieldType::Integer,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
            carry: None,
        });
        let result = DeclarativeParser::evaluate(
            &spec,
            json_record(r#"{"count": "not-a-number"}"#),
            &test_ctx(),
            &BindingConfig::default(),
        );
        assert!(matches!(result, Err(ParserError::Field(_))));
        Ok(())
    }

    #[sinex_test]
    async fn coerce_float_with_fraction_errors_for_integer() -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "n".into(),
            source: FieldSource::JsonPointer {
                pointer: "/n".into(),
            },
            field_type: FieldType::Integer,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
            carry: None,
        });
        // 3.14 must error for FieldType::Integer.
        let err = DeclarativeParser::evaluate(
            &spec,
            json_record(r#"{"n": 3.14}"#),
            &test_ctx(),
            &BindingConfig::default(),
        );
        assert!(matches!(err, Err(ParserError::Field(_))));
        // 3.0 must coerce to integer 3.
        let ok = DeclarativeParser::evaluate(
            &spec,
            json_record(r#"{"n": 3.0}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert_eq!(ok[0].payload["n"], 3);
        Ok(())
    }

    #[sinex_test]
    async fn invalid_utf8_record_errors_with_decode_variant() -> xtask::sandbox::TestResult<()> {
        let spec = minimal_spec();
        let record = SourceRecord {
            material_id: Id::from_uuid(uuid::Uuid::nil()),
            anchor: MaterialAnchor::ByteRange { start: 0, len: 2 },
            bytes: vec![0xFF, 0xFE], // not valid UTF-8
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        };
        let result =
            DeclarativeParser::evaluate(&spec, record, &test_ctx(), &BindingConfig::default());
        assert!(matches!(result, Err(ParserError::Decode(_))));
        Ok(())
    }

    #[sinex_test]
    async fn suppress_if_whole_event_without_privacy_context_drops_event()
    -> xtask::sandbox::TestResult<()> {
        // Cover the `else if suppressed_by_predicate` branch: no privacy_context
        // but whole_event = true. Must produce zero intents.
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "secret".into(),
            source: FieldSource::JsonPointer {
                pointer: "/secret".into(),
            },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            occurrence_key: false,
            timestamp: None,
            suppress_if: Some(SuppressPredicate {
                binding_field: "private_mode".into(),
                whole_event: true,
            }),
            carry: None,
        });
        let binding = BindingConfig::new().with_flag("private_mode", true);
        let intents = DeclarativeParser::evaluate(
            &spec,
            json_record(r#"{"secret": "x"}"#),
            &test_ctx(),
            &binding,
        )
        .unwrap();
        assert!(
            intents.is_empty(),
            "whole_event suppression must yield no intents"
        );
        Ok(())
    }

    #[sinex_test]
    async fn mismatched_source_format_returns_field_error() -> xtask::sandbox::TestResult<()> {
        // TabSeparated input with a JsonPointer source should fail with a clear
        // "incompatible" error, not silently produce an empty value.
        let mut spec = minimal_spec();
        spec.input_format = InputFormat::TabSeparated;
        spec.fields.push(FieldSpec {
            name: "f".into(),
            source: FieldSource::JsonPointer {
                pointer: "/x".into(),
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
        });
        let record = SourceRecord {
            material_id: Id::from_uuid(uuid::Uuid::nil()),
            anchor: MaterialAnchor::Line {
                byte_start: 0,
                line: 1,
            },
            bytes: b"a\tb\tc".to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        };
        let result =
            DeclarativeParser::evaluate(&spec, record, &test_ctx(), &BindingConfig::default());
        assert!(matches!(result, Err(ParserError::Field(_))));
        Ok(())
    }

    #[sinex_test]
    async fn occurrence_key_with_skip_payload_contributes_key_but_not_payload()
    -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "rowid".into(),
            source: FieldSource::JsonPointer {
                pointer: "/rowid".into(),
            },
            field_type: FieldType::Integer,
            required: true,
            default: None,
            skip_payload: true,
            privacy_context: None,
            occurrence_key: true,
            timestamp: None,
            suppress_if: None,
            carry: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            json_record(r#"{"rowid": 7}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert!(intents[0].payload.get("rowid").is_none());
        let key = intents[0].occurrence_key.as_ref().expect("occurrence_key");
        assert_eq!(key.fields, vec![("rowid".into(), "7".into())]);
        Ok(())
    }

    #[sinex_test]
    async fn default_value_is_type_coerced_into_payload() -> xtask::sandbox::TestResult<()> {
        // A string-typed field with a numeric default should arrive in the
        // payload as a *string*, because coerce_field runs on the default.
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "label".into(),
            source: FieldSource::JsonPointer {
                pointer: "/label".into(),
            },
            field_type: FieldType::String,
            required: false,
            default: Some(serde_json::json!(42)),
            skip_payload: false,
            privacy_context: None,
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
            carry: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            json_record(r"{}"), // label missing
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert_eq!(intents[0].payload["label"], "42");
        Ok(())
    }

    #[sinex_test]
    async fn csv_row_uses_column_name_extraction() -> xtask::sandbox::TestResult<()> {
        // CsvRow decodes bytes as JSON object; ColumnName extracts by key.
        let mut spec = minimal_spec();
        spec.input_format = InputFormat::CsvRow;
        spec.fields.push(FieldSpec {
            name: "col".into(),
            source: FieldSource::ColumnName { name: "col".into() },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
            carry: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            json_record(r#"{"col": "val"}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert_eq!(intents[0].payload["col"], "val");
        Ok(())
    }
}

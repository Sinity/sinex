//! Declarative parser substrate (#1100).
//!
//! Both `#[derive(SourceRecord)]` (compile-time, in `sinex-macros`) and the
//! YAML loader (runtime, see [`yaml_loader`](super::yaml_loader)) compile into
//! a [`DeclarativeParserSpec`]. The [`DeclarativeParser::evaluate`] method
//! takes a spec + a [`SourceRecord`] and produces zero or more
//! [`ParsedEventIntent`] values — the same code path regardless of how the
//! spec was authored.
//!
//! See `crate/sinexd/docs/declarative_parser.md` for the locked
//! design and the macro attribute catalog.
//!
//! # v1 scope (this file)
//!
//! - JSON / tab-separated / SQLite-row / CSV-row / raw-line input formats
//! - Field extraction via JSON Pointer, column index, column name, raw line
//! - Type coercion for string, integer, number, boolean, JSON
//! - Field-level privacy context hints for the event-engine policy layer
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

use crate::Timestamp;
use crate::domain::{EventSource, EventType};
use crate::parser::{
    BindingConfig, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId, SourceId,
    SourceRecord, TimingConfidence, TimingEvidence,
};
use crate::privacy::{ProcessingContext, SensitivityHint};
use serde::{Deserialize, Serialize};
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
    pub source_id: SourceId,
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

    /// Privacy context hint for downstream DB/user policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub privacy_context: Option<ProcessingContext>,

    /// Semantic sensitivity-class hints exported for DB/user policy tooling.
    /// Never auto-acts (#1611).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sensitivity: Vec<SensitivityHint>,

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

    // --- Extension G: validation / normalization hooks (#1750) ---
    /// If `Some`, a normalization transform applied to the coerced value before
    /// it contributes to the payload, occurrence key, or timestamp. See
    /// [`FieldTransform`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transform: Option<FieldTransform>,

    /// If `Some`, a range / plausibility validator applied to the
    /// (post-transform) value. A failed validator rejects the whole record with
    /// a [`ParserError::Field`]. See [`FieldValidator`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validate: Option<FieldValidator>,
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

// =============================================================================
// Extension G — validation / normalization hooks (#1750)
// =============================================================================

/// A declarative normalization transform applied to a field's coerced value.
///
/// V1 is a deliberately small, named-builtin set (not a general transform
/// language). It exists to recover normalizations that imperative parsers
/// performed at the field boundary and that the `SourceDefinition` migration
/// otherwise dropped (#1750, #1727).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FieldTransform {
    /// Keep the substring before the first occurrence of `separator`.
    ///
    /// This is the Atuin `host:user` → `host` normalization
    /// (`split_once(separator)`, first segment). A no-op when the separator is
    /// absent. Requires a string-typed value.
    SplitFirst { separator: String },
}

/// A declarative range / plausibility validator applied to a field's
/// (post-transform) value. A failure rejects the whole record.
///
/// V1 is a deliberately small, named-builtin set (not an expression language):
/// it recovers the bounds checks imperative parsers performed (#1750, #1727).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FieldValidator {
    /// Integer value must fall within `[min, max]` (inclusive). Use for
    /// arbitrary numeric bounds.
    IntRange { min: i64, max: i64 },
    /// Integer value must fit in an `i32` (`i32::MIN..=i32::MAX`). The
    /// narrowing check imperative parsers applied to exit codes and similar.
    /// Equivalent to [`FieldValidator::IntRange`] with the `i32` bounds.
    I32,
    /// Integer nanosecond value must lie in the representable [`Timestamp`]
    /// range (i.e. `Timestamp::from_unix_timestamp_nanos` succeeds). The
    /// timestamp range check the imperative Atuin parser performed.
    TimestampNanos,
    /// String value must be non-empty after trimming surrounding whitespace.
    NonEmptyString,
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
        record: &SourceRecord,
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
        record: &SourceRecord,
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
    record: &SourceRecord,
    ctx: &ParserContext,
    binding: &BindingConfig,
    carry_state: &mut BTreeMap<String, serde_json::Value>,
) -> Result<Vec<ParsedEventIntent>, ParserError> {
    let decoded = decode_record(spec.input_format, record)?;

    let mut payload = serde_json::Map::new();
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

        // --- Extension G: normalization transform, then validation (#1750) ---
        let coerced = apply_transform(coerced, field.transform.as_ref(), &field.name)?;
        apply_validator(&coerced, field.validate.as_ref(), &field.name)?;

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

        // Privacy context is declarative metadata only. DB/user policy is
        // applied later at the event-engine chokepoint.
        let final_value = if suppressed_by_predicate {
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
            source_id: ctx.source_id.clone(),
            fields: occurrence_fields,
        })
    };

    Ok(vec![
        ParsedEventIntent::builder()
            .source_id(ctx.source_id.clone())
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

/// Apply a [`FieldTransform`] to a coerced value (#1750). Returns the value
/// unchanged when no transform is declared.
fn apply_transform(
    value: serde_json::Value,
    transform: Option<&FieldTransform>,
    field_name: &str,
) -> Result<serde_json::Value, ParserError> {
    let Some(transform) = transform else {
        return Ok(value);
    };
    match transform {
        FieldTransform::SplitFirst { separator } => match &value {
            serde_json::Value::String(s) => {
                let first = s
                    .split_once(separator.as_str())
                    .map_or(s.as_str(), |(head, _)| head);
                Ok(serde_json::Value::String(first.to_string()))
            }
            other => Err(ParserError::Field(format!(
                "transform split_first on '{field_name}' requires a string value, got {other:?}"
            ))),
        },
    }
}

/// Apply a [`FieldValidator`] to a (post-transform) value (#1750). A failure
/// rejects the whole record. Returns `Ok(())` when no validator is declared.
fn apply_validator(
    value: &serde_json::Value,
    validator: Option<&FieldValidator>,
    field_name: &str,
) -> Result<(), ParserError> {
    let Some(validator) = validator else {
        return Ok(());
    };
    match validator {
        FieldValidator::IntRange { min, max } => {
            let n = value.as_i64().ok_or_else(|| {
                ParserError::Field(format!(
                    "validator int_range on '{field_name}' requires an integer value, got {value:?}"
                ))
            })?;
            if n < *min || n > *max {
                return Err(ParserError::Field(format!(
                    "'{field_name}' = {n} is outside the permitted range [{min}, {max}]"
                )));
            }
        }
        FieldValidator::I32 => {
            let n = value.as_i64().ok_or_else(|| {
                ParserError::Field(format!(
                    "validator i32 on '{field_name}' requires an integer value, got {value:?}"
                ))
            })?;
            if i32::try_from(n).is_err() {
                return Err(ParserError::Field(format!(
                    "'{field_name}' = {n} does not fit in an i32"
                )));
            }
        }
        FieldValidator::TimestampNanos => {
            let n = value.as_i64().ok_or_else(|| {
                ParserError::Field(format!(
                    "validator timestamp_nanos on '{field_name}' requires an integer value, \
                     got {value:?}"
                ))
            })?;
            if Timestamp::from_unix_timestamp_nanos(i128::from(n)).is_none() {
                return Err(ParserError::Field(format!(
                    "'{field_name}' = {n} ns is outside the representable timestamp range"
                )));
            }
        }
        FieldValidator::NonEmptyString => {
            let s = value.as_str().ok_or_else(|| {
                ParserError::Field(format!(
                    "validator non_empty on '{field_name}' requires a string value, got {value:?}"
                ))
            })?;
            if s.trim().is_empty() {
                return Err(ParserError::Field(format!(
                    "'{field_name}' must be a non-empty string"
                )));
            }
        }
    }
    Ok(())
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
#[path = "declarative_test.rs"]
mod tests;

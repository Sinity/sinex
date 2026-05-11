#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]

//! Procedural macro crate for Sinex.
//!
//! This crate intentionally stays small: Rust requires procedural macros to live
//! in a separate crate.

mod event_payload;
mod source_record;

use proc_macro::TokenStream;

/// Derive macro for `EventPayload` trait
///
/// Automatically implements `EventPayload` trait with SOURCE and `EVENT_TYPE` constants.
///
/// # Examples
///
/// ```rust
/// use sinex_macros::EventPayload;
/// use serde::{Serialize, Deserialize};
/// use schemars::JsonSchema;
///
/// #[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
/// #[event_payload(source = "fs-watcher", event_type = "file.created")]
/// pub struct FileCreatedPayload {
///     pub path: String,
///     pub size: u64,
/// }
/// ```
#[proc_macro_derive(EventPayload, attributes(event_payload))]
pub fn derive_event_payload(input: TokenStream) -> TokenStream {
    event_payload::derive_event_payload_impl(input)
}

/// Derive macro for [`MaterialParser`] from a struct's `#[source_record(...)]`
/// attribute and per-field annotations.
///
/// Generates:
///   - A `pub fn parser_spec() -> &'static DeclarativeParserSpec` on the
///     struct, returning the spec built from the attributes
///   - An `impl MaterialParser for <Struct>` that delegates to
///     `DeclarativeParser::evaluate(Self::parser_spec(), ...)`
///
/// # Struct attribute
///
/// `#[source_record(id, source_unit_id, input_shape, event_type, ...)]`
///
/// Required keys: `id`, `source_unit_id`, `input_shape`, `event_type`.
/// Optional keys: `event_source` (defaults to first segment of `source_unit_id`),
/// `default_privacy_context` (defaults to `Metadata`), `version` (defaults to `"1.0.0"`).
///
/// `input_shape` ∈ `json | tab_separated | csv_row | sqlite_row | raw_line`.
///
/// # Field attributes
///
/// `#[source(json_pointer = "...")]` — extract via JSON Pointer (for json/csv_row/sqlite_row)
/// `#[source(column_index = N)]` — extract by 0-based index (tab_separated)
/// `#[source(column_name = "...")]` — extract by column name (csv_row/sqlite_row)
/// `#[source(raw_line)]` — entire record as one string (raw_line)
///
/// `#[required]` — fail the record if the field is missing
/// `#[default = "..."]` — default value (parsed as JSON, falls back to string)
/// `#[skip]` — exclude this field from the emitted payload
/// `#[occurrence_key]` — include in composite OccurrenceKey
/// `#[privacy(context = "Command")]` — run through privacy::process at parse time
/// `#[timestamp(format = "rfc3339", fallback = "material_timing")]` — derive ts_orig
/// `#[suppress_if(binding_field = "private_mode_active", whole_event = false)]`
///
/// Field types are inferred from the Rust type:
/// `String` → String, integers → Integer, `f32`/`f64` → Number, `bool` → Boolean,
/// anything else → Json (passed through as a JSON subtree).
///
/// # Example
///
/// ```ignore
/// #[derive(SourceRecord)]
/// #[source_record(
///     id = "atuin-history",
///     source_unit_id = "terminal.atuin-history",
///     input_shape = "sqlite_row",
///     event_type = "command.executed",
///     default_privacy_context = "Command",
/// )]
/// pub struct AtuinHistoryRecord {
///     #[source(column_name = "rowid")]
///     #[occurrence_key]
///     #[skip]
///     pub rowid: i64,
///
///     #[source(column_name = "timestamp")]
///     #[timestamp(format = "unix_seconds_nanos", fallback = "material_timing")]
///     pub timestamp: i64,
///
///     #[source(column_name = "command")]
///     #[privacy(context = "Command")]
///     pub command: String,
///
///     #[source(column_name = "exit")]
///     #[default = "0"]
///     pub exit: i64,
/// }
/// ```
///
/// See `crate/lib/sinex-node-sdk/docs/declarative_parser.md` for the locked
/// design.
#[proc_macro_derive(
    SourceRecord,
    attributes(
        source_record,
        source,
        required,
        skip,
        occurrence_key,
        privacy,
        timestamp,
        suppress_if,
        default,
    )
)]
pub fn derive_source_record(input: TokenStream) -> TokenStream {
    source_record::derive_source_record_impl(input)
}

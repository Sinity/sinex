#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]

//! Procedural macro crate for Sinex.
//!
//! This crate intentionally stays small: Rust requires procedural macros to live
//! in a separate crate.

mod db_check;
mod event_payload;
mod sinex_config;
mod source_definition;
mod source_meta;
mod source_record;
mod source_registration;

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
/// `#[source_record(id, source_id, input_shape, event_type, ...)]`
///
/// Required keys: `id`, `source_id`, `input_shape`, `event_type`.
/// Optional keys: `event_source` (defaults to first segment of `source_id`),
/// `default_privacy_context` (defaults to `Metadata`), `version` (defaults to `"1.0.0"`).
///
/// `input_shape` ∈ `json | tab_separated | csv_row | sqlite_row | raw_line`.
///
/// # Field attributes
///
/// `#[source(json_pointer = "...")]` — extract via JSON Pointer (for `json/csv_row/sqlite_row`)
/// `#[source(column_index = N)]` — extract by 0-based index (`tab_separated`)
/// `#[source(column_name = "...")]` — extract by column name (`csv_row/sqlite_row`)
/// `#[source(raw_line)]` — entire record as one string (`raw_line`)
///
/// `#[required]` — fail the record if the field is missing
/// `#[default = "..."]` — default value (parsed as JSON, falls back to string)
/// `#[skip]` — exclude this field from the emitted payload
/// `#[occurrence_key]` — include in composite `OccurrenceKey`
/// `#[privacy(context = "Command")]` — emit a field privacy-context hint
/// `#[privacy(sensitivity = "free_text, potentially_sensitive")]` — emit one or
///   more comma-separated semantic sensitivity-class hints. Vocabulary:
///   `potentially_sensitive | free_text | credential_bearing |
///   person_name_candidate | source_path`. These are exported through the parser
///   manifest for DB/user policy tooling and never auto-act (#1611).
/// `#[timestamp(format = "rfc3339", fallback = "material_timing")]` — derive `ts_orig`
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
///     source_id = "terminal.atuin-history",
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
/// See `crate/sinexd/docs/declarative_parser.md` for the locked
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
        event_dispatch,
        carry_across_records,
        transform,
        validate,
    )
)]
pub fn derive_source_record(input: TokenStream) -> TokenStream {
    source_record::derive_source_record_impl(input)
}

/// Derive macro that unifies source registration from one struct.
///
/// `#[derive(SourceDefinition)]` collapses the four registration sites a source
/// author would otherwise hand-wire — `SourceContract`, `SourceRuntimeBinding`,
/// the `register_source!` adapter+parser factory, and `impl MaterialParser` —
/// into a single annotated struct. Site 4 reuses the exact declarative-parser
/// code path of [`macro@SourceRecord`]; the field attributes
/// (`#[source(...)]`, `#[privacy(...)]`, `#[timestamp(...)]`,
/// `#[occurrence_key]`, `#[event_dispatch(...)]`, `#[transform(...)]`,
/// `#[validate(...)]`) are identical.
///
/// # Struct attribute
///
/// `#[source_definition(...)]` keys (all string literals):
///
/// String-literal keys — Required: `id`, `namespace`, `event_type`,
/// `event_source`, `input_shape`, `adapter`. Optional: `default_privacy_context`,
/// `version`, `baseline_adapter_config`, `implementation`, `event_types` (extra
/// comma-separated emitted types), `capabilities`.
///
/// Typed enum-path/expression keys (written as Rust paths, e.g.
/// `privacy_tier = PrivacyTier::Sensitive`) — Required: `occurrence_identity`
/// (e.g. `OccurrenceIdentity::Anchor`). Optional: `privacy_tier`,
/// `horizons(Horizon::Continuous, ..)`, `retention`, `access_scope` (e.g.
/// `AccessScope::TargetHome { path: ".." }`), `privacy_context`
/// (`ProcessingContext::*`), `resource_profile` (`ResourceProfile::*`),
/// `runner_pack` (`RunnerPack::*`), `checkpoint_family`, `runtime_shape`.
///
/// # Compile-fail invariants (slice 1 subset)
///
/// - Missing `occurrence_identity` fails to compile.
/// - An `#[event_dispatch(... => "type")]` target not in the definition's
///   declared event types (`event_type` ∪ `event_types`) fails to compile.
///
/// See issue #1727 (SNX-41).
#[proc_macro_derive(
    SourceDefinition,
    attributes(
        source_definition,
        source,
        required,
        skip,
        occurrence_key,
        privacy,
        timestamp,
        suppress_if,
        default,
        event_dispatch,
        carry_across_records,
        transform,
        validate,
    )
)]
pub fn derive_source_definition(input: TokenStream) -> TokenStream {
    source_definition::derive_source_definition_impl(input)
}

/// Derive macro that unifies source *registration* for sources with a
/// hand-written parser.
///
/// `#[derive(SourceMeta)]` is the imperative sibling of
/// [`macro@SourceDefinition`]. It collapses only the three registration sites —
/// `SourceContract`, `SourceRuntimeBinding`, and the `register_source!`
/// adapter+parser factory — into a single annotated struct, and does **not**
/// generate a `MaterialParser`. Use it when the parser needs logic the
/// declarative DSL cannot express (stateful dedup, multi-line state machines,
/// multi-event fan-out, custom timestamp parsing): the author keeps their
/// `impl MaterialParser`, and `SourceMeta` removes the two error-prone
/// `register_source_contract!` / `register_source_runtime_binding!` calls.
///
/// The derive is applied directly to the hand-written parser struct (the
/// `MaterialParser` implementor); the factory wiring references that struct as
/// its parser type. The struct must provide `Default` (the factory constructs
/// the parser via `Default::default()`).
///
/// External producers that publish `EventIntent` envelopes themselves can set
/// `factory = "none"` to emit only the `SourceContract` and
/// `SourceRuntimeBinding` registration sites. That mode intentionally skips
/// `register_source!` factory wiring and does not require the marker struct to
/// implement `Default`.
///
/// # Struct attribute
///
/// `#[source_meta(...)]` keys:
///
/// String-literal keys — Required: `id`, `namespace`, `event_type`,
/// `event_source`, `adapter`. Optional: `implementation`, `event_types` (extra
/// comma-separated emitted types), `capabilities`, and for monitor-emit sources
/// `monitor_emit_fn` / `monitor_phase`. External producers may set
/// `factory = "none"`; the default is `factory = "adapter_parser"`.
///
/// Typed enum-path/expression keys (written as Rust paths) — Required:
/// `occurrence_identity` (e.g. `OccurrenceIdentity::Anchor`). Optional:
/// `privacy_tier`, `horizons(Horizon::*, ..)`, `retention`, `access_scope`
/// (e.g. `AccessScope::TargetHome { path: ".." }`), `privacy_context`
/// (`ProcessingContext::*`), `resource_profile` (`ResourceProfile::*`),
/// `runner_pack` (`RunnerPack::*`), `checkpoint_family`, `runtime_shape`.
///
/// Unlike `SourceDefinition` there are no parser-spec keys (`input_shape`,
/// `default_privacy_context`, `version`, `baseline_adapter_config`) — those
/// belong to the declarative parser this derive does not generate.
///
/// # Compile-fail invariants (slice 3 subset)
///
/// - Missing `occurrence_identity` fails to compile.
///
/// See issue #1727 (SNX-41).
#[proc_macro_derive(SourceMeta, attributes(source_meta))]
pub fn derive_source_meta(input: TokenStream) -> TokenStream {
    source_meta::derive_source_meta_impl(input)
}

/// Derive `from_env()` for env-driven configuration structs.
///
/// Generates an `impl <Struct> { pub fn from_env() -> Self }` body that
/// reads each non-`skip` field from an environment variable using the
/// `sinex_primitives::env::*` helpers. Field types drive helper selection:
/// `bool` → `bool_or`, `Option<PathBuf>` → `path_optional`,
/// `Option<String>` → `var_optional`, `Option<T>` → `parse_optional`,
/// `String` → `var_or`, other `T: FromStr` → `parse_or`.
///
/// # Required struct attributes
///
/// ```ignore
/// #[derive(SinexConfig)]
/// #[sinex_config(prefix = "SINEX_DB", context = "database pool")]
/// pub struct PoolConfig { /* ... */ }
/// ```
///
/// # Field attributes
///
/// - `#[sinex_config(env = "MY_ENV_VAR")]` — override the full env-var name
///   (default: `{prefix}_{FIELD_NAME_UPPERCASED}`)
/// - `#[sinex_config(default = LIT)]` — literal default for fields whose
///   type doesn't otherwise have one (bool defaults to false, String to "")
/// - `#[sinex_config(default_expr = "EXPR")]` — non-literal default
///   (e.g. `"Seconds::from_secs(30)"`)
/// - `#[sinex_config(parser = path::to::fn)]` — custom parser
///   `fn(&str) -> Result<T, _>`; requires a default fallback
/// - `#[sinex_config(skip)]` — leave the field at `Default::default()`
///
/// See `thoughtspace/crystal/decisions/sinex-config-derive.md` for design.
#[proc_macro_derive(SinexConfig, attributes(sinex_config))]
pub fn derive_sinex_config(input: TokenStream) -> TokenStream {
    sinex_config::expand(input)
}

/// Derive a DB CHECK constraint specification for an enum whose `Display`
/// rendering is stored as a text column.
///
/// Generates `impl <Enum> { const DB_CHECK: DbCheckSpec = ... }` and registers
/// the spec in the global `inventory` so the schema-apply engine can iterate
/// every `DbCheck`-derived enum at runtime.
///
/// # Required struct attribute
///
/// ```ignore
/// #[derive(DbCheck)]
/// #[db_check(table = "manifests", column = "manifest_type", version = 1)]
/// pub enum ModuleKind {
///     Ingestor,
///     Automaton,
///     Service,
/// }
/// ```
///
/// Optional `schema = "core"` (default `"core"`).
///
/// # Variant rename
///
/// ```ignore
/// #[db_check(rename = "failure")]
/// Failed,
/// ```
///
/// The default rendering converts `PascalCase` variant idents to `snake_case`,
/// matching `serde(rename_all = "snake_case")`. Override when the
/// `Display` impl emits something else (e.g. `OperationStatus::Failed` →
/// `"failure"`).
///
/// See `crate/sinex-primitives/src/schema_constraints.rs` for the
/// generated spec type and `crate/sinex-schema/src/apply.rs` for the
/// schema-apply convergence integration.
#[proc_macro_derive(DbCheck, attributes(db_check))]
pub fn derive_db_check(input: TokenStream) -> TokenStream {
    db_check::expand(input)
}

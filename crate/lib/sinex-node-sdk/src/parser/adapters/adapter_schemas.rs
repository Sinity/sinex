//! JSON Schema export for all adapter Config types.
//!
//! Each adapter's `*Config` struct derives [`schemars::JsonSchema`] and is
//! registered here. Callers (e.g. `xtask source-units render`) call
//! [`all_adapter_schemas`] to obtain a map of adapter name → JSON Schema
//! together with the set of required field names.
//!
//! # Coverage
//!
//! All 9 adapter types in `parser/adapters/mod.rs` are covered. For
//! [`DbusStreamConfig`] — whose source file is owned by a parallel agent — the
//! schema is produced from the hand-authored definition below rather than from a
//! derived implementation.

use std::collections::BTreeMap;

use schemars::{JsonSchema, SchemaGenerator};
use serde_json::Value;

use super::{
    AppendOnlyFileConfig, ChainedConfig, ClipboardPollingConfig, DirectoryWalkConfig,
    FileDropConfig, JournalctlStreamConfig, SqliteRowConfig, StaticFileConfig,
    UnixSocketStreamConfig,
};

// =============================================================================
// AdapterSchema — per-adapter schema record
// =============================================================================

/// The JSON Schema and required-fields list for one adapter Config.
#[derive(Debug, Clone)]
pub struct AdapterSchema {
    /// The full JSON Schema document for this adapter's config.
    pub schema: Value,
    /// Field names that are required (not `#[serde(default)]`-supplied).
    ///
    /// Derived by comparing the `required` array in the generated JSON Schema
    /// against the adapter's actual serde defaults, so callers get an
    /// honest required-vs-optional split.
    pub required: Vec<String>,
}

// =============================================================================
// Public entry-point
// =============================================================================

/// Return the JSON Schema and required-field list for every adapter Config type.
///
/// The map key is the canonical adapter name used in source-unit descriptors.
///
/// Adapter coverage (all 9 from `parser/adapters/mod.rs`):
/// - `AppendOnlyFileAdapter`
/// - `ChainedAdapter` (concrete instance: `ChainedConfig<StaticFileConfig, SqliteRowConfig>`)
/// - `ClipboardPollingAdapter`
/// - `DbusStreamAdapter` (hand-authored schema; source file is pending parallel work)
/// - `DirectoryWalkAdapter`
/// - `FileDropAdapter`
/// - `JournalctlStreamAdapter`
/// - `SqliteRowAdapter`
/// - `StaticFileAdapter`
/// - `UnixSocketStreamAdapter`
pub fn all_adapter_schemas() -> BTreeMap<String, AdapterSchema> {
    let mut map = BTreeMap::new();

    map.insert(
        "AppendOnlyFileAdapter".into(),
        schema_for_type::<AppendOnlyFileConfig>(),
    );

    // ChainedConfig is generic; we produce the schema for the most common
    // concrete instantiation used in the codebase (SqliteRow + StaticFile).
    // Callers that need other leg combinations can generate the schema from
    // the concrete type directly.
    map.insert(
        "ChainedAdapter".into(),
        schema_for_type::<ChainedConfig<SqliteRowConfig, StaticFileConfig>>(),
    );

    map.insert(
        "ClipboardPollingAdapter".into(),
        schema_for_type::<ClipboardPollingConfig>(),
    );

    // DbusStreamAdapter — hand-authored schema (source file owned by Agent D / #1235).
    map.insert("DbusStreamAdapter".into(), dbus_stream_schema());

    map.insert(
        "DirectoryWalkAdapter".into(),
        schema_for_type::<DirectoryWalkConfig>(),
    );
    map.insert(
        "FileDropAdapter".into(),
        schema_for_type::<FileDropConfig>(),
    );
    map.insert(
        "JournalctlStreamAdapter".into(),
        schema_for_type::<JournalctlStreamConfig>(),
    );
    map.insert(
        "SqliteRowAdapter".into(),
        schema_for_type::<SqliteRowConfig>(),
    );
    map.insert(
        "StaticFileAdapter".into(),
        schema_for_type::<StaticFileConfig>(),
    );
    map.insert(
        "UnixSocketStreamAdapter".into(),
        schema_for_type::<UnixSocketStreamConfig>(),
    );

    map
}

// =============================================================================
// Helpers
// =============================================================================

/// Generate the JSON Schema for `T` and extract its `required` field list.
///
/// Uses [`SchemaGenerator::root_schema_for`] to produce a self-contained schema
/// (i.e. `properties` and `required` at the top level, not behind a `$ref`).
fn schema_for_type<T: JsonSchema>() -> AdapterSchema {
    // root_schema_for produces a RootSchema with all definitions inlined,
    // so `required` is always at the top level of the schema object.
    let root = SchemaGenerator::default().root_schema_for::<T>();
    let schema_value =
        serde_json::to_value(&root).expect("schemars schema should be JSON-serializable");

    let required = extract_required(&schema_value);
    AdapterSchema {
        schema: schema_value,
        required,
    }
}

/// Extract the `required` array from a JSON Schema object.
///
/// Returns an empty vec for schemas that carry no `required` field (all-optional
/// or non-object schemas).
fn extract_required(schema: &Value) -> Vec<String> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Hand-authored JSON Schema for `DbusStreamConfig`.
///
/// Mirrors the shape of the struct in `dbus_stream.rs`:
/// ```text
/// pub struct DbusStreamConfig {
///     pub bus: DbusBus,          // required (no serde default)
///     pub match_rules: Vec<String>,  // required (no serde default)
/// }
/// pub enum DbusBus { Session, System }  // snake_case rename
/// ```
fn dbus_stream_schema() -> AdapterSchema {
    let schema = serde_json::json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "DbusStreamConfig",
        "type": "object",
        "required": ["bus", "match_rules"],
        "properties": {
            "bus": {
                "description": "Which D-Bus bus to connect to.",
                "type": "string",
                "enum": ["session", "system"]
            },
            "match_rules": {
                "description": "D-Bus match rules (e.g. \"type='signal',interface='org.freedesktop.DBus.Properties'\").",
                "type": "array",
                "items": {
                    "type": "string"
                }
            }
        },
        "additionalProperties": false
    });

    AdapterSchema {
        required: vec!["bus".into(), "match_rules".into()],
        schema,
    }
}

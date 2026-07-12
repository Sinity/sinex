//! Schema registry definitions for `EventPayload` types
//!
//! This module provides the core types for payload registration via inventory.

use crate::domain::{EventSource, EventType, SchemaVersion};
use crate::error::Result;
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::OnceLock;

/// How admission reconciles a fresh interpretation against an existing live
/// event that shares its occurrence identity (`equivalence_key`).
///
/// Declared per payload type via `#[event_payload(revision_policy = "...")]`
/// so the choice is a typed, registry-level property of the event contract —
/// never an ad-hoc string match scattered across admission call sites.
///
/// The distinction is load-bearing: occurrence-stable equivalence keys
/// (sinex-ecy) make every legitimate revision of an interval/window/session
/// re-arrive with the SAME key. Under [`Self::SuppressDuplicate`] admission
/// silently discards those revisions; [`Self::SupersedeOnChange`] instead
/// archives the stale interpretation and admits the new one when the content
/// actually changed (sinex-n9a — the two beads only work together).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RevisionPolicy {
    /// A live event already carrying this occurrence's `equivalence_key`
    /// suppresses the new interpretation unconditionally. The exact
    /// pre-n9a behavior, and the default for every payload type.
    #[default]
    SuppressDuplicate,
    /// A live event with the same `equivalence_key` is compared by content
    /// hash: identical content still suppresses (idempotent re-emit), but
    /// changed content archives the live row and admits the revision.
    ///
    /// The attribute spellings (`"suppress_duplicate"` / `"supersede_on_change"`)
    /// are parsed by the `#[derive(EventPayload)]` macro
    /// (`sinex-macros/src/event_payload.rs`), which emits these variant
    /// identifiers directly into the generated `PayloadInfo` — the macro's
    /// match is the single spelling table.
    SupersedeOnChange,
}

/// Information about a payload type collected by the inventory registry.
///
/// This struct holds metadata for a single registered `EventPayload` type,
/// including its source/type identifiers and a function to generate its JSON schema.
pub struct PayloadInfo {
    /// Type name of the payload struct
    pub type_name: &'static str,
    /// Event source identifier (e.g., "fs-watcher")
    pub source: &'static str,
    /// Event type identifier (e.g., "file.created")
    pub event_type: &'static str,
    /// Payload version (typically semantic version like "1.0.0")
    pub version: &'static str,
    /// Function that generates the JSON schema for this payload
    pub schema_fn: fn() -> Result<Value>,
    /// Admission revision policy for events of this type (see
    /// [`RevisionPolicy`]). Defaults to [`RevisionPolicy::SuppressDuplicate`]
    /// unless the payload opts into supersession.
    pub revision_policy: RevisionPolicy,
}

// Register PayloadInfo for inventory collection
inventory::collect!(PayloadInfo);

static REVISION_POLICY_CACHE: OnceLock<HashMap<&'static str, RevisionPolicy>> = OnceLock::new();

/// Resolve the [`RevisionPolicy`] for an event type string.
///
/// Built once from the inventory of registered payloads and cached. Event
/// types with no registered payload (dynamic/escape-hatch events) and any
/// type that did not opt in resolve to [`RevisionPolicy::SuppressDuplicate`],
/// so admission behavior is unchanged for everything that does not explicitly
/// declare `revision_policy = "supersede_on_change"`.
#[must_use]
pub fn revision_policy_for_event_type(event_type: &str) -> RevisionPolicy {
    REVISION_POLICY_CACHE
        .get_or_init(|| {
            // Only carry non-default entries; the accessor defaults the rest.
            get_all_payloads()
                .filter(|payload| payload.revision_policy != RevisionPolicy::SuppressDuplicate)
                .map(|payload| (payload.event_type, payload.revision_policy))
                .collect()
        })
        .get(event_type)
        .copied()
        .unwrap_or_default()
}

static ALL_SCHEMAS_CACHE: OnceLock<HashMap<(String, String, String), Value>> = OnceLock::new();
static ALL_SCHEMA_BUNDLE_CACHE: OnceLock<SchemaBundle> = OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaBundleEntry {
    pub source: String,
    pub event_type: String,
    pub version: String,
    pub schema_content: Value,
    pub content_hash: String,
}

impl SchemaBundleEntry {
    pub fn new(
        source: impl Into<String>,
        event_type: impl Into<String>,
        version: impl Into<String>,
        schema_content: Value,
    ) -> Result<Self> {
        let source = source.into();
        let event_type = event_type.into();
        let version = version.into();
        EventSource::new(source.clone())?;
        EventType::new(event_type.clone())?;
        SchemaVersion::new(&version).validate().map_err(|error| {
            crate::error::SinexError::validation(format!(
                "Invalid schema version '{version}': {error}"
            ))
        })?;

        let schema_content =
            annotate_schema_bundle_json(schema_content, &source, &event_type, &version)?;
        let content_hash =
            calculate_schema_content_hash(&source, &event_type, &version, &schema_content)?;

        Ok(Self {
            source,
            event_type,
            version,
            schema_content,
            content_hash,
        })
    }

    pub fn from_payload_info(payload: &PayloadInfo) -> Result<Self> {
        let schema = (payload.schema_fn)().map_err(|error| {
            error
                .with_context("payload_type", payload.type_name)
                .with_context("source", payload.source)
                .with_context("event_type", payload.event_type)
                .with_context("version", payload.version)
        })?;
        Self::new(payload.source, payload.event_type, payload.version, schema)
    }

    #[must_use]
    pub fn sync_key(&self) -> (String, String, String) {
        (
            self.source.clone(),
            self.event_type.clone(),
            self.version.clone(),
        )
    }

    pub fn major_version(&self) -> Result<u64> {
        schema_bundle_major_version(&self.version)
    }

    #[must_use]
    pub fn registry_path(&self) -> String {
        format!("{}/{}.json", self.source, self.event_type)
    }

    pub fn bundle_relative_path(&self) -> Result<PathBuf> {
        let major = self.major_version()?;
        Ok(PathBuf::from(format!(
            "v{major}/{}/{}.json",
            self.source, self.event_type
        )))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaBundle {
    entries: Vec<SchemaBundleEntry>,
}

impl SchemaBundle {
    #[must_use]
    pub fn new(mut entries: Vec<SchemaBundleEntry>) -> Self {
        entries.sort_by(|left, right| {
            (
                left.source.as_str(),
                left.event_type.as_str(),
                left.version.as_str(),
            )
                .cmp(&(
                    right.source.as_str(),
                    right.event_type.as_str(),
                    right.version.as_str(),
                ))
        });
        Self { entries }
    }

    #[must_use]
    pub fn entries(&self) -> &[SchemaBundleEntry] {
        &self.entries
    }

    #[must_use]
    pub fn into_entries(self) -> Vec<SchemaBundleEntry> {
        self.entries
    }

    #[must_use]
    pub fn into_schema_map(self) -> HashMap<(String, String, String), Value> {
        self.entries
            .into_iter()
            .map(|entry| (entry.sync_key(), entry.schema_content))
            .collect()
    }
}

/// Get all registered payload types via the inventory registry.
///
/// Returns an iterator over all `PayloadInfo` structs that have been registered
/// via the `#[derive(EventPayload)]` macro.
pub fn get_all_payloads() -> impl Iterator<Item = &'static PayloadInfo> {
    inventory::iter::<PayloadInfo>()
}

pub fn calculate_schema_content_hash(
    source: &str,
    event_type: &str,
    version: &str,
    schema_content: &Value,
) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(source.as_bytes());
    hasher.update(b":");
    hasher.update(event_type.as_bytes());
    hasher.update(b":");
    hasher.update(version.as_bytes());
    hasher.update(b":");
    let serialized = serde_json::to_vec(schema_content).map_err(|error| {
        crate::error::SinexError::validation(format!(
            "Failed to serialize schema content for hashing: {error}"
        ))
    })?;
    hasher.update(&serialized);
    Ok(hasher.finalize().to_hex().to_string())
}

pub fn annotate_schema_bundle_json(
    schema: Value,
    source: &str,
    event_type: &str,
    version: &str,
) -> Result<Value> {
    let Value::Object(mut object) = schema else {
        return Err(crate::error::SinexError::validation(
            "event payload schema root must be a JSON object",
        ));
    };
    object.insert(
        "x-sinex-source".to_string(),
        Value::String(source.to_string()),
    );
    object.insert(
        "x-sinex-event-type".to_string(),
        Value::String(event_type.to_string()),
    );
    object.insert(
        "x-sinex-version".to_string(),
        Value::String(version.to_string()),
    );
    Ok(Value::Object(object))
}

pub fn schema_bundle_major_version(version: &str) -> Result<u64> {
    version
        .split('.')
        .next()
        .ok_or_else(|| crate::error::SinexError::validation("schema version cannot be empty"))?
        .parse::<u64>()
        .map_err(|error| {
            crate::error::SinexError::validation(format!(
                "failed to parse schema major version from '{version}': {error}"
            ))
        })
}

pub fn generate_schema_bundle() -> Result<SchemaBundle> {
    if let Some(bundle) = ALL_SCHEMA_BUNDLE_CACHE.get() {
        return Ok(bundle.clone());
    }

    let bundle = generate_schema_bundle_from_payloads(get_all_payloads())?;
    let _ = ALL_SCHEMA_BUNDLE_CACHE.set(bundle.clone());
    Ok(bundle)
}

/// Generate JSON schemas for all registered payload types.
///
/// Returns a `HashMap` keyed by (source, `event_type`, version) tuples, mapping to their JSON schemas.
/// Used for schema synchronization and validation.
pub fn generate_all_schemas() -> Result<HashMap<(String, String, String), Value>> {
    if let Some(schemas) = ALL_SCHEMAS_CACHE.get() {
        return Ok(schemas.clone());
    }

    let schemas = generate_schema_bundle()?.into_schema_map();
    let _ = ALL_SCHEMAS_CACHE.set(schemas.clone());
    Ok(schemas)
}

fn generate_schema_bundle_from_payloads<'a, I>(payloads: I) -> Result<SchemaBundle>
where
    I: IntoIterator<Item = &'a PayloadInfo>,
{
    let mut entries = Vec::new();

    for payload in payloads {
        entries.push(SchemaBundleEntry::from_payload_info(payload)?);
    }

    Ok(SchemaBundle::new(entries))
}

#[cfg(test)]
#[path = "schema_registry_test.rs"]
mod tests;

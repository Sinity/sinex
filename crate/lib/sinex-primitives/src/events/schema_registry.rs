//! Schema registry definitions for `EventPayload` types
//!
//! This module provides the core types for payload registration via inventory.

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
    pub schema_fn: fn() -> serde_json::Value,
}

// Register PayloadInfo for inventory collection
inventory::collect!(PayloadInfo);

use serde_json::Value;
use std::collections::HashMap;

/// Get all registered payload types via the inventory registry.
///
/// Returns an iterator over all `PayloadInfo` structs that have been registered
/// via the `#[derive(EventPayload)]` macro.
pub fn get_all_payloads() -> impl Iterator<Item = &'static PayloadInfo> {
    inventory::iter::<PayloadInfo>()
}

/// Generate JSON schemas for all registered payload types.
///
/// Returns a `HashMap` keyed by (source, `event_type`, version) tuples, mapping to their JSON schemas.
/// Used for schema synchronization and validation.
#[must_use]
pub fn generate_all_schemas() -> HashMap<(String, String, String), Value> {
    let mut schemas = HashMap::new();

    for payload in get_all_payloads() {
        let key = (
            payload.source.to_string(),
            payload.event_type.to_string(),
            payload.version.to_string(),
        );
        let schema = (payload.schema_fn)();
        schemas.insert(key, schema);
    }

    schemas
}

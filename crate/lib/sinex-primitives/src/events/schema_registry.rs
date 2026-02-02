//! Schema registry definitions for `EventPayload` types
//!
//! This module provides the core types for payload registration via inventory.

/// Information about a payload type collected by inventory
pub struct PayloadInfo {
    pub type_name: &'static str,
    pub source: &'static str,
    pub event_type: &'static str,
    pub version: &'static str,
    pub schema_fn: fn() -> serde_json::Value,
}

// Register PayloadInfo for inventory collection
inventory::collect!(PayloadInfo);

use serde_json::Value;
use std::collections::HashMap;

/// Get all registered payload types via inventory
pub fn get_all_payloads() -> impl Iterator<Item = &'static PayloadInfo> {
    inventory::iter::<PayloadInfo>()
}

/// Generate schemas for all registered payload types
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

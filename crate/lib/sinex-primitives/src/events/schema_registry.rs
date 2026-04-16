//! Schema registry definitions for `EventPayload` types
//!
//! This module provides the core types for payload registration via inventory.

use crate::error::Result;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::OnceLock;

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
}

// Register PayloadInfo for inventory collection
inventory::collect!(PayloadInfo);

static ALL_SCHEMAS_CACHE: OnceLock<HashMap<(String, String, String), Value>> = OnceLock::new();

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
pub fn generate_all_schemas() -> Result<HashMap<(String, String, String), Value>> {
    if let Some(schemas) = ALL_SCHEMAS_CACHE.get() {
        return Ok(schemas.clone());
    }

    let schemas = generate_schemas_from_payloads(get_all_payloads())?;
    let _ = ALL_SCHEMAS_CACHE.set(schemas.clone());
    Ok(schemas)
}

fn generate_schemas_from_payloads<'a, I>(
    payloads: I,
) -> Result<HashMap<(String, String, String), Value>>
where
    I: IntoIterator<Item = &'a PayloadInfo>,
{
    let mut schemas = HashMap::new();

    for payload in payloads {
        let key = (
            payload.source.to_string(),
            payload.event_type.to_string(),
            payload.version.to_string(),
        );
        let schema = (payload.schema_fn)().map_err(|error| {
            error
                .with_context("payload_type", payload.type_name)
                .with_context("source", payload.source)
                .with_context("event_type", payload.event_type)
                .with_context("version", payload.version)
        })?;
        schemas.insert(key, schema);
    }

    Ok(schemas)
}

#[cfg(test)]
mod tests {
    use super::{PayloadInfo, generate_schemas_from_payloads};
    use serde_json::json;
    use xtask::sandbox::prelude::*;

    fn schema_ok() -> crate::error::Result<serde_json::Value> {
        Ok(json!({"type": "object"}))
    }

    fn schema_err() -> crate::error::Result<serde_json::Value> {
        Err(crate::error::SinexError::serialization(
            "failed to serialize event payload schema",
        ))
    }

    #[sinex_test]
    async fn generate_schemas_collects_entries() -> TestResult<()> {
        let payloads = [PayloadInfo {
            type_name: "test::Payload",
            source: "test-source",
            event_type: "test.event",
            version: "1.0.0",
            schema_fn: schema_ok,
        }];

        let schemas = generate_schemas_from_payloads(payloads.iter())?;
        assert_eq!(schemas.len(), 1);
        assert_eq!(
            schemas.get(&(
                "test-source".to_string(),
                "test.event".to_string(),
                "1.0.0".to_string()
            )),
            Some(&json!({"type": "object"}))
        );
        Ok(())
    }

    #[sinex_test]
    async fn generate_schemas_surfaces_schema_generation_failures() -> TestResult<()> {
        let payloads = [PayloadInfo {
            type_name: "test::BrokenPayload",
            source: "test-source",
            event_type: "test.broken",
            version: "1.0.0",
            schema_fn: schema_err,
        }];

        let error = generate_schemas_from_payloads(payloads.iter())
            .expect_err("schema generation failures must stay explicit");
        let rendered = format!("{error:#}");
        assert!(rendered.contains("failed to serialize event payload schema"));
        assert!(rendered.contains("test::BrokenPayload"));
        assert!(rendered.contains("test.broken"));
        Ok(())
    }
}

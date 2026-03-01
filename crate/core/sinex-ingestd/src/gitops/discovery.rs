//! Schema discovery by walking a repository and matching files against a glob pattern.
//!
//! For each matching JSON file, metadata is extracted either from:
//! 1. `x-sinex` fields inside the JSON (`x-sinex-source`, `x-sinex-event-type`, `x-sinex-version`)
//! 2. Path convention: `schemas/{source}/{event_type}/{version}.json`

use crate::gitops::types::DiscoveredSchema;
use crate::{IngestdResult, SinexError};
use globset::{Glob, GlobMatcher};
use std::path::{Path, PathBuf};
use tracing::{debug, warn};

/// Discovers JSON schema files in a repository checkout.
pub struct SchemaDiscovery;

impl SchemaDiscovery {
    /// Walk `repo_path` recursively, match files against `pattern`, and parse
    /// each matched file as a JSON schema.
    pub fn discover_schemas(
        repo_path: &Path,
        pattern: &str,
    ) -> IngestdResult<Vec<DiscoveredSchema>> {
        let glob = Glob::new(pattern).map_err(|e| {
            SinexError::validation(format!("Invalid glob pattern '{pattern}': {e}"))
                .with_operation("gitops.discover_schemas")
        })?;
        let matcher = glob.compile_matcher();

        let mut schemas = Vec::new();
        walk_directory(repo_path, repo_path, &matcher, &mut schemas)?;

        debug!(
            count = schemas.len(),
            pattern = %pattern,
            "Discovered schemas in repository"
        );

        Ok(schemas)
    }
}

/// Recursively walk a directory tree and collect schemas that match the glob.
fn walk_directory(
    root: &Path,
    dir: &Path,
    matcher: &GlobMatcher,
    schemas: &mut Vec<DiscoveredSchema>,
) -> IngestdResult<()> {
    let entries = std::fs::read_dir(dir).map_err(|e| {
        SinexError::io(format!("Failed to read directory {}", dir.display())).with_source(e)
    })?;

    for entry in entries {
        let entry =
            entry.map_err(|e| SinexError::io("Failed to read directory entry").with_source(e))?;
        let path = entry.path();

        if path.is_dir() {
            // Skip hidden directories (e.g. .git)
            if entry
                .file_name()
                .to_str()
                .is_some_and(|n| n.starts_with('.'))
            {
                continue;
            }
            walk_directory(root, &path, matcher, schemas)?;
        } else if path.is_file() {
            // Compute the relative path from the repo root for matching
            let relative = path.strip_prefix(root).unwrap_or(&path);
            let relative_str = relative.to_string_lossy();

            if matcher.is_match(relative) {
                match parse_schema_file(&path, &relative_str) {
                    Ok(schema) => schemas.push(schema),
                    Err(e) => {
                        warn!(
                            path = %relative_str,
                            error = %e,
                            "Skipping file that failed to parse as schema"
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

/// Parse a single JSON file and extract schema metadata.
fn parse_schema_file(path: &Path, relative_path: &str) -> IngestdResult<DiscoveredSchema> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| SinexError::io(format!("Failed to read {}", path.display())).with_source(e))?;

    let json: serde_json::Value = serde_json::from_str(&content).map_err(|e| {
        SinexError::serialization(format!("Failed to parse JSON from {}: {e}", path.display()))
    })?;

    // Try x-sinex metadata fields first
    if let Some(schema) = try_extract_from_metadata(&json, relative_path) {
        return Ok(schema);
    }

    // Fall back to path-based extraction
    if let Some(schema) = try_extract_from_path(&json, relative_path) {
        return Ok(schema);
    }

    Err(SinexError::validation(format!(
        "Cannot extract schema metadata from {relative_path}. \
             Expected either x-sinex-source/x-sinex-event-type/x-sinex-version fields in JSON, \
             or path convention: schemas/{{source}}/{{event_type}}/{{version}}.json"
    ))
    .with_operation("gitops.parse_schema_file"))
}

/// Extract metadata from `x-sinex-*` fields in the JSON document.
fn try_extract_from_metadata(
    json: &serde_json::Value,
    relative_path: &str,
) -> Option<DiscoveredSchema> {
    let source = json.get("x-sinex-source")?.as_str()?;
    let event_type = json.get("x-sinex-event-type")?.as_str()?;
    let version = json.get("x-sinex-version")?.as_str()?;

    Some(DiscoveredSchema {
        source: source.to_string(),
        event_type: event_type.to_string(),
        version: version.to_string(),
        schema_content: json.clone(),
        file_path: relative_path.to_string(),
    })
}

/// Extract metadata from the file path.
///
/// Expected convention: `schemas/{source}/{event_type}/{version}.json`
/// or any path ending in `{source}/{event_type}/{version}.json` with at least
/// 3 path segments before the extension.
fn try_extract_from_path(
    json: &serde_json::Value,
    relative_path: &str,
) -> Option<DiscoveredSchema> {
    let path = PathBuf::from(relative_path);
    let components: Vec<&str> = path
        .components()
        .filter_map(|c| c.as_os_str().to_str())
        .collect();

    // Need at least: <prefix>/<source>/<event_type>/<version>.json
    if components.len() < 3 {
        return None;
    }

    let file_stem = path.file_stem()?.to_str()?;
    let ext = path.extension()?.to_str()?;
    if ext != "json" {
        return None;
    }

    // Take the last 3 meaningful components: source / event_type / version.json
    let len = components.len();
    let source = components[len - 3];
    let event_type = components[len - 2];
    let version = file_stem;

    Some(DiscoveredSchema {
        source: source.to_string(),
        event_type: event_type.to_string(),
        version: version.to_string(),
        schema_content: json.clone(),
        file_path: relative_path.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn extract_from_metadata_fields() -> TestResult<()> {
        let json = json!({
            "type": "object",
            "x-sinex-source": "fs-watcher",
            "x-sinex-event-type": "file.created",
            "x-sinex-version": "1.0.0",
            "properties": {}
        });

        let schema = try_extract_from_metadata(&json, "schemas/test.json");
        assert!(schema.is_some());
        let schema = schema.expect("should extract");
        assert_eq!(schema.source, "fs-watcher");
        assert_eq!(schema.event_type, "file.created");
        assert_eq!(schema.version, "1.0.0");
        Ok(())
    }

    #[sinex_test]
    async fn extract_from_path_convention() -> TestResult<()> {
        let json = json!({"type": "object"});

        let schema = try_extract_from_path(&json, "schemas/fs-watcher/file.created/1.0.0.json");
        assert!(schema.is_some());
        let schema = schema.expect("should extract");
        assert_eq!(schema.source, "fs-watcher");
        assert_eq!(schema.event_type, "file.created");
        assert_eq!(schema.version, "1.0.0");
        Ok(())
    }

    #[sinex_test]
    async fn extract_from_path_too_short_fails() -> TestResult<()> {
        let json = json!({"type": "object"});
        let schema = try_extract_from_path(&json, "1.0.0.json");
        assert!(schema.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn metadata_fields_preferred_over_path() -> TestResult<()> {
        let json = json!({
            "type": "object",
            "x-sinex-source": "metadata-source",
            "x-sinex-event-type": "metadata.type",
            "x-sinex-version": "2.0.0",
        });

        // parse_schema_file reads from disk, so this test validates the helpers directly
        let schema = try_extract_from_metadata(&json, "any/path.json");
        assert!(schema.is_some());
        let schema = schema.expect("should extract metadata");
        assert_eq!(schema.source, "metadata-source");
        assert_eq!(schema.event_type, "metadata.type");
        assert_eq!(schema.version, "2.0.0");
        Ok(())
    }
}

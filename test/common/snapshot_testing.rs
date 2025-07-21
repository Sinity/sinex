// Snapshot testing utilities for capturing and comparing complex test outputs
//
// This module provides a comprehensive snapshot testing framework that:
// - Captures JSON, events, checkpoints and custom types as snapshots
// - Manages snapshot files with automatic updates
// - Provides diff visualization for failures
// - Supports redaction and fuzzy matching for dynamic data
//
// Usage:
//   assert_snapshot!(events, "user_session_events");
//   assert_inline_snapshot!(result, @r###"{"status": "success"}"###);
//   UPDATE_SNAPSHOTS=1 cargo test

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use similar::{ChangeTag, TextDiff};
use sinex_db::RawEvent;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use sinex_ulid::Ulid;
use once_cell::sync::Lazy;

// Global state for ULID redaction
static ULID_COUNTER: Mutex<u32> = Mutex::new(0);
static ULID_MAP: Lazy<Mutex<HashMap<String, String>>> = Lazy::new(|| {
    Mutex::new(HashMap::new())
});

// =============================================================================
// Core Types
// =============================================================================

/// Configuration for snapshot testing
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    /// Base directory for snapshot files
    pub snapshot_dir: PathBuf,
    /// Whether to update snapshots (from UPDATE_SNAPSHOTS env var)
    pub update_mode: bool,
    /// Whether to show diffs in color
    pub color_diff: bool,
    /// Redaction settings
    pub redactions: Vec<Redaction>,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            snapshot_dir: PathBuf::from("test/snapshots"),
            update_mode: std::env::var("UPDATE_SNAPSHOTS").is_ok(),
            color_diff: true,
            redactions: vec![
                Redaction::timestamps(),
                Redaction::ulids(),
                Redaction::dynamic_ids(),
            ],
        }
    }
}

/// Thread-safe global configuration
static CONFIG: Mutex<Option<SnapshotConfig>> = Mutex::new(None);

/// Get or create the global snapshot configuration
pub fn config() -> SnapshotConfig {
    let mut guard = CONFIG.lock().unwrap();
    if guard.is_none() {
        *guard = Some(SnapshotConfig::default());
    }
    guard.as_ref().unwrap().clone()
}

/// Set custom snapshot configuration
pub fn set_config(config: SnapshotConfig) {
    *CONFIG.lock().unwrap() = Some(config);
}

// =============================================================================
// Redaction System
// =============================================================================

/// Redaction rule for dynamic/sensitive data
#[derive(Debug, Clone)]
pub enum Redaction {
    /// Replace timestamps with fixed values
    Timestamps,
    /// Replace ULIDs with sequential values
    Ulids,
    /// Replace dynamic IDs (PIDs, window IDs, etc.)
    DynamicIds,
    /// Custom regex pattern replacement
    Regex {
        pattern: String,
        replacement: String,
    },
    /// Custom field path redaction
    FieldPath {
        path: String,
        replacement: Value,
    },
}

impl Redaction {
    pub fn timestamps() -> Self {
        Self::Timestamps
    }

    pub fn ulids() -> Self {
        Self::Ulids
    }

    pub fn dynamic_ids() -> Self {
        Self::DynamicIds
    }

    pub fn regex(pattern: &str, replacement: &str) -> Self {
        Self::Regex {
            pattern: pattern.to_string(),
            replacement: replacement.to_string(),
        }
    }

    pub fn field(path: &str, replacement: Value) -> Self {
        Self::FieldPath {
            path: path.to_string(),
            replacement,
        }
    }

    /// Apply redaction to a JSON value
    pub fn apply(&self, value: &mut Value) {
        match self {
            Self::Timestamps => redact_timestamps(value),
            Self::Ulids => redact_ulids(value),
            Self::DynamicIds => redact_dynamic_ids(value),
            Self::Regex { pattern, replacement } => {
                let re = regex::Regex::new(pattern).unwrap();
                redact_regex(value, &re, replacement);
            }
            Self::FieldPath { path, replacement } => {
                redact_field_path(value, path, replacement.clone());
            }
        }
    }
}

fn redact_timestamps(value: &mut Value) {
    match value {
        Value::String(s) => {
            // Try parsing as ISO8601 timestamp
            if DateTime::parse_from_rfc3339(s).is_ok() {
                *s = "2024-01-01T00:00:00Z".to_string();
            }
        }
        Value::Object(map) => {
            for (k, v) in map.iter_mut() {
                if k.contains("timestamp") || k.contains("_at") || k.contains("_time") {
                    if let Value::String(_) = v {
                        *v = Value::String("2024-01-01T00:00:00Z".to_string());
                    }
                }
                redact_timestamps(v);
            }
        }
        Value::Array(arr) => {
            for v in arr.iter_mut() {
                redact_timestamps(v);
            }
        }
        _ => {}
    }
}

fn redact_ulids(value: &mut Value) {

    match value {
        Value::String(s) => {
            // Check if it's a ULID
            if s.len() == 26 && Ulid::from_string(s).is_ok() {
                let mut map = ULID_MAP.lock().unwrap();
                if let Some(replacement) = map.get(s) {
                    *s = replacement.clone();
                } else {
                    let mut counter = ULID_COUNTER.lock().unwrap();
                    *counter += 1;
                    let replacement = format!("ULID_{:04}", counter);
                    map.insert(s.clone(), replacement.clone());
                    *s = replacement;
                }
            }
        }
        Value::Object(map) => {
            for v in map.values_mut() {
                redact_ulids(v);
            }
        }
        Value::Array(arr) => {
            for v in arr.iter_mut() {
                redact_ulids(v);
            }
        }
        _ => {}
    }
}

fn redact_dynamic_ids(value: &mut Value) {
    match value {
        Value::Object(map) => {
            // Redact common dynamic ID fields
            let id_fields = ["pid", "process_id", "window_id", "session_id", "thread_id"];
            for field in &id_fields {
                if let Some(v) = map.get_mut(*field) {
                    if let Value::Number(_) = v {
                        *v = Value::Number(serde_json::Number::from(12345));
                    }
                }
            }
            
            // Recurse
            for v in map.values_mut() {
                redact_dynamic_ids(v);
            }
        }
        Value::Array(arr) => {
            for v in arr.iter_mut() {
                redact_dynamic_ids(v);
            }
        }
        _ => {}
    }
}

fn redact_regex(value: &mut Value, re: &regex::Regex, replacement: &str) {
    match value {
        Value::String(s) => {
            *s = re.replace_all(s, replacement).to_string();
        }
        Value::Object(map) => {
            for v in map.values_mut() {
                redact_regex(v, re, replacement);
            }
        }
        Value::Array(arr) => {
            for v in arr.iter_mut() {
                redact_regex(v, re, replacement);
            }
        }
        _ => {}
    }
}

fn redact_field_path(value: &mut Value, path: &str, replacement: Value) {
    let parts: Vec<&str> = path.split('.').collect();
    redact_field_path_inner(value, &parts, replacement);
}

fn redact_field_path_inner(value: &mut Value, path: &[&str], replacement: Value) {
    if path.is_empty() {
        return;
    }

    if path.len() == 1 {
        if let Value::Object(map) = value {
            if let Some(v) = map.get_mut(path[0]) {
                *v = replacement;
            }
        }
    } else if let Value::Object(map) = value {
        if let Some(v) = map.get_mut(path[0]) {
            redact_field_path_inner(v, &path[1..], replacement);
        }
    }
}

// =============================================================================
// Snapshot Storage
// =============================================================================

/// Represents a stored snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Snapshot metadata
    pub metadata: SnapshotMetadata,
    /// The actual snapshot content
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    /// Version of the snapshot format
    pub version: u32,
    /// When the snapshot was created
    pub created_at: String,
    /// Optional description
    pub description: Option<String>,
    /// Content type (json, events, etc.)
    pub content_type: String,
    /// Whether redactions were applied
    pub redacted: bool,
}

impl Snapshot {
    pub fn new(content: String, content_type: &str) -> Self {
        Self {
            metadata: SnapshotMetadata {
                version: 1,
                created_at: Utc::now().to_rfc3339(),
                description: None,
                content_type: content_type.to_string(),
                redacted: true,
            },
            content,
        }
    }

    /// Load snapshot from file
    pub fn load(path: &Path) -> Result<Self, String> {
        let content = fs::read_to_string(path)
            .map_err(|e| format!("Failed to read snapshot: {}", e))?;
        
        // Parse snapshot format
        if content.starts_with("# Snapshot v1") {
            Self::parse_v1(&content)
        } else {
            // Legacy format - just content
            Ok(Self::new(content.trim().to_string(), "unknown"))
        }
    }

    /// Save snapshot to file
    pub fn save(&self, path: &Path) -> Result<(), String> {
        // Ensure directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create snapshot directory: {}", e))?;
        }

        let content = self.serialize();
        fs::write(path, content)
            .map_err(|e| format!("Failed to write snapshot: {}", e))?;
        
        Ok(())
    }

    /// Serialize snapshot to string format
    fn serialize(&self) -> String {
        format!(
            "# Snapshot v1\n\
             # Created: {}\n\
             # Type: {}\n\
             # Redacted: {}\n\
             {}\n\
             ---\n\
             {}",
            self.metadata.created_at,
            self.metadata.content_type,
            self.metadata.redacted,
            self.metadata.description
                .as_ref()
                .map(|d| format!("# Description: {}\n", d))
                .unwrap_or_default(),
            self.content
        )
    }

    /// Parse v1 snapshot format
    fn parse_v1(content: &str) -> Result<Self, String> {
        let lines: Vec<&str> = content.lines().collect();
        let mut metadata = SnapshotMetadata {
            version: 1,
            created_at: String::new(),
            description: None,
            content_type: "unknown".to_string(),
            redacted: false,
        };

        let mut content_start = 0;
        for (i, line) in lines.iter().enumerate() {
            if line.starts_with("# Created:") {
                metadata.created_at = line.trim_start_matches("# Created:").trim().to_string();
            } else if line.starts_with("# Type:") {
                metadata.content_type = line.trim_start_matches("# Type:").trim().to_string();
            } else if line.starts_with("# Redacted:") {
                metadata.redacted = line.trim_start_matches("# Redacted:").trim() == "true";
            } else if line.starts_with("# Description:") {
                metadata.description = Some(line.trim_start_matches("# Description:").trim().to_string());
            } else if line.trim() == "---" {
                content_start = i + 1;
                break;
            }
        }

        let content = lines[content_start..].join("\n");
        Ok(Self { metadata, content })
    }
}

// =============================================================================
// Snapshot Assertions
// =============================================================================

/// Assert that a value matches a stored snapshot
#[macro_export]
macro_rules! assert_snapshot {
    ($value:expr) => {
        $crate::common::snapshot_testing::assert_snapshot_impl(
            &$value,
            module_path!(),
            concat!(file!(), ":", line!(), ":", column!()),
            None,
        )
    };
    ($value:expr, $name:expr) => {
        $crate::common::snapshot_testing::assert_snapshot_impl(
            &$value,
            module_path!(),
            $name,
            None,
        )
    };
    ($value:expr, $name:expr, $($redaction:expr),+) => {
        $crate::common::snapshot_testing::assert_snapshot_impl(
            &$value,
            module_path!(),
            $name,
            Some(vec![$($redaction),+]),
        )
    };
}

/// Assert that a value matches an inline snapshot
#[macro_export]
macro_rules! assert_inline_snapshot {
    ($value:expr, @$snapshot:tt) => {
        $crate::common::snapshot_testing::assert_inline_snapshot_impl(
            &$value,
            stringify!($snapshot).trim_matches(|c| c == 'r' || c == '#' || c == '"'),
            concat!(file!(), ":", line!()),
        )
    };
}

/// Implementation for assert_snapshot macro
pub fn assert_snapshot_impl<T: SnapshotValue>(
    value: &T,
    module_path: &str,
    name: &str,
    custom_redactions: Option<Vec<Redaction>>,
) {
    let config = config();
    let snapshot_path = build_snapshot_path(module_path, name);
    
    // Convert value to snapshot format
    let mut content = value.to_snapshot();
    
    // Apply redactions
    let redactions = custom_redactions.unwrap_or_else(|| config.redactions.clone());
    if let Ok(mut json_value) = serde_json::from_str::<Value>(&content) {
        for redaction in &redactions {
            redaction.apply(&mut json_value);
        }
        content = serde_json::to_string_pretty(&json_value).unwrap();
    }

    // Create snapshot
    let new_snapshot = Snapshot::new(content, T::snapshot_type());

    if config.update_mode {
        // Update mode - save the snapshot
        new_snapshot.save(&snapshot_path).unwrap();
        println!("Updated snapshot: {}", snapshot_path.display());
    } else {
        // Test mode - compare with existing
        match Snapshot::load(&snapshot_path) {
            Ok(existing) => {
                if existing.content != new_snapshot.content {
                    let diff = create_diff(&existing.content, &new_snapshot.content, config.color_diff);
                    panic!(
                        "Snapshot mismatch for '{}'\n\
                         {}\n\
                         To update snapshots, run with UPDATE_SNAPSHOTS=1",
                        name, diff
                    );
                }
            }
            Err(_) => {
                // No existing snapshot
                panic!(
                    "No snapshot found for '{}'\n\
                     Expected at: {}\n\
                     To create snapshot, run with UPDATE_SNAPSHOTS=1\n\n\
                     Actual value:\n{}",
                    name,
                    snapshot_path.display(),
                    new_snapshot.content
                );
            }
        }
    }
}

/// Implementation for assert_inline_snapshot macro
pub fn assert_inline_snapshot_impl<T: SnapshotValue>(
    value: &T,
    expected: &str,
    location: &str,
) {
    let mut content = value.to_snapshot();
    
    // Apply default redactions for inline snapshots
    let config = config();
    if let Ok(mut json_value) = serde_json::from_str::<Value>(&content) {
        for redaction in &config.redactions {
            redaction.apply(&mut json_value);
        }
        content = serde_json::to_string_pretty(&json_value).unwrap();
    }

    let expected = expected.trim();
    let actual = content.trim();

    if actual != expected {
        let diff = create_diff(expected, actual, config.color_diff);
        panic!(
            "Inline snapshot mismatch at {}\n\
             {}\n\
             To update inline snapshot, replace with:\n\
             @r###\"\n{}\n\"###",
            location, diff, actual
        );
    }
}

/// Build snapshot file path from module and name
fn build_snapshot_path(module_path: &str, name: &str) -> PathBuf {
    let config = config();
    let module_parts: Vec<&str> = module_path.split("::").collect();
    
    // Convert module path to directory structure
    let mut path = config.snapshot_dir.clone();
    for part in module_parts.iter().skip(1) { // Skip crate name
        path.push(part);
    }
    
    // Add snapshot file
    path.push(format!("{}.snap", name));
    path
}

/// Create a colored diff between two strings
fn create_diff(old: &str, new: &str, use_color: bool) -> String {
    let diff = TextDiff::from_lines(old, new);
    let mut output = String::new();
    
    output.push_str("──────────────────────────────────────\n");
    
    for change in diff.iter_all_changes() {
        let sign = match change.tag() {
            ChangeTag::Delete => "-",
            ChangeTag::Insert => "+",
            ChangeTag::Equal => " ",
        };
        
        if use_color {
            let formatted = match change.tag() {
                ChangeTag::Delete => format!("\x1b[31m{} {}\x1b[0m", sign, change),
                ChangeTag::Insert => format!("\x1b[32m{} {}\x1b[0m", sign, change),
                ChangeTag::Equal => format!("{} {}", sign, change),
            };
            output.push_str(&formatted);
        } else {
            output.push_str(&format!("{} {}", sign, change));
        }
    }
    
    output.push_str("──────────────────────────────────────\n");
    output
}

// =============================================================================
// Snapshot Value Trait
// =============================================================================

/// Trait for types that can be snapshot tested
pub trait SnapshotValue {
    /// Convert the value to a snapshot string
    fn to_snapshot(&self) -> String;
    
    /// Get the snapshot content type
    fn snapshot_type() -> &'static str;
}

// Implement for common types
impl SnapshotValue for Value {
    fn to_snapshot(&self) -> String {
        serde_json::to_string_pretty(self).unwrap()
    }
    
    fn snapshot_type() -> &'static str {
        "json"
    }
}

impl SnapshotValue for String {
    fn to_snapshot(&self) -> String {
        self.clone()
    }
    
    fn snapshot_type() -> &'static str {
        "text"
    }
}

impl SnapshotValue for &str {
    fn to_snapshot(&self) -> String {
        self.to_string()
    }
    
    fn snapshot_type() -> &'static str {
        "text"
    }
}

impl<T: Serialize> SnapshotValue for Vec<T> {
    fn to_snapshot(&self) -> String {
        serde_json::to_string_pretty(self).unwrap()
    }
    
    fn snapshot_type() -> &'static str {
        "json_array"
    }
}

impl SnapshotValue for RawEvent {
    fn to_snapshot(&self) -> String {
        let json = serde_json::json!({
            "id": self.id.to_string(),
            "source": self.source,
            "event_type": self.event_type,
            "ts_orig": self.ts_orig.map(|ts| ts.to_rfc3339()),
            "ts_ingest": self.ts_ingest.to_rfc3339(),
            "host": self.host,
            "payload": self.payload,
        });
        serde_json::to_string_pretty(&json).unwrap()
    }
    
    fn snapshot_type() -> &'static str {
        "event"
    }
}

impl SnapshotValue for Vec<RawEvent> {
    fn to_snapshot(&self) -> String {
        let events: Vec<_> = self.iter().map(|e| {
            serde_json::json!({
                "id": e.id.to_string(),
                "source": e.source,
                "event_type": e.event_type,
                "ts_orig": e.ts_orig.map(|ts| ts.to_rfc3339()),
                "ts_ingest": e.ts_ingest.to_rfc3339(),
                "host": e.host,
                "payload": e.payload,
            })
        }).collect();
        serde_json::to_string_pretty(&events).unwrap()
    }
    
    fn snapshot_type() -> &'static str {
        "event_list"
    }
}

// =============================================================================
// Snapshot Testing Utilities
// =============================================================================

/// Builder for creating custom snapshot assertions
pub struct SnapshotAssertionBuilder<T> {
    value: T,
    name: Option<String>,
    redactions: Vec<Redaction>,
    fuzzy_matchers: Vec<FuzzyMatcher>,
}

impl<T: SnapshotValue> SnapshotAssertionBuilder<T> {
    pub fn new(value: T) -> Self {
        Self {
            value,
            name: None,
            redactions: Vec::new(),
            fuzzy_matchers: Vec::new(),
        }
    }

    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    pub fn redact_timestamps(mut self) -> Self {
        self.redactions.push(Redaction::Timestamps);
        self
    }

    pub fn redact_ulids(mut self) -> Self {
        self.redactions.push(Redaction::Ulids);
        self
    }

    pub fn redact_field(mut self, path: &str, replacement: Value) -> Self {
        self.redactions.push(Redaction::field(path, replacement));
        self
    }

    pub fn fuzzy_match(mut self, matcher: FuzzyMatcher) -> Self {
        self.fuzzy_matchers.push(matcher);
        self
    }

    pub fn assert(self) {
        let name = self.name.unwrap_or_else(|| {
            format!("snapshot_{}", chrono::Utc::now().timestamp_millis())
        });
        
        assert_snapshot_impl(
            &self.value,
            module_path!(),
            &name,
            Some(self.redactions),
        );
    }
}

/// Fuzzy matcher for dynamic content
#[derive(Debug, Clone)]
pub enum FuzzyMatcher {
    /// Match any timestamp
    AnyTimestamp,
    /// Match any ULID
    AnyUlid,
    /// Match any number
    AnyNumber,
    /// Match with regex
    Regex(String),
}

/// Create a snapshot assertion builder
pub fn snapshot<T: SnapshotValue>(value: T) -> SnapshotAssertionBuilder<T> {
    SnapshotAssertionBuilder::new(value)
}

// =============================================================================
// Test Helpers
// =============================================================================

/// Clear all cached redaction mappings (useful between tests)
pub fn clear_redaction_cache() {
    // Clear ULID mappings
    *ULID_MAP.lock().unwrap() = HashMap::new();
    *ULID_COUNTER.lock().unwrap() = 0;
}

/// List all snapshots in a directory
pub fn list_snapshots(dir: &Path) -> Result<Vec<PathBuf>, std::io::Error> {
    let mut snapshots = Vec::new();
    
    if dir.exists() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("snap") {
                snapshots.push(path);
            }
        }
    }
    
    Ok(snapshots)
}

/// Remove orphaned snapshots (snapshots without corresponding tests)
pub fn clean_orphaned_snapshots(test_root: &Path) -> Result<usize, String> {
    // This would require parsing test files to find snapshot assertions
    // For now, just return 0
    Ok(0)
}

// =============================================================================
// Integration with test macros
// =============================================================================

/// Extension trait for test context to support snapshots
pub trait SnapshotTestExt {
    /// Assert events match snapshot
    fn assert_events_snapshot(&self, events: Vec<RawEvent>, name: &str);
    
    /// Assert JSON matches snapshot
    fn assert_json_snapshot(&self, value: Value, name: &str);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_timestamp_redaction() {
        let mut value = json!({
            "created_at": "2024-03-15T10:30:00Z",
            "updated_at": "2024-03-15T11:00:00Z",
            "data": {
                "timestamp": "2024-03-15T12:00:00Z"
            }
        });

        redact_timestamps(&mut value);

        assert_eq!(value["created_at"], "2024-01-01T00:00:00Z");
        assert_eq!(value["updated_at"], "2024-01-01T00:00:00Z");
        assert_eq!(value["data"]["timestamp"], "2024-01-01T00:00:00Z");
    }

    #[test]
    fn test_ulid_redaction() {
        let ulid1 = Ulid::new().to_string();
        let ulid2 = Ulid::new().to_string();
        
        let mut value = json!({
            "id": ulid1.clone(),
            "parent_id": ulid2.clone(),
            "related": [ulid1.clone(), ulid2.clone()]
        });

        clear_redaction_cache();
        redact_ulids(&mut value);

        assert_eq!(value["id"], "ULID_0001");
        assert_eq!(value["parent_id"], "ULID_0002");
        assert_eq!(value["related"][0], "ULID_0001"); // Same ULID gets same replacement
        assert_eq!(value["related"][1], "ULID_0002");
    }

    #[test]
    fn test_field_path_redaction() {
        let mut value = json!({
            "user": {
                "name": "John Doe",
                "email": "john@example.com",
                "preferences": {
                    "theme": "dark"
                }
            }
        });

        redact_field_path(&mut value, "user.email", json!("[REDACTED]"));
        redact_field_path(&mut value, "user.preferences.theme", json!("default"));

        assert_eq!(value["user"]["email"], "[REDACTED]");
        assert_eq!(value["user"]["preferences"]["theme"], "default");
    }

    #[test]
    fn test_snapshot_format() {
        let snapshot = Snapshot::new("test content".to_string(), "text");
        let serialized = snapshot.serialize();
        
        assert!(serialized.contains("# Snapshot v1"));
        assert!(serialized.contains("# Type: text"));
        assert!(serialized.contains("test content"));
        
        // Test parsing
        let parsed = Snapshot::parse_v1(&serialized).unwrap();
        assert_eq!(parsed.content, "test content");
        assert_eq!(parsed.metadata.content_type, "text");
    }
}
//! Schema version support and migration utilities

use crate::error::SinexError;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::HashMap;

/// A versioned wrapper for event payloads
/// This is a simple wrapper that retains the version information alongside the payload
#[derive(Debug, Clone)]
pub struct Versioned<T: super::EventPayload> {
    pub inner: T,
    pub version: String,
}

impl<T: super::EventPayload> Versioned<T> {
    /// Create a new versioned payload
    pub fn new(inner: T) -> Self {
        Self {
            version: T::VERSION.to_string(),
            inner,
        }
    }

    /// Get the inner payload
    pub fn into_inner(self) -> T {
        self.inner
    }
}

/// Registry for tracking compatible versions
pub struct VersionRegistry {
    compatibilities: HashMap<(String, String), Vec<String>>,
}

impl VersionRegistry {
    /// Create a new version registry
    pub fn new() -> Self {
        Self {
            compatibilities: HashMap::new(),
        }
    }

    /// Register compatible versions for a schema
    pub fn register_compatibility(
        &mut self,
        source: &str,
        event_type: &str,
        compatible_versions: Vec<String>,
    ) {
        let key = (source.to_string(), event_type.to_string());
        self.compatibilities.insert(key, compatible_versions);
    }

    /// Check if a version is compatible
    pub fn is_compatible(&self, source: &str, event_type: &str, version: &str) -> bool {
        let key = (source.to_string(), event_type.to_string());
        self.compatibilities
            .get(&key)
            .map(|versions| versions.contains(&version.to_string()))
            .unwrap_or(false)
    }
}

impl Default for VersionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper trait for types that can provide version info
pub trait VersionInfo {
    /// Get the list of compatible versions this type can deserialize from
    fn compatible_versions() -> &'static [&'static str];
}

/// Blanket implementation helper for version-aware deserialization
pub trait VersionAwareDeserialize: super::EventPayload + Sized {
    /// Deserialize with automatic version handling
    fn deserialize_versioned(value: Value, version: &str) -> Result<Self, SinexError>
    where
        Self: DeserializeOwned,
    {
        Self::try_from_legacy(value, version)
    }
}

// Blanket implementation for all EventPayload types
impl<T: super::EventPayload> VersionAwareDeserialize for T {}

/// Helper macro for version comparisons
#[macro_export]
macro_rules! version_newer_than {
    ($major1:expr, $minor1:expr, $patch1:expr, $major2:expr, $minor2:expr, $patch2:expr) => {
        ($major1 > $major2)
            || ($major1 == $major2 && $minor1 > $minor2)
            || ($major1 == $major2 && $minor1 == $minor2 && $patch1 > $patch2)
    };
}

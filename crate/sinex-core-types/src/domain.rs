//! Domain-specific typed strings for the Sinex system
//!
//! This module provides strongly-typed string wrappers to prevent
//! accidental mixing of different string types (e.g., EventSource vs EventType).

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Macro to define a new string type with common implementations
macro_rules! define_string_type {
    (
        $(#[$meta:meta])*
        $name:ident
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Create a new instance from a string
            pub fn new(s: impl Into<String>) -> Self {
                Self(s.into())
            }

            /// Get the underlying string
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Convert to owned String
            pub fn into_string(self) -> String {
                self.0
            }

            /// Check if the value is empty
            pub fn is_empty(&self) -> bool {
                self.0.is_empty()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl FromStr for $name {
            type Err = std::convert::Infallible;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(Self::new(s))
            }
        }

        impl From<String> for $name {
            fn from(s: String) -> Self {
                Self(s)
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                Self(s.to_string())
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl std::ops::Deref for $name {
            type Target = str;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
    };
}

// Macro to add SQLx support for string types
#[cfg(feature = "sqlx")]
macro_rules! impl_sqlx_for_string_type {
    ($name:ident) => {
        impl sqlx::Type<sqlx::Postgres> for $name {
            fn type_info() -> sqlx::postgres::PgTypeInfo {
                <String as sqlx::Type<sqlx::Postgres>>::type_info()
            }

            fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
                <String as sqlx::Type<sqlx::Postgres>>::compatible(ty)
            }
        }

        impl sqlx::postgres::PgHasArrayType for $name {
            fn array_type_info() -> sqlx::postgres::PgTypeInfo {
                <String as sqlx::postgres::PgHasArrayType>::array_type_info()
            }
        }

        impl sqlx::Encode<'_, sqlx::Postgres> for $name {
            fn encode_by_ref(
                &self,
                buf: &mut sqlx::postgres::PgArgumentBuffer,
            ) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Send + Sync + 'static>>
            {
                <String as sqlx::Encode<sqlx::Postgres>>::encode_by_ref(&self.0, buf)
            }
        }

        impl sqlx::Decode<'_, sqlx::Postgres> for $name {
            fn decode(
                value: sqlx::postgres::PgValueRef<'_>,
            ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
                let s = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
                Ok(Self::new(s))
            }
        }
    };
}

// Core event types
define_string_type! {
    /// The source of an event (e.g., "fs-watcher", "terminal", "desktop")
    EventSource
}

define_string_type! {
    /// The type of an event (e.g., "file.created", "command.executed")
    EventType
}

define_string_type! {
    /// The hostname where an event occurred
    HostName
}

define_string_type! {
    /// The name of an ingestor service
    IngestorName
}

define_string_type! {
    /// The name of a processor/automaton
    ProcessorName
}

define_string_type! {
    /// A version string for a schema
    SchemaVersion
}

define_string_type! {
    /// A schema name
    SchemaName
}

// Command and shell types
define_string_type! {
    /// A command line text
    CommandText
}

define_string_type! {
    /// A shell name (e.g., "bash", "zsh", "fish")
    ShellName
}

// Network types
define_string_type! {
    /// A network hostname
    Hostname
}

define_string_type! {
    /// An IP address string
    IpAddress
}

// Git types
define_string_type! {
    /// A git commit hash
    CommitHash
}

define_string_type! {
    /// A git branch name
    BranchName
}

define_string_type! {
    /// A git remote name
    RemoteName
}

// Pattern types
define_string_type! {
    /// A glob pattern for file matching
    GlobPattern
}

define_string_type! {
    /// A regex pattern
    RegexPattern
}

// Consumer group types for processors
define_string_type! {
    /// A consumer group name for distributed processing
    ConsumerGroup
}

define_string_type! {
    /// A consumer name within a group
    ConsumerName
}

// Validation for specific types
impl EventType {
    /// Validate that the event type follows the hierarchical naming convention
    pub fn validate(&self) -> Result<(), String> {
        if self.is_empty() {
            return Err("Event type cannot be empty".to_string());
        }

        // Check for valid hierarchical format (e.g., "file.created", "command.executed")
        if !self
            .0
            .chars()
            .all(|c| c.is_ascii_lowercase() || c == '.' || c == '_' || c == '-')
        {
            return Err(
                "Event type must contain only lowercase letters, dots, underscores, and hyphens"
                    .to_string(),
            );
        }

        // Must not start or end with a dot
        if self.0.starts_with('.') || self.0.ends_with('.') {
            return Err("Event type cannot start or end with a dot".to_string());
        }

        // Must not have consecutive dots
        if self.0.contains("..") {
            return Err("Event type cannot contain consecutive dots".to_string());
        }

        Ok(())
    }
}

impl EventSource {
    /// Validate that the event source follows naming conventions
    pub fn validate(&self) -> Result<(), String> {
        if self.is_empty() {
            return Err("Event source cannot be empty".to_string());
        }

        // Check for valid format (e.g., "fs-watcher", "terminal", "desktop")
        if !self
            .0
            .chars()
            .all(|c| c.is_ascii_lowercase() || c == '-' || c == '_')
        {
            return Err(
                "Event source must contain only lowercase letters, hyphens, and underscores"
                    .to_string(),
            );
        }

        Ok(())
    }
}

impl SchemaVersion {
    /// Validate semantic version format
    pub fn validate(&self) -> Result<(), String> {
        if self.is_empty() {
            return Err("Schema version cannot be empty".to_string());
        }

        // Basic semver validation (not comprehensive)
        let parts: Vec<&str> = self.0.split('.').collect();
        if parts.len() != 3 {
            return Err("Schema version must be in format X.Y.Z".to_string());
        }

        for part in parts {
            if part.parse::<u32>().is_err() {
                return Err("Schema version parts must be numeric".to_string());
            }
        }

        Ok(())
    }
}

// Apply SQLx support to all string types used in database operations
#[cfg(feature = "sqlx")]
mod sqlx_impl {
    use super::*;

    // Event-related types
    impl_sqlx_for_string_type!(EventSource);
    impl_sqlx_for_string_type!(EventType);
    impl_sqlx_for_string_type!(HostName);
    impl_sqlx_for_string_type!(IngestorName);
    impl_sqlx_for_string_type!(ProcessorName);
    impl_sqlx_for_string_type!(SchemaVersion);
    impl_sqlx_for_string_type!(SchemaName);

    // Command and shell types
    impl_sqlx_for_string_type!(CommandText);
    impl_sqlx_for_string_type!(ShellName);

    // Network types
    impl_sqlx_for_string_type!(Hostname);
    impl_sqlx_for_string_type!(IpAddress);

    // Git types
    impl_sqlx_for_string_type!(CommitHash);
    impl_sqlx_for_string_type!(BranchName);
    impl_sqlx_for_string_type!(RemoteName);

    // Pattern types
    impl_sqlx_for_string_type!(GlobPattern);
    impl_sqlx_for_string_type!(RegexPattern);

    // Consumer group types
    impl_sqlx_for_string_type!(ConsumerGroup);
    impl_sqlx_for_string_type!(ConsumerName);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_type_creation() {
        let source = EventSource::new("fs-watcher");
        assert_eq!(source.as_str(), "fs-watcher");
        assert_eq!(source.to_string(), "fs-watcher");

        let event_type = EventType::from("file.created");
        assert_eq!(event_type.as_str(), "file.created");
    }

    #[test]
    fn test_event_type_validation() {
        // Valid event types
        assert!(EventType::new("file.created").validate().is_ok());
        assert!(EventType::new("command.executed").validate().is_ok());
        assert!(EventType::new("window.focus-changed").validate().is_ok());

        // Invalid event types
        assert!(EventType::new("").validate().is_err());
        assert!(EventType::new(".file").validate().is_err());
        assert!(EventType::new("file.").validate().is_err());
        assert!(EventType::new("file..created").validate().is_err());
        assert!(EventType::new("File.Created").validate().is_err()); // uppercase not allowed
    }

    #[test]
    fn test_event_source_validation() {
        // Valid sources
        assert!(EventSource::new("fs-watcher").validate().is_ok());
        assert!(EventSource::new("terminal").validate().is_ok());
        assert!(EventSource::new("desktop_monitor").validate().is_ok());

        // Invalid sources
        assert!(EventSource::new("").validate().is_err());
        assert!(EventSource::new("FS-Watcher").validate().is_err()); // uppercase not allowed
        assert!(EventSource::new("fs watcher").validate().is_err()); // spaces not allowed
    }

    #[test]
    fn test_schema_version_validation() {
        // Valid versions
        assert!(SchemaVersion::new("1.0.0").validate().is_ok());
        assert!(SchemaVersion::new("0.1.0").validate().is_ok());
        assert!(SchemaVersion::new("10.20.30").validate().is_ok());

        // Invalid versions
        assert!(SchemaVersion::new("").validate().is_err());
        assert!(SchemaVersion::new("1.0").validate().is_err());
        assert!(SchemaVersion::new("1.0.0.0").validate().is_err());
        assert!(SchemaVersion::new("1.0.alpha").validate().is_err());
    }

    #[test]
    fn test_type_safety() {
        let source = EventSource::new("test");
        let event_type = EventType::new("test");

        // This would fail to compile if uncommented:
        // let _wrong: EventSource = event_type;

        // But we can compare the underlying strings if needed
        assert_eq!(source.as_str(), event_type.as_str());
    }
}

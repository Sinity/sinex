//! Domain-specific typed strings for the Sinex system
//!
//! This module provides strongly-typed string wrappers to prevent
//! accidental mixing of different string types (e.g., EventSource vs EventType).

use camino::{Utf8Path, Utf8PathBuf};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;

/// Macro to define a new string type with common implementations
macro_rules! define_string_type {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Cow<'static, str>);

        impl $name {
            /// Create a new instance from a string
            pub fn new(s: impl Into<String>) -> Self {
                Self(Cow::Owned(s.into()))
            }

            /// Create a const instance from a static string
            pub const fn from_static(s: &'static str) -> Self {
                Self(Cow::Borrowed(s))
            }

            /// Get the underlying string
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Convert to owned String
            pub fn into_string(self) -> String {
                self.0.into_owned()
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
                Self(Cow::Owned(s))
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                Self(Cow::Owned(s.to_string()))
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

/// Macro to define a new string type that requires validation
/// This version has a fallible FromStr implementation
macro_rules! define_validated_string_type {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Cow<'static, str>);

        impl $name {
            /// Create a new instance from a string without validation
            /// For validated creation, use FromStr::from_str
            pub fn new(s: impl Into<String>) -> Self {
                Self(Cow::Owned(s.into()))
            }

            /// Create a new instance from a string without validation
            /// Alias for new() for clarity
            pub fn new_unchecked(s: impl Into<String>) -> Self {
                Self::new(s)
            }

            /// Create a const instance from a static string
            pub const fn from_static(s: &'static str) -> Self {
                Self(Cow::Borrowed(s))
            }

            /// Get the underlying string
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Convert to owned String
            pub fn into_string(self) -> String {
                self.0.into_owned()
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

        // FromStr implementation will be provided by the specific type
        // This allows for validation in the FromStr impl

        impl From<String> for $name {
            fn from(s: String) -> Self {
                Self(Cow::Owned(s))
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
                <&str as sqlx::Encode<sqlx::Postgres>>::encode_by_ref(&self.0.as_ref(), buf)
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
define_string_type!(
    #[doc = "The source of an event (e.g., `fs-watcher`, `terminal`, `desktop`)"]
    EventSource
);

define_string_type!(
    #[doc = "The type of an event (e.g., `file.created`, `command.executed`)"]
    EventType
);

define_string_type!(
    #[doc = "The hostname where an event occurred"]
    HostName
);

define_string_type!(
    #[doc = "The name of an ingestor service"]
    IngestorName
);

define_string_type!(
    #[doc = "The name of a processor/automaton"]
    ProcessorName
);

define_string_type!(
    #[doc = "A version string for a schema"]
    SchemaVersion
);

define_string_type!(
    #[doc = "A schema name"]
    SchemaName
);

// Command and shell types
define_string_type!(
    #[doc = "A command line text"]
    CommandText
);

define_string_type!(
    #[doc = "A shell name (e.g., `bash`, `zsh`, `fish`)"]
    ShellName
);

// Network types
define_string_type!(
    #[doc = "A network hostname"]
    Hostname
);

define_string_type!(
    #[doc = "An IP address string"]
    IpAddress
);

// Git types
define_string_type!(
    #[doc = "A git commit hash"]
    CommitHash
);

define_string_type!(
    #[doc = "A git branch name"]
    BranchName
);

define_string_type!(
    #[doc = "A git remote name"]
    RemoteName
);

// Pattern types
define_string_type!(
    #[doc = "A glob pattern for file matching"]
    GlobPattern
);

define_string_type!(
    #[doc = "A regex pattern"]
    RegexPattern
);

// Consumer group types for processors
define_string_type!(
    #[doc = "A consumer group name for distributed processing"]
    ConsumerGroup
);

define_string_type!(
    #[doc = "A consumer name within a group"]
    ConsumerName
);

// Path and URI types
define_validated_string_type!(
    #[doc = "A path that has been validated and cleaned"]
    SanitizedPath
);

define_validated_string_type!(
    #[doc = "A path known to be relative"]
    RelativePath
);

define_validated_string_type!(
    #[doc = "A URI that is guaranteed to be absolute"]
    AbsoluteUri
);

// Hash types
define_validated_string_type!(
    #[doc = "BLAKE3 hash (64 hex characters)"]
    Blake3Hash
);

define_validated_string_type!(
    #[doc = "SHA256 hash (64 hex characters)"]
    Sha256Hash
);

// Semantic identifiers
define_string_type!(
    #[doc = "Service identification"]
    ServiceName
);

define_string_type!(
    #[doc = "Background job identifiers"]
    JobId
);

define_validated_string_type!(
    #[doc = "Git-annex keys"]
    AnnexKey
);

define_validated_string_type!(
    #[doc = "NATS subjects"]
    NatsSubject
);

// Validation for specific types
impl EventType {
    /// Validate that the event type follows the hierarchical naming convention
    pub fn validate(&self) -> Result<(), String> {
        if self.is_empty() {
            return Err("Event type cannot be empty".into());
        }

        // Check for valid hierarchical format (e.g., "file.created", "command.executed")
        if !self
            .0
            .chars()
            .all(|c| c.is_ascii_lowercase() || c == '.' || c == '_' || c == '-')
        {
            return Err(
                "Event type must contain only lowercase letters, dots, underscores, and hyphens"
                    .into(),
            );
        }

        // Must not start or end with a dot
        if self.0.starts_with('.') || self.0.ends_with('.') {
            return Err("Event type cannot start or end with a dot".into());
        }

        // Must not have consecutive dots
        if self.0.contains("..") {
            return Err("Event type cannot contain consecutive dots".into());
        }

        Ok(())
    }
}

impl EventSource {
    /// Validate that the event source follows naming conventions
    pub fn validate(&self) -> Result<(), String> {
        if self.is_empty() {
            return Err("Event source cannot be empty".into());
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
            return Err("Schema version cannot be empty".into());
        }

        // Basic semver validation (not comprehensive)
        let parts: Vec<&str> = self.0.split('.').collect();
        if parts.len() != 3 {
            return Err("Schema version must be in format X.Y.Z".into());
        }

        for part in parts {
            if part.parse::<u32>().is_err() {
                return Err("Schema version parts must be numeric".into());
            }
        }

        Ok(())
    }
}

// Custom implementations for types with validation

impl SanitizedPath {
    /// Validate and create a sanitized path
    pub fn validate(path: &str) -> Result<Utf8PathBuf, String> {
        if path.is_empty() {
            return Err("Path cannot be empty".into());
        }

        // Check for directory traversal attempts
        if path.contains("..") {
            return Err("Path cannot contain directory traversal sequences (..)".into());
        }

        // Ensure path is valid UTF-8 by parsing it
        let utf8_path = Utf8Path::new(path);
        let canonical = utf8_path
            .canonicalize_utf8()
            .map_err(|e| format!("Failed to canonicalize path: {}", e))?;

        Ok(canonical)
    }

    /// Create a validated sanitized path from a string
    pub fn from_str_validated(s: &str) -> Result<Self, String> {
        let validated_path = Self::validate(s)?;
        Ok(Self(Cow::Owned(validated_path.to_string())))
    }
}

impl FromStr for SanitizedPath {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_validated(s)
    }
}

impl RelativePath {
    /// Validate that the path is relative
    pub fn validate(path: &str) -> Result<(), String> {
        if path.is_empty() {
            return Err("Path cannot be empty".into());
        }

        let utf8_path = Utf8Path::new(path);
        if utf8_path.is_absolute() {
            return Err("Path must be relative".into());
        }

        // Check for directory traversal attempts
        if path.contains("..") {
            return Err("Path cannot contain directory traversal sequences (..)".into());
        }

        Ok(())
    }
}

impl FromStr for RelativePath {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::validate(s)?;
        Ok(Self(Cow::Owned(s.to_string())))
    }
}

impl AbsoluteUri {
    /// Validate that the URI is absolute
    pub fn validate(uri: &str) -> Result<(), String> {
        if uri.is_empty() {
            return Err("URI cannot be empty".into());
        }

        use url::Url;
        let parsed = Url::parse(uri).map_err(|e| format!("Invalid URI: {}", e))?;

        if !parsed.scheme().is_empty() && parsed.cannot_be_a_base() {
            return Err("URI must be absolute".into());
        }

        Ok(())
    }
}

impl FromStr for AbsoluteUri {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::validate(s)?;
        Ok(Self(Cow::Owned(s.to_string())))
    }
}

impl Blake3Hash {
    /// Validate BLAKE3 hash format (64 hex characters)
    pub fn validate(hash: &str) -> Result<(), String> {
        if hash.is_empty() {
            return Err("BLAKE3 hash cannot be empty".into());
        }

        if hash.len() != 64 {
            return Err("BLAKE3 hash must be exactly 64 characters".into());
        }

        if !hash.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err("BLAKE3 hash must contain only hexadecimal characters".into());
        }

        Ok(())
    }
}

impl FromStr for Blake3Hash {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::validate(s)?;
        Ok(Self(Cow::Owned(s.to_lowercase())))
    }
}

impl Sha256Hash {
    /// Validate SHA256 hash format (64 hex characters)
    pub fn validate(hash: &str) -> Result<(), String> {
        if hash.is_empty() {
            return Err("SHA256 hash cannot be empty".into());
        }

        if hash.len() != 64 {
            return Err("SHA256 hash must be exactly 64 characters".into());
        }

        if !hash.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err("SHA256 hash must contain only hexadecimal characters".into());
        }

        Ok(())
    }
}

impl FromStr for Sha256Hash {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::validate(s)?;
        Ok(Self(Cow::Owned(s.to_lowercase())))
    }
}

impl AnnexKey {
    /// Parse git-annex key format: BACKEND-sNNN-mEEE--KEYNAME
    pub fn parse_components(&self) -> Option<(String, Option<u64>, Option<u64>, String)> {
        let key_str = self.as_str();

        // Find the double dash separator
        let double_dash_pos = key_str.rfind("--")?;
        let (prefix, key_name) = key_str.split_at(double_dash_pos);
        let key_name = &key_name[2..]; // Remove the "--"

        // Parse the prefix: BACKEND-sNNN-mEEE
        let parts: Vec<&str> = prefix.split('-').collect();
        if parts.is_empty() {
            return None;
        }

        let backend = parts[0].to_string();
        let mut size = None;
        let mut mtime = None;

        // Parse optional size and mtime components
        for part in &parts[1..] {
            if let Some(size_str) = part.strip_prefix('s') {
                if let Ok(s) = size_str.parse() {
                    size = Some(s);
                }
            } else if let Some(mtime_str) = part.strip_prefix('m') {
                if let Ok(m) = mtime_str.parse() {
                    mtime = Some(m);
                }
            }
        }

        Some((backend, size, mtime, key_name.to_string()))
    }

    /// Validate git-annex key format
    pub fn validate(key: &str) -> Result<(), String> {
        if key.is_empty() {
            return Err("Annex key cannot be empty".into());
        }

        if !key.contains("--") {
            return Err("Annex key must contain double dash separator".into());
        }

        // Basic structure validation - more detailed parsing available via parse_components
        let parts: Vec<&str> = key.split("--").collect();
        if parts.len() != 2 {
            return Err("Annex key must have exactly one double dash separator".into());
        }

        if parts[0].is_empty() || parts[1].is_empty() {
            return Err("Annex key prefix and suffix cannot be empty".into());
        }

        Ok(())
    }
}

impl FromStr for AnnexKey {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::validate(s)?;
        Ok(Self(Cow::Owned(s.to_string())))
    }
}

impl NatsSubject {
    /// Validate NATS subject format
    pub fn validate(subject: &str) -> Result<(), String> {
        if subject.is_empty() {
            return Err("NATS subject cannot be empty".into());
        }

        // NATS subjects can contain letters, digits, and dots
        // They cannot start or end with dots, and cannot have consecutive dots
        if subject.starts_with('.') || subject.ends_with('.') {
            return Err("NATS subject cannot start or end with a dot".into());
        }

        if subject.contains("..") {
            return Err("NATS subject cannot contain consecutive dots".into());
        }

        // Check for valid characters
        if !subject
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_')
        {
            return Err(
                "NATS subject can only contain letters, digits, dots, hyphens, and underscores"
                    .into(),
            );
        }

        Ok(())
    }
}

impl FromStr for NatsSubject {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::validate(s)?;
        Ok(Self(Cow::Owned(s.to_string())))
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

    // Path and URI types
    impl_sqlx_for_string_type!(SanitizedPath);
    impl_sqlx_for_string_type!(RelativePath);
    impl_sqlx_for_string_type!(AbsoluteUri);

    // Hash types
    impl_sqlx_for_string_type!(Blake3Hash);
    impl_sqlx_for_string_type!(Sha256Hash);

    // Semantic identifiers
    impl_sqlx_for_string_type!(ServiceName);
    impl_sqlx_for_string_type!(JobId);
    impl_sqlx_for_string_type!(AnnexKey);
    impl_sqlx_for_string_type!(NatsSubject);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::events::EventPayload;

    #[test]
    fn test_string_type_creation() {
        let source = crate::events::payloads::filesystem::FileCreatedPayload::SOURCE;
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
        assert!(
            crate::events::payloads::filesystem::FileCreatedPayload::SOURCE
                .validate()
                .is_ok()
        );
        assert!(
            crate::events::payloads::shell::TerminalMonitoringStartedPayload::SOURCE
                .validate()
                .is_ok()
        );
        assert!(
            crate::events::payloads::desktop::DesktopMonitoringStartedPayload::SOURCE
                .validate()
                .is_ok()
        );

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

    #[test]
    fn test_sanitized_path_validation() {
        // Valid paths should work (in test environment, assuming /tmp exists)
        // Note: In real environments this would do actual path canonicalization
        // assert!(SanitizedPath::from_str("/tmp").is_ok());

        // Invalid paths
        assert!(SanitizedPath::from_str("").is_err());
        assert!(SanitizedPath::from_str("../etc/passwd").is_err());
        assert!(SanitizedPath::from_str("/path/with/../traversal").is_err());
    }

    #[test]
    fn test_relative_path_validation() {
        // Valid relative paths
        assert!(RelativePath::from_str("file.txt").is_ok());
        assert!(RelativePath::from_str("dir/file.txt").is_ok());
        assert!(RelativePath::from_str("./file.txt").is_ok());

        // Invalid relative paths
        assert!(RelativePath::from_str("").is_err());
        assert!(RelativePath::from_str("/absolute/path").is_err());
        assert!(RelativePath::from_str("../parent").is_err());
    }

    #[test]
    fn test_absolute_uri_validation() {
        // Valid absolute URIs
        assert!(AbsoluteUri::from_str("https://example.com").is_ok());
        assert!(AbsoluteUri::from_str("file:///path/to/file").is_ok());
        assert!(AbsoluteUri::from_str("postgresql://user:pass@host:5432/db").is_ok());

        // Invalid URIs
        assert!(AbsoluteUri::from_str("").is_err());
        assert!(AbsoluteUri::from_str("not-a-uri").is_err());
        assert!(AbsoluteUri::from_str("relative/path").is_err());
    }

    #[test]
    fn test_blake3_hash_validation() {
        // Valid BLAKE3 hash (64 hex chars)
        let valid_hash = "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3";
        assert!(Blake3Hash::from_str(valid_hash).is_ok());
        assert!(Blake3Hash::from_str(&valid_hash.to_uppercase()).is_ok());

        // Invalid BLAKE3 hashes
        assert!(Blake3Hash::from_str("").is_err());
        assert!(Blake3Hash::from_str("too_short").is_err());
        assert!(Blake3Hash::from_str(
            "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3X"
        )
        .is_err()); // 65 chars
        assert!(Blake3Hash::from_str(
            "g665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3"
        )
        .is_err()); // invalid hex char

        // Verify normalization to lowercase
        let hash = Blake3Hash::from_str(&valid_hash.to_uppercase()).unwrap();
        assert_eq!(hash.as_str(), valid_hash);
    }

    #[test]
    fn test_sha256_hash_validation() {
        // Valid SHA256 hash (64 hex chars)
        let valid_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert!(Sha256Hash::from_str(valid_hash).is_ok());
        assert!(Sha256Hash::from_str(&valid_hash.to_uppercase()).is_ok());

        // Invalid SHA256 hashes
        assert!(Sha256Hash::from_str("").is_err());
        assert!(Sha256Hash::from_str("too_short").is_err());
        assert!(Sha256Hash::from_str(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855X"
        )
        .is_err()); // 65 chars
        assert!(Sha256Hash::from_str(
            "g3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )
        .is_err()); // invalid hex char

        // Verify normalization to lowercase
        let hash = Sha256Hash::from_str(&valid_hash.to_uppercase()).unwrap();
        assert_eq!(hash.as_str(), valid_hash);
    }

    #[test]
    fn test_annex_key_validation() {
        // Valid annex keys
        assert!(AnnexKey::from_str("SHA256E-s12345--filename.txt").is_ok());
        assert!(AnnexKey::from_str("BLAKE2B--somefile").is_ok());
        assert!(AnnexKey::from_str("SHA1-s1024-m1234567890--document.pdf").is_ok());

        // Invalid annex keys
        assert!(AnnexKey::from_str("").is_err());
        assert!(AnnexKey::from_str("no-double-dash").is_err());
        assert!(AnnexKey::from_str("--no-prefix").is_err());
        assert!(AnnexKey::from_str("prefix--").is_err());
        assert!(AnnexKey::from_str("multiple--double--dashes").is_err());
    }

    #[test]
    fn test_annex_key_parsing() {
        let key = AnnexKey::from_str("SHA256E-s12345-m1234567890--filename.txt").unwrap();
        let (backend, size, mtime, filename) = key.parse_components().unwrap();

        assert_eq!(backend, "SHA256E");
        assert_eq!(size, Some(12345));
        assert_eq!(mtime, Some(1234567890));
        assert_eq!(filename, "filename.txt");

        // Test key without optional components
        let simple_key = AnnexKey::from_str("BLAKE2B--document.pdf").unwrap();
        let (backend, size, mtime, filename) = simple_key.parse_components().unwrap();

        assert_eq!(backend, "BLAKE2B");
        assert_eq!(size, None);
        assert_eq!(mtime, None);
        assert_eq!(filename, "document.pdf");
    }

    #[test]
    fn test_nats_subject_validation() {
        // Valid NATS subjects
        assert!(NatsSubject::from_str("events").is_ok());
        assert!(NatsSubject::from_str("events.filesystem").is_ok());
        assert!(NatsSubject::from_str("events.filesystem.file-created").is_ok());
        assert!(NatsSubject::from_str("system_monitor.cpu_usage").is_ok());

        // Invalid NATS subjects
        assert!(NatsSubject::from_str("").is_err());
        assert!(NatsSubject::from_str(".events").is_err());
        assert!(NatsSubject::from_str("events.").is_err());
        assert!(NatsSubject::from_str("events..filesystem").is_err());
        assert!(NatsSubject::from_str("events.file system").is_err()); // space not allowed
        assert!(NatsSubject::from_str("events.file@system").is_err()); // @ not allowed
    }

    #[test]
    fn test_service_name_and_job_id() {
        // These use the basic string type without additional validation
        assert!(ServiceName::from_str("sinex-ingestd").is_ok());
        assert!(ServiceName::from_str("fs-watcher").is_ok());
        assert!(JobId::from_str("job_12345").is_ok());
        assert!(JobId::from_str("background-task-001").is_ok());
    }
}

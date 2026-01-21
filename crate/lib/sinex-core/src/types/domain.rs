//! Domain-specific typed strings for the Sinex system
//!
//! This module provides strongly-typed string wrappers to prevent
//! accidental mixing of different string types (e.g., EventSource vs EventType).

use camino::{Utf8Path, Utf8PathBuf};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;

/// Macro to define a new string type with common implementations
macro_rules! define_string_type {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
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
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
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

// Macro to add SQLx support for string types (unvalidated)
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

// Macro to add SQLx support for validated string types (uses FromStr)
#[cfg(feature = "sqlx")]
macro_rules! impl_sqlx_for_validated_string_type {
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
                Self::from_str(&s).map_err(|e| {
                    Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
                        as Box<dyn std::error::Error + Send + Sync>
                })
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
    /// Validate and create a sanitized path using lexical cleaning
    pub fn validate(path: &str) -> Result<Utf8PathBuf, String> {
        if path.is_empty() {
            return Err("Path cannot be empty".into());
        }

        // Reject inputs containing traversal sequences upfront to be conservative
        if path.contains("..") {
            return Err("Path contains directory traversal sequences".into());
        }

        // Parse as UTF-8 path for validation
        let utf8_path = Utf8Path::new(path);

        // Lexically clean the path without filesystem access
        let cleaned = normalize_path_lexically(utf8_path);

        // Check for directory traversal after normalization
        if path_contains_traversal(&cleaned) {
            return Err("Path contains directory traversal sequences".into());
        }

        // Check for null bytes which could be used for path injection
        if path.contains('\0') {
            return Err("Path cannot contain null bytes".into());
        }

        Ok(cleaned)
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
        let parsed = Url::parse(uri).map_err(|e| format!("Invalid URI: {e}"))?;

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

        // Check for obviously invalid patterns
        let lower_hash = hash.to_lowercase();
        if lower_hash.chars().all(|c| c == '0') {
            return Err("Hash appears to be a zero placeholder".into());
        }
        if lower_hash.chars().all(|c| c == 'f') {
            return Err("Hash appears to be an all-F placeholder".into());
        }

        // Check for suspiciously repetitive patterns (same character repeating)
        let mut prev_char = '\0';
        let mut same_char_count = 0;
        let mut max_same_char_run = 0;
        for c in lower_hash.chars() {
            if c == prev_char {
                same_char_count += 1;
                max_same_char_run = max_same_char_run.max(same_char_count);
            } else {
                same_char_count = 1;
                prev_char = c;
            }
        }
        if max_same_char_run > 8 {
            return Err("Hash contains suspiciously long runs of the same character".into());
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

        // Check for obviously invalid patterns
        let lower_hash = hash.to_lowercase();
        if lower_hash.chars().all(|c| c == '0') {
            return Err("Hash appears to be a zero placeholder".into());
        }
        if lower_hash.chars().all(|c| c == 'f') {
            return Err("Hash appears to be an all-F placeholder".into());
        }

        // Check for suspiciously repetitive patterns (same character repeating)
        let mut prev_char = '\0';
        let mut same_char_count = 0;
        let mut max_same_char_run = 0;
        for c in lower_hash.chars() {
            if c == prev_char {
                same_char_count += 1;
                max_same_char_run = max_same_char_run.max(same_char_count);
            } else {
                same_char_count = 1;
                prev_char = c;
            }
        }
        if max_same_char_run > 8 {
            return Err("Hash contains suspiciously long runs of the same character".into());
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

    // Event-related types (unvalidated)
    impl_sqlx_for_string_type!(EventSource);
    impl_sqlx_for_string_type!(EventType);
    impl_sqlx_for_string_type!(HostName);
    impl_sqlx_for_string_type!(IngestorName);
    impl_sqlx_for_string_type!(ProcessorName);
    impl_sqlx_for_string_type!(SchemaVersion);
    impl_sqlx_for_string_type!(SchemaName);

    // Command and shell types (unvalidated)
    impl_sqlx_for_string_type!(CommandText);
    impl_sqlx_for_string_type!(ShellName);

    // Network types (unvalidated)
    impl_sqlx_for_string_type!(Hostname);
    impl_sqlx_for_string_type!(IpAddress);

    // Git types (unvalidated)
    impl_sqlx_for_string_type!(CommitHash);
    impl_sqlx_for_string_type!(BranchName);
    impl_sqlx_for_string_type!(RemoteName);

    // Pattern types (unvalidated)
    impl_sqlx_for_string_type!(GlobPattern);
    impl_sqlx_for_string_type!(RegexPattern);

    // Consumer group types (unvalidated)
    impl_sqlx_for_string_type!(ConsumerGroup);
    impl_sqlx_for_string_type!(ConsumerName);

    // Path and URI types (validated)
    impl_sqlx_for_validated_string_type!(SanitizedPath);
    impl_sqlx_for_validated_string_type!(RelativePath);
    impl_sqlx_for_validated_string_type!(AbsoluteUri);

    // Hash types (validated)
    impl_sqlx_for_validated_string_type!(Blake3Hash);
    impl_sqlx_for_validated_string_type!(Sha256Hash);

    // Semantic identifiers
    impl_sqlx_for_string_type!(ServiceName);
    impl_sqlx_for_string_type!(JobId);

    // Validated semantic identifiers
    impl_sqlx_for_validated_string_type!(AnnexKey);
    impl_sqlx_for_validated_string_type!(NatsSubject);
}

/// Helper function to normalize a path lexically (without filesystem access)
fn normalize_path_lexically(path: &Utf8Path) -> Utf8PathBuf {
    let mut components: Vec<String> = Vec::new();
    let mut is_absolute = path.is_absolute();

    for component in path.components() {
        match component {
            camino::Utf8Component::Normal(name) => {
                if name == ".." {
                    // Pop the last component if it's not a ".." itself
                    if let Some(last) = components.last() {
                        if *last != ".." {
                            components.pop();
                            continue;
                        }
                    }
                } else if name == "." {
                    // Skip current directory references
                    continue;
                }
                components.push(name.to_string());
            }
            camino::Utf8Component::RootDir => {
                components.clear();
                is_absolute = true;
            }
            camino::Utf8Component::CurDir => {
                // Skip current directory references
                continue;
            }
            camino::Utf8Component::ParentDir => {
                // Treat as ".." component
                if let Some(last) = components.last() {
                    if last != ".." {
                        components.pop();
                        continue;
                    }
                }
                components.push("..".to_string());
            }
            camino::Utf8Component::Prefix(_) => {
                // Handle Windows prefixes by keeping them
                components.push(component.as_str().to_string());
            }
        }
    }

    if components.is_empty() {
        if is_absolute {
            Utf8PathBuf::from("/")
        } else {
            Utf8PathBuf::from(".")
        }
    } else if is_absolute {
        Utf8PathBuf::from(format!("/{}", components.join("/")))
    } else {
        Utf8PathBuf::from(components.join("/"))
    }
}

/// Check if a normalized path contains directory traversal attempts
fn path_contains_traversal(path: &Utf8PathBuf) -> bool {
    let path_str = path.as_str();

    // Check for obvious traversal patterns
    if path_str.contains("../") || path_str.starts_with("..") || path_str.ends_with("..") {
        return true;
    }

    // Check for components that are exactly ".."
    for component in path.components() {
        if let camino::Utf8Component::ParentDir = component {
            return true;
        }
        if let camino::Utf8Component::Normal(name) = component {
            if name == ".." {
                return true;
            }
        }
    }

    false
}

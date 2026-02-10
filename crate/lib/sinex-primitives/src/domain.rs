//! Domain-specific typed strings for the Sinex system
//!
//! This module provides strongly-typed string wrappers to prevent
//! accidental mixing of different string types (e.g., `EventSource` vs `EventType`).

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
/// This version has a fallible `FromStr` implementation
macro_rules! define_validated_string_type {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
        #[serde(transparent)]
        pub struct $name(Cow<'static, str>);

        impl $name {
            /// Create a new instance from a string without validation
            ///
            /// # Safety
            /// This bypasses validation. Prefer `from_str` for untrusted input.
            #[deprecated(note = "Use from_str for validation, or new_unchecked if you are sure.")]
            pub fn new(s: impl Into<String>) -> Self {
                Self(Cow::Owned(s.into()))
            }

            /// Create a new instance from a string without validation
            /// Alias for new() for clarity, but explicit about bypassing checks.
            pub fn new_unchecked(s: impl Into<String>) -> Self {
                #[allow(deprecated)]
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
                <Self as std::str::FromStr>::from_str(&s).map_err(|e| {
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

// ─────────────────────────────────────────────────────────────
// Coordination and Node Types
// ─────────────────────────────────────────────────────────────

define_string_type!(
    #[doc = "A unique identifier for a node instance"]
    NodeId
);

define_string_type!(
    #[doc = "A unique identifier for a distributed instance (used in leader election)"]
    InstanceId
);

define_string_type!(
    #[doc = "The type of relationship between entities (e.g., `works_on`, `mentions`, `depends_on`)"]
    RelationType
);

define_string_type!(
    #[doc = "The type of an entity (e.g., `person`, `project`, `document`)"]
    EntityTypeName
);

define_string_type!(
    #[doc = "User identifier for attribution"]
    UserId
);

/// State of a processing node
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum NodeState {
    /// Node is actively processing events
    Running,
    /// Node is gracefully stopping (finishing current work)
    Draining,
    /// Node is paused and not processing
    Paused,
    /// Node has encountered a fatal error
    Failed,
    /// Node state is unknown
    #[default]
    Unknown,
}

impl std::fmt::Display for NodeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Running => write!(f, "running"),
            Self::Draining => write!(f, "draining"),
            Self::Paused => write!(f, "paused"),
            Self::Failed => write!(f, "failed"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

impl std::str::FromStr for NodeState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "running" => Ok(Self::Running),
            "draining" => Ok(Self::Draining),
            "paused" => Ok(Self::Paused),
            "failed" => Ok(Self::Failed),
            "unknown" => Ok(Self::Unknown),
            _ => Err(format!("unknown node state: {s}")),
        }
    }
}

/// Result status of an operation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    /// Operation completed successfully
    Success,
    /// Operation failed
    Failed,
    /// Operation is pending/in progress
    Pending,
}

impl std::fmt::Display for OperationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Success => write!(f, "success"),
            Self::Failed => write!(f, "failed"),
            Self::Pending => write!(f, "pending"),
        }
    }
}

impl std::str::FromStr for OperationStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "success" | "ok" => Ok(Self::Success),
            "failed" | "error" => Ok(Self::Failed),
            "pending" | "in_progress" => Ok(Self::Pending),
            _ => Err(format!("unknown operation status: {s}")),
        }
    }
}

/// Type of node in the system
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NodeType {
    /// Ingestor node (captures events from external sources)
    Ingestor,
    /// Automaton node (processes events and generates derived data)
    Automaton,
    /// Service node (provides API endpoints)
    Service,
    /// Processor node (transforms events)
    Processor,
}

impl std::fmt::Display for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ingestor => write!(f, "ingestor"),
            Self::Automaton => write!(f, "automaton"),
            Self::Service => write!(f, "service"),
            Self::Processor => write!(f, "processor"),
        }
    }
}

impl std::str::FromStr for NodeType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "ingestor" => Ok(Self::Ingestor),
            "automaton" => Ok(Self::Automaton),
            "service" => Ok(Self::Service),
            "processor" => Ok(Self::Processor),
            _ => Err(format!("unknown node type: {s}")),
        }
    }
}

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

// ─────────────────────────────────────────────────────────────
// SQLx Feature Support
// ─────────────────────────────────────────────────────────────

#[cfg(feature = "sqlx")]
mod sqlx_impls {
    use super::{
        AnnexKey, BranchName, CommandText, CommitHash, ConsumerGroup, ConsumerName, EntityTypeName,
        EventSource, EventType, GlobPattern, HostName, Hostname, IngestorName, InstanceId,
        IpAddress, JobId, NatsSubject, NodeId, ProcessorName, RegexPattern, RelationType,
        RemoteName, SanitizedPath, SchemaName, SchemaVersion, ServiceName, ShellName, UserId,
    };

    // Register string types without validation
    impl_sqlx_for_string_type!(EventSource);
    impl_sqlx_for_string_type!(EventType);
    impl_sqlx_for_string_type!(HostName);
    impl_sqlx_for_string_type!(IngestorName);
    impl_sqlx_for_string_type!(ProcessorName);
    impl_sqlx_for_string_type!(SchemaVersion);
    impl_sqlx_for_string_type!(SchemaName);
    impl_sqlx_for_string_type!(CommandText);
    impl_sqlx_for_string_type!(ShellName);
    impl_sqlx_for_string_type!(Hostname);
    impl_sqlx_for_string_type!(IpAddress);
    impl_sqlx_for_string_type!(CommitHash);
    impl_sqlx_for_string_type!(BranchName);
    impl_sqlx_for_string_type!(RemoteName);
    impl_sqlx_for_string_type!(GlobPattern);
    impl_sqlx_for_string_type!(RegexPattern);
    impl_sqlx_for_string_type!(ConsumerGroup);
    impl_sqlx_for_string_type!(ConsumerName);
    impl_sqlx_for_string_type!(ServiceName);
    impl_sqlx_for_string_type!(JobId);
    impl_sqlx_for_string_type!(NodeId);
    impl_sqlx_for_string_type!(InstanceId);
    impl_sqlx_for_string_type!(RelationType);
    impl_sqlx_for_string_type!(EntityTypeName);
    impl_sqlx_for_string_type!(UserId);

    // Register validated string types
    impl_sqlx_for_validated_string_type!(SanitizedPath);
    impl_sqlx_for_validated_string_type!(AnnexKey);
    impl_sqlx_for_validated_string_type!(NatsSubject);
}

// ─────────────────────────────────────────────────────────────
// Helper Functions
// ─────────────────────────────────────────────────────────────

fn normalize_path_lexically(path: &Utf8Path) -> Utf8PathBuf {
    let mut components: Vec<String> = Vec::new();
    let mut is_absolute = path.is_absolute();

    for component in path.components() {
        match component {
            camino::Utf8Component::Normal(name) => {
                if name == ".." {
                    // Pop the last component if it's not a ".." itself
                    if let Some(last) = components.last() {
                        if *last == ".." {
                            components.push(name.to_string());
                        } else {
                            components.pop();
                        }
                    } else {
                        components.push(name.to_string());
                    }
                } else if name != "." {
                    // Skip current directory references
                    components.push(name.to_string());
                }
            }
            camino::Utf8Component::RootDir => {
                components.clear();
                is_absolute = true;
            }
            camino::Utf8Component::CurDir => {
                // Skip current directory references
            }
            camino::Utf8Component::ParentDir => {
                // Treat as ".." component
                if let Some(last) = components.last() {
                    if last == ".." {
                        components.push("..".to_string());
                    } else {
                        components.pop();
                    }
                } else {
                    components.push("..".to_string());
                }
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
        if component == camino::Utf8Component::ParentDir {
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

impl AnnexKey {
    /// Validate git-annex key format.
    ///
    /// Git-annex keys have the form `BACKEND[-sNNN][-mNNN]--FILENAME`, where
    /// `--` separates the backend/metadata prefix from the key name.
    pub fn validate(key: &str) -> Result<(), String> {
        if key.is_empty() {
            return Err("Annex key cannot be empty".into());
        }

        // Must contain exactly one `--` separator
        let parts: Vec<&str> = key.splitn(3, "--").collect();
        if parts.len() < 2 {
            return Err("Annex key must contain '--' separator".into());
        }
        if parts[0].is_empty() {
            return Err("Annex key must have a backend prefix before '--'".into());
        }
        if parts[1].is_empty() {
            return Err("Annex key must have a name after '--'".into());
        }
        // Reject multiple `--` separators
        if parts.len() > 2 {
            return Err("Annex key must contain exactly one '--' separator".into());
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
    /// Validate NATS subject format.
    ///
    /// NATS subjects are dot-delimited hierarchies (e.g. `events.filesystem.created`).
    /// Each segment must be non-empty and contain only alphanumeric, hyphen, or underscore.
    pub fn validate(subject: &str) -> Result<(), String> {
        if subject.is_empty() {
            return Err("NATS subject cannot be empty".into());
        }
        if subject.starts_with('.') {
            return Err("NATS subject cannot start with '.'".into());
        }
        if subject.ends_with('.') {
            return Err("NATS subject cannot end with '.'".into());
        }
        if subject.contains("..") {
            return Err("NATS subject cannot contain empty segments ('..')".into());
        }
        for segment in subject.split('.') {
            if segment.is_empty() {
                return Err("NATS subject segments cannot be empty".into());
            }
            for ch in segment.chars() {
                if !ch.is_alphanumeric() && ch != '-' && ch != '_' && ch != '*' && ch != '>' {
                    return Err(format!(
                        "NATS subject segment contains invalid character '{ch}'"
                    ));
                }
            }
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

/// Marker type for Entity IDs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entity;

/// Marker type for `EntityRelation` IDs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityRelation;

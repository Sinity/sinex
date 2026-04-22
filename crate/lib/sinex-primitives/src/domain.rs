//! Domain-specific typed strings for the Sinex system
//!
//! This module provides strongly-typed string wrappers to prevent
//! accidental mixing of different string types (e.g., `EventSource` vs `EventType`).

use camino::Utf8PathBuf;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;

// ─── Compile-time validation helpers ─────────────────────────────────────────

/// Compile-time assertion: string must be non-empty and contain no null bytes.
/// Used by both `define_string_type!` and `define_validated_string_type!` macros.
/// Panics at compile time (E0080) on invalid input — zero runtime cost.
const fn const_assert_non_empty_no_nulls(_type_name: &str, s: &str) {
    let bytes = s.as_bytes();
    assert!(!bytes.is_empty(), "string type value cannot be empty");
    let mut i = 0;
    while i < bytes.len() {
        assert!(bytes[i] != 0, "string type value cannot contain null bytes");
        i += 1;
    }
}

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

            /// Create a const instance from a static string.
            ///
            /// Validated at compile time: rejects empty strings and null bytes.
            pub const fn from_static(s: &'static str) -> Self {
                const_assert_non_empty_no_nulls(stringify!($name), s);
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
            type Err = !;

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
    // Variant with custom const validator: type provides its own `from_static`
    ($(#[$meta:meta])* $name:ident, custom_from_static) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
        #[serde(transparent)]
        pub struct $name(Cow<'static, str>);

        impl $name {
            /// Create a new instance from a string without validation.
            ///
            /// Prefer `from_str` for untrusted input.
            pub fn new(s: impl Into<String>) -> Self {
                Self(Cow::Owned(s.into()))
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
    // Default variant: generates from_static with non-empty + no-null check
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
        #[serde(transparent)]
        pub struct $name(Cow<'static, str>);

        impl $name {
            /// Create a new instance from a string without validation.
            ///
            /// Prefer `from_str` for untrusted input.
            pub fn new(s: impl Into<String>) -> Self {
                Self(Cow::Owned(s.into()))
            }

            /// Create a const instance from a static string.
            ///
            /// Validated at compile time: rejects empty strings and null bytes.
            pub const fn from_static(s: &'static str) -> Self {
                const_assert_non_empty_no_nulls(stringify!($name), s);
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

// Macro to add SQLx support for enum types with Display (for encoding) and FromStr (for decoding).
// Unlike the string-type macros, this works on enums by calling Display::to_string() for encoding.
#[cfg(feature = "sqlx")]
macro_rules! impl_sqlx_for_enum_type {
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
                let s = self.to_string();
                <String as sqlx::Encode<sqlx::Postgres>>::encode_by_ref(&s, buf)
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

        // Required by sqlx::query_as! macro for TEXT → custom type mapping
        impl From<String> for $name {
            fn from(s: String) -> Self {
                <Self as std::str::FromStr>::from_str(&s).unwrap_or_else(|_| {
                    panic!("Invalid {} value from database: {:?}", stringify!($name), s)
                })
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

// ─── Core event types (parse, don't validate) ───────────────────────────────
//
// If you have an `EventSource` or `EventType`, it is valid. No unchecked
// construction exists. All runtime paths go through `new()` which validates
// and returns `Result`. The only exception is `from_static()` for compile-time
// constants (validated by tests and code review).
//
// Parse points:
//   EventSource::new("fs-watcher")?       — runtime construction
//   "fs-watcher".parse::<EventSource>()?  — FromStr (used by clap, serde, etc.)
//   EventSource::from_static("fs-watcher") — const fn, derive(EventPayload)

/// The source of an event (e.g., `fs-watcher`, `terminal`, `desktop`).
///
/// Always valid by construction. Use [`EventSource::new`] to parse a string
/// into a validated source, or [`EventSource::from_static`] for compile-time
/// constants generated by `#[derive(EventPayload)]`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct EventSource(Cow<'static, str>);

impl<'de> serde::Deserialize<'de> for EventSource {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::validate_str(&s).map_err(serde::de::Error::custom)?;
        Ok(Self(Cow::Owned(s)))
    }
}

impl EventSource {
    /// Parse a string into a validated `EventSource`.
    ///
    /// Returns an error if the value is empty or contains characters
    /// other than lowercase ASCII letters, digits, hyphens, underscores, and dots.
    pub fn new(s: impl Into<String>) -> Result<Self, crate::SinexError> {
        let s = s.into();
        Self::validate_str(&s)?;
        Ok(Self(Cow::Owned(s)))
    }

    /// Create a const instance from a static string literal.
    ///
    /// Validated at compile time — invalid values produce a compile error (E0080).
    /// Used by `#[derive(EventPayload)]` for compile-time constants.
    #[must_use]
    pub const fn from_static(s: &'static str) -> Self {
        Self::const_validate_source(s);
        Self(Cow::Borrowed(s))
    }

    /// Get the underlying string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Get the underlying `&'static str`.
    ///
    /// Only valid for values constructed with [`EventSource::from_static`].
    /// Panics at runtime if the inner value is an owned `String` (i.e., not `'static`).
    #[must_use]
    pub fn as_static_str(&self) -> &'static str {
        match &self.0 {
            Cow::Borrowed(s) => s,
            Cow::Owned(_) => unreachable!(
                "EventSource::as_static_str called on a dynamically-allocated value; use from_static for static values"
            ),
        }
    }

    /// Convert to owned `String`.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0.into_owned()
    }

    /// Check if the value is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Compile-time validation for event source strings.
    /// Panics at compile time if the string is invalid.
    #[allow(
        clippy::panic,
        reason = "const-fn validator: panic is the only error channel available"
    )]
    const fn const_validate_source(s: &str) {
        let bytes = s.as_bytes();
        assert!(!bytes.is_empty(), "EventSource cannot be empty");
        let mut i = 0;
        while i < bytes.len() {
            let b = bytes[i];
            if !((b >= b'a' && b <= b'z')
                || (b >= b'0' && b <= b'9')
                || b == b'-'
                || b == b'_'
                || b == b'.')
            {
                panic!(
                    "EventSource must contain only lowercase letters, digits, hyphens, underscores, and dots"
                );
            }
            i += 1;
        }
    }

    /// Validate that an event source string follows naming conventions.
    fn validate_str(s: &str) -> Result<(), crate::SinexError> {
        if s.is_empty() {
            return Err(crate::SinexError::validation(
                "Event source cannot be empty",
            ));
        }
        if !s.chars().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_' || c == '.'
        }) {
            return Err(
                crate::SinexError::validation(
                    "Event source must contain only lowercase letters, digits, hyphens, underscores, and dots",
                )
                .with_context("value", s),
            );
        }
        Ok(())
    }
}

impl fmt::Display for EventSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for EventSource {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::validate_str(s).map_err(|e| e.to_string())?;
        Ok(Self(Cow::Owned(s.to_string())))
    }
}

impl From<String> for EventSource {
    /// Convert a `String` to `EventSource`, panicking if invalid.
    ///
    /// Used by `sqlx::query_as!` and `.into()` conversions from trusted sources (DB rows).
    /// If the string is invalid, this indicates data corruption — panic is appropriate.
    /// For untrusted input, use [`EventSource::new`] which returns `Result`.
    #[allow(
        clippy::panic,
        reason = "Invalid DB data = corruption; panic is the intended surface"
    )]
    fn from(s: String) -> Self {
        Self::new(&s).unwrap_or_else(|_| panic!("invalid EventSource value: {s:?}"))
    }
}

impl From<&str> for EventSource {
    /// Convert a `&str` to `EventSource`, panicking if invalid.
    ///
    /// For untrusted input, use [`EventSource::new`] which returns `Result`.
    #[allow(
        clippy::panic,
        reason = "Invalid literal = programmer error; panic is the intended surface"
    )]
    fn from(s: &str) -> Self {
        Self::new(s).unwrap_or_else(|_| panic!("invalid EventSource value: {s:?}"))
    }
}

impl AsRef<str> for EventSource {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for EventSource {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// The type of an event (e.g., `file.created`, `command.executed`).
///
/// Always valid by construction. Use [`EventType::new`] to parse a string
/// into a validated event type, or [`EventType::from_static`] for compile-time
/// constants generated by `#[derive(EventPayload)]`.
///
/// Valid format: lowercase ASCII + digits + dots + underscores + hyphens,
/// no leading/trailing dots, no consecutive dots.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct EventType(Cow<'static, str>);

impl<'de> serde::Deserialize<'de> for EventType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::validate_str(&s).map_err(serde::de::Error::custom)?;
        Ok(Self(Cow::Owned(s)))
    }
}

impl EventType {
    /// Parse a string into a validated `EventType`.
    ///
    /// Returns an error if the value is empty, contains invalid characters,
    /// starts/ends with a dot, or contains consecutive dots.
    pub fn new(s: impl Into<String>) -> Result<Self, crate::SinexError> {
        let s = s.into();
        Self::validate_str(&s)?;
        Ok(Self(Cow::Owned(s)))
    }

    /// Create a const instance from a static string literal.
    ///
    /// Validated at compile time — invalid values produce a compile error (E0080).
    /// Used by `#[derive(EventPayload)]` for compile-time constants.
    #[must_use]
    pub const fn from_static(s: &'static str) -> Self {
        Self::const_validate_event_type(s);
        Self(Cow::Borrowed(s))
    }

    /// Get the underlying string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Get the underlying `&'static str`.
    ///
    /// Only valid for values constructed with [`EventType::from_static`].
    /// Panics at runtime if the inner value is an owned `String` (i.e., not `'static`).
    #[must_use]
    pub fn as_static_str(&self) -> &'static str {
        match &self.0 {
            Cow::Borrowed(s) => s,
            Cow::Owned(_) => unreachable!(
                "EventType::as_static_str called on a dynamically-allocated value; use from_static for static values"
            ),
        }
    }

    /// Convert to owned `String`.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0.into_owned()
    }

    /// Check if the value is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Compile-time validation for event type strings.
    /// Panics at compile time if the string is invalid.
    #[allow(
        clippy::panic,
        reason = "const-fn validator: panic is the only error channel available"
    )]
    const fn const_validate_event_type(s: &str) {
        let bytes = s.as_bytes();
        assert!(!bytes.is_empty(), "EventType cannot be empty");
        // Check charset
        let mut i = 0;
        while i < bytes.len() {
            let b = bytes[i];
            if !((b >= b'a' && b <= b'z')
                || (b >= b'0' && b <= b'9')
                || b == b'.'
                || b == b'_'
                || b == b'-')
            {
                panic!(
                    "EventType must contain only lowercase letters, digits, dots, underscores, and hyphens"
                );
            }
            i += 1;
        }
        // No leading/trailing dots
        assert!(bytes[0] != b'.', "EventType cannot start with a dot");
        assert!(
            bytes[bytes.len() - 1] != b'.',
            "EventType cannot end with a dot"
        );
        // No consecutive dots
        let mut i = 1;
        while i < bytes.len() {
            assert!(
                !(bytes[i] == b'.' && bytes[i - 1] == b'.'),
                "EventType cannot contain consecutive dots"
            );
            i += 1;
        }
    }

    /// Validate that an event type string follows the hierarchical naming convention.
    fn validate_str(s: &str) -> Result<(), crate::SinexError> {
        if s.is_empty() {
            return Err(crate::SinexError::validation("Event type cannot be empty"));
        }
        if !s.chars().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '_' || c == '-'
        }) {
            return Err(
                crate::SinexError::validation(
                    "Event type must contain only lowercase letters, digits, dots, underscores, and hyphens",
                )
                .with_context("value", s),
            );
        }
        if s.starts_with('.') || s.ends_with('.') {
            return Err(
                crate::SinexError::validation("Event type cannot start or end with a dot")
                    .with_context("value", s),
            );
        }
        if s.contains("..") {
            return Err(crate::SinexError::validation(
                "Event type cannot contain consecutive dots",
            )
            .with_context("value", s));
        }
        Ok(())
    }
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for EventType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::validate_str(s).map_err(|e| e.to_string())?;
        Ok(Self(Cow::Owned(s.to_string())))
    }
}

impl From<String> for EventType {
    /// Convert a `String` to `EventType`, panicking if invalid.
    ///
    /// Used by `sqlx::query_as!` and `.into()` conversions from trusted sources (DB rows).
    /// If the string is invalid, this indicates data corruption — panic is appropriate.
    /// For untrusted input, use [`EventType::new`] which returns `Result`.
    #[allow(
        clippy::panic,
        reason = "Invalid DB data = corruption; panic is the intended surface"
    )]
    fn from(s: String) -> Self {
        Self::new(&s).unwrap_or_else(|_| panic!("invalid EventType value: {s:?}"))
    }
}

impl From<&str> for EventType {
    /// Convert a `&str` to `EventType`, panicking if invalid.
    ///
    /// For untrusted input, use [`EventType::new`] which returns `Result`.
    #[allow(
        clippy::panic,
        reason = "Invalid literal = programmer error; panic is the intended surface"
    )]
    fn from(s: &str) -> Self {
        Self::new(s).unwrap_or_else(|_| panic!("invalid EventType value: {s:?}"))
    }
}

impl AsRef<str> for EventType {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for EventType {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// The hostname where an event occurred.
///
/// Always valid by construction. Use [`HostName::new`] to parse runtime input,
/// or [`HostName::from_static`] for compile-time literals.
///
/// Valid format: ASCII alphanumeric labels separated by dots, with optional
/// interior hyphens. Labels may not start or end with `-`, may not be empty,
/// and the full hostname must not exceed 255 bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct HostName(Cow<'static, str>);

impl<'de> serde::Deserialize<'de> for HostName {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::validate_str(&s).map_err(serde::de::Error::custom)?;
        Ok(Self(Cow::Owned(s)))
    }
}

impl HostName {
    /// Parse a string into a validated `HostName`.
    pub fn new(s: impl Into<String>) -> Result<Self, crate::SinexError> {
        let s = s.into();
        Self::validate_str(&s)?;
        Ok(Self(Cow::Owned(s)))
    }

    /// Create a const instance from a static string literal.
    #[must_use]
    pub const fn from_static(s: &'static str) -> Self {
        Self::const_validate_hostname(s);
        Self(Cow::Borrowed(s))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn into_string(self) -> String {
        self.0.into_owned()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    const fn const_validate_hostname(s: &str) {
        let bytes = s.as_bytes();
        assert!(!bytes.is_empty(), "HostName cannot be empty");
        assert!(bytes.len() <= 255, "HostName cannot exceed 255 bytes");

        let mut i = 0;
        let mut label_len = 0;
        while i < bytes.len() {
            let b = bytes[i];
            if b == b'.' {
                assert!(label_len != 0, "HostName cannot contain empty labels");
                assert!(bytes[i - 1] != b'-', "HostName labels cannot end with '-'");
                label_len = 0;
                i += 1;
                continue;
            }

            let is_ascii_alnum =
                (b >= b'a' && b <= b'z') || (b >= b'A' && b <= b'Z') || (b >= b'0' && b <= b'9');
            assert!(
                is_ascii_alnum || b == b'-',
                "HostName must contain only ASCII letters, digits, hyphens, and dots"
            );
            assert!(
                !(label_len == 0 && b == b'-'),
                "HostName labels cannot start with '-'"
            );
            label_len += 1;
            assert!(label_len <= 63, "HostName labels cannot exceed 63 bytes");
            i += 1;
        }

        assert!(label_len != 0, "HostName cannot end with '.'");
        assert!(
            bytes[bytes.len() - 1] != b'-',
            "HostName labels cannot end with '-'"
        );
    }

    fn validate_str(s: &str) -> Result<(), crate::SinexError> {
        let bytes = s.as_bytes();
        if bytes.is_empty() {
            return Err(crate::SinexError::validation("HostName cannot be empty"));
        }
        if bytes.len() > 255 {
            return Err(crate::SinexError::validation(
                "HostName cannot exceed 255 bytes",
            ));
        }

        let mut label_len = 0usize;
        for (idx, &b) in bytes.iter().enumerate() {
            if b == b'.' {
                if label_len == 0 {
                    return Err(crate::SinexError::validation(
                        "HostName cannot contain empty labels",
                    ));
                }
                if bytes[idx - 1] == b'-' {
                    return Err(crate::SinexError::validation(
                        "HostName labels cannot end with '-'",
                    ));
                }
                label_len = 0;
                continue;
            }

            let is_ascii_alnum = b.is_ascii_alphanumeric();
            if !(is_ascii_alnum || b == b'-') {
                return Err(crate::SinexError::validation(
                    "HostName must contain only ASCII letters, digits, hyphens, and dots",
                ));
            }
            if label_len == 0 && b == b'-' {
                return Err(crate::SinexError::validation(
                    "HostName labels cannot start with '-'",
                ));
            }
            label_len += 1;
            if label_len > 63 {
                return Err(crate::SinexError::validation(
                    "HostName labels cannot exceed 63 bytes",
                ));
            }
        }

        if label_len == 0 {
            return Err(crate::SinexError::validation(
                "HostName cannot end with '.'",
            ));
        }
        if bytes[bytes.len() - 1] == b'-' {
            return Err(crate::SinexError::validation(
                "HostName labels cannot end with '-'",
            ));
        }

        Ok(())
    }
}

impl FromStr for HostName {
    type Err = crate::SinexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl From<String> for HostName {
    #[allow(
        clippy::panic,
        reason = "Invalid trusted value = programmer error; panic is the intended surface"
    )]
    fn from(s: String) -> Self {
        Self::new(s.clone()).unwrap_or_else(|_| panic!("invalid HostName value: {s:?}"))
    }
}

impl From<&str> for HostName {
    #[allow(
        clippy::panic,
        reason = "Invalid literal = programmer error; panic is the intended surface"
    )]
    fn from(s: &str) -> Self {
        Self::new(s).unwrap_or_else(|_| panic!("invalid HostName value: {s:?}"))
    }
}

impl AsRef<str> for HostName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for HostName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for HostName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

define_string_type!(
    #[doc = "The name of a node (ingestor, automaton, service)"]
    NodeName
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

// Consumer group types for nodes
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
    SanitizedPath,
    custom_from_static
);

impl SanitizedPath {
    /// Create a const instance with compile-time validation.
    ///
    /// Validates: non-empty, no null bytes. Full path traversal checks
    /// are only available at runtime via `from_str`.
    #[must_use]
    pub const fn from_static(s: &'static str) -> Self {
        const_assert_non_empty_no_nulls("SanitizedPath", s);
        Self(Cow::Borrowed(s))
    }
}

define_validated_string_type!(
    #[doc = "A path recorded from observational data (filesystem events, shell CWDs). Preserved verbatim except null bytes."]
    RecordedPath,
    custom_from_static
);

impl RecordedPath {
    /// Create a const instance with compile-time validation.
    ///
    /// Validates: non-empty, no null bytes.
    #[must_use]
    pub const fn from_static(s: &'static str) -> Self {
        const_assert_non_empty_no_nulls("RecordedPath", s);
        Self(Cow::Borrowed(s))
    }
}

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
    #[doc = "Content-store keys"]
    ContentKey,
    custom_from_static
);

/// Parsed view of a content-store key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentKeyComponents<'a> {
    /// Full prefix before `--` (backend plus optional metadata modifiers).
    pub prefix: &'a str,
    /// Backend token (prefix up to first `-`, or full prefix if no metadata modifiers).
    pub backend: &'a str,
    /// Key name after `--`.
    pub name: &'a str,
}

impl ContentKey {
    /// Create a const instance with compile-time content key validation.
    ///
    /// Validates: non-empty, contains `--` separator with non-empty prefix and suffix.
    #[must_use]
    pub const fn from_static(s: &'static str) -> Self {
        let bytes = s.as_bytes();
        assert!(!bytes.is_empty(), "ContentKey cannot be empty");
        // Find `--` separator
        let mut found_sep = false;
        let mut sep_pos = 0;
        let mut i = 0;
        while i + 1 < bytes.len() {
            if bytes[i] == b'-' && bytes[i + 1] == b'-' {
                found_sep = true;
                sep_pos = i;
                // Don't break — we want the first occurrence
                break;
            }
            i += 1;
        }
        assert!(found_sep, "ContentKey must contain '--' separator");
        assert!(
            sep_pos != 0,
            "ContentKey must have a backend prefix before '--'"
        );
        assert!(
            sep_pos + 2 < bytes.len(),
            "ContentKey must have a name after '--'"
        );
        Self(Cow::Borrowed(s))
    }

    /// Parse the content key into prefix/backend/name components.
    ///
    /// This method is infallible for valid `ContentKey` values.
    #[must_use]
    pub fn parse_components(&self) -> ContentKeyComponents<'_> {
        let raw = self.as_str();
        let Some((prefix, name)) = raw.split_once("--") else {
            // ContentKey construction validates the `--` separator; this branch is unreachable.
            unreachable!("ContentKey invariant violated: missing '--' separator");
        };
        let backend = prefix.split('-').next().unwrap_or(prefix);
        ContentKeyComponents {
            prefix,
            backend,
            name,
        }
    }
}

define_validated_string_type!(
    #[doc = "NATS subjects"]
    NatsSubject,
    custom_from_static
);

impl NatsSubject {
    /// Create a const instance with compile-time NATS subject validation.
    ///
    /// Validates: non-empty, no leading/trailing/consecutive dots, valid segment chars
    /// (alphanumeric, hyphen, underscore, `*`, `>`).
    #[must_use]
    #[allow(
        clippy::panic,
        reason = "const-fn validator: panic is the only error channel available"
    )]
    pub const fn from_static(s: &'static str) -> Self {
        let bytes = s.as_bytes();
        assert!(!bytes.is_empty(), "NatsSubject cannot be empty");
        assert!(bytes[0] != b'.', "NatsSubject cannot start with '.'");
        assert!(
            bytes[bytes.len() - 1] != b'.',
            "NatsSubject cannot end with '.'"
        );
        let mut i = 0;
        while i < bytes.len() {
            let b = bytes[i];
            if b == b'.' {
                assert!(
                    !(i + 1 < bytes.len() && bytes[i + 1] == b'.'),
                    "NatsSubject cannot contain consecutive dots"
                );
            } else if !((b >= b'a' && b <= b'z')
                || (b >= b'A' && b <= b'Z')
                || (b >= b'0' && b <= b'9')
                || b == b'-'
                || b == b'_'
                || b == b'*'
                || b == b'>')
            {
                panic!("NatsSubject segment contains invalid character");
            }
            i += 1;
        }
        Self(Cow::Borrowed(s))
    }
}

// ─────────────────────────────────────────────────────────────
// Temporal Vocabulary
// ─────────────────────────────────────────────────────────────

/// How the capture timestamp was determined for a material slice.
///
/// Stored as `source_type` in `raw.temporal_ledger`. Shared between schema
/// CHECK constraints, DB repositories, and SDK-side `LedgerReader`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TemporalSourceType {
    /// Timestamp recorded at the moment of live data capture
    RealtimeCapture,
    /// Timestamp parsed from the content itself (e.g., log line timestamp)
    IntrinsicContent,
    /// Inferred from file modification time
    InferredMtime,
    /// Inferred from file creation time
    InferredCtime,
    /// User-provided timestamp
    InferredUser,
    /// Fallback: timestamp recorded when the slice was staged for ingestion
    StagedAt,
}

impl std::fmt::Display for TemporalSourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RealtimeCapture => write!(f, "realtime_capture"),
            Self::IntrinsicContent => write!(f, "intrinsic_content"),
            Self::InferredMtime => write!(f, "inferred_mtime"),
            Self::InferredCtime => write!(f, "inferred_ctime"),
            Self::InferredUser => write!(f, "inferred_user"),
            Self::StagedAt => write!(f, "staged_at"),
        }
    }
}

impl std::str::FromStr for TemporalSourceType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "realtime_capture" => Ok(Self::RealtimeCapture),
            "intrinsic_content" => Ok(Self::IntrinsicContent),
            "inferred_mtime" => Ok(Self::InferredMtime),
            "inferred_ctime" => Ok(Self::InferredCtime),
            "inferred_user" => Ok(Self::InferredUser),
            "staged_at" => Ok(Self::StagedAt),
            _ => Err(format!("unknown temporal source type: {s}")),
        }
    }
}

/// Precision of a temporal ledger entry.
///
/// Stored as `precision` in `raw.temporal_ledger`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TemporalPrecision {
    /// Exact timestamp with no meaningful uncertainty
    Exact,
    /// Bounded timestamp with known or estimated uncertainty
    Bounded,
}

impl std::fmt::Display for TemporalPrecision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exact => write!(f, "exact"),
            Self::Bounded => write!(f, "bounded"),
        }
    }
}

impl std::str::FromStr for TemporalPrecision {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "exact" => Ok(Self::Exact),
            "bounded" => Ok(Self::Bounded),
            _ => Err(format!("unknown temporal precision: {s}")),
        }
    }
}

/// Clock source used for a temporal ledger entry.
///
/// Stored as `clock` in `raw.temporal_ledger`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TemporalClock {
    /// Monotonic clock (guarantees ordering, not absolute time)
    Monotonic,
    /// Wall clock (real-time, subject to NTP adjustments)
    Wall,
}

impl std::fmt::Display for TemporalClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Monotonic => write!(f, "monotonic"),
            Self::Wall => write!(f, "wall"),
        }
    }
}

impl std::str::FromStr for TemporalClock {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "monotonic" => Ok(Self::Monotonic),
            "wall" => Ok(Self::Wall),
            _ => Err(format!("unknown temporal clock: {s}")),
        }
    }
}

/// How a synthetic event's `ts_orig` was determined.
///
/// Declared per-output by derived nodes. Persisted as `temporal_policy`
/// on `core.events` for synthetic rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyntheticTemporalPolicy {
    /// Inherit `ts_orig` from the single parent event (1:1 transforms)
    InheritParent,
    /// Use the latest contributing input's `ts_orig`
    LatestInput,
    /// Use the window boundary timestamp (e.g., window end)
    WindowBoundary,
    /// Use an explicitly declared effective timestamp from domain logic
    DeclaredEffective,
}

impl std::fmt::Display for SyntheticTemporalPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InheritParent => write!(f, "inherit_parent"),
            Self::LatestInput => write!(f, "latest_input"),
            Self::WindowBoundary => write!(f, "window_boundary"),
            Self::DeclaredEffective => write!(f, "declared_effective"),
        }
    }
}

impl std::str::FromStr for SyntheticTemporalPolicy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "inherit_parent" => Ok(Self::InheritParent),
            "latest_input" => Ok(Self::LatestInput),
            "window_boundary" => Ok(Self::WindowBoundary),
            "declared_effective" => Ok(Self::DeclaredEffective),
            _ => Err(format!("unknown synthetic temporal policy: {s}")),
        }
    }
}

/// Classification of a derived node's computation model.
///
/// Each derived node must declare which model it uses, which determines
/// how the SDK prepares inputs and manages scope/window state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DerivedNodeModel {
    /// Processes one triggering event at a time; deterministic fallback order is `id ASC`
    Transducer,
    /// Declares window identity and completion logic; SDK prepares completed windows
    Windowed,
    /// Declares `trigger→scope_key` mapping; loads persisted working set for deterministic recomputation
    ScopeReconciler,
}

impl std::fmt::Display for DerivedNodeModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transducer => write!(f, "transducer"),
            Self::Windowed => write!(f, "windowed"),
            Self::ScopeReconciler => write!(f, "scope_reconciler"),
        }
    }
}

impl std::str::FromStr for DerivedNodeModel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "transducer" => Ok(Self::Transducer),
            "windowed" => Ok(Self::Windowed),
            "scope_reconciler" => Ok(Self::ScopeReconciler),
            _ => Err(format!("unknown derived node model: {s}")),
        }
    }
}

/// The mode in which a node is currently processing events.
///
/// Provided via trigger context so node logic can distinguish live arrival
/// from historical scan, replay recomputation, etc.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessingMode {
    /// Normal live event arrival
    Live,
    /// Historical scan of existing material
    HistoricalScan,
    /// Replay-driven recomputation
    Replay,
    /// Late backfill of previously unseen material
    Backfill,
}

impl std::fmt::Display for ProcessingMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Live => write!(f, "live"),
            Self::HistoricalScan => write!(f, "historical_scan"),
            Self::Replay => write!(f, "replay"),
            Self::Backfill => write!(f, "backfill"),
        }
    }
}

impl std::str::FromStr for ProcessingMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "live" => Ok(Self::Live),
            "historical_scan" => Ok(Self::HistoricalScan),
            "replay" => Ok(Self::Replay),
            "backfill" => Ok(Self::Backfill),
            _ => Err(format!("unknown processing mode: {s}")),
        }
    }
}

/// What caused a derived node to be triggered.
///
/// Provided in the trigger context so nodes can distinguish between
/// new evidence, late backfill, scope invalidation, etc.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerKind {
    /// A new event arrived in the subscribed stream
    NewEvent,
    /// Late historical data was backfilled
    LateBackfill,
    /// An existing scope was invalidated (e.g., by archival)
    ScopeInvalidation,
    /// A replay operation triggered recomputation
    ReplayRecompute,
}

impl std::fmt::Display for TriggerKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NewEvent => write!(f, "new_event"),
            Self::LateBackfill => write!(f, "late_backfill"),
            Self::ScopeInvalidation => write!(f, "scope_invalidation"),
            Self::ReplayRecompute => write!(f, "replay_recompute"),
        }
    }
}

impl std::str::FromStr for TriggerKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "new_event" => Ok(Self::NewEvent),
            "late_backfill" => Ok(Self::LateBackfill),
            "scope_invalidation" => Ok(Self::ScopeInvalidation),
            "replay_recompute" => Ok(Self::ReplayRecompute),
            _ => Err(format!("unknown trigger kind: {s}")),
        }
    }
}

/// What happened to a persisted fact that triggered scope invalidation.
///
/// Carried by `DerivedScopeInvalidation` so derived nodes know whether
/// to recompute, archive their outputs, or both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvalidationAction {
    /// A new event was inserted (live arrival or late backfill)
    Inserted,
    /// An existing event was archived (e.g., by replay)
    Archived,
    /// An event was replaced by a new version (archive + re-insert)
    Replaced,
}

impl std::fmt::Display for InvalidationAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inserted => write!(f, "inserted"),
            Self::Archived => write!(f, "archived"),
            Self::Replaced => write!(f, "replaced"),
        }
    }
}

impl std::str::FromStr for InvalidationAction {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "inserted" => Ok(Self::Inserted),
            "archived" => Ok(Self::Archived),
            "replaced" => Ok(Self::Replaced),
            _ => Err(format!("unknown invalidation action: {s}")),
        }
    }
}

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
    /// Node has stopped after completing its run
    Stopped,
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
            Self::Stopped => write!(f, "stopped"),
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
            "stopped" => Ok(Self::Stopped),
            "unknown" => Ok(Self::Unknown),
            _ => Err(format!("unknown node state: {s}")),
        }
    }
}

/// Result status of an operation in the operations log.
///
/// Matches the values stored in `core.operations_log.result_status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    /// Operation is actively running
    Running,
    /// Operation completed successfully
    Success,
    /// Operation failed
    Failed,
    /// Operation was cancelled before completion
    Cancelled,
    /// Operation is queued but not yet started
    Pending,
}

impl std::fmt::Display for OperationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Running => write!(f, "running"),
            Self::Success => write!(f, "success"),
            Self::Failed => write!(f, "failure"),
            Self::Cancelled => write!(f, "cancelled"),
            Self::Pending => write!(f, "pending"),
        }
    }
}

impl std::str::FromStr for OperationStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "running" | "in_progress" => Ok(Self::Running),
            "success" | "ok" => Ok(Self::Success),
            "failed" | "failure" | "error" | "expired" => Ok(Self::Failed),
            "cancelled" | "canceled" => Ok(Self::Cancelled),
            "pending" => Ok(Self::Pending),
            _ => Err(format!("unknown operation status: {s}")),
        }
    }
}

/// Three-tier data lifecycle: Live ↔ Archive → Tombstone.
///
/// Matches the values stored as tier names in lifecycle status responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataTier {
    /// Events available for real-time queries
    Live,
    /// Events moved to cold storage, still queryable
    Archive,
    /// Events permanently deleted
    Tombstone,
}

impl std::fmt::Display for DataTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Live => write!(f, "live"),
            Self::Archive => write!(f, "archive"),
            Self::Tombstone => write!(f, "tombstone"),
        }
    }
}

impl std::str::FromStr for DataTier {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "live" => Ok(Self::Live),
            "archive" => Ok(Self::Archive),
            "tombstone" => Ok(Self::Tombstone),
            _ => Err(format!("unknown data tier: {s}")),
        }
    }
}

/// Health status of a component or the overall system.
///
/// Matches the values used in system health RPC responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    /// All subsystems operating normally
    Healthy,
    /// Operational but with degraded performance or partial failures
    Degraded,
    /// One or more critical subsystems are unavailable
    Unhealthy,
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Healthy => write!(f, "healthy"),
            Self::Degraded => write!(f, "degraded"),
            Self::Unhealthy => write!(f, "unhealthy"),
        }
    }
}

impl std::str::FromStr for HealthStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "healthy" => Ok(Self::Healthy),
            "degraded" => Ok(Self::Degraded),
            "unhealthy" => Ok(Self::Unhealthy),
            _ => Err(format!("unknown health status: {s}")),
        }
    }
}

/// Common trait for components that can be health-checked.
pub trait HealthCheck: Send + Sync {
    async fn check_health(&self) -> Result<HealthStatus, crate::error::SinexError>;
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
}

impl std::fmt::Display for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ingestor => write!(f, "ingestor"),
            Self::Automaton => write!(f, "automaton"),
            Self::Service => write!(f, "service"),
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
            _ => Err(format!("unknown node type: {s}")),
        }
    }
}

/// Verification status of a stored blob.
///
/// Matches the values stored in `core.blobs.verification_status`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize, JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum BlobVerificationStatus {
    /// Blob has not yet been verified
    Pending,
    /// Blob content matches its stored checksum
    Verified,
    /// Blob content does not match its stored checksum
    Corrupted,
}

impl std::fmt::Display for BlobVerificationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Verified => write!(f, "verified"),
            Self::Corrupted => write!(f, "corrupted"),
        }
    }
}

impl std::str::FromStr for BlobVerificationStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "pending" => Ok(Self::Pending),
            "verified" | "ok" => Ok(Self::Verified),
            "corrupted" | "failed" | "invalid" => Ok(Self::Corrupted),
            _ => Err(format!("unknown blob verification status: {s}")),
        }
    }
}

/// Outcome of a completed replay operation.
///
/// Stored in the `outcome` field of `ReplayOperation` (serialized to JSON).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplayOutcome {
    /// Replay completed successfully
    Success,
    /// Replay failed due to an error
    Failed,
    /// Replay was cancelled
    Cancelled,
}

impl std::fmt::Display for ReplayOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Success => write!(f, "success"),
            Self::Failed => write!(f, "failed"),
            Self::Cancelled => write!(f, "cancelled"),
        }
    }
}

impl std::str::FromStr for ReplayOutcome {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "success" | "ok" => Ok(Self::Success),
            "failed" | "failure" | "error" => Ok(Self::Failed),
            "cancelled" | "canceled" => Ok(Self::Cancelled),
            _ => Err(format!("unknown replay outcome: {s}")),
        }
    }
}

// Validation for specific types (EventSource and EventType validation is in their manual impl blocks above)

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
    /// Validate and create a sanitized path, delegating all security checks to
    /// `crate::validation::validate_path` (null bytes, traversal, length, percent-encoding).
    pub fn validate(path: &str) -> Result<Utf8PathBuf, String> {
        crate::validation::validate_path(path).map_err(|e| e.message().to_string())
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

impl RecordedPath {
    /// Create a new `RecordedPath`, rejecting only null bytes
    pub fn from_observed(path: impl Into<String>) -> Result<Self, String> {
        let s = path.into();
        if s.contains('\0') {
            return Err("Recorded path cannot contain null bytes".into());
        }
        if s.is_empty() {
            return Err("Recorded path cannot be empty".into());
        }
        Ok(Self(Cow::Owned(s)))
    }

    /// Create a validated `RecordedPath` from a string
    pub fn from_str_validated(s: &str) -> Result<Self, String> {
        Self::from_observed(s)
    }
}

impl FromStr for RecordedPath {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_validated(s)
    }
}

impl From<&std::path::Path> for RecordedPath {
    #[allow(clippy::expect_used)] // From trait cannot return Result; null bytes in paths are not possible
    fn from(path: &std::path::Path) -> Self {
        Self::from_observed(path.to_string_lossy().to_string())
            .expect("Path should not contain null bytes")
    }
}

impl From<std::path::PathBuf> for RecordedPath {
    fn from(path: std::path::PathBuf) -> Self {
        Self::from(&path as &std::path::Path)
    }
}

impl From<&str> for RecordedPath {
    #[allow(clippy::expect_used)] // From trait cannot return Result; null bytes in str are not possible
    fn from(s: &str) -> Self {
        Self::from_observed(s)
            .expect("RecordedPath::from(&str) value should not contain null bytes")
    }
}

impl From<String> for RecordedPath {
    #[allow(clippy::expect_used)] // From trait cannot return Result; null bytes in String are not possible
    fn from(s: String) -> Self {
        Self::from_observed(s)
            .expect("RecordedPath::from(String) value should not contain null bytes")
    }
}

// ─────────────────────────────────────────────────────────────
// SQLx Feature Support
// ─────────────────────────────────────────────────────────────

#[cfg(feature = "sqlx")]
mod sqlx_impls {
    use super::{
        ContentKey, BlobVerificationStatus, BranchName, CommandText, CommitHash, ConsumerGroup,
        ConsumerName, DataTier, DerivedNodeModel, EntityTypeName, EventSource, EventType,
        GlobPattern, HealthStatus, HostName, InstanceId, InvalidationAction, IpAddress, JobId,
        NatsSubject, NodeId, NodeName, NodeState, NodeType, OperationStatus, ProcessingMode,
        RecordedPath, RegexPattern, RelationType, RemoteName, SanitizedPath, SchemaName,
        SchemaVersion, ServiceName, ShellName, SyntheticTemporalPolicy, TemporalClock,
        TemporalPrecision, TemporalSourceType, TriggerKind, UserId,
    };

    // Register validated string types (construction-validated)
    impl_sqlx_for_validated_string_type!(EventSource);
    impl_sqlx_for_validated_string_type!(EventType);

    // Register string types without validation
    impl_sqlx_for_validated_string_type!(HostName);
    impl_sqlx_for_string_type!(NodeName);
    impl_sqlx_for_string_type!(SchemaVersion);
    impl_sqlx_for_string_type!(SchemaName);
    impl_sqlx_for_string_type!(CommandText);
    impl_sqlx_for_string_type!(ShellName);
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
    impl_sqlx_for_validated_string_type!(RecordedPath);
    impl_sqlx_for_validated_string_type!(ContentKey);
    impl_sqlx_for_validated_string_type!(NatsSubject);

    // Register enum types (use Display for encoding, FromStr for decoding)
    impl_sqlx_for_enum_type!(OperationStatus);
    impl_sqlx_for_enum_type!(NodeState);
    impl_sqlx_for_enum_type!(NodeType);
    impl_sqlx_for_enum_type!(DataTier);
    impl_sqlx_for_enum_type!(HealthStatus);
    impl_sqlx_for_enum_type!(BlobVerificationStatus);

    // Temporal vocabulary enums
    impl_sqlx_for_enum_type!(TemporalSourceType);
    impl_sqlx_for_enum_type!(TemporalPrecision);
    impl_sqlx_for_enum_type!(TemporalClock);
    impl_sqlx_for_enum_type!(SyntheticTemporalPolicy);
    impl_sqlx_for_enum_type!(DerivedNodeModel);
    impl_sqlx_for_enum_type!(ProcessingMode);
    impl_sqlx_for_enum_type!(TriggerKind);
    impl_sqlx_for_enum_type!(InvalidationAction);
}

impl ContentKey {
    /// Validate content-store key format.
    ///
    /// Content-store keys have the form `BACKEND[-sNNN][-mNNN]--FILENAME`, where
    /// `--` separates the backend/metadata prefix from the key name.
    pub fn validate(key: &str) -> Result<(), String> {
        if key.is_empty() {
            return Err("Content key cannot be empty".into());
        }

        // Must contain exactly one `--` separator
        let parts: Vec<&str> = key.splitn(3, "--").collect();
        if parts.len() < 2 {
            return Err("Content key must contain '--' separator".into());
        }
        if parts[0].is_empty() {
            return Err("Content key must have a backend prefix before '--'".into());
        }
        if parts[1].is_empty() {
            return Err("Content key must have a name after '--'".into());
        }
        // Reject multiple `--` separators
        if parts.len() > 2 {
            return Err("Content key must contain exactly one '--' separator".into());
        }

        Ok(())
    }
}

impl FromStr for ContentKey {
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

/// Service metadata for registration and discovery
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServiceInfo {
    pub name: String,
    pub version: String,
    pub kind: ServiceKind,
    pub status: HealthStatus,
    pub started_at: crate::events::Timestamp,
    pub metadata: std::collections::HashMap<String, serde_json::Value>,
}

/// Types of services in the Sinex ecosystem
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceKind {
    Ingestor,
    Automaton,
    Gateway,
    Collector,
}

impl std::fmt::Display for ServiceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ingestor => write!(f, "ingestor"),
            Self::Automaton => write!(f, "automaton"),
            Self::Gateway => write!(f, "gateway"),
            Self::Collector => write!(f, "collector"),
        }
    }
}

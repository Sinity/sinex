//! Identifier Definition Macros
//!
//! Provides macros for defining type-safe string identifiers with automatic
//! trait implementations and validation.

/// Define a type-safe string identifier with validation
///
/// # Examples
///
/// ```
/// use sinex_identifiers::define_identifier;
///
/// // Simple identifier
/// define_identifier!(UserId);
///
/// // Identifier with validation
/// define_identifier!(EmailAddress, |s: &str| {
///     if s.contains('@') && s.len() > 3 {
///         Ok(())
///     } else {
///         Err("Invalid email format".to_string())
///     }
/// });
///
/// // Identifier with custom Display
/// define_identifier!(ServiceName, |s: &str| {
///     if s.len() > 0 && s.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_') {
///         Ok(())
///     } else {
///         Err("Service name must be alphanumeric with dashes or underscores".to_string())
///     }
/// }, display = "service:{}" );
/// ```
#[macro_export]
macro_rules! define_identifier {
    // Simple identifier without validation
    ($name:ident) => {
        $crate::define_identifier!($name, |_: &str| Ok(()));
    };

    // Identifier with validation function
    ($name:ident, $validator:expr) => {
        $crate::define_identifier!($name, $validator, display = "{}");
    };

    // Identifier with validation and custom display format
    ($name:ident, $validator:expr, display = $format:literal) => {
        #[derive(
            Debug,
            Clone,
            PartialEq,
            Eq,
            Hash,
            PartialOrd,
            Ord,
            serde::Serialize,
            serde::Deserialize,
            schemars::JsonSchema,
        )]
        pub struct $name(String);

        impl $name {
            /// Create a new identifier from a string
            pub fn new(value: impl Into<String>) -> $crate::IdentifierResult<Self> {
                let value = value.into();
                let validator = $validator;
                validator(&value).map_err(|e| $crate::IdentifierError::Validation {
                    identifier_type: stringify!($name),
                    value: value.clone(),
                    reason: e,
                })?;
                Ok(Self(value))
            }

            /// Create a new identifier without validation (use with caution)
            pub fn new_unchecked(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            /// Get the inner string value
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Convert to owned String
            pub fn into_string(self) -> String {
                self.0
            }

            /// Get the length of the identifier
            pub fn len(&self) -> usize {
                self.0.len()
            }

            /// Check if the identifier is empty
            pub fn is_empty(&self) -> bool {
                self.0.is_empty()
            }
        }

        impl $crate::Identifier for $name {
            fn as_str(&self) -> &str {
                &self.0
            }

            fn into_string(self) -> String {
                self.0
            }
        }

        impl $crate::ValidatedIdentifier for $name {
            fn validate(value: &str) -> $crate::IdentifierResult<()> {
                let validator = $validator;
                validator(value).map_err(|e| $crate::IdentifierError::Validation {
                    identifier_type: stringify!($name),
                    value: value.to_string(),
                    reason: e,
                })
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, $format, self.0)
            }
        }

        impl std::str::FromStr for $name {
            type Err = $crate::IdentifierError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::new(s)
            }
        }

        impl From<$name> for String {
            fn from(id: $name) -> String {
                id.0
            }
        }

        impl std::convert::AsRef<str> for $name {
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

        // SQLx support for database operations
        impl sqlx::Type<sqlx::Postgres> for $name {
            fn type_info() -> sqlx::postgres::PgTypeInfo {
                <String as sqlx::Type<sqlx::Postgres>>::type_info()
            }
        }

        impl sqlx::Encode<'_, sqlx::Postgres> for $name {
            fn encode_by_ref(
                &self,
                buf: &mut sqlx::postgres::PgArgumentBuffer,
            ) -> sqlx::encode::IsNull {
                <String as sqlx::Encode<sqlx::Postgres>>::encode_by_ref(&self.0, buf)
            }
        }

        impl sqlx::Decode<'_, sqlx::Postgres> for $name {
            fn decode(
                value: sqlx::postgres::PgValueRef<'_>,
            ) -> Result<Self, sqlx::error::BoxDynError> {
                let s = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
                Ok(Self::new_unchecked(s))
            }
        }
    };
}

/// Define an identifier that can be automatically generated
#[macro_export]
macro_rules! define_generated_identifier {
    ($name:ident, $generator:expr) => {
        $crate::define_generated_identifier!($name, $generator, |_: &str| Ok(()));
    };

    ($name:ident, $generator:expr, $validator:expr) => {
        $crate::define_identifier!($name, $validator);

        impl $crate::GeneratedIdentifier for $name {
            fn generate() -> Self {
                let generator: fn() -> String = $generator;
                Self::new_unchecked(generator())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::generate()
            }
        }
    };
}

/// Define a ULID-based identifier
#[macro_export]
macro_rules! define_ulid_identifier {
    ($name:ident) => {
        $crate::define_generated_identifier!(
            $name,
            || $crate::sinex_ulid::Ulid::new().to_string(),
            |s: &str| {
                s.parse::<$crate::sinex_ulid::Ulid>()
                    .map(|_| ())
                    .map_err(|e| format!("Invalid ULID: {}", e))
            }
        );

        impl $name {
            /// Create from a ULID
            pub fn from_ulid(ulid: $crate::sinex_ulid::Ulid) -> Self {
                Self::new_unchecked(ulid.to_string())
            }

            /// Parse as ULID
            pub fn as_ulid(&self) -> Result<$crate::sinex_ulid::Ulid, $crate::IdentifierError> {
                self.0.parse::<$crate::sinex_ulid::Ulid>().map_err(|e| {
                    $crate::IdentifierError::Validation {
                        identifier_type: stringify!($name),
                        value: self.0.clone(),
                        reason: format!("Invalid ULID: {}", e),
                    }
                })
            }

            /// Get timestamp from ULID
            pub fn timestamp(
                &self,
            ) -> Result<$crate::chrono::DateTime<$crate::chrono::Utc>, $crate::IdentifierError>
            {
                Ok(self.as_ulid()?.timestamp())
            }
        }
    };
}

/// Define a UUID-based identifier
#[macro_export]
macro_rules! define_uuid_identifier {
    ($name:ident) => {
        $crate::define_generated_identifier!(
            $name,
            || uuid::Uuid::new_v4().to_string(),
            |s: &str| {
                uuid::Uuid::parse_str(s)
                    .map(|_| ())
                    .map_err(|e| format!("Invalid UUID: {}", e))
            }
        );

        impl $name {
            /// Create from a UUID
            pub fn from_uuid(uuid: uuid::Uuid) -> Self {
                Self::new_unchecked(uuid.to_string())
            }

            /// Parse as UUID
            pub fn as_uuid(&self) -> Result<uuid::Uuid, $crate::IdentifierError> {
                uuid::Uuid::parse_str(&self.0).map_err(|e| $crate::IdentifierError::Validation {
                    identifier_type: stringify!($name),
                    value: self.0.clone(),
                    reason: format!("Invalid UUID: {}", e),
                })
            }
        }
    };
}

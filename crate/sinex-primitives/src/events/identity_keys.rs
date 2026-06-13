//! Typed wrappers for event identity keys at the persistence boundary.
//!
//! These newtypes replace raw `Option<String>` for `scope_key` and
//! `equivalence_key` fields on persistence-layer row types, making
//! the role of each key explicit at the type level.
//!
//! # Usage
//!
//! - [`ScopeKey`] — the input-set identifier for a scope-reconciler automaton
//!   (e.g. `"task:abc"`, `"day:2026-06-13"`). Determines which automaton scope
//!   owns a derived event.
//! - [`EquivalenceKey`] — the output-slot identifier inside a scope for targeted
//!   replacement. Events with the same `EquivalenceKey` within a scope replace
//!   each other on replay (e.g. `"analytics:window:42"`, `"daily:2026-06-13"`).

use serde::{Deserialize, Serialize};

/// Typed wrapper for the `scope_key` DB column.
///
/// Identifies the input-set (scope) that a scope-reconciler automaton processes.
/// Only set on derived events from `ScopeReconciler` automatons. Raw `String`
/// throughout the rest of the codebase; typed at the persistence boundary.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ScopeKey(String);

/// Typed wrapper for the `equivalence_key` DB column.
///
/// Identifies the output slot inside a scope for targeted replacement. A live
/// event with the same `EquivalenceKey` is superseded when a new event with the
/// same key arrives. Only set on derived events. Raw `String` throughout the
/// rest of the codebase; typed at the persistence boundary.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EquivalenceKey(String);

macro_rules! impl_identity_key {
    ($name:ident) => {
        impl $name {
            /// Create a new instance from any string.
            pub fn new(s: impl Into<String>) -> Self {
                Self(s.into())
            }

            /// Return the inner string slice.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consume and return the inner `String`.
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl std::ops::Deref for $name {
            type Target = str;
            fn deref(&self) -> &str {
                &self.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl From<String> for $name {
            fn from(s: String) -> Self {
                Self(s)
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                Self(s.to_owned())
            }
        }

        impl From<$name> for String {
            fn from(key: $name) -> String {
                key.0
            }
        }

        #[cfg(feature = "sqlx")]
        impl sqlx::Type<sqlx::Postgres> for $name {
            fn type_info() -> sqlx::postgres::PgTypeInfo {
                <String as sqlx::Type<sqlx::Postgres>>::type_info()
            }

            fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
                <String as sqlx::Type<sqlx::Postgres>>::compatible(ty)
            }
        }

        #[cfg(feature = "sqlx")]
        impl sqlx::Encode<'_, sqlx::Postgres> for $name {
            fn encode_by_ref(
                &self,
                buf: &mut sqlx::postgres::PgArgumentBuffer,
            ) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Send + Sync + 'static>>
            {
                <&str as sqlx::Encode<sqlx::Postgres>>::encode_by_ref(&&*self.0, buf)
            }
        }

        #[cfg(feature = "sqlx")]
        impl sqlx::Decode<'_, sqlx::Postgres> for $name {
            fn decode(
                value: sqlx::postgres::PgValueRef<'_>,
            ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
                let s = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
                Ok(Self(s))
            }
        }
    };
}

impl_identity_key!(ScopeKey);
impl_identity_key!(EquivalenceKey);

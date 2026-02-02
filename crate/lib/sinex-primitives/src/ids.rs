//! Generic strongly-typed ID implementation for domain types
//!
//! This module provides a generic Id<T> type that creates strongly-typed
//! identifiers for domain types, preventing ID mixing at compile time.

use crate::temporal::Timestamp;
use serde::{Deserialize, Serialize};
pub use sinex_schema::ulid::Ulid;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

/// A strongly-typed ID that prevents mixing different ID types
///
/// Use this with any domain type T to create type-safe identifiers:
/// - `Id<Event>` for events
/// - `Id<User>` for users
/// - `Id<YourType>` for any custom domain type
///
/// This wraps the primitive Ulid type from sinex-schema to provide
/// domain-level type safety while keeping schema records primitive.
#[derive(Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Id<T> {
    ulid: Ulid,
    #[serde(skip)]
    _phantom: PhantomData<T>,
}

// Manual implementations that don't require T to implement the traits
impl<T> Clone for Id<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Id<T> {}

impl<T> PartialEq for Id<T> {
    fn eq(&self, other: &Self) -> bool {
        self.ulid == other.ulid
    }
}

impl<T> Eq for Id<T> {}

impl<T> PartialOrd for Id<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Id<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ulid.cmp(&other.ulid)
    }
}

impl<T> Hash for Id<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.ulid.hash(state);
    }
}

impl<T> Id<T> {
    /// Create a new ID with a fresh ULID
    #[must_use]
    pub fn new() -> Self {
        Self {
            ulid: Ulid::new(),
            _phantom: PhantomData,
        }
    }

    /// Get the underlying ULID
    #[must_use]
    pub fn as_ulid(&self) -> &Ulid {
        &self.ulid
    }

    /// Convert to UUID for `PostgreSQL` compatibility
    #[must_use]
    pub fn to_uuid(&self) -> uuid::Uuid {
        self.ulid.to_uuid()
    }

    /// Create from a ULID
    #[must_use]
    pub fn from_ulid(ulid: Ulid) -> Self {
        Self {
            ulid,
            _phantom: PhantomData,
        }
    }

    /// Create from a UUID
    #[must_use]
    pub fn from_uuid(uuid: uuid::Uuid) -> Self {
        Self::from_ulid(Ulid::from_uuid(uuid))
    }

    /// Get the timestamp when this ID was created
    #[must_use]
    pub fn timestamp(&self) -> Timestamp {
        self.ulid.timestamp()
    }
}

impl<T> Default for Id<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> fmt::Display for Id<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.ulid)
    }
}

impl<T> std::str::FromStr for Id<T> {
    type Err = sinex_schema::ulid::UlidError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::from_ulid(s.parse()?))
    }
}

impl<T> From<Ulid> for Id<T> {
    fn from(ulid: Ulid) -> Self {
        Self::from_ulid(ulid)
    }
}

impl<T> From<Id<T>> for Ulid {
    fn from(id: Id<T>) -> Self {
        id.ulid
    }
}

impl<T> AsRef<Ulid> for Id<T> {
    fn as_ref(&self) -> &Ulid {
        &self.ulid
    }
}

// SQLx support for all ID types (Optional Feature)
#[cfg(feature = "sqlx")]
mod sqlx_impl {
    use super::Id;
    use sqlx::encode::IsNull;
    use sqlx::error::BoxDynError;
    use sqlx::postgres::{PgArgumentBuffer, PgHasArrayType, PgTypeInfo, PgValueRef};
    use sqlx::{Decode, Encode, Postgres, Type};
    use std::error::Error as StdError;

    // Generic implementation for all Id<T> types
    impl<T> Type<Postgres> for Id<T> {
        fn type_info() -> PgTypeInfo {
            <uuid::Uuid as Type<Postgres>>::type_info()
        }
    }

    impl<T> PgHasArrayType for Id<T> {
        fn array_type_info() -> PgTypeInfo {
            <uuid::Uuid as PgHasArrayType>::array_type_info()
        }
    }

    impl<T> Encode<'_, Postgres> for Id<T> {
        fn encode_by_ref(
            &self,
            buf: &mut PgArgumentBuffer,
        ) -> Result<IsNull, Box<dyn StdError + Send + Sync + 'static>> {
            self.to_uuid().encode_by_ref(buf)
        }
    }

    impl<'r, T> Decode<'r, Postgres> for Id<T> {
        fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
            let uuid = <uuid::Uuid as Decode<Postgres>>::decode(value)?;
            Ok(Self::from_uuid(uuid))
        }
    }
}

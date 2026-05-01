//! Generic strongly-typed ID implementation for domain types
//!
//! This module provides a generic Id<T> type that creates strongly-typed
//! identifiers for domain types, preventing ID mixing at compile time.

use crate::temporal::Timestamp;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use uuid::Uuid;

/// A strongly-typed ID that prevents mixing different ID types
///
/// Use this with any domain type T to create type-safe identifiers:
/// - `Id<Event>` for events
/// - `Id<User>` for users
/// - `Id<YourType>` for any custom domain type
///
/// This wraps the primitive Uuid type to provide
/// domain-level type safety while keeping schema records primitive.
#[derive(Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Id<T> {
    uuid: Uuid,
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
        self.uuid == other.uuid
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
        self.uuid.cmp(&other.uuid)
    }
}

impl<T> Hash for Id<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.uuid.hash(state);
    }
}

impl<T> Id<T> {
    /// Create a new ID with a fresh `UUIDv7`
    #[must_use]
    pub fn new() -> Self {
        Self {
            uuid: Uuid::now_v7(),
            _phantom: PhantomData,
        }
    }

    /// Convert to UUID for `PostgreSQL` storage/query APIs
    #[must_use]
    pub fn to_uuid(&self) -> Uuid {
        self.uuid
    }

    /// Get the underlying UUID
    #[must_use]
    pub fn as_uuid(&self) -> &Uuid {
        &self.uuid
    }

    /// Create from a UUID
    #[must_use]
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self {
            uuid,
            _phantom: PhantomData,
        }
    }

    /// Extract timestamp from the `UUIDv7`
    #[must_use]
    pub fn timestamp(&self) -> Timestamp {
        if let Some(ts) = self.uuid.get_timestamp() {
            let (secs, nanos) = ts.to_unix();
            match time::OffsetDateTime::from_unix_timestamp(secs as i64) {
                Ok(dt) => {
                    let full_dt = dt + time::Duration::nanoseconds(i64::from(nanos));
                    Timestamp::new(full_dt)
                }
                Err(_) => Timestamp::now(), // Fallback
            }
        } else {
            Timestamp::now() // Fallback if not v7/v6/v1
        }
    }
}

impl<T> Default for Id<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> fmt::Display for Id<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.uuid)
    }
}

impl<T> std::str::FromStr for Id<T> {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::from_uuid(s.parse()?))
    }
}

impl<T> From<Uuid> for Id<T> {
    fn from(uuid: Uuid) -> Self {
        Self::from_uuid(uuid)
    }
}

impl<T> From<Id<T>> for Uuid {
    fn from(id: Id<T>) -> Self {
        id.uuid
    }
}

impl<T> AsRef<Uuid> for Id<T> {
    fn as_ref(&self) -> &Uuid {
        &self.uuid
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

        fn compatible(ty: &PgTypeInfo) -> bool {
            <uuid::Uuid as Type<Postgres>>::compatible(ty)
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

// ---------------------------------------------------------------------------
// UUIDv5 derivations
// ---------------------------------------------------------------------------

/// Derive the deterministic document id used by the document layer (#733).
///
/// `source_unit` is `"dendron"` or `"terminal"`; `natural_key` is the
/// vault-relative path (Dendron) or the parent `command.canonical` event
/// id stringified (terminal). The result is `UUIDv5(NAMESPACE_OID,
/// "sinex.documents.v1/<source_unit>/<natural_key>")`.
///
/// Anchoring on RFC 4122's well-known `NAMESPACE_OID` (rather than a fresh
/// random 16-byte constant) keeps the derivation auditable: every byte that
/// determines the output is either RFC-defined or appears in the input
/// string. There is no opaque sinex-specific salt to pin down or to
/// suspect of fabrication. Replaying parser logic across cluster
/// generations produces the same document id because the inputs are
/// stable, not because a constant is privileged.
#[must_use]
pub fn derive_document_id(source_unit: &str, natural_key: &str) -> ::uuid::Uuid {
    let canonical = format!("sinex.documents.v1/{source_unit}/{natural_key}");
    ::uuid::Uuid::new_v5(&::uuid::Uuid::NAMESPACE_OID, canonical.as_bytes())
}

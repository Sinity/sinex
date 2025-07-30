//! Strongly-typed identifiers for the Sinex system
//!
//! This module provides type-safe wrappers around ULID values to prevent
//! accidental mixing of different ID types (e.g., EventId vs CheckpointId).
//! All IDs are backed by ULIDs for time-ordering and global uniqueness.

use serde::{Deserialize, Serialize};
use sinex_ulid::Ulid;
use std::fmt;

// Re-export ULID for convenience
pub use sinex_ulid::Ulid as RawUlid;

/// Macro to define a new ID type with common implementations
macro_rules! define_id {
    (
        $(#[$meta:meta])*
        $name:ident
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Ulid);

        impl $name {
            /// Create a new ID with a fresh ULID
            pub fn new() -> Self {
                Self(Ulid::new())
            }

            /// Get the underlying ULID
            pub fn as_ulid(&self) -> &Ulid {
                &self.0
            }

            /// Convert to UUID for PostgreSQL compatibility
            pub fn to_uuid(&self) -> uuid::Uuid {
                self.0.to_uuid()
            }

            /// Create from a ULID
            pub fn from_ulid(ulid: Ulid) -> Self {
                Self(ulid)
            }

            /// Create from a UUID
            pub fn from_uuid(uuid: uuid::Uuid) -> Self {
                Self(Ulid::from_uuid(uuid))
            }

            /// Get the timestamp when this ID was created
            pub fn timestamp(&self) -> chrono::DateTime<chrono::Utc> {
                self.0.timestamp()
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl std::str::FromStr for $name {
            type Err = sinex_ulid::UlidError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(Self::from_ulid(s.parse()?))
            }
        }

        impl From<Ulid> for $name {
            fn from(ulid: Ulid) -> Self {
                Self::from_ulid(ulid)
            }
        }

        impl From<$name> for Ulid {
            fn from(id: $name) -> Self {
                id.0
            }
        }

        impl AsRef<Ulid> for $name {
            fn as_ref(&self) -> &Ulid {
                &self.0
            }
        }
    };
}

// Core ID types
define_id! {
    /// Unique identifier for events
    EventId
}

define_id! {
    /// Unique identifier for processor checkpoints
    CheckpointId
}

define_id! {
    /// Unique identifier for event payload schemas
    SchemaId
}

define_id! {
    /// Unique identifier for binary blobs
    BlobId
}

define_id! {
    /// Unique identifier for processing sessions
    SessionId
}

define_id! {
    /// Unique identifier for source material (external files)
    MaterialId
}

define_id! {
    /// Unique identifier for operations
    OperationId
}

define_id! {
    /// Unique identifier for event annotations
    AnnotationId
}

define_id! {
    /// Unique identifier for processors/automata
    ProcessorId
}

define_id! {
    /// Unique identifier for entities in the knowledge graph
    EntityId
}

define_id! {
    /// Unique identifier for entity relations
    RelationId
}

define_id! {
    /// Unique identifier for processor manifests
    ProcessorManifestId
}

// SQLx support for all ID types
#[cfg(feature = "sqlx")]
mod sqlx_impl {
    use super::*;
    use sqlx::postgres::{PgArgumentBuffer, PgHasArrayType, PgTypeInfo, PgValueRef};
    use sqlx::{Postgres, Type, TypeInfo};

    macro_rules! impl_sqlx_for_id {
        ($name:ident) => {
            impl Type<Postgres> for $name {
                fn type_info() -> PgTypeInfo {
                    // Register as ULID type
                    PgTypeInfo::with_name("ulid")
                }

                fn compatible(ty: &PgTypeInfo) -> bool {
                    // Compatible with both ulid and uuid types
                    ty.name() == "ulid" || ty.name() == "uuid"
                }
            }

            impl PgHasArrayType for $name {
                fn array_type_info() -> PgTypeInfo {
                    PgTypeInfo::with_name("_ulid")
                }
            }

            impl sqlx::Encode<'_, Postgres> for $name {
                fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Send + Sync + 'static>> {
                    self.0.encode_by_ref(buf)
                }
            }

            impl sqlx::Decode<'_, Postgres> for $name {
                fn decode(
                    value: PgValueRef<'_>,
                ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
                    let ulid = Ulid::decode(value)?;
                    Ok(Self::from_ulid(ulid))
                }
            }
        };
    }

    // Implement SQLx support for all ID types
    impl_sqlx_for_id!(EventId);
    impl_sqlx_for_id!(CheckpointId);
    impl_sqlx_for_id!(SchemaId);
    impl_sqlx_for_id!(BlobId);
    impl_sqlx_for_id!(SessionId);
    impl_sqlx_for_id!(MaterialId);
    impl_sqlx_for_id!(OperationId);
    impl_sqlx_for_id!(AnnotationId);
    impl_sqlx_for_id!(ProcessorId);
    impl_sqlx_for_id!(EntityId);
    impl_sqlx_for_id!(RelationId);
    impl_sqlx_for_id!(ProcessorManifestId);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_different_id_types_cannot_be_mixed() {
        let event_id = EventId::new();
        let checkpoint_id = CheckpointId::new();

        // This would fail to compile if uncommented:
        // let _wrong: EventId = checkpoint_id;

        // But we can compare the underlying ULIDs if needed
        assert_ne!(event_id.as_ulid(), checkpoint_id.as_ulid());
    }

    #[test]
    fn test_id_creation_and_conversion() {
        let id = EventId::new();

        // Convert to string and back
        let id_string = id.to_string();
        let parsed_id: EventId = id_string.parse().unwrap();
        assert_eq!(id, parsed_id);

        // Convert to UUID and back
        let uuid = id.to_uuid();
        let from_uuid = EventId::from_uuid(uuid);
        assert_eq!(id, from_uuid);
    }

    #[test]
    fn test_timestamp_extraction() {
        let before = chrono::Utc::now() - chrono::Duration::milliseconds(1);
        let id = EventId::new();
        let after = chrono::Utc::now() + chrono::Duration::milliseconds(1);

        let timestamp = id.timestamp();
        assert!(timestamp >= before);
        assert!(timestamp <= after);
    }

    #[test]
    fn test_ordering() {
        let id1 = EventId::new();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let id2 = EventId::new();

        // IDs should be time-ordered
        assert!(id1 < id2);
    }
}

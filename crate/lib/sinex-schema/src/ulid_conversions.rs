//! ULID/UUID conversion functions for database boundaries
//!
//! This module provides conversion functions between ULID and UUID types
//! specifically for database operations. These are pure schema-level utilities
//! that handle the boundary between Rust ULID types and PostgreSQL UUID storage.

use crate::ulid::Ulid;
use sqlx::types::Uuid as SqlxUuid;

/// Convert ULID to PostgreSQL UUID type (primary conversion function)
#[inline]
pub fn ulid_to_uuid(ulid: Ulid) -> SqlxUuid {
    SqlxUuid::from_bytes(*ulid.to_uuid().as_bytes())
}

/// Convert PostgreSQL UUID to ULID (primary conversion function)
#[inline]
pub fn uuid_to_ulid(uuid: SqlxUuid) -> Ulid {
    Ulid::from_uuid(uuid::Uuid::from_bytes(*uuid.as_bytes()))
}

// Shorter aliases for common use
pub use ulid_to_uuid as to_db;
pub use uuid_to_ulid as from_db;

/// Extension trait for ULID types to provide database conversions
pub trait UlidExt: Sized {
    /// Convert to database UUID representation
    fn to_db(&self) -> SqlxUuid;

    /// Convert an optional ULID to optional database UUID
    fn to_db_opt(opt: Option<Self>) -> Option<SqlxUuid>;
}

impl UlidExt for Ulid {
    #[inline]
    fn to_db(&self) -> SqlxUuid {
        ulid_to_uuid(*self)
    }

    #[inline]
    fn to_db_opt(opt: Option<Self>) -> Option<SqlxUuid> {
        opt.map(|ulid| ulid.to_db())
    }
}

/// Extension trait for database UUID types to provide ULID conversions
pub trait DbUuidExt {
    /// Convert from database UUID to ULID
    fn to_ulid(self) -> Ulid;
}

impl DbUuidExt for SqlxUuid {
    #[inline]
    fn to_ulid(self) -> Ulid {
        uuid_to_ulid(self)
    }
}

/// Helper trait for ULID collections
pub trait UlidArrayExt {
    fn to_uuid_vec(&self) -> Vec<SqlxUuid>;
    fn to_db_vec(&self) -> Vec<SqlxUuid> {
        self.to_uuid_vec()
    }
}

impl<T: AsRef<[Ulid]>> UlidArrayExt for T {
    fn to_uuid_vec(&self) -> Vec<SqlxUuid> {
        self.as_ref().iter().map(|&id| ulid_to_uuid(id)).collect()
    }
}

/// Extension trait for collections of database UUIDs
pub trait DbUuidCollectionExt {
    /// Convert collection of database UUIDs to ULIDs
    fn to_ulid_vec(self) -> Vec<Ulid>;
}

impl DbUuidCollectionExt for Vec<SqlxUuid> {
    fn to_ulid_vec(self) -> Vec<Ulid> {
        self.into_iter().map(uuid_to_ulid).collect()
    }
}

impl DbUuidCollectionExt for Option<Vec<SqlxUuid>> {
    fn to_ulid_vec(self) -> Vec<Ulid> {
        self.map(|v| v.to_ulid_vec()).unwrap_or_default()
    }
}

/// Convenience functions for common optional patterns
#[inline]
pub fn opt_to_db(ulid: Option<Ulid>) -> Option<SqlxUuid> {
    ulid.map(ulid_to_uuid)
}

#[inline]
pub fn opt_from_db(uuid: Option<SqlxUuid>) -> Option<Ulid> {
    uuid.map(uuid_to_ulid)
}

#[inline]
pub fn opt_vec_to_db(ulids: Option<Vec<Ulid>>) -> Option<Vec<SqlxUuid>> {
    ulids.map(|v| v.to_uuid_vec())
}

#[inline]
pub fn opt_vec_from_db(uuids: Option<Vec<SqlxUuid>>) -> Option<Vec<Ulid>> {
    uuids.map(|v| v.to_ulid_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ulid_conversion() {
        let ulid = Ulid::new();
        let uuid = ulid_to_uuid(ulid);
        let converted_back = uuid_to_ulid(uuid);
        assert_eq!(ulid, converted_back);
    }

    #[test]
    fn test_ulid_array_conversion() {
        let ulids = vec![Ulid::new(), Ulid::new(), Ulid::new()];
        let uuids = ulids.to_uuid_vec();
        assert_eq!(ulids.len(), uuids.len());

        for (ulid, uuid) in ulids.iter().zip(uuids.iter()) {
            assert_eq!(*ulid, uuid_to_ulid(*uuid));
        }
    }
}

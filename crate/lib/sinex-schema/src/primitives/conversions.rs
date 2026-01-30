//! Database conversion utilities for ULID types.

use super::ulid::Ulid;
use sqlx::types::Uuid as SqlxUuid;

/// Convert ULID to PostgreSQL UUID type
#[inline]
pub fn ulid_to_uuid(ulid: Ulid) -> SqlxUuid {
    SqlxUuid::from_bytes(*ulid.to_uuid().as_bytes())
}

/// Convert PostgreSQL UUID to ULID
#[inline]
pub fn uuid_to_ulid(uuid: SqlxUuid) -> Ulid {
    Ulid::from_uuid(uuid::Uuid::from_bytes(*uuid.as_bytes()))
}

/// Convert PostgreSQL UUID to ULID with validation
pub fn uuid_to_ulid_safe(uuid: SqlxUuid) -> Result<Ulid, String> {
    let uuid_std = uuid::Uuid::from_bytes(*uuid.as_bytes());
    let bytes = uuid_std.as_bytes();

    // Extract timestamp (first 6 bytes)
    let timestamp_bytes = [bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5]];
    let timestamp_ms = u64::from_be_bytes([
        0,
        0,
        timestamp_bytes[0],
        timestamp_bytes[1],
        timestamp_bytes[2],
        timestamp_bytes[3],
        timestamp_bytes[4],
        timestamp_bytes[5],
    ]);

    // Validate timestamp range
    const MIN_TIMESTAMP_MS: u64 = 1262304000000; // 2010-01-01
    const MAX_TIMESTAMP_MS: u64 = 4102444800000; // 2100-01-01

    if !(MIN_TIMESTAMP_MS..=MAX_TIMESTAMP_MS).contains(&timestamp_ms) {
        return Err(format!(
            "UUID timestamp {timestamp_ms} is outside valid ULID range ({MIN_TIMESTAMP_MS}-{MAX_TIMESTAMP_MS})"
        ));
    }

    Ok(Ulid::from_uuid(uuid_std))
}

// Shorter aliases
pub use ulid_to_uuid as to_db;
pub use uuid_to_ulid as from_db;
pub use uuid_to_ulid_safe as from_db_safe;

/// Extension trait for ULID database conversions
pub trait UlidExt: Sized {
    fn to_db(&self) -> SqlxUuid;
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

/// Extension trait for database UUID to ULID conversions
pub trait DbUuidExt {
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

/// Convenience functions for optional patterns
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

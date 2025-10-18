#![doc = include_str!("../doc/ulid_conversions.md")]

use crate::ulid::Ulid;
use sqlx::types::Uuid as SqlxUuid;

/// Convert ULID to PostgreSQL UUID type (primary conversion function)
///
/// This is the core conversion function for preparing ULIDs for database storage.
/// PostgreSQL stores ULIDs as UUID types via the pgx_ulid extension, so this
/// function handles the type conversion while preserving the underlying 16-byte
/// representation.
///
/// ## Performance
///
/// This is a zero-copy operation - the same 16 bytes are simply reinterpreted
/// as a different type. The conversion is essentially free at runtime.
///
/// ## Example
///
/// ```rust
/// use sinex_schema::ulid::Ulid;
/// use sinex_schema::ulid_conversions::ulid_to_uuid;
///
/// let ulid = Ulid::new();
/// let db_uuid = ulid_to_uuid(ulid);
/// // Now ready for database parameter binding
/// ```
#[inline]
pub fn ulid_to_uuid(ulid: Ulid) -> SqlxUuid {
    SqlxUuid::from_bytes(*ulid.to_uuid().as_bytes())
}

/// Convert PostgreSQL UUID to ULID (primary conversion function)
///
/// This function converts UUIDs retrieved from the database back into ULIDs
/// for use in application logic. Since PostgreSQL stores ULIDs as UUIDs,
/// this conversion is needed when deserializing query results.
///
/// ## Important Note
///
/// This function assumes the UUID was originally a ULID. Converting arbitrary
/// UUIDs to ULIDs will work technically, but the resulting ULID may not have
/// valid timestamp or monotonic properties.
///
/// ## Performance
///
/// Like `ulid_to_uuid`, this is a zero-copy operation that just reinterprets
/// the 16-byte representation.
///
/// ## Example
///
/// ```rust
/// use sinex_schema::ulid::Ulid;
/// use sinex_schema::ulid_conversions::{ulid_to_uuid, uuid_to_ulid};
///
/// let original = Ulid::new();
/// let db_uuid = ulid_to_uuid(original);
/// let restored = uuid_to_ulid(db_uuid);
/// assert_eq!(original, restored);
/// ```
#[inline]
pub fn uuid_to_ulid(uuid: SqlxUuid) -> Ulid {
    Ulid::from_uuid(uuid::Uuid::from_bytes(*uuid.as_bytes()))
}

/// Convert PostgreSQL UUID to ULID with validation (safe conversion function)
///
/// This function validates that the UUID follows ULID format constraints before
/// conversion. Use this when you need to ensure the UUID was originally a valid ULID.
///
/// ## Returns
///
/// - `Ok(Ulid)` if the UUID follows ULID format
/// - `Err(String)` if the UUID doesn't follow ULID format constraints
///
/// ## Example
///
/// ```rust
/// use sinex_schema::ulid::Ulid;
/// use sinex_schema::ulid_conversions::{ulid_to_uuid, uuid_to_ulid_safe};
///
/// // Valid ULID conversion
/// let original = Ulid::new();
/// let db_uuid = ulid_to_uuid(original);
/// let restored = uuid_to_ulid_safe(db_uuid).unwrap();
/// assert_eq!(original, restored);
///
/// // Invalid UUID would return an error
/// ```
pub fn uuid_to_ulid_safe(uuid: SqlxUuid) -> Result<Ulid, String> {
    let uuid_std = uuid::Uuid::from_bytes(*uuid.as_bytes());

    // Validate ULID format: Check if the timestamp part is reasonable
    // ULIDs have a 48-bit timestamp (milliseconds since Unix epoch)
    let bytes = uuid_std.as_bytes();

    // Extract the first 6 bytes (48 bits) as the timestamp
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

    // Reasonable timestamp range: after 2010 and before year 2100
    const MIN_TIMESTAMP_MS: u64 = 1262304000000; // 2010-01-01
    const MAX_TIMESTAMP_MS: u64 = 4102444800000; // 2100-01-01

    if !(MIN_TIMESTAMP_MS..=MAX_TIMESTAMP_MS).contains(&timestamp_ms) {
        return Err(format!(
            "UUID timestamp {timestamp_ms} is outside valid ULID range ({MIN_TIMESTAMP_MS}-{MAX_TIMESTAMP_MS})"
        ));
    }

    Ok(Ulid::from_uuid(uuid_std))
}

// Shorter aliases for common use
pub use ulid_to_uuid as to_db;
pub use uuid_to_ulid as from_db;

// Safe conversion alias
pub use uuid_to_ulid_safe as from_db_safe;

/// Extension trait for ULID types to provide database conversions
///
/// This trait adds convenience methods to ULID types for database operations.
/// It provides both instance methods and static methods for various conversion
/// scenarios commonly encountered in database code.
///
/// ## Usage
///
/// ```rust
/// use sinex_schema::ulid::Ulid;
/// use sinex_schema::ulid_conversions::UlidExt;
///
/// let ulid = Ulid::new();
/// let db_uuid = ulid.to_db();
///
/// // For optional values
/// let maybe_uuid = Ulid::to_db_opt(Some(ulid));
/// ```
pub trait UlidExt: Sized {
    /// Convert to database UUID representation
    ///
    /// This is equivalent to calling `ulid_to_uuid(self)` but provides
    /// a more fluent API when chaining operations.
    fn to_db(&self) -> SqlxUuid;

    /// Convert an optional ULID to optional database UUID
    ///
    /// This is a convenience method for handling `Option<Ulid>` values
    /// without explicit pattern matching. Returns `None` if the input
    /// is `None`, otherwise converts the contained ULID.
    ///
    /// ## Example
    ///
    /// ```rust
    /// use sinex_schema::ulid::Ulid;
    /// use sinex_schema::ulid_conversions::UlidExt;
    ///
    /// let some_ulid = Some(Ulid::new());
    /// let some_uuid = Ulid::to_db_opt(some_ulid);
    /// assert!(some_uuid.is_some());
    ///
    /// let none_ulid: Option<Ulid> = None;
    /// let none_uuid = Ulid::to_db_opt(none_ulid);
    /// assert!(none_uuid.is_none());
    /// ```
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

// Note: We cannot implement From traits for external types like Option<T>
// SQLX queries need to use the conversion functions directly

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

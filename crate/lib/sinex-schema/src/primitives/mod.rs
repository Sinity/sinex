//! Core primitive types for Sinex: ULID and Timestamp.

pub mod conversions;
pub mod timestamp;
pub mod ulid;

// Re-export main types
pub use timestamp::Timestamp;
pub use ulid::{Ulid, UlidError};

// Re-export conversion utilities
pub use conversions::{
    DbUuidCollectionExt, DbUuidExt, UlidArrayExt, UlidExt, from_db, from_db_safe, opt_from_db,
    opt_to_db, opt_vec_from_db, opt_vec_to_db, to_db, ulid_to_uuid, uuid_to_ulid,
    uuid_to_ulid_safe,
};

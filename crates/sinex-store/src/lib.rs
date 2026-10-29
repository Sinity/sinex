//! PostgreSQL and the content store. Admission is the only route by which an
//! event is stored; its statements live here and run inside the writer task's
//! transaction in `sinexd`. This crate knows no source or derivation by name.
//!
//! SQL is checked at compile time against a live development database with
//! `sqlx::query!`; there is no offline cache.
pub mod admission;
mod admission_context;
pub use admission_context::{AdmissionContext, AdmissionTransaction};
pub mod bindings;
pub mod blobs;
pub mod commit_log;
pub mod copy_text;
pub mod coverage;
pub mod derivations;
pub mod diagnostics;
pub mod excision;
pub mod live;
pub mod material_bytes;
pub mod payloads;
pub mod positions;
pub mod privacy;
pub mod producer_reads;
pub mod query;
pub mod read_models;
pub mod registry;
pub mod replay;
pub mod runs;
pub mod sealed;
mod stage;
pub mod telemetry;
pub mod writer_lock;

pub use admission::{
    AffectedRange, ComputedKey, DerivedScope, DirtyKeys, KeyInput, NoDerivations, Outcome, ReadMark,
};
pub use blobs::Blobs;
pub use blobs::Verified;
/// The connection the store's reads take, for crates that call them without
/// depending on the database driver.
pub use sqlx::PgConnection as Connection;

/// The stored payload bytes one page of events reads at most. Pages of events
/// are bounded by count and by size together, so a run of large payloads (a
/// notification carrying its image) never fills memory; a page always holds
/// its first event, however large. Stored payloads are compressed, and one
/// parsed in memory can take ten times its stored size.
pub const PAGE_BYTES: i64 = 8 << 20;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Query(#[from] sinex_model::query::QueryError),
    #[error(transparent)]
    Schema(#[from] sinex_schema::Error),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Blob(#[from] hashed_blobs::Error),
    #[error("{0}")]
    Invalid(String),
    #[error("query result changed; start the query again")]
    StaleCursor,
    #[error(
        "{0}: its class is sealed, so it must be sealed where its material is stored; it arrived unsealed"
    )]
    Unsealed(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod test_schema;

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Blob(error.into())
    }
}

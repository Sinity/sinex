//! PostgreSQL and the content store. Admission is the only route by which an
//! event is stored; its statements live here and run inside the writer task's
//! transaction in `sinexd`. This crate knows no source or derivation by name.
//!
//! SQL is checked at compile time against a live development database with
//! `sqlx::query!`; there is no offline cache.
pub mod admission;
pub mod analysis;
pub mod bindings;
pub mod blobs;
pub mod commit_log;
pub mod derivations;
pub mod diagnostics;
pub mod excision;
pub mod live;
pub mod material_bytes;
pub mod payloads;
pub mod positions;
pub mod privacy;
pub mod query;
pub mod read_models;
pub mod registry;
pub mod replay;
pub mod runs;
pub mod sealed;
mod stage;
pub mod writer_lock;

pub use admission::{DerivedScope, DirtyKeys, KeyInput, KeyScope, NoDerivations, Outcome, admit};
pub use blobs::Blobs;
pub use hashed_blobs::Verified;
/// The connection the store's reads take, for crates that call them without
/// depending on the database driver.
pub use sqlx::PgConnection as Connection;

#[derive(Debug, thiserror::Error)]
pub enum Error {
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

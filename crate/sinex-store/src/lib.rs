//! PostgreSQL and the content store. Admission is the only route by which an
//! event is stored (architecture §4); its statements live here and run inside
//! the writer task's transaction in `sinexd`. This crate knows no source or
//! derivation by name.
//!
//! SQL is checked at compile time against a live development database with
//! `sqlx::query!`; there is no offline cache (operator ruling, 2025-08-10).
pub mod admission;
pub mod blobs;
pub mod committed;
pub mod derived;
pub mod diagnostics;
pub mod excision;
pub mod positions;
pub mod privacy;
pub mod query;
pub mod read_models;
pub mod replay;
pub mod runs;
pub mod sealed;
mod stage;

pub use admission::{DerivedScope, DirtyKeys, KeyInput, NoDerivations, Outcome, admit};
pub use blobs::Blobs;
pub use hashed_blobs::Verified;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Blob(#[from] hashed_blobs::Error),
    #[error("{0}")]
    Invalid(String),
    #[error(
        "{0}: its class is sealed, so it must be sealed where its material is stored; it arrived unsealed"
    )]
    Unsealed(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
pub(crate) mod testdb;

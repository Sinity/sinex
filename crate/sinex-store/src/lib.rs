//! PostgreSQL and the content store. Admission is the only route by which an
//! event is stored (architecture §4); its statements live here and run inside
//! the writer task's transaction in `sinexd`. This crate knows no source or
//! derivation by name.
//!
//! SQL is checked at compile time against a live development database with
//! `sqlx::query!`; there is no offline cache (operator ruling, 2025-08-10).
pub mod admission;
pub mod blobs;
pub mod positions;
pub mod privacy;
pub mod query;
pub mod read_models;
pub mod runs;
mod stage;

pub use admission::{DirtyKeys, KeyInput, NoDerivations, Outcome, admit};
pub use blobs::Blobs;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Blob(#[from] hashed_blobs::Error),
    #[error("{0}")]
    Invalid(String),
    #[error("sealed data needs the encryption path, which is not built yet (architecture §3.5)")]
    SealedNotBuilt,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
pub(crate) mod testdb;

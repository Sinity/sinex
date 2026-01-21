//! Persistent build/test history stored in SQLite.
//!
//! Provides queryable history of xtask invocations, test results, and build diagnostics.

mod db;
mod tests;

pub use db::{HistoryDb, InvocationStatus};
pub use tests::Confidence;

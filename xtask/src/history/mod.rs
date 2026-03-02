//! Persistent build/test history stored in `SQLite`.
//!
//! Provides queryable history of xtask invocations, test results, and build diagnostics.
//! Also tracks background jobs via the unified invocations table.

mod db;
mod tests;

pub use db::{
    BackgroundJob, CommandStats, DiagnosticCounts, DiagnosticTrendPoint, HistoryDb, Invocation,
    InvocationStatus, InvocationWithFingerprint, StoredDiagnostic,
};
pub use tests::Confidence;

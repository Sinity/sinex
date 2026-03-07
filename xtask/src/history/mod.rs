//! Persistent build/test history stored in `SQLite`.
//!
//! Provides queryable history of xtask invocations, test results, and build diagnostics.
//! Also tracks background jobs via the unified invocations table.

mod db;
pub mod query;
mod tests;
pub mod tracing_layer;

pub use db::{
    BackgroundJob, CommandStats, DiagnosticCounts, DiagnosticTrendPoint, HistoryDb, Invocation,
    InvocationStatus, InvocationWithFingerprint, StageTiming, StoredDiagnostic, TestProgress,
};
pub use query::{
    DiagnosticQuery, DiagnosticScope, HistoryAnalysis, InvocationQuery, PackageHealth, Regression,
    TestResultQuery,
};
pub use tests::{Confidence, TestResult, TestStatus};
pub use tracing_layer::{CURRENT_INVOCATION_ID, HistoryTracingLayer};

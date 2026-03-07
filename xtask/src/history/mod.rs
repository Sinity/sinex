//! Persistent build/test history stored in `SQLite`.
//!
//! Provides queryable history of xtask invocations, test results, and build diagnostics.
//! Also tracks background jobs via the unified invocations table.

mod db;
pub mod query;
mod tests;
pub mod tracing_layer;

pub use db::{
    BackgroundJob, CommandStats, DiagnosticCounts, DiagnosticDelta, DiagnosticTrendPoint,
    FixSession, HistoryDb, Invocation, InvocationStatus, InvocationWithFingerprint, StageStats,
    StageTiming, StageTrendPoint, StoredDiagnostic, TestProgress,
};
pub use query::{
    DiagnosticQuery, DiagnosticScope, HistoryAnalysis, InvocationQuery, PackageHealth, Regression,
    TestResultQuery,
};
pub use tests::{
    Confidence, PackageTestStats, RegressionTest, TestOutputEntry, TestResult, TestStatus,
};
pub use tracing_layer::{CURRENT_INVOCATION_ID, HistoryTracingLayer};

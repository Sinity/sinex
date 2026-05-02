//! Persistent build/test history stored in `SQLite`.
//!
//! Provides queryable history of xtask invocations, test results, and build diagnostics.
//! Also tracks background jobs via the unified invocations table.

mod analysis;
mod db;
pub mod query;
pub mod seed;
mod tests;
pub mod tracing_layer;

pub use analysis::{
    AnalyticsSnapshot, DiagnosticHotspot, HistoryAnalysis, PackageHealth, PackageReliability,
    Recommendation, Regression, VelocityTrend, WorkspaceHealthReport,
};
pub use db::{
    BackgroundJob, CommandStats, DiagnosticCounts, DiagnosticDelta, DiagnosticLifecycle,
    DiagnosticTrendPoint, ExerciseResultRow, ExerciseRunRow, FixSession, HistoryDb, Invocation,
    InvocationFull, InvocationProgress, InvocationStatus, InvocationTimelineEntry,
    InvocationWithFingerprint, JobLifecycleStatus, LifecycleStatus, ResourceUsage, StageStats,
    StageTiming, StageTrendPoint, StoredDiagnostic, WorkingSession,
};
pub use query::{DiagnosticQuery, DiagnosticScope, InvocationQuery, TestResultQuery};
pub use seed::SeedOptions;
pub use tests::{
    Confidence, FailingTest, PackageTestStats, RegressionTest, ResolvedTestRun, TestOutputEntry,
    TestResult, TestStatus,
};
pub use tracing_layer::{CURRENT_INVOCATION_ID, HistoryTracingLayer};

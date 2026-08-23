//! Persistent build/test history stored in `SQLite`.
//!
//! Provides queryable history of xtask invocations, test results, and build diagnostics.

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
    CommandStats, DiagnosticCounts, DiagnosticDelta, DiagnosticLifecycle, DiagnosticTrendPoint,
    DriftGuardBypass, ExerciseResultRow, ExerciseRunRow, FixSession, HistoryDb, ImpactAuditRunRow,
    Invocation, InvocationFull, InvocationProgress, InvocationStatus, InvocationTimelineEntry,
    InvocationWithFingerprint, LifecycleStatus, ProofEvidence, ResourceUsage, StagePressure,
    StageStats, StageTiming, StageTrendPoint, StoredDiagnostic, TestProofUnit, TraceEventRow,
    WorkingSession, WrapperEventRow,
};
pub use query::{DiagnosticQuery, DiagnosticScope, InvocationQuery, TestResultQuery};
pub use seed::SeedOptions;
pub use tests::{
    Confidence, FailingTest, HistoricalSlowTest, HostPressureFailureClassification,
    PackageTestStats, RegressionTest, ResolvedTestRun, TestOutputEntry, TestResult,
    TestRunOverhead, TestStatus, TestSuiteAnalysis,
};
pub use tracing_layer::{CURRENT_INVOCATION_ID, HistoryTracingLayer};

//! Persistent build/test history stored in `SQLite`.
//!
//! Provides queryable history of xtask invocations, test results, and build diagnostics.
//! Also tracks background jobs via the unified invocations table.

mod analysis;
mod db;
pub mod merge;
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
    DiagnosticTrendPoint, DriftGuardBypass, ExerciseResultRow, ExerciseRunRow, FixSession,
    HistoryDb, ImpactAuditRunRow, Invocation, InvocationFull, InvocationProgress, InvocationStatus,
    InvocationTimelineEntry, InvocationWithFingerprint, JobLifecycleStatus, LifecycleStatus,
    ProofEvidence, ResourceUsage, StagePressure, StageStats, StageTiming, StageTrendPoint,
    StoredDiagnostic, TestProofUnit, TraceEventRow, WorkingSession, WrapperEventRow,
};
pub use merge::{
    BackfillReport, ImportReport, WorkspaceAttribution, attribution_for_workspace_db,
    backfill_workspace_attribution, import_history, recorded_workspace_roots,
};
pub use query::{DiagnosticQuery, DiagnosticScope, InvocationQuery, TestResultQuery};
pub use seed::SeedOptions;
pub use tests::{
    Confidence, FailingTest, HistoricalSlowTest, HostPressureFailureClassification,
    PackageTestStats, RegressionTest, ResolvedTestRun, TestOutputEntry, TestResult,
    TestRunOverhead, TestStatus, TestSuiteAnalysis,
};
pub use tracing_layer::{CURRENT_INVOCATION_ID, HistoryTracingLayer};

use super::git::GitState;
use crate::history::{
    DiagnosticCounts, Invocation, InvocationStatus, Recommendation, VelocityTrend,
    WorkspaceHealthReport,
};
use crate::infra::probe::{NatsProbe, PostgresProbe};
use crate::runtime_metrics::{RuntimeAssessment, RuntimeMetrics};
use crate::runtime_target::RuntimeTargetSummary;
use serde::Serialize;
use sinex_primitives::{RuntimeStatusSnapshot, RuntimeTargetDescriptor};

/// Structured status output for JSON mode.
#[derive(Debug, Serialize)]
pub(super) struct StatusOutput {
    pub(super) runtime_target: RuntimeTargetSummary,
    pub(super) runtime_snapshot: RuntimeStatusSnapshot,
    pub(super) infrastructure: InfrastructureStatus,
    pub(super) services: Vec<ServiceStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) runtime: Option<RuntimeMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) runtime_assessment: Option<RuntimeAssessment>,
    pub(super) history: HistoryStatusOutput,
    pub(super) jobs: JobsStatus,
    pub(super) recent_activity: Vec<ActivityEntry>,
    pub(super) warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(super) struct InfrastructureStatus {
    pub(super) postgres: ComponentStatus,
    pub(super) nats: ComponentStatus,
}

#[derive(Debug, Serialize)]
pub(super) struct ComponentStatus {
    pub(super) status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) latency_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) message: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ServiceStatus {
    pub(super) name: String,
    pub(super) status: ServiceRunStatus,
    pub(super) probe: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) message: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum ServiceRunStatus {
    Running,
    Stopped,
    Skipped,
    Unknown,
}

#[derive(Debug, Serialize)]
pub(super) struct JobsStatus {
    pub(super) active: usize,
    pub(super) recent_failures: usize,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct HistoryStatusOutput {
    pub(super) status: String,
    pub(super) synthetic: bool,
    pub(super) recent_invocations: usize,
    pub(super) diagnostic_errors: usize,
    pub(super) diagnostic_warnings: usize,
    pub(super) fixable_diagnostics: usize,
    pub(super) flaky_tests: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) message: Option<String>,
}

#[derive(Debug, Serialize)]
pub(super) struct ActivityEntry {
    pub(super) command: String,
    pub(super) status: String,
    pub(super) duration_secs: Option<f64>,
    pub(super) timestamp: String,
}

/// Summary (MOTD) output structure.
#[derive(Debug, Serialize)]
pub(super) struct SummaryOutput {
    pub(super) runtime_target: RuntimeTargetSummary,
    pub(super) runtime_snapshot: RuntimeStatusSnapshot,
    pub(super) health: String,
    /// Condensed single-field grade: "ok" | "warn" | "error" | "infra"
    pub(super) health_indicator: String,
    pub(super) summary: String,
    pub(super) infrastructure: SummaryInfraHealth,
    pub(super) last_commands: SummaryLastCommands,
    pub(super) diagnostics: SummaryDiagnostics,
    pub(super) active_jobs: usize,
    pub(super) git: SummaryGitState,
    pub(super) warnings: Vec<String>,
    pub(super) history: HistoryStatusOutput,
    // --- Rich fields ---
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) health_score: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) velocity: Option<Vec<VelocityTrendOutput>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) baseline_velocity: Option<Vec<VelocityTrendOutput>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) recommendations: Option<Vec<RecommendationOutput>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) runtime: Option<RuntimeMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) services: Option<Vec<ServiceStatus>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) last_commit: Option<CommitInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) stash_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) files_changed: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) uncommitted_count: Option<usize>,
}

#[derive(Debug, Serialize)]
pub(super) struct VelocityTrendOutput {
    pub(super) command: String,
    pub(super) scope_label: Option<String>,
    pub(super) recent_avg_secs: Option<f64>,
    pub(super) delta_pct: Option<f64>,
    pub(super) trend: String,
    pub(super) sample_count: usize,
}

impl From<&VelocityTrend> for VelocityTrendOutput {
    fn from(v: &VelocityTrend) -> Self {
        Self {
            command: v.command.clone(),
            scope_label: v.scope_label.clone(),
            recent_avg_secs: v.recent_avg_secs,
            delta_pct: v.delta_pct,
            trend: v.trend.clone(),
            sample_count: v.sample_count,
        }
    }
}

#[derive(Debug, Serialize)]
pub(super) struct RecommendationOutput {
    pub(super) severity: String,
    pub(super) category: String,
    pub(super) description: String,
    pub(super) action: String,
}

impl From<&Recommendation> for RecommendationOutput {
    fn from(r: &Recommendation) -> Self {
        Self {
            severity: r.severity.clone(),
            category: r.category.clone(),
            description: r.description.clone(),
            action: r.action.clone(),
        }
    }
}

#[derive(Debug, Serialize)]
pub(super) struct CommitInfo {
    pub(super) hash: String,
    pub(super) message: String,
    pub(super) age_mins: Option<i64>,
}

#[derive(Debug, Serialize)]
pub(super) struct SummaryDiagnostics {
    pub(super) errors: usize,
    pub(super) warnings: usize,
    /// Auto-fixable warnings (MachineApplicable)
    pub(super) fixable: usize,
    /// Tests that passed on retry (flaky)
    pub(super) flaky_tests: usize,
}

#[derive(Debug, Serialize)]
pub(super) struct SummaryInfraHealth {
    pub(super) postgres: bool,
    pub(super) nats: bool,
}

#[derive(Debug, Serialize)]
pub(super) struct SummaryLastCommands {
    pub(super) check: Option<SummaryCommandInfo>,
    pub(super) test: Option<SummaryCommandInfo>,
    pub(super) build: Option<SummaryCommandInfo>,
}

#[derive(Debug, Serialize)]
pub(super) struct SummaryCommandInfo {
    pub(super) status: InvocationStatus,
    pub(super) duration_secs: Option<f64>,
    pub(super) age_mins: i64,
}

#[derive(Debug, Serialize)]
pub(super) struct SummaryGitState {
    pub(super) branch: Option<String>,
    pub(super) dirty: bool,
    pub(super) ahead: u32,
    pub(super) behind: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) message: Option<String>,
}

/// Active job detail for rich MOTD.
pub(super) struct ActiveJobDetail {
    pub(super) command: String,
    pub(super) elapsed_secs: f64,
}

#[derive(Default)]
pub(super) struct JobsSnapshot {
    pub(super) active: Vec<crate::jobs::Job>,
    pub(super) recent: Vec<crate::jobs::Job>,
    pub(super) issues: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct HistorySnapshot {
    pub(super) available: bool,
    pub(super) recent: Vec<Invocation>,
    pub(super) diag_counts: DiagnosticCounts,
    pub(super) error_packages: Vec<String>,
    pub(super) flaky_count: usize,
    pub(super) is_synthetic: bool,
    pub(super) health_report: Option<WorkspaceHealthReport>,
    pub(super) velocity: Vec<VelocityTrend>,
    pub(super) baseline_velocity: Vec<VelocityTrend>,
    pub(super) recommendations: Vec<Recommendation>,
    pub(super) issues: Vec<String>,
}

impl HistorySnapshot {
    pub(super) fn unavailable(message: String) -> Self {
        Self {
            available: false,
            issues: vec![message],
            ..Self::default()
        }
    }

    pub(super) fn status(&self) -> &'static str {
        if !self.available {
            "unavailable"
        } else if self.is_synthetic {
            "synthetic"
        } else if self.issues.is_empty() {
            "available"
        } else {
            "degraded"
        }
    }

    pub(super) fn message(&self) -> Option<String> {
        (!self.issues.is_empty()).then(|| self.issues.join("; "))
    }

    pub(super) fn output(&self) -> HistoryStatusOutput {
        HistoryStatusOutput {
            status: self.status().to_string(),
            synthetic: self.is_synthetic,
            recent_invocations: self.recent.len(),
            diagnostic_errors: self.diag_counts.errors,
            diagnostic_warnings: self.diag_counts.warnings,
            fixable_diagnostics: self.diag_counts.fixable,
            flaky_tests: self.flaky_count,
            message: self.message(),
        }
    }
}

/// All collected summary data.
pub(super) struct SummaryData {
    pub(super) runtime_target: RuntimeTargetDescriptor,
    pub(super) pg_probe: PostgresProbe,
    pub(super) nats_probe: NatsProbe,
    pub(super) services: Vec<ServiceStatus>,
    pub(super) git: GitState,
    pub(super) active_job_details: Vec<ActiveJobDetail>,
    pub(super) active_job_count: usize,
    pub(super) history: HistorySnapshot,
    pub(super) job_issues: Vec<String>,
    pub(super) runtime_metrics: Option<RuntimeMetrics>,
}

use color_eyre::eyre::Result;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use super::diagnostics::StoredDiagnostic;

/// A devshell wrapper rebuild event persisted into the `wrapper_events` table
/// from `xtask-wrapper-events.jsonl`. Mirrors the JSONL fields needed to make
/// checkout-local rebuild cost SQL-queryable and joinable with `invocations`.
#[derive(Debug, Clone)]
pub struct WrapperEventRow {
    pub event: String,
    pub status: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub duration_secs: Option<f64>,
    pub command: Option<String>,
    pub args: Option<String>,
    pub force_rebuild: bool,
    pub rebuild_reason: Option<String>,
    pub stage_durations_json: Option<String>,
}

/// Status of a command invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InvocationStatus {
    Running,
    Success,
    Failed,
    Cancelled,
}

impl InvocationStatus {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Success => "success",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub(crate) fn try_from_str(s: &str) -> Result<Self> {
        match s {
            "running" => Ok(Self::Running),
            "success" => Ok(Self::Success),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(color_eyre::eyre::eyre!(
                "invalid invocation status in history DB: {s}"
            )),
        }
    }
}

/// Process lifecycle status for background jobs (separate from invocation success/failure).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobLifecycleStatus {
    Running,
    Completed,
    Failed,
    Orphaned,
    Killed,
    TimedOut,
}

impl JobLifecycleStatus {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Orphaned => "orphaned",
            Self::Killed => "killed",
            Self::TimedOut => "timed_out",
        }
    }

    pub(crate) fn try_from_str(s: &str) -> Result<Self> {
        match s {
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "orphaned" => Ok(Self::Orphaned),
            "killed" => Ok(Self::Killed),
            "timed_out" => Ok(Self::TimedOut),
            _ => Err(color_eyre::eyre::eyre!("invalid job lifecycle status: {s}")),
        }
    }

    pub(crate) fn is_terminal(&self) -> bool {
        !matches!(self, Self::Running)
    }

    #[must_use]
    pub(crate) fn from_invocation_status(status: InvocationStatus) -> Self {
        match status {
            InvocationStatus::Running => Self::Running,
            InvocationStatus::Success => Self::Completed,
            InvocationStatus::Failed => Self::Failed,
            InvocationStatus::Cancelled => Self::Killed,
        }
    }
}

/// A recorded command invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Invocation {
    pub id: i64,
    pub command: String,
    pub subcommand: Option<String>,
    pub profile: Option<String>,
    pub args_json: Option<String>,
    pub git_commit: Option<String>,
    pub git_dirty: bool,
    pub started_at: OffsetDateTime,
    pub finished_at: Option<OffsetDateTime>,
    pub duration_secs: Option<f64>,
    pub exit_code: Option<i32>,
    pub status: InvocationStatus,
    pub host: String,
    pub cwd: String,
    /// Currently executing pipeline stage (NULL when idle or finished).
    pub live_stage: Option<String>,
}

/// A recorded drift guard bypass event (#1565).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftGuardBypass {
    pub id: i64,
    pub recorded_at: String,
    pub git_branch: Option<String>,
    pub head_sha: Option<String>,
    pub push_succeeded: Option<bool>,
}

/// A recorded impact-plan audit run (skip-accuracy evidence). Surfaced via
/// `xtask history view impact-audit` so the table needs no raw `sqlite3`.
#[derive(Debug, Clone, Serialize)]
pub struct ImpactAuditRunRow {
    pub id: i64,
    pub invocation_id: Option<i64>,
    pub sample_size: i64,
    pub status: String,
    pub false_negative_count: i64,
    pub created_at: String,
}

/// A recorded internal trace event. Surfaced via `xtask history view traces`
/// so the table needs no raw `sqlite3`.
#[derive(Debug, Clone, Serialize)]
pub struct TraceEventRow {
    pub id: i64,
    pub invocation_id: Option<i64>,
    pub ts: String,
    pub level: String,
    pub target: String,
    pub message: String,
}

// ─── I: Semantic Query Intelligence types ─────────────────────────────────────

/// One entry in the cross-invocation timeline view (I4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationTimelineEntry {
    pub id: i64,
    pub command: String,
    pub status: InvocationStatus,
    pub started_at: String,
    pub duration_secs: Option<f64>,
    pub stage_count: usize,
    pub error_count: usize,
    pub warning_count: usize,
    /// Change in (error + warning) count vs the previous timeline entry.
    pub diagnostic_delta: i64,
}

/// A contiguous working session: invocations grouped by < N min gaps (I6).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkingSession {
    pub session_index: usize,
    pub first_started: String,
    pub last_finished: Option<String>,
    pub invocation_count: usize,
    pub commands: Vec<String>,
    pub total_duration_secs: f64,
    pub success_count: usize,
    pub failure_count: usize,
}

/// Complete invocation picture: record + stages + diagnostics (I7).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationFull {
    pub invocation: Invocation,
    pub stages: Vec<StageTiming>,
    pub diagnostics: Vec<StoredDiagnostic>,
    pub error_count: usize,
    pub warning_count: usize,
}

/// Live progress snapshot for a running invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationProgress {
    pub invocation_id: i64,
    pub phase: Option<String>,
    pub step: Option<String>,
    /// 0.0–100.0, None if indeterminate
    pub pct_done: Option<f64>,
    pub items_done: Option<i64>,
    pub items_total: Option<i64>,
    pub updated_at: String,
    /// "indeterminate" | "determinate"
    pub mode: Option<String>,
    /// "packages" | "files" | "bytes" | "tests"
    pub unit_kind: Option<String>,
    /// items/sec computed from recent deltas
    pub rate_per_sec: Option<f64>,
    /// "none" | "rough" | "calibrated"
    pub eta_confidence: Option<String>,
    /// One-line human display string
    pub terminal_summary: Option<String>,
}

/// Recorded timing for a single pipeline stage within an invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageTiming {
    pub invocation_id: i64,
    pub stage_name: String,
    pub started_at: String,
    pub duration_secs: f64,
    pub success: bool,
    /// End-of-stage PSI io.full avg10 snapshot (None if /proc/pressure unavailable).
    pub io_full_avg10: Option<f64>,
    /// End-of-stage PSI cpu.some avg10 snapshot.
    pub cpu_some_avg10: Option<f64>,
    /// End-of-stage PSI memory.some avg10 snapshot.
    pub memory_some_avg10: Option<f64>,
    /// Delta of /proc/pressure io.full `total=` stall μs over the stage.
    pub io_full_stall_us: Option<i64>,
    /// Delta of /proc/pressure cpu.some `total=` stall μs over the stage.
    pub cpu_some_stall_us: Option<i64>,
    /// Delta of /proc/pressure memory.some `total=` stall μs over the stage.
    pub memory_some_stall_us: Option<i64>,
}

/// Per-stage pressure-stall metrics recorded alongside a stage timing.
///
/// Bundles the tail-biased end-of-stage avg10 snapshot with the precise,
/// length-independent stall-microsecond delta over the stage window. Passed as
/// a single struct to `record_stage_timing` to keep its signature manageable.
#[derive(Debug, Clone, Copy, Default)]
pub struct StagePressure {
    /// End-of-stage PSI io.full avg10 snapshot.
    pub io_full_avg10: Option<f64>,
    /// End-of-stage PSI cpu.some avg10 snapshot.
    pub cpu_some_avg10: Option<f64>,
    /// End-of-stage PSI memory.some avg10 snapshot.
    pub memory_some_avg10: Option<f64>,
    /// Delta of /proc/pressure io.full `total=` stall μs over the stage.
    pub io_full_stall_us: Option<i64>,
    /// Delta of /proc/pressure cpu.some `total=` stall μs over the stage.
    pub cpu_some_stall_us: Option<i64>,
    /// Delta of /proc/pressure memory.some `total=` stall μs over the stage.
    pub memory_some_stall_us: Option<i64>,
}

/// A background job record from the history database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackgroundJob {
    /// Background job ID (`background_jobs.id`) — the process handle.
    pub id: i64,
    /// Invocation ID (`invocations.id`) — the durable execution record.
    pub invocation_id: Option<i64>,
    pub command: String,
    pub args: Vec<String>,
    pub started_at: OffsetDateTime,
    pub pid: Option<u32>,
    pub stdout_path: Option<String>,
    pub stderr_path: Option<String>,
    /// Process lifecycle status (running/completed/failed/orphaned/killed).
    pub job_status: JobLifecycleStatus,
    pub exit_code: Option<i32>,
}

/// Resource usage snapshot for a single invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUsage {
    pub command: String,
    pub status: String,
    pub started_at: String,
    pub duration_secs: Option<f64>,
    pub process_cpu_usage_avg: Option<f64>,
    pub process_memory_usage_max_mb: Option<f64>,
    pub root_process_cpu_usage_avg: Option<f64>,
    pub root_process_memory_usage_max_mb: Option<f64>,
    pub shared_nix_daemon_cpu_usage_avg: Option<f64>,
    pub shared_nix_daemon_memory_usage_max_mb: Option<f64>,
    pub shared_nix_build_slice_cpu_usage_avg: Option<f64>,
    pub shared_nix_build_slice_memory_usage_max_mb: Option<f64>,
    pub shared_background_slice_cpu_usage_avg: Option<f64>,
    pub shared_background_slice_memory_usage_max_mb: Option<f64>,
    pub process_count_max: Option<u32>,
    pub sample_count: Option<u32>,
    pub host_cpu_usage_avg: Option<f64>,
    pub host_memory_usage_max_mb: Option<f64>,
    pub host_cpu_pressure_some_avg10_max: Option<f64>,
    pub host_io_pressure_some_avg10_max: Option<f64>,
    pub host_io_pressure_full_avg10_max: Option<f64>,
    pub host_memory_pressure_some_avg10_max: Option<f64>,
    pub host_memory_pressure_full_avg10_max: Option<f64>,
    pub host_block_read_mib_delta: Option<f64>,
    pub host_block_write_mib_delta: Option<f64>,
    pub host_block_read_iops_avg: Option<f64>,
    pub host_block_write_iops_avg: Option<f64>,
    pub host_block_busiest_device: Option<String>,
    pub host_block_busiest_device_total_mib_delta: Option<f64>,
    pub host_block_busiest_device_read_iops_avg: Option<f64>,
    pub host_block_busiest_device_write_iops_avg: Option<f64>,
    pub host_block_busiest_device_weighted_io_ms_per_s: Option<f64>,
    pub shm_free_min_mb: Option<f64>,
    pub shm_used_max_mb: Option<f64>,
}

impl ResourceUsage {
    #[must_use]
    pub fn has_samples(&self) -> bool {
        self.process_cpu_usage_avg.is_some()
            || self.process_memory_usage_max_mb.is_some()
            || self.root_process_cpu_usage_avg.is_some()
            || self.root_process_memory_usage_max_mb.is_some()
            || self.shared_nix_daemon_cpu_usage_avg.is_some()
            || self.shared_nix_daemon_memory_usage_max_mb.is_some()
            || self.shared_nix_build_slice_cpu_usage_avg.is_some()
            || self.shared_nix_build_slice_memory_usage_max_mb.is_some()
            || self.shared_background_slice_cpu_usage_avg.is_some()
            || self.shared_background_slice_memory_usage_max_mb.is_some()
            || self.process_count_max.is_some()
            || self.sample_count.is_some()
            || self.host_cpu_usage_avg.is_some()
            || self.host_memory_usage_max_mb.is_some()
            || self.host_cpu_pressure_some_avg10_max.is_some()
            || self.host_io_pressure_some_avg10_max.is_some()
            || self.host_io_pressure_full_avg10_max.is_some()
            || self.host_memory_pressure_some_avg10_max.is_some()
            || self.host_memory_pressure_full_avg10_max.is_some()
            || self.host_block_read_mib_delta.is_some()
            || self.host_block_write_mib_delta.is_some()
            || self.host_block_read_iops_avg.is_some()
            || self.host_block_write_iops_avg.is_some()
            || self.host_block_busiest_device.is_some()
            || self.host_block_busiest_device_total_mib_delta.is_some()
            || self.host_block_busiest_device_read_iops_avg.is_some()
            || self.host_block_busiest_device_write_iops_avg.is_some()
            || self
                .host_block_busiest_device_weighted_io_ms_per_s
                .is_some()
            || self.shm_free_min_mb.is_some()
            || self.shm_used_max_mb.is_some()
    }
}

/// Stage timing summary entry (G2 — slowest stages view).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageStats {
    pub stage_name: String,
    pub avg_duration_secs: f64,
    pub max_duration_secs: f64,
    pub run_count: usize,
}

/// A single data point in a stage timing trend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageTrendPoint {
    pub invocation_id: i64,
    pub started_at: String,
    pub duration_secs: f64,
    pub success: bool,
}

/// A fix session: an invocation of `xtask fix` with before/after diagnostic snapshot (G3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixSession {
    pub invocation_id: i64,
    pub started_at: String,
    pub duration_secs: Option<f64>,
    pub pre_fix_errors: Option<i64>,
    pub pre_fix_warnings: Option<i64>,
    pub pre_fix_fixable: Option<i64>,
}

/// An invocation with its coordination fingerprint data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationWithFingerprint {
    pub id: i64,
    pub status: InvocationStatus,
    pub duration_secs: Option<f64>,
    pub tree_fingerprint: Option<String>,
    pub scope_key: Option<String>,
}

/// A durable proof row produced by a successful xtask invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProofEvidence {
    pub id: i64,
    pub invocation_id: i64,
    pub command: String,
    pub proof_kind: String,
    pub scope_key: String,
    pub input_fingerprint: String,
    pub status: InvocationStatus,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub duration_secs: Option<f64>,
    pub scope_json: Option<String>,
    pub artifact_json: Option<String>,
}

/// A resolved test execution plan that can be reused when its inputs match.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestProofUnit {
    pub id: i64,
    pub invocation_id: i64,
    pub proof_kind: String,
    pub scope_key: String,
    pub input_fingerprint: String,
    pub manifest_json: String,
    pub test_filter: Option<String>,
    pub reusable: bool,
    pub status: InvocationStatus,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub duration_secs: Option<f64>,
}

/// Statistics for a command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandStats {
    pub total: i64,
    pub successes: i64,
    pub failures: i64,
    pub avg_duration_secs: Option<f64>,
}

/// One row from `exercise_runs` joined to `invocations`.
pub struct ExerciseRunRow {
    pub run_id: i64,
    pub invocation_id: Option<i64>,
    pub tier: Option<String>,
    pub total: i64,
    pub passed: i64,
    pub failed: i64,
    pub skipped: i64,
    pub duration_secs: f64,
    pub recorded_at: String,
    pub invocation_status: Option<String>,
    pub git_commit: Option<String>,
}

/// One row from `exercise_results`.
pub struct ExerciseResultRow {
    pub exercise_id: String,
    pub exercise_tier: Option<String>,
    pub passed: bool,
    pub duration_secs: f64,
    pub error: Option<String>,
    pub step_count: i64,
}

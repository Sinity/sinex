//! Operator-facing source status RPC types.
//!
//! Mirrors `rpc::automata` for the source-side: every registered source
//! manifest, joined to its latest run, latest
//! `health.status` event, and recent event-emission stats. Distinct from
//! `rpc::runtime` (which carries coordinator-style state — drain/resume/horizon).

use crate::domain::{HealthStatus, ModuleName};
use crate::env as shared_env;
use crate::rpc::{RpcDomain, RpcMethod, RpcMutability, RpcRole, RpcStability, methods};
use crate::views::{SourceCoverageListView, ViewEnvelope};
use crate::{Result, Timestamp, Uuid};
use serde::{Deserialize, Serialize};

fn default_stale_after_secs() -> u64 {
    300
}

fn default_recent_window_secs() -> u64 {
    300
}

pub const SOURCES_STATUS_METHOD: RpcMethod<SourcesStatusRequest, SourcesStatusResponse> =
    RpcMethod::new(
        methods::SOURCES_STATUS,
        RpcRole::ReadOnly,
        RpcDomain::Sources,
        RpcStability::Experimental,
        RpcMutability::ReadOnly,
    );

pub const SOURCES_STATUS_VIEW_METHOD: RpcMethod<
    SourcesStatusViewRequest,
    ViewEnvelope<SourceCoverageListView>,
> = RpcMethod::new(
    methods::SOURCES_STATUS_VIEW,
    RpcRole::ReadOnly,
    RpcDomain::Sources,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

/// Request: `sources.status`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesStatusRequest {
    /// Heartbeats older than this make the source non-live.
    #[serde(default = "default_stale_after_secs")]
    pub stale_after_secs: u64,
    /// Window used for recent-event-count context.
    #[serde(default = "default_recent_window_secs")]
    pub recent_window_secs: u64,
}

fn default_exact_counts() -> bool {
    true
}

/// Request: `sources.status.view`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesStatusViewRequest {
    /// Optional source id or substring to inspect.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Optional source family filter, matching the CLI family aliases.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub family: Option<String>,
    /// Whether event counts must be exact. Filtered interactive views may set
    /// this false to use bounded presence probes instead of full-table counts.
    #[serde(default = "default_exact_counts")]
    pub exact_counts: bool,
}

impl Default for SourcesStatusViewRequest {
    fn default() -> Self {
        Self {
            source: None,
            family: None,
            exact_counts: default_exact_counts(),
        }
    }
}

impl Default for SourcesStatusRequest {
    fn default() -> Self {
        Self {
            stale_after_secs: default_stale_after_secs(),
            recent_window_secs: default_recent_window_secs(),
        }
    }
}

/// Response: `sources.status`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesStatusResponse {
    pub generated_at: Timestamp,
    pub stale_after_secs: u64,
    pub recent_window_secs: u64,
    pub sources: Vec<SourceStatus>,
}

/// Operator-visible state for one registered source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceStatus {
    pub module_name: ModuleName,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub manifest_status: String,
    pub live: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module_run_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_at: Option<Timestamp>,
    /// Current health from the latest `health.status` event for this component.
    /// `None` if the source has never emitted a transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_health: Option<HealthStatus>,
    /// When the current health was last emitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_changed_at: Option<Timestamp>,
    /// Reason text from the most recent health transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_reason: Option<String>,
    /// Count of events emitted by this source inside the recent window.
    pub recent_output_count: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_output_at: Option<Timestamp>,
}

// ─────────────────────────────────────────────────────────────
// Emit-rate stall detection
// ─────────────────────────────────────────────────────────────
//
// Heartbeats prove the process is alive; they do not prove it is doing
// work. The forensic on 2026-05-15 caught fs-watcher silent for 2 min,
// dbus silent for 45 min, and activitywatch silent forever — all
// heartbeating as healthy. This classifier surfaces that failure mode
// (issue #992) from the existing `SourceStatus` fields without
// requiring any new pipeline plumbing.

/// Default uptime gate: do not classify as stalled inside the first 10 min
/// after startup. Initialization work (snapshot scans, `JetStream` ack-pending
/// catch-up, etc.) legitimately produces no new events for several minutes.
pub const DEFAULT_EMIT_STALL_UPTIME_GATE_SECS: u64 = 600;

/// Default quiet window: zero events for 10 min while heartbeating triggers
/// a degraded verdict. Matches the recent-output window default (300s) plus a
/// generous buffer so a single sparse heartbeat does not flap the verdict.
pub const DEFAULT_EMIT_STALL_QUIET_SECS: u64 = 600;

/// Configurable thresholds for emit-rate stall detection.
///
/// Loaded from environment variables, with safe fallbacks:
/// - `SINEX_EMIT_STALL_UPTIME_GATE_SECS` (default 600)
/// - `SINEX_EMIT_STALL_QUIET_SECS` (default 600)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmitStallThresholds {
    /// Minimum uptime before the verdict can fire.
    pub uptime_gate_secs: u64,
    /// How long a unit may go without emitting before being called stalled.
    pub quiet_secs: u64,
}

impl Default for EmitStallThresholds {
    fn default() -> Self {
        Self {
            uptime_gate_secs: DEFAULT_EMIT_STALL_UPTIME_GATE_SECS,
            quiet_secs: DEFAULT_EMIT_STALL_QUIET_SECS,
        }
    }
}

impl EmitStallThresholds {
    /// Construct thresholds from environment variables. Missing or unparseable
    /// values fall back to defaults via `env::parse_or`.
    #[must_use]
    pub fn from_env_or_default() -> Self {
        Self {
            uptime_gate_secs: shared_env::parse_or(
                "SINEX_EMIT_STALL_UPTIME_GATE_SECS",
                DEFAULT_EMIT_STALL_UPTIME_GATE_SECS,
                "emit-stall uptime gate",
            ),
            quiet_secs: shared_env::parse_or(
                "SINEX_EMIT_STALL_QUIET_SECS",
                DEFAULT_EMIT_STALL_QUIET_SECS,
                "emit-stall quiet window",
            ),
        }
    }

    /// Same as `from_env_or_default` but surfaces parse errors instead of
    /// silently falling back. Useful for `sinexctl` flag plumbing.
    pub fn from_env_strict() -> Result<Self> {
        let mut t = Self::default();
        if let Some(v) = shared_env::strict_parsed::<u64>("SINEX_EMIT_STALL_UPTIME_GATE_SECS")? {
            t.uptime_gate_secs = v;
        }
        if let Some(v) = shared_env::strict_parsed::<u64>("SINEX_EMIT_STALL_QUIET_SECS")? {
            t.quiet_secs = v;
        }
        Ok(t)
    }
}

/// Verdict for a single source's emit-rate health.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmitStallVerdict {
    /// Recent emissions present — emit rate is healthy.
    Emitting,
    /// Uptime below the gate or unit not live — too early to call.
    Initializing,
    /// Heartbeating and live, but produced no events for ≥ `quiet_secs`.
    /// This is the failure mode #992 targets.
    Stalled,
    /// Process is not heartbeating; emit-rate is not the right diagnosis
    /// (operator should look at the run/heartbeat fields directly).
    NotLive,
    /// Insufficient data (e.g., no manifest, no `started_at`).
    Unknown,
}

impl EmitStallVerdict {
    /// Human-readable label suitable for `sinexctl status` rows.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Emitting => "emitting",
            Self::Initializing => "initializing",
            Self::Stalled => "stalled",
            Self::NotLive => "not-live",
            Self::Unknown => "unknown",
        }
    }

    /// Whether this verdict represents a degraded source that should
    /// surface in operator dashboards.
    #[must_use]
    pub fn is_degraded(self) -> bool {
        matches!(self, Self::Stalled)
    }
}

impl SourceStatus {
    /// Classify this source's emit-rate health against the supplied
    /// thresholds and reference instant (`now`).
    ///
    /// Pure function over the snapshot — no I/O, no clock side-effects — so it
    /// can be unit-tested with fabricated timestamps.
    #[must_use]
    pub fn classify_emit_stall(
        &self,
        thresholds: EmitStallThresholds,
        now: Timestamp,
    ) -> EmitStallVerdict {
        let Some(started_at) = self.started_at else {
            return EmitStallVerdict::Unknown;
        };
        if !self.live {
            return EmitStallVerdict::NotLive;
        }
        // The unit is currently heartbeating. Apply the uptime gate so a
        // legitimately-still-starting process does not flap to stalled.
        let uptime_secs = (now - started_at).whole_seconds();
        if uptime_secs < 0 || (uptime_secs as u64) < thresholds.uptime_gate_secs {
            return EmitStallVerdict::Initializing;
        }
        // If we've emitted any output in the recent window, we're fine.
        if self.recent_output_count > 0 {
            return EmitStallVerdict::Emitting;
        }
        // Last-output age (or full uptime if never emitted) ≥ quiet window?
        let quiet_secs = match self.last_output_at {
            Some(last) => (now - last).whole_seconds().max(0) as u64,
            None => uptime_secs as u64,
        };
        if quiet_secs >= thresholds.quiet_secs {
            EmitStallVerdict::Stalled
        } else {
            EmitStallVerdict::Emitting
        }
    }
}

#[cfg(test)]
#[path = "source_status_emit_stall_tests.rs"]
mod emit_stall_tests;

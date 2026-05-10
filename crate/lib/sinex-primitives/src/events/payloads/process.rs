//! Process lifecycle event payloads

use crate::domain::NodeType;
use crate::events::enums::ShutdownReason;
use crate::units::{ExitCode, ProcessId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;
use std::fmt;

/// Strongly typed status for process heartbeat payloads
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProcessStatus {
    Healthy,
    Degraded,
    Failed,
}

impl fmt::Display for ProcessStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            ProcessStatus::Healthy => "healthy",
            ProcessStatus::Degraded => "degraded",
            ProcessStatus::Failed => "failed",
        };

        f.write_str(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex", event_type = "process.started")]
pub struct ProcessStartedPayload {
    pub process_name: String,
    pub process_type: NodeType,
    pub pid: ProcessId,
    pub version: String,
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex", event_type = "process.degraded")]
pub struct ProcessDegradedPayload {
    pub process_name: String,
    pub uptime_seconds: u64,
    pub errors_in_window: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex", event_type = "process.failed")]
pub struct ProcessFailedPayload {
    pub process_name: String,
    pub uptime_seconds: u64,
    pub errors_in_window: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex", event_type = "process.shutdown")]
pub struct ProcessShutdownPayload {
    pub process_name: String,
    pub process_type: NodeType,
    pub pid: ProcessId,
    pub uptime_seconds: u64,
    pub shutdown_reason: ShutdownReason,
    pub exit_code: ExitCode,
}

// Automaton error events

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex", event_type = "automaton.error")]
pub struct AutomatonErrorPayload {
    pub automaton_name: String,
    pub error_message: String,
    pub error_code: Option<String>,
    pub stack_trace: Option<String>,
    pub context: Option<serde_json::Value>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Source-unit descriptors for sinex.* self-observation infra events.
//
// `sinex.process.*` and `sinex.automaton.error` are emitted by every long-
// running sinex binary as part of standard lifecycle/health observability.
// These are infra source units in the same sense as `blob-storage`: the
// events exist today (ingestd/gateway/nodes publish them on boot, on
// degradation, on shutdown), but they are not produced by a dedicated
// systemd service — every binary participates. We register descriptors so
// `sinexctl verify --source-units` finds a claim for each declared
// `(source, event_type)` payload pair. They have no `SourceUnitBinding`
// because the runtime owners are existing pack bindings.
// ─────────────────────────────────────────────────────────────────────────────

use crate::proof::{
    CheckpointFamily as SuCheckpointFamily, Horizon as SuHorizon,
    OccurrenceIdentity as SuOccurrenceIdentity, PrivacyTier as SuPrivacyTier,
    RetentionPolicy as SuRetentionPolicy, RuntimeShape as SuRuntimeShape, SourceUnitBinding,
    SourceUnitBuildImpact, SourceUnitDescriptor, SubjectRef,
};
use crate::{register_source_unit, register_source_unit_binding};

register_source_unit! {
    SourceUnitDescriptor {
        id: "sinex-process-lifecycle",
        namespace: "infra",
        event_types: &[
            ("sinex", "process.started"),
            ("sinex", "process.degraded"),
            ("sinex", "process.failed"),
            ("sinex", "process.shutdown"),
        ],
        privacy_tier: SuPrivacyTier::Public,
        horizons: &[SuHorizon::Continuous],
        retention: SuRetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: SuOccurrenceIdentity::Natural,
        access_policy: "embedded_in_every_sinex_binary",
    }
}

register_source_unit! {
    SourceUnitDescriptor {
        id: "sinex-automaton-error",
        namespace: "infra",
        event_types: &[("sinex", "automaton.error")],
        privacy_tier: SuPrivacyTier::Public,
        horizons: &[SuHorizon::Continuous],
        retention: SuRetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: SuOccurrenceIdentity::Natural,
        access_policy: "embedded_in_automaton_runtime",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:sinex-process-lifecycle"),
        "sinex-process-lifecycle",
        "infra",
    )
    .implementation("sinex-primitives::process")
    .adapter("EmbeddedEmitter")
    .output_event_type("process.started")
    .privacy_context("none")
    .material_policy("none")
    .checkpoint_policy("live_observation")
    .resource_shape("embedded_emitter")
    .source_unit_id("sinex-process-lifecycle")
    .proposed(true)
    .runner_pack("infra")
    .checkpoint_family(SuCheckpointFamily::LiveObservation)
    .runtime_shape(SuRuntimeShape::Continuous)
    .package_impact("no_new_output")
    .implementation_mode("rust_in_every_sinex_binary")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:sinex-automaton-error"),
        "sinex-automaton-error",
        "infra",
    )
    .implementation("sinex-process")
    .adapter("EmbeddedEmitter")
    .output_event_type("automaton.error")
    .privacy_context("none")
    .material_policy("none")
    .checkpoint_policy("live_observation")
    .resource_shape("embedded_emitter")
    .source_unit_id("sinex-automaton-error")
    .proposed(true)
    .runner_pack("infra")
    .checkpoint_family(SuCheckpointFamily::LiveObservation)
    .runtime_shape(SuRuntimeShape::Continuous)
    .package_impact("no_new_output")
    .implementation_mode("rust_in_pack:process")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

// Test helpers for external tests
#[cfg(any(test, feature = "testing"))]
impl ProcessStartedPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            process_name: "test-process".into(),
            process_type: NodeType::Ingestor,
            pid: ProcessId::from(0u32),
            version: "0.0.0".into(),
            config: serde_json::json!({}),
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl ProcessShutdownPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            process_name: "test-process".into(),
            process_type: NodeType::Ingestor,
            pid: ProcessId::from(0u32),
            uptime_seconds: 0,
            shutdown_reason: ShutdownReason::Requested,
            exit_code: ExitCode::SUCCESS,
        }
    }
}

//! `system.monitor` — fire-once startup event for the system source pack.
//!
//! Registers `system.monitor` with the source contract inventory and
//! with the source factory registry via [`register_source!`]. On every
//! source boot this emits one [`SystemMonitoringStartedPayload`]
//! anchored to a synthetic material, then exits.
//!
//! Deployment shape: a `Type=oneshot` systemd unit that runs at boot under
//! `sinex-runtime.target`.

use crate::runtime::{RuntimeResult, stream::RuntimeContext};
use futures::future::BoxFuture;
use sinex_macros::SourceMeta;
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape,
};
use sinex_primitives::{
    JsonValue, SinexError,
    events::payloads::system::SystemMonitoringStartedPayload,
    events::{Event, EventPayload, SourceMaterial},
    ids::Id,
    temporal::Timestamp,
};

#[derive(Debug, Default, SourceMeta)]
#[source_meta(
    id = "system.monitor",
    namespace = "system",
    event_type = "monitoring.started",
    event_source = "system",
    adapter = "MonitorDriver",
    privacy_tier = PrivacyTier::Public,
    horizons(Horizon::Continuous),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Natural,
    access_scope = AccessScope::Internal,
    implementation = "sinexd",
    privacy_context = ProcessingContext::Metadata,
    resource_profile = ResourceProfile::Oneshot,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::LiveObservation,
    runtime_shape = RuntimeShape::OnDemand,
    monitor_emit_fn = "emit_system_monitor",
    monitor_phase = "ServiceStart",
)]
pub struct SystemMonitorSource;

// ---------------------------------------------------------------------------
// Emit function
// ---------------------------------------------------------------------------

/// Build the [`SystemMonitoringStartedPayload`] event anchored to `material_id`.
///
/// The enabled/configured flags default to `true` for all four subsystems.
/// A future pass can wire actual config from `RuntimeContext::raw_config`.
fn emit_system_monitor(
    _runtime: RuntimeContext,
    material_id: Id<SourceMaterial>,
) -> BoxFuture<'static, RuntimeResult<Vec<Event<JsonValue>>>> {
    Box::pin(async move {
        let payload = SystemMonitoringStartedPayload {
            dbus_enabled: true,
            journal_enabled: true,
            udev_enabled: true,
            systemd_enabled: true,
            start_time: Timestamp::now(),
        };

        let event = payload
            .from_material(material_id)
            .build()
            .map_err(|e| SinexError::processing(format!("system.monitor build failed: {e}")))?
            .to_json_event()
            .map_err(|e| SinexError::serialization(e.to_string()))?;

        Ok(vec![event])
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "monitor_test.rs"]
mod tests;

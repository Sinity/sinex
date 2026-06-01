//! `system.monitor` — fire-once startup event for the system source pack.
//!
//! Registers `system.monitor` with the source-unit descriptor inventory and
//! with the node factory registry via [`register_monitor_unit!`]. On every
//! source-worker boot this emits one [`SystemMonitoringStartedPayload`]
//! anchored to a synthetic material, then exits.
//!
//! Deployment shape: a `Type=oneshot` systemd unit that runs at boot under
//! `sinex-runtime.target`.

use crate::node_sdk::{NodeResult, runtime::stream::NodeRuntimeState};
use futures::future::BoxFuture;
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceUnitBinding, SourceUnitBuildImpact, SourceUnitDescriptor, SubjectRef,
};
use sinex_primitives::{
    JsonValue, SinexError,
    events::payloads::system::SystemMonitoringStartedPayload,
    events::{Event, EventPayload, SourceMaterial},
    ids::Id,
    temporal::Timestamp,
};
use sinex_primitives::{register_source_unit, register_source_unit_binding};

use crate::register_monitor_unit;
use crate::sources::monitor_node::MonitorPhase;

// ---------------------------------------------------------------------------
// Source-unit descriptor + binding
// ---------------------------------------------------------------------------

register_source_unit! {
    SourceUnitDescriptor {
        id: "system.monitor",
        namespace: "system",
        event_types: &[("system", "monitoring.started")],
        privacy_tier: PrivacyTier::Public,
        horizons: &[Horizon::Continuous],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "obligation:source_unit.material_provenance",
        ],
        occurrence_identity: OccurrenceIdentity::Natural,
        access_policy: "lifecycle_hook:none",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:system.monitor"),
        "system.monitor",
        "system",
    )
    .implementation("sinex-source-worker")
    .adapter("MonitorDriverNode")
    .output_event_type("monitoring.started")
    .sensitivity_profile("Metadata")
    .material_policy("synthetic_oneshot")
    .checkpoint_policy("stateless")
    .resource_shape("oneshot_bounded_memory")
    .source_unit_id("system.monitor")
    .runner_pack("source-worker")
    .checkpoint_family(CheckpointFamily::LiveObservation)
    .runtime_shape(RuntimeShape::OnDemand)
    .package_impact("system_monitor_unit")
    .implementation_mode("rust_in_pack:source-worker")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

// ---------------------------------------------------------------------------
// Source-unit registration
// ---------------------------------------------------------------------------

register_monitor_unit!(
    source_unit_id: "system.monitor",
    emit_at: MonitorPhase::ServiceStart,
    emit: emit_system_monitor,
);

// ---------------------------------------------------------------------------
// Emit function
// ---------------------------------------------------------------------------

/// Build the [`SystemMonitoringStartedPayload`] event anchored to `material_id`.
///
/// The enabled/configured flags default to `true` for all four subsystems.
/// A future pass can wire actual config from `NodeRuntimeState::raw_config`.
fn emit_system_monitor(
    _runtime: NodeRuntimeState,
    material_id: Id<SourceMaterial>,
) -> BoxFuture<'static, NodeResult<Vec<Event<JsonValue>>>> {
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
mod tests {
    use super::*;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn test_system_monitor_payload_builds() -> TestResult<()> {
        let material_id: Id<SourceMaterial> = Id::new();

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
            .map_err(|e| SinexError::processing(e.to_string()))
            .and_then(|e| {
                e.to_json_event()
                    .map_err(|err| SinexError::serialization(err.to_string()))
            });

        assert!(
            event.is_ok(),
            "payload build/erase failed: {:?}",
            event.err()
        );

        let event = event.unwrap();
        assert_eq!(event.event_type.as_str(), "monitoring.started");
        assert_eq!(event.source.as_str(), "system");

        Ok(())
    }
}

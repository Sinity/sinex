//! `system.monitor` — fire-once startup event for the system source pack.
//!
//! Registers `system.monitor` with the node factory registry via
//! [`register_monitor_unit!`]. On every source-worker boot this emits one
//! [`SystemMonitoringStartedPayload`] anchored to a synthetic material.
//!
//! The descriptor is already registered in `sinex-system-ingestor/src/lib.rs`
//! via `register_source_unit!`; this module only wires the factory/emit path.

use futures::future::BoxFuture;
use sinex_node_sdk::{NodeResult, runtime::stream::NodeRuntimeState};
use sinex_primitives::{
    SinexError,
    events::{Event, EventPayload, SourceMaterial},
    events::payloads::system::SystemMonitoringStartedPayload,
    ids::Id,
    temporal::Timestamp,
    JsonValue,
};

use crate::register_monitor_unit;
use crate::monitor_node::MonitorPhase;

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

        assert!(event.is_ok(), "payload build/erase failed: {:?}", event.err());

        let event = event.unwrap();
        assert_eq!(event.event_type.as_str(), "monitoring.started");
        assert_eq!(event.source.as_str(), "system");

        Ok(())
    }
}

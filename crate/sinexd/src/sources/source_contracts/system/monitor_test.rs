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

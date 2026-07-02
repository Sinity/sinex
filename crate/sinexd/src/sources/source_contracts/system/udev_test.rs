use super::*;
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::MaterialAnchor;
use sinex_primitives::primitives::Uuid;
use xtask::sandbox::prelude::*;

fn make_ctx(mid: Id<SourceMaterial>) -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static("system.udev"),
        source_material_id: mid,
        record_anchor: MaterialAnchor::DirectoryEntry {
            path: "/sys/bus/usb/devices/1-1".into(),
            content_hash: None,
        },
        operation_id: Uuid::new_v4(),
        job_id: Uuid::new_v4(),
        host: "test-host".into(),
        acquisition_time: Timestamp::now(),
    }
}

fn make_udev_record(mid: Id<SourceMaterial>, path: &str, kind: &str) -> SourceRecord {
    SourceRecord {
        material_id: mid,
        anchor: MaterialAnchor::DirectoryEntry {
            path: path.into(),
            content_hash: None,
        },
        bytes: path.as_bytes().to_vec(),
        logical_path: Some(path.into()),
        source_ts_hint: None,
        metadata: serde_json::json!({
            "event_kind": kind,
            "path": path,
        }),
    }
}

#[sinex_test]
async fn test_udev_parser_device_connected() -> TestResult<()> {
    let mid = Id::<SourceMaterial>::new();
    let record = make_udev_record(mid, "/sys/bus/usb/devices/1-1", "Created");

    let mut parser = UdevParser;
    let ctx = make_ctx(mid);
    let intents = parser.parse_record(record, &ctx).await.unwrap();

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].event_type.as_str(), "device.connected");
    assert_eq!(intents[0].event_source.as_str(), "udev");
    assert_eq!(
        intents[0].payload["device_path"],
        "/sys/bus/usb/devices/1-1"
    );
    Ok(())
}

#[sinex_test]
async fn test_udev_parser_device_disconnected() -> TestResult<()> {
    let mid = Id::<SourceMaterial>::new();
    let record = make_udev_record(mid, "/sys/bus/usb/devices/1-2", "Deleted");

    let mut parser = UdevParser;
    let ctx = make_ctx(mid);
    let intents = parser.parse_record(record, &ctx).await.unwrap();

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].event_type.as_str(), "device.disconnected");
    Ok(())
}

#[sinex_test]
async fn test_udev_parser_untyped_metadata_emits_other() -> TestResult<()> {
    // When the record metadata carries no recognizable event kind, the parser
    // deliberately classifies the event as `device.other` rather than guessing
    // a connect/disconnect from absent data (see `parse_record`: "emitting Other
    // action instead of guessing kind"). Properly-typed records still classify
    // as connected/disconnected — see the sibling tests.
    let mid = Id::<SourceMaterial>::new();
    let mut record = make_udev_record(mid, "/sys/bus/usb/devices/1-3", "Deleted");
    record.metadata = serde_json::json!({});

    let mut parser = UdevParser;
    let ctx = make_ctx(mid);
    let intents = parser.parse_record(record, &ctx).await.unwrap();

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].event_type.as_str(), "device.other");
    Ok(())
}

#[sinex_test]
async fn test_infer_device_type() -> TestResult<()> {
    assert!(matches!(
        infer_device_type("/sys/bus/usb/devices/1-1"),
        DeviceType::Usb
    ));
    assert!(matches!(
        infer_device_type("/sys/block/sda"),
        DeviceType::Storage
    ));
    assert!(matches!(
        infer_device_type("/sys/class/net/eth0"),
        DeviceType::Network
    ));
    assert!(matches!(
        infer_device_type("/sys/bus/other"),
        DeviceType::Other
    ));
    Ok(())
}

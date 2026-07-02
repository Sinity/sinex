use super::*;

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::MaterialAnchor;
use sinex_primitives::primitives::Uuid;
use xtask::sandbox::prelude::*;

fn make_ctx(mid: Id<SourceMaterial>) -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static("system.dbus"),
        source_material_id: mid,
        record_anchor: MaterialAnchor::StreamFrame {
            material_offset: 0,
            frame_index: 0,
        },
        operation_id: Uuid::new_v4(),
        job_id: Uuid::new_v4(),
        host: "test-host".into(),
        acquisition_time: Timestamp::now(),
    }
}

fn make_dbus_record(
    mid: Id<SourceMaterial>,
    interface: &str,
    member: &str,
    body: serde_json::Value,
) -> SourceRecord {
    SourceRecord {
        material_id: mid,
        anchor: MaterialAnchor::StreamFrame {
            material_offset: 0,
            frame_index: 0,
        },
        bytes: serde_json::to_vec(&body).unwrap(),
        logical_path: None,
        source_ts_hint: None,
        metadata: serde_json::json!({
            "interface": interface,
            "member": member,
            "path": "/org/test",
            "sender": ":1.42",
        }),
    }
}

#[sinex_test]
async fn test_dbus_parser_signal_received() -> TestResult<()> {
    let mid = Id::<SourceMaterial>::new();
    let record = make_dbus_record(
        mid,
        "org.example.Unknown",
        "SomeSignal",
        serde_json::json!({"key": "value"}),
    );

    let mut parser = DbusParser;
    let ctx = make_ctx(mid);
    let intents = parser.parse_record(record, &ctx).await.unwrap();

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].event_type.as_str(), "signal.received");
    assert_eq!(intents[0].event_source.as_str(), "dbus");
    Ok(())
}

#[sinex_test]
async fn test_dbus_parser_notification() -> TestResult<()> {
    let mid = Id::<SourceMaterial>::new();
    let record = make_dbus_record(
        mid,
        "org.freedesktop.Notifications",
        "Notify",
        serde_json::json!(["MyApp", 0, "", "Summary", "Body", [], {}, -1]),
    );

    let mut parser = DbusParser;
    let ctx = make_ctx(mid);
    let intents = parser.parse_record(record, &ctx).await.unwrap();

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].event_type.as_str(), "notification.sent");
    assert_eq!(intents[0].payload["summary"], "Summary");
    assert_eq!(intents[0].payload["body"], "Body");
    assert_eq!(intents[0].privacy_context, ProcessingContext::Notification);
    Ok(())
}

#[sinex_test]
async fn test_classify_dbus_event() -> TestResult<()> {
    assert_eq!(
        classify_dbus_event("org.freedesktop.Notifications", "Notify"),
        "notification.sent"
    );
    assert_eq!(
        classify_dbus_event("org.mpris.MediaPlayer2", "PropertiesChanged"),
        "media.state_changed"
    );
    assert_eq!(
        classify_dbus_event("org.example.Unknown", "Signal"),
        "signal.received"
    );
    Ok(())
}

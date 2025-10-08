use serde_json::json;
use sinex_core::db::sanitization::EventSanitizer;
use sinex_core::payloads::filesystem::FileCreatedPayload;
use sinex_core::types::domain::SanitizedPath;
use sinex_core::{Event, EventSource, EventType, Id, Provenance};
use sinex_test_utils::{sinex_test, TestContext};

#[sinex_test]
async fn path_traversal_sanitization(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let mut event = Event::dynamic(
        EventSource::new("../../../etc/passwd"),
        EventType::new("security.test"),
        json!({"path": "../../sensitive/file.txt"}),
    )
    .with_provenance(Provenance::from_material(Id::new(), 0, None, None))
    .build();

    let was_modified = EventSanitizer::sanitize_event(&mut event).unwrap();
    assert!(was_modified);
    assert!(!event.source.as_str().contains(".."));

    if let Some(path) = event.payload.get("path").and_then(|v| v.as_str()) {
        assert!(!path.contains(".."));
    }
    Ok(())
}

#[sinex_test]
async fn null_byte_sanitization(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let mut event = Event::dynamic(
        EventSource::new("test\0source"),
        EventType::new("security.test"),
        json!({"data": "test\0value"}),
    )
    .with_provenance(Provenance::from_material(Id::new(), 0, None, None))
    .build();

    let was_modified = EventSanitizer::sanitize_event(&mut event).unwrap();
    assert!(was_modified);
    assert!(!event.source.contains('\0'));
    Ok(())
}

#[sinex_test]
async fn sql_injection_payload_preserved(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let mut event = Event::dynamic(
        EventSource::new("security.test"),
        EventType::new("sql.injection"),
        json!({"query": "'; DROP TABLE events; --"}),
    )
    .with_provenance(Provenance::from_material(Id::new(), 0, None, None))
    .build();

    let was_modified = EventSanitizer::sanitize_event(&mut event).unwrap();
    assert!(!was_modified);
    assert_eq!(
        event
            .payload
            .get("query")
            .and_then(|v| v.as_str())
            .expect("test setup should include query field"),
        "'; DROP TABLE events; --"
    );
    Ok(())
}

#[sinex_test]
async fn generic_sanitizer_with_typed_event(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let payload = FileCreatedPayload {
        path: SanitizedPath::from("../../../malicious/file.txt".to_string()),
        size: 1024,
        created_at: chrono::Utc::now(),
        permissions: Some(0o644),
    };

    let mut event = Event::builder(payload)
        .with_provenance(Provenance::from_material(Id::new(), 0, None, None))
        .build();

    let was_modified = EventSanitizer::sanitize_event_generic(&mut event).unwrap();
    assert!(was_modified);
    assert!(!event.source.as_str().contains(".."));
    assert!(!event.source.as_str().contains("malicious"));
    Ok(())
}

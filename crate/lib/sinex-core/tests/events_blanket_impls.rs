use sinex_core::types::events::payloads::filesystem::FileCreatedPayload;
use sinex_core::EventPayload;
use sinex_test_utils::sinex_test;

#[sinex_test]
fn option_payload_supports_legacy_conversion() -> Result<(), sinex_core::SinexError> {
    let value = serde_json::json!(null);
    let result: Option<FileCreatedPayload> =
        Option::<FileCreatedPayload>::try_from_legacy(value, "1.0.0").unwrap();
    assert!(result.is_none());

    let value = serde_json::json!({
        "path": "/test.txt",
        "size": 100,
        "created_at": "2024-01-01T00:00:00Z"
    });
    let result: Option<FileCreatedPayload> =
        Option::<FileCreatedPayload>::try_from_legacy(value, "1.0.0").unwrap();
    assert!(result.is_some());
    Ok(())
}

#[sinex_test]
fn vec_payload_supports_legacy_conversion() -> Result<(), sinex_core::SinexError> {
    let value = serde_json::json!([
        {
            "path": "/test1.txt",
            "size": 100,
            "created_at": "2024-01-01T00:00:00Z"
        },
        {
            "path": "/test2.txt",
            "size": 200,
            "created_at": "2024-01-01T00:00:00Z"
        }
    ]);

    let result: Vec<FileCreatedPayload> =
        Vec::<FileCreatedPayload>::try_from_legacy(value, "1.0.0").unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].path.as_str(), "/test1.txt");
    assert_eq!(result[1].path.as_str(), "/test2.txt");
    Ok(())
}

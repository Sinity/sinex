use sinex_ingestd::schema_sync::compute_content_hash_for_testing;
use sinex_test_utils::sinex_test;
use sinex_test_utils::TestResult;

#[sinex_test]
fn content_hash_is_sha256() -> TestResult<()> {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "name": { "type": "string" }
        }
    });

    let hash = compute_content_hash_for_testing(&schema);
    assert!(!hash.is_empty());
    assert_eq!(hash.len(), 64); // SHA-256 -> 32 bytes -> 64 hex chars
    Ok(())
}

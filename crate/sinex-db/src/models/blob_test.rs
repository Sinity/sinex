use super::*;
use serde_json::json;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn blob_record_rejects_invalid_verification_status() -> ::xtask::sandbox::TestResult<()> {
    let record = BlobRecord {
        id: uuid::Uuid::now_v7(),
        annex_backend: "SHA256E".to_string(),
        content_hash: "abc".to_string(),
        size_bytes: 42,
        checksum_blake3: None,
        original_filename: "blob.bin".to_string(),
        mime_type: None,
        metadata: json!({}),
        created_at: Timestamp::now(),
        last_verified_at: None,
        verification_status: Some("mystery".to_string()),
    };

    let err = Blob::try_from(record).expect_err("invalid status must be rejected");
    assert!(err.contains("invalid blob verification_status"));
    Ok(())
}

#[sinex_test]
async fn content_key_parser_rejects_invalid_size() -> ::xtask::sandbox::TestResult<()> {
    let err = Blob::parse_content_store_key("SHA256E-sabc--deadbeef")
        .expect_err("invalid content-store size must fail honestly");
    assert!(err.contains("invalid size `abc`"));
    Ok(())
}

#[sinex_test]
async fn content_key_parser_rejects_missing_hash_fragment() -> ::xtask::sandbox::TestResult<()>
{
    let err = Blob::parse_content_store_key("SHA256E-s42")
        .expect_err("missing backend digest fragment must fail honestly");
    assert!(err.contains("missing hash fragment"));
    Ok(())
}

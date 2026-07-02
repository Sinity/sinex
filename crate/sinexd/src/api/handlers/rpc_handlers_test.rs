use super::*;
use sinex_db::models::blob::Blob;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn blob_response_payload_encodes_base64() -> TestResult<()> {
    let blob = Blob::builder()
        .storage_backend("SHA256".into())
        .content_hash("deadbeef".into())
        .original_filename("blob.bin".into())
        .size_bytes(2)
        .mime_type("application/octet-stream".into())
        .build();

    let response = blob_response_payload(b"hi", &blob)?;
    assert_eq!(response.content, "aGk=");
    assert_eq!(
        response.content_type.as_deref(),
        Some("application/octet-stream")
    );
    assert_eq!(response.size, 2);
    Ok(())
}

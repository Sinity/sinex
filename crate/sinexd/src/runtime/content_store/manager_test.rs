use super::{
    attach_verification_status_update_error, content_hash_is_backend_digest,
    material_name_for_blob, require_ingest_filename, verification_status_persist_error,
};
use crate::runtime::SinexError;
use camino::Utf8Path;
use sinex_db::models::Blob;
use sinex_primitives::domain::BlobVerificationStatus;
use xtask::sandbox::sinex_test;

// Inline because these cover private blob verification error helpers only.
#[sinex_test]
async fn verification_status_persist_error_is_explicit() -> TestResult<()> {
    let error = verification_status_persist_error(
        "SHA256E-s1--deadbeef.txt",
        BlobVerificationStatus::Verified,
        &SinexError::database("write failed"),
    );

    assert!(
        error
            .to_string()
            .contains("failed to persist blob verification status")
    );
    assert_eq!(
        error.context_map().get("verification_status"),
        Some(&BlobVerificationStatus::Verified.to_string()),
    );
    assert!(
        error
            .sources()
            .iter()
            .any(|source| source.contains("write failed"))
    );
    Ok(())
}

#[sinex_test]
async fn verification_status_update_error_is_attached_to_mismatch() -> TestResult<()> {
    let mismatch = SinexError::processing("Blob content hash mismatch");
    let combined = attach_verification_status_update_error(
        mismatch,
        &SinexError::processing("failed to persist blob verification status"),
    );

    assert_eq!(
        combined
            .context_map()
            .get("verification_status_update_error"),
        Some(&"Processing error: failed to persist blob verification status".to_string()),
    );
    Ok(())
}

#[sinex_test]
async fn material_name_for_blob_uses_content_key_when_filename_missing() -> TestResult<()> {
    let blob = Blob::builder()
        .storage_backend("SHA256E".to_string())
        .content_hash("deadbeef".to_string())
        .size_bytes(42)
        .build();

    assert_eq!(material_name_for_blob(&blob), "SHA256E-s42--deadbeef");
    Ok(())
}

#[sinex_test]
async fn local_cas_content_hash_is_not_treated_as_annex_digest() -> TestResult<()> {
    let blob = Blob::builder()
        .storage_backend("SINEXBLAKE3".to_string())
        .content_hash("b3f00d".to_string())
        .size_bytes(42)
        .build();

    assert!(!content_hash_is_backend_digest(&blob));
    Ok(())
}

#[sinex_test]
async fn git_annex_content_hash_is_verified_as_annex_digest() -> TestResult<()> {
    let blob = Blob::builder()
        .storage_backend("SHA256E".to_string())
        .content_hash("deadbeef".to_string())
        .size_bytes(42)
        .build();

    assert!(content_hash_is_backend_digest(&blob));
    Ok(())
}

#[sinex_test]
async fn require_ingest_filename_prefers_explicit_filename() -> TestResult<()> {
    let path = Utf8Path::new("/tmp/example.txt");

    let filename =
        require_ingest_filename(path, Some("provided.txt")).expect("explicit filename");

    assert_eq!(filename, "provided.txt");
    Ok(())
}

#[sinex_test]
async fn require_ingest_filename_rejects_paths_without_final_component() -> TestResult<()> {
    let error = require_ingest_filename(Utf8Path::new("/"), None)
        .expect_err("paths without a filename must fail honestly");

    assert!(
        error
            .to_string()
            .contains("Blob ingestion requires a file name"),
        "unexpected error: {error}"
    );
    Ok(())
}

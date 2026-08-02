use super::{
    ContentStoreManager, attach_verification_status_update_error, content_hash_is_backend_digest,
    material_name_for_blob, require_ingest_filename, verification_status_persist_error,
};
use crate::runtime::SinexError;
use crate::runtime::content_store::ContentStoreConfig;
use camino::{Utf8Path, Utf8PathBuf};
use sinex_db::models::Blob;
use sinex_primitives::domain::BlobVerificationStatus;
use xtask::sandbox::prelude::*;

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

fn manager_fixture(ctx: &TestContext) -> TestResult<(ContentStoreManager, tempfile::TempDir)> {
    let temp_dir = tempfile::TempDir::new()?;
    let root_path = Utf8PathBuf::from_path_buf(temp_dir.path().join("content-store"))
        .map_err(|_| color_eyre::eyre::eyre!("content-store path must be valid UTF-8"))?;
    let config = ContentStoreConfig {
        root_path,
        num_copies: Some(1),
        large_files: Some("anything".to_string()),
        ..Default::default()
    };
    let manager = ContentStoreManager::new(config, ctx.pool().clone(), None)?;
    Ok((manager, temp_dir))
}

/// Regression test for sinex-audit-cas-zombie-blob-rows: a `core.blobs` row can
/// outlive its CAS file (e.g. delete-on-tombstone dropped the file once the
/// blob had zero remaining references). Before the fix, `check_dedup` trusted
/// any row matching the BLAKE3 hash and returned `deduplicated: true` WITHOUT
/// ever calling `store_file` again -- silently believing content was present
/// when it was permanently gone. Confirm re-ingesting the same bytes after the
/// backing file was removed re-writes the CAS file instead of trusting the
/// stale row.
#[sinex_test]
async fn ingest_repairs_zombie_blob_row_by_rewriting_missing_content(
    ctx: TestContext,
) -> TestResult<()> {
    let (manager, _tmp) = manager_fixture(&ctx)?;
    let payload = b"sinex zombie-blob-row regression payload";

    let first = manager
        .ingest_from_bytes(payload, "zombie.txt", "text/plain")
        .await?;
    let content_key = first.content_key();

    // Simulate the CAS file having been dropped out from under a live
    // core.blobs row (e.g. by an earlier delete-on-tombstone pass, or any
    // other path that removes the file without going through the DB).
    let local_path = manager
        .content_store
        .path_if_local(&content_key)?
        .expect("payload is stored via the local BLAKE3 CAS backend");
    assert!(local_path.exists(), "fixture sanity: file must exist after ingest");
    tokio::fs::remove_file(&local_path).await?;
    assert!(!local_path.exists(), "fixture sanity: file must be gone before re-ingest");

    // Re-ingesting identical content must detect the zombie row and re-write
    // the file rather than short-circuiting on the stale dedup hit.
    let second = manager
        .ingest_from_bytes(payload, "zombie.txt", "text/plain")
        .await?;
    assert_eq!(
        second.id, first.id,
        "re-ingest must repair the same blob row (same BLAKE3 hash), not fork a new one"
    );
    assert!(
        local_path.exists(),
        "zombie row must be repaired by re-writing the missing CAS file"
    );

    // And the content is actually retrievable again -- not just a file that
    // happens to exist.
    let retrieved = manager.retrieve_content(&content_key).await?;
    assert_eq!(retrieved, payload);

    Ok(())
}

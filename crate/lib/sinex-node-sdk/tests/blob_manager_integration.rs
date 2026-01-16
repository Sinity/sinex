//! Integration tests that exercise the current `BlobManager` API surface.
//!
//! The historical suite depended on retired helpers and never compiled after
//! the JetStream migration. These focused scenarios ensure deduplication,
//! round-tripping, and integrity verification keep working against git-annex.

use camino::Utf8PathBuf;
use sinex_node_sdk::annex::{AnnexConfig, BlobManager};
use sinex_test_utils::prelude::*;
use tempfile::TempDir;

const TEST_BYTES: &[u8] = b"sinex-blob-manager-integration";

async fn blob_manager_fixture(ctx: &TestContext) -> color_eyre::Result<(BlobManager, TempDir)> {
    system_test_preflight()?;
    let temp_dir = TempDir::new()?;
    let annex_path = temp_dir.path().join("annex");
    let repo_utf8 = Utf8PathBuf::from_path_buf(annex_path.clone())
        .map_err(|_| color_eyre::eyre::eyre!("annex path must be valid UTF-8"))?;

    let annex_config = AnnexConfig {
        repo_path: repo_utf8,
        num_copies: Some(1),
        large_files: Some("anything".to_string()),
    };

    let manager = BlobManager::new(annex_config, ctx.pool().clone(), None)?;
    Ok((manager, temp_dir))
}

#[sinex_test]
async fn blob_manager_deduplicates_content(ctx: TestContext) -> color_eyre::Result<()> {
    let (manager, _tmp) = blob_manager_fixture(&ctx).await?;

    let first = manager
        .ingest_from_bytes(TEST_BYTES, "dedupe-a.txt", "text/plain")
        .await?;
    let second = manager
        .ingest_from_bytes(TEST_BYTES, "dedupe-b.txt", "text/plain")
        .await?;

    ctx.assert("dedupe should return same annex key").that(
        first.annex_key() == second.annex_key(),
        "Annex keys must match",
    )?;
    ctx.assert("stored checksum should be reused").that(
        first.checksum_blake3 == second.checksum_blake3,
        "Checksums must match",
    )?;

    Ok(())
}

#[sinex_test]
async fn blob_manager_round_trips_content(ctx: TestContext) -> color_eyre::Result<()> {
    let (manager, _tmp) = blob_manager_fixture(&ctx).await?;
    let blob = manager
        .ingest_from_bytes(TEST_BYTES, "roundtrip.txt", "text/plain")
        .await?;
    let key = blob.annex_key();

    let retrieved = manager.retrieve_content(&key).await?;
    ctx.assert("round trip payload should match").that(
        retrieved == TEST_BYTES,
        "Round-trip bytes must match original",
    )?;

    Ok(())
}

#[sinex_test]
async fn blob_manager_detects_corruption_on_retrieve(ctx: TestContext) -> color_eyre::Result<()> {
    let (manager, _tmp) = blob_manager_fixture(&ctx).await?;
    let blob = manager
        .ingest_from_bytes(
            b"integrity-check",
            "corruption.bin",
            "application/octet-stream",
        )
        .await?;
    let key = blob.annex_key();

    // Force git-annex to materialize the content, then tamper with it.
    let _ = manager.retrieve_content(&key).await?;
    let blob_path = manager.get_blob_path(&key).await?;
    let metadata = tokio::fs::metadata(&blob_path).await?;
    let mut perms = metadata.permissions();
    perms.set_readonly(false);
    tokio::fs::set_permissions(&blob_path, perms).await?;
    tokio::fs::write(&blob_path, b"tampered payload").await?;

    let err = manager.retrieve_content(&key).await.unwrap_err();
    ctx.assert("corrupted blob should fail verification").that(
        err.to_string().to_ascii_lowercase().contains("mismatch"),
        &format!("Expected mismatch error, got {err}"),
    )?;

    Ok(())
}

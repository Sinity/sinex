//! Integration tests that exercise the current `ContentStoreManager` API surface.
//!
//! These scenarios ensure deduplication, round-tripping, and integrity
//! verification keep working against the current large-object backend.

use camino::Utf8PathBuf;
use sinex_node_sdk::content_store::{ContentStoreConfig, ContentStoreManager};
use std::os::unix::fs::PermissionsExt;
use tempfile::TempDir;
use xtask::sandbox::prelude::*;

const TEST_BYTES: &[u8] = b"sinex-content-store-manager-integration";

#[allow(clippy::unused_async)]
async fn manager_fixture(ctx: &TestContext) -> color_eyre::Result<(ContentStoreManager, TempDir)> {
    system_test_preflight()?;
    let temp_dir = TempDir::new()?;
    let content_store_path = temp_dir.path().join("content-store");
    let repo_utf8 = Utf8PathBuf::from_path_buf(content_store_path)
        .map_err(|_| color_eyre::eyre::eyre!("content-store path must be valid UTF-8"))?;

    let content_store_config = ContentStoreConfig {
        root_path: repo_utf8,
        num_copies: Some(1),
        large_files: Some("anything".to_string()),
        legacy_annex_enabled: true,
        ..Default::default()
    };

    let manager = ContentStoreManager::new(content_store_config, ctx.pool().clone(), None)?;
    Ok((manager, temp_dir))
}

#[sinex_test]
#[ignore = "external"]
async fn manager_deduplicates_content(ctx: TestContext) -> color_eyre::Result<()> {
    let (manager, _tmp) = manager_fixture(&ctx).await?;

    let first = manager
        .ingest_from_bytes(TEST_BYTES, "dedupe-a.txt", "text/plain")
        .await?;
    let second = manager
        .ingest_from_bytes(TEST_BYTES, "dedupe-b.txt", "text/plain")
        .await?;

    ctx.assert("dedupe should return same content-store key")
        .that(
            first.content_key() == second.content_key(),
            "Content-store keys must match",
        )?;
    ctx.assert("stored checksum should be reused").that(
        first.checksum_blake3 == second.checksum_blake3,
        "Checksums must match",
    )?;

    Ok(())
}

#[sinex_test]
#[ignore = "external"]
async fn manager_round_trips_content(ctx: TestContext) -> color_eyre::Result<()> {
    let (manager, _tmp) = manager_fixture(&ctx).await?;
    let blob = manager
        .ingest_from_bytes(TEST_BYTES, "roundtrip.txt", "text/plain")
        .await?;
    let key = blob.content_key();

    let retrieved = manager.retrieve_content(&key).await?;
    ctx.assert("round trip payload should match").that(
        retrieved == TEST_BYTES,
        "Round-trip bytes must match original",
    )?;

    Ok(())
}

#[sinex_test]
#[ignore = "external"]
async fn manager_detects_corruption_on_retrieve(ctx: TestContext) -> color_eyre::Result<()> {
    let (manager, _tmp) = manager_fixture(&ctx).await?;
    let blob = manager
        .ingest_from_bytes(
            b"integrity-check",
            "corruption.bin",
            "application/octet-stream",
        )
        .await?;
    let key = blob.content_key();

    // Force the backend to materialize the content, then tamper with it.
    let _ = manager.retrieve_content(&key).await?;
    let blob_path = manager.get_blob_path(&key).await?;
    let perms = std::fs::Permissions::from_mode(0o644);
    tokio::fs::set_permissions(&blob_path, perms).await?;
    tokio::fs::write(&blob_path, b"tampered payload").await?;

    let err = manager.retrieve_content(&key).await.unwrap_err();
    ctx.assert("corrupted blob should fail verification").that(
        err.to_string().to_ascii_lowercase().contains("mismatch"),
        &format!("Expected mismatch error, got {err}"),
    )?;

    Ok(())
}

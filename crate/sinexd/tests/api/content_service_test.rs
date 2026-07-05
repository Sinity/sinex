//! Integration tests for the gateway-owned content service.
//!
//! Core roundtrip tests (store -> retrieve -> verify) run against the default
//! local BLAKE3 CAS backend. The legacy git-annex compatibility roundtrip is
//! gated behind `#[ignore = "external: requires git-annex on PATH"]`.

use camino::Utf8PathBuf;
use sinexd::api::content_service::ContentService;
use sinexd::runtime::content_store::{ContentStoreConfig, ContentStoreKey, ContentStoreManager};
use std::os::unix::fs::symlink;
use std::sync::Arc;
use tempfile::TempDir;
use xtask::sandbox::prelude::*;

/// Preflight: fail loudly when the explicit external git-annex gate is requested
/// without its named prerequisite.
fn require_git_annex() -> TestResult<()> {
    if which::which("git-annex").is_err() {
        return Err(color_eyre::eyre::eyre!(
            "git-annex not found on PATH; run this external test only where git-annex is installed"
        ));
    }
    Ok(())
}

fn content_service_fixture(ctx: &TestContext) -> TestResult<(ContentService, TempDir)> {
    content_service_fixture_with_backend(ctx, false)
}

fn legacy_annex_content_service_fixture(
    ctx: &TestContext,
) -> TestResult<(ContentService, TempDir)> {
    require_git_annex()?;
    content_service_fixture_with_backend(ctx, true)
}

fn content_service_fixture_with_backend(
    ctx: &TestContext,
    legacy_annex_enabled: bool,
) -> TestResult<(ContentService, TempDir)> {
    content_service_fixture_with_backend_and_max_size(ctx, legacy_annex_enabled, None)
}

fn content_service_fixture_with_backend_and_max_size(
    ctx: &TestContext,
    legacy_annex_enabled: bool,
    max_blob_size: Option<usize>,
) -> TestResult<(ContentService, TempDir)> {
    let temp_dir = TempDir::new()?;
    let content_store_path = temp_dir.path().join("content-store");
    let repo_utf8 = Utf8PathBuf::from_path_buf(content_store_path)
        .map_err(|_| color_eyre::eyre::eyre!("content-store path must be valid UTF-8"))?;

    let mut content_store_config = ContentStoreConfig {
        root_path: repo_utf8,
        num_copies: Some(1),
        large_files: Some("anything".to_string()),
        legacy_annex_enabled,
        ..Default::default()
    };
    if let Some(max_blob_size) = max_blob_size {
        content_store_config.max_blob_size = max_blob_size;
    }

    let content_store = ContentStoreManager::new(content_store_config, ctx.pool().clone(), None)?;
    let service = ContentService::new(ctx.pool().clone(), Arc::new(content_store));
    Ok((service, temp_dir))
}

fn local_cas_path(temp_dir: &TempDir, content_key: &str) -> TestResult<std::path::PathBuf> {
    let key = ContentStoreKey::parse(content_key)?;
    let prefix_a = key
        .digest
        .get(0..2)
        .ok_or_else(|| color_eyre::eyre::eyre!("digest missing first prefix"))?;
    let prefix_b = key
        .digest
        .get(2..4)
        .ok_or_else(|| color_eyre::eyre::eyre!("digest missing second prefix"))?;
    Ok(temp_dir
        .path()
        .join("content-store")
        .join("sinex-cas")
        .join(prefix_a)
        .join(prefix_b)
        .join(&key.digest))
}

#[sinex_test]
async fn content_store_retrieve_roundtrip(ctx: TestContext) -> TestResult<()> {
    let (service, _tmp) = content_service_fixture(&ctx)?;

    let payload = b"sinex content roundtrip test payload";
    let content_key = service
        .store_content(
            payload,
            "roundtrip.txt",
            "text/plain",
            "test-harness",
            "test-harness",
        )
        .await?;

    assert!(
        !content_key.is_empty(),
        "content-store key should be non-empty"
    );

    let retrieved = service.retrieve_content(&content_key).await?;
    assert_eq!(
        retrieved.as_slice(),
        payload,
        "retrieved content must match original"
    );

    Ok(())
}

#[sinex_test]
async fn content_retrieve_rejects_file_that_exceeds_retrieval_limit(
    ctx: TestContext,
) -> TestResult<()> {
    let (service, tmp) = content_service_fixture_with_backend_and_max_size(&ctx, false, Some(64))?;

    let payload = b"small enough";
    let content_key = service
        .store_content(
            payload,
            "limited.bin",
            "application/octet-stream",
            "test-harness",
            "test-harness",
        )
        .await?;
    let cas_path = local_cas_path(&tmp, &content_key)?;
    tokio::fs::write(&cas_path, vec![b'x'; 128]).await?;

    let err = service
        .retrieve_content(&content_key)
        .await
        .expect_err("oversized on-disk content must fail before full read");
    let err_text = err.to_string();
    assert!(
        err_text.contains("exceeds retrieval limit"),
        "unexpected error: {err_text}"
    );

    Ok(())
}

#[sinex_test]
async fn content_retrieve_rejects_local_cas_symlink_escape(ctx: TestContext) -> TestResult<()> {
    let (service, tmp) = content_service_fixture(&ctx)?;

    let payload = b"inside cas";
    let content_key = service
        .store_content(
            payload,
            "escape.bin",
            "application/octet-stream",
            "test-harness",
            "test-harness",
        )
        .await?;
    let cas_path = local_cas_path(&tmp, &content_key)?;
    let outside_path = tmp.path().join("outside-secret.bin");
    tokio::fs::write(&outside_path, b"outside").await?;
    tokio::fs::remove_file(&cas_path).await?;
    symlink(&outside_path, &cas_path)?;

    let err = service
        .retrieve_content(&content_key)
        .await
        .expect_err("local CAS symlink escape must fail before read");
    let err_text = err.to_string();
    assert!(
        err_text.contains("escapes content-store root"),
        "unexpected error: {err_text}"
    );

    Ok(())
}

#[sinex_test]
async fn content_verify_after_store(ctx: TestContext) -> TestResult<()> {
    let (service, _tmp) = content_service_fixture(&ctx)?;

    let payload = b"verify me";
    let content_key = service
        .store_content(
            payload,
            "verify.bin",
            "application/octet-stream",
            "test-harness",
            "test-harness",
        )
        .await?;

    let ok = service.verify_content(&content_key).await?;
    assert!(ok, "freshly stored content should verify successfully");

    Ok(())
}

#[sinex_test]
async fn content_metadata_after_store(ctx: TestContext) -> TestResult<()> {
    let (service, _tmp) = content_service_fixture(&ctx)?;

    let payload = b"metadata test";
    let content_key = service
        .store_content(
            payload,
            "meta.txt",
            "text/plain",
            "test-harness",
            "test-harness",
        )
        .await?;

    let meta = service.get_content_metadata(&content_key).await?;
    assert_eq!(meta.size_bytes, payload.len() as i64);
    assert!(
        meta.checksum_blake3.is_some(),
        "BLAKE3 checksum should be populated"
    );
    assert_eq!(meta.original_filename.as_deref(), Some("meta.txt"));

    Ok(())
}

#[sinex_test]
async fn content_deduplication(ctx: TestContext) -> TestResult<()> {
    let (service, _tmp) = content_service_fixture(&ctx)?;

    let payload = b"deduplicate this content";
    let key_a = service
        .store_content(
            payload,
            "a.txt",
            "text/plain",
            "test-harness",
            "test-harness",
        )
        .await?;
    let key_b = service
        .store_content(
            payload,
            "b.txt",
            "text/plain",
            "test-harness",
            "test-harness",
        )
        .await?;

    assert_eq!(
        key_a, key_b,
        "identical content should produce the same content-store key"
    );

    Ok(())
}

#[sinex_test]
async fn content_store_logs_operation(ctx: TestContext) -> TestResult<()> {
    let (service, _tmp) = content_service_fixture(&ctx)?;

    let payload = b"log this store operation";
    let _key = service
        .store_content(
            payload,
            "logged.txt",
            "text/plain",
            "external-source",
            "audit-actor",
        )
        .await?;

    let row: (String, Option<String>) = sqlx::query_as(
        "SELECT operator, scope->>'source' FROM core.operations_log \
         WHERE operation_type = 'content.store' ORDER BY id DESC LIMIT 1",
    )
    .fetch_one(ctx.pool())
    .await?;

    assert_eq!(row.0, "audit-actor");
    assert_eq!(row.1.as_deref(), Some("external-source"));

    Ok(())
}

#[sinex_test]
#[ignore = "external: requires git-annex on PATH"]
async fn legacy_annex_content_store_retrieve_roundtrip(ctx: TestContext) -> TestResult<()> {
    let (service, _tmp) = legacy_annex_content_service_fixture(&ctx)?;

    let payload = b"sinex legacy annex roundtrip test payload";
    let content_key = service
        .store_content(
            payload,
            "legacy-roundtrip.txt",
            "text/plain",
            "test-harness",
            "test-harness",
        )
        .await?;

    assert!(
        !content_key.is_empty(),
        "legacy content-store key should be non-empty"
    );

    let retrieved = service.retrieve_content(&content_key).await?;
    assert_eq!(
        retrieved.as_slice(),
        payload,
        "legacy annex retrieved content must match original"
    );

    Ok(())
}

#[sinex_test]
async fn retrieve_nonexistent_key_returns_service_error(ctx: TestContext) -> TestResult<()> {
    let (service, _tmp) = content_service_fixture(&ctx)?;

    let result = service.retrieve_content("SHA256E-s0--nonexistent").await;
    assert!(result.is_err(), "retrieve of nonexistent key should fail");

    let err = result.unwrap_err();
    let err_str = format!("{err:?}");
    assert!(
        err_str.contains("retrieval failed") || err_str.contains("service"),
        "error should be wrapped as service error, got: {err_str}"
    );

    Ok(())
}

#[sinex_test]
async fn verify_nonexistent_key_returns_service_error(ctx: TestContext) -> TestResult<()> {
    let (service, _tmp) = content_service_fixture(&ctx)?;

    let result = service.verify_content("SHA256E-s0--nonexistent").await;
    assert!(result.is_err(), "verify of nonexistent key should fail");

    let err = result.unwrap_err();
    let err_str = format!("{err:?}");
    assert!(
        err_str.contains("verification failed") || err_str.contains("service"),
        "error should be wrapped as service error, got: {err_str}"
    );

    Ok(())
}

#[sinex_test]
async fn metadata_nonexistent_key_returns_service_error(ctx: TestContext) -> TestResult<()> {
    let (service, _tmp) = content_service_fixture(&ctx)?;

    let result = service
        .get_content_metadata("SHA256E-s0--nonexistent")
        .await;
    assert!(result.is_err(), "metadata of nonexistent key should fail");

    let err = result.unwrap_err();
    let err_str = format!("{err:?}");
    assert!(
        err_str.contains("metadata") || err_str.contains("service"),
        "error should be wrapped as service error, got: {err_str}"
    );

    Ok(())
}

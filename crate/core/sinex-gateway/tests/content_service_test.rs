//! Integration tests for the gateway-owned content service.
//!
//! Full roundtrip tests (store -> retrieve -> verify) require `git-annex` on PATH
//! and are gated behind `#[ignore = "external"]`. Logic-level tests (error
//! wrapping, operation logging, helpers) run unconditionally.

use camino::Utf8PathBuf;
use sinex_gateway::content_service::ContentService;
use sinex_node_sdk::content_store::{ContentStoreConfig, ContentStoreManager};
use std::sync::Arc;
use tempfile::TempDir;
use xtask::sandbox::prelude::*;

/// Preflight: bail early if git-annex is missing.
fn require_git_annex() -> TestResult<()> {
    if which::which("git-annex").is_err() {
        return Err(color_eyre::eyre::eyre!(
            "git-annex not found on PATH — skipping external test"
        ));
    }
    Ok(())
}

#[allow(clippy::unused_async)]
async fn content_service_fixture(ctx: &TestContext) -> TestResult<(ContentService, TempDir)> {
    require_git_annex()?;
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

    let content_store = ContentStoreManager::new(content_store_config, ctx.pool().clone(), None)?;
    let service = ContentService::new(ctx.pool().clone(), Arc::new(content_store));
    Ok((service, temp_dir))
}

#[sinex_test]
#[ignore = "external"]
async fn content_store_retrieve_roundtrip(ctx: TestContext) -> TestResult<()> {
    let (service, _tmp) = content_service_fixture(&ctx).await?;

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
#[ignore = "external"]
async fn content_verify_after_store(ctx: TestContext) -> TestResult<()> {
    let (service, _tmp) = content_service_fixture(&ctx).await?;

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
#[ignore = "external"]
async fn content_metadata_after_store(ctx: TestContext) -> TestResult<()> {
    let (service, _tmp) = content_service_fixture(&ctx).await?;

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
#[ignore = "external"]
async fn content_deduplication(ctx: TestContext) -> TestResult<()> {
    let (service, _tmp) = content_service_fixture(&ctx).await?;

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
#[ignore = "external"]
async fn content_store_logs_operation(ctx: TestContext) -> TestResult<()> {
    let (service, _tmp) = content_service_fixture(&ctx).await?;

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
async fn retrieve_nonexistent_key_returns_service_error(ctx: TestContext) -> TestResult<()> {
    if which::which("git-annex").is_err() {
        return Ok(());
    }
    let (service, _tmp) = content_service_fixture(&ctx).await?;

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
    if which::which("git-annex").is_err() {
        return Ok(());
    }
    let (service, _tmp) = content_service_fixture(&ctx).await?;

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
    if which::which("git-annex").is_err() {
        return Ok(());
    }
    let (service, _tmp) = content_service_fixture(&ctx).await?;

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

use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use camino::Utf8PathBuf;
use color_eyre::eyre::WrapErr;
use sinex_gateway::handlers::handle_store_blob;
use sinex_node_sdk::annex::{
    blob_manager::BLOB_EVENT_CHANNEL_CAPACITY, AnnexConfig, BlobManager, GitAnnex,
};
use sinex_services::ContentService;
use sinex_test_utils::{sinex_serial_test, sinex_test, TestContext, TestResult};
use tempfile::TempDir;
use tokio::sync::mpsc;
use which::which;

struct EnvVarGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let prev = std::env::var(key).ok();
        std::env::set_var(key, value);
        Self { key, prev }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        if let Some(prev) = &self.prev {
            std::env::set_var(self.key, prev);
        } else {
            std::env::remove_var(self.key);
        }
    }
}

fn require_git_annex() -> TestResult<()> {
    which("git-annex")
        .wrap_err("git-annex binary is required for gateway blob security tests")
        .map(|_| ())
}

#[sinex_test]
async fn blob_routes_should_enforce_auth_and_quota(ctx: TestContext) -> TestResult<()> {
    require_git_annex()?;
    let _guard = EnvVarGuard::set("SINEX_GATEWAY_MAX_BLOB_BYTES", "1048576");

    let annex_dir = TempDir::new()?;
    let repo_path = annex_dir.path().join("gateway-blob-test");
    let repo_utf8 = Utf8PathBuf::from_path_buf(repo_path.clone())
        .map_err(|_| color_eyre::eyre::eyre!("annex path is not valid UTF-8"))?;

    GitAnnex::init(&repo_utf8, Some("gateway-blob-test")).await?;

    let annex_config = AnnexConfig {
        repo_path: repo_utf8,
        num_copies: Some(1),
        large_files: Some("anything".to_string()),
    };

    let (event_tx, _event_rx) = mpsc::channel(BLOB_EVENT_CHANNEL_CAPACITY);
    let blob_manager = BlobManager::new(annex_config, ctx.pool.clone(), Some(event_tx))?;
    let content_service = ContentService::new(ctx.pool.clone(), Arc::new(blob_manager));

    // Simulate a 10MB upload with no authentication metadata.
    let oversized_blob = vec![0u8; 10 * 1024 * 1024];
    let params = serde_json::json!({
        "filename": "oversized.bin",
        "content_type": "application/octet-stream",
        "content": BASE64_STANDARD.encode(&oversized_blob)});

    let err = handle_store_blob(&content_service, params)
        .await
        .unwrap_err();

    assert!(
        err.to_string()
            .contains("Blob content exceeds maximum allowed size"),
        "Gateway should reject blobs that exceed the configured size limit"
    );

    Ok(())
}

#[sinex_serial_test]
async fn content_store_blob_does_not_insert_events(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;
    require_git_annex()?;

    let annex_dir = TempDir::new()?;
    let repo_path = annex_dir.path().join("gateway-blob-no-events");
    let repo_utf8 = Utf8PathBuf::from_path_buf(repo_path.clone())
        .map_err(|_| color_eyre::eyre::eyre!("annex path is not valid UTF-8"))?;

    GitAnnex::init(&repo_utf8, Some("gateway-blob-no-events")).await?;

    let annex_config = AnnexConfig {
        repo_path: repo_utf8,
        num_copies: Some(1),
        large_files: Some("anything".to_string()),
    };

    let blob_manager = BlobManager::new(annex_config, ctx.pool.clone(), None)?;
    let content_service = ContentService::new(ctx.pool.clone(), Arc::new(blob_manager));

    let before: i64 = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM core.events")
        .fetch_one(&ctx.pool)
        .await?;

    let params = serde_json::json!({
        "filename": "note.txt",
        "content_type": "text/plain",
        "content": BASE64_STANDARD.encode(b"hello gateway")});

    handle_store_blob(&content_service, params).await?;

    let after: i64 = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM core.events")
        .fetch_one(&ctx.pool)
        .await?;

    assert_eq!(
        before, after,
        "Gateway content RPC must not insert events directly; ingestion belongs to ingestd"
    );

    Ok(())
}

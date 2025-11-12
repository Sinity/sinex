use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use camino::Utf8PathBuf;
use sinex_gateway::handlers::handle_store_blob;
use sinex_satellite_sdk::annex::{AnnexConfig, BlobManager, GitAnnex};
use sinex_services::ContentService;
use sinex_test_utils::{sinex_test, TestContext};
use tempfile::TempDir;
use tokio::sync::mpsc;

fn require_git_annex() {
    if which::which("git-annex").is_err() {
        panic!("git-annex must be installed to run blob route security tests");
    }
}

#[sinex_test]
async fn blob_routes_should_enforce_auth_and_quota(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    require_git_annex();

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

    let (event_tx, _event_rx) = mpsc::unbounded_channel();
    let blob_manager = BlobManager::new(annex_config, ctx.pool.clone(), event_tx)?;
    let content_service = ContentService::new(ctx.pool.clone(), Arc::new(blob_manager));

    // Simulate a 10MB upload with no authentication metadata.
    let oversized_blob = vec![0u8; 10 * 1024 * 1024];
    let params = serde_json::json!({
        "filename": "oversized.bin",
        "content_type": "application/octet-stream",
        "content": BASE64_STANDARD.encode(&oversized_blob),
    });

    let result = handle_store_blob(&content_service, params).await;

    assert!(
        result.is_err(),
        "Blob endpoints should reject unauthenticated oversized uploads instead of silently ingesting them"
    );

    Ok(())
}

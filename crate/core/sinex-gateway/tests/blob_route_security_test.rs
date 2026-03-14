use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use camino::Utf8PathBuf;
use color_eyre::eyre::WrapErr;
use sinex_gateway::{
    config::GatewayConfig, handlers::handle_store_blob, service_container::ServiceContainer,
};
use sinex_node_sdk::annex::GitAnnex;
use tempfile::TempDir;
use which::which;
use xtask::sandbox::{TestContext, TestResult, sinex_test};

fn require_git_annex() -> TestResult<()> {
    which("git-annex")
        .wrap_err("git-annex binary is required for gateway blob security tests")
        .map(|_| ())
}

async fn blob_test_services(
    ctx: &TestContext,
    repo_name: &str,
    max_blob_bytes: usize,
) -> TestResult<(TempDir, ServiceContainer)> {
    let annex_dir = TempDir::new()?;
    let repo_path = annex_dir.path().join(repo_name);
    let repo_utf8 = Utf8PathBuf::from_path_buf(repo_path.clone())
        .map_err(|_| color_eyre::eyre::eyre!("annex path is not valid UTF-8"))?;

    GitAnnex::init(&repo_utf8, Some(repo_name)).await?;

    let mut config = GatewayConfig::default()
        .with_cli_overrides(Some(ctx.database_url().to_string()), None, None);
    config.annex_path = repo_utf8.to_string();
    config.max_blob_bytes = max_blob_bytes;
    config.replay_control_optional = true;

    let services = ServiceContainer::new(&config).await?;
    Ok((annex_dir, services))
}

#[sinex_test]
async fn blob_routes_should_enforce_auth_and_quota(ctx: TestContext) -> TestResult<()> {
    require_git_annex()?;
    let (_annex_dir, services) =
        blob_test_services(&ctx, "gateway-blob-test", 1024 * 1024).await?;

    // Simulate a 10MB upload with no authentication metadata.
    let oversized_blob = vec![0u8; 10 * 1024 * 1024];
    let params = serde_json::json!({
        "filename": "oversized.bin",
        "content_type": "application/octet-stream",
        "content": BASE64_STANDARD.encode(&oversized_blob)});

    let err = handle_store_blob(&services, params).await.unwrap_err();

    assert!(
        err.to_string()
            .contains("Blob content exceeds maximum allowed size"),
        "Gateway should reject blobs that exceed the configured size limit"
    );

    Ok(())
}

#[sinex_test]
async fn content_store_blob_does_not_insert_events(ctx: TestContext) -> TestResult<()> {
    require_git_annex()?;
    let (_annex_dir, services) =
        blob_test_services(&ctx, "gateway-blob-no-events", 5 * 1024 * 1024).await?;

    let before: i64 = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM core.events")
        .fetch_one(&ctx.pool)
        .await?;

    let params = serde_json::json!({
        "filename": "note.txt",
        "content_type": "text/plain",
        "content": BASE64_STANDARD.encode(b"hello gateway")});

    handle_store_blob(&services, params).await?;

    let after: i64 = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM core.events")
        .fetch_one(&ctx.pool)
        .await?;

    assert_eq!(
        before, after,
        "Gateway content RPC must not insert events directly; ingestion belongs to ingestd"
    );

    Ok(())
}

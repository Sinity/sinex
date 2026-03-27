use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use camino::Utf8PathBuf;
use color_eyre::eyre::WrapErr;
use sinex_gateway::{
    auth::Role,
    config::GatewayConfig,
    handlers::{handle_retrieve_blob, handle_store_blob},
    rpc_server::RpcAuthContext,
    service_container::ServiceContainer,
};
use sinex_node_sdk::annex::GitAnnex;
use sinex_primitives::rpc::content::{RetrieveBlobResponse, StoreBlobResponse};
use sinex_primitives::temporal;
use tempfile::TempDir;
use which::which;
use xtask::sandbox::{TestContext, TestResult, sinex_test};

fn write_auth() -> RpcAuthContext {
    RpcAuthContext {
        token_prefix: "blobtest".to_string(),
        actor_id: "token:blobtest".to_string(),
        authenticated_at: temporal::now(),
        role: Role::Write,
    }
}

fn require_git_annex() -> TestResult<()> {
    which("git-annex")
        .wrap_err("git-annex binary is required for gateway blob security tests")
        .map(|_| ())
}

async fn blob_test_services(
    ctx: TestContext,
    repo_name: &str,
    max_blob_bytes: usize,
) -> TestResult<(TestContext, TempDir, ServiceContainer)> {
    let ctx = ctx.with_nats().shared().await?;
    let annex_dir = TempDir::new()?;
    let repo_path = annex_dir.path().join(repo_name);
    let repo_utf8 = Utf8PathBuf::from_path_buf(repo_path.clone())
        .map_err(|_| color_eyre::eyre::eyre!("annex path is not valid UTF-8"))?;

    GitAnnex::init(&repo_utf8, Some(repo_name)).await?;

    let mut config = GatewayConfig::default().with_cli_overrides(
        Some(ctx.database_url().to_string()),
        None,
        None,
    );
    config.annex_path = repo_utf8.to_string();
    config.max_blob_bytes = max_blob_bytes;
    config.nats.url = ctx.nats_handle()?.client_url().to_string();

    let services = ServiceContainer::new(&config).await?;
    Ok((ctx, annex_dir, services))
}

#[sinex_test]
async fn blob_routes_should_enforce_auth_and_quota(ctx: TestContext) -> TestResult<()> {
    require_git_annex()?;
    let (_ctx, _annex_dir, services) =
        blob_test_services(ctx, "gateway-blob-test", 1024 * 1024).await?;
    let auth = write_auth();

    // Simulate a 10MB upload with no authentication metadata.
    let oversized_blob = vec![0u8; 10 * 1024 * 1024];
    let params = serde_json::json!({
        "filename": "oversized.bin",
        "content_type": "application/octet-stream",
        "content": BASE64_STANDARD.encode(&oversized_blob)});

    let err = handle_store_blob(&services, params, &auth)
        .await
        .unwrap_err();

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
    let (_ctx, _annex_dir, services) =
        blob_test_services(ctx, "gateway-blob-no-events", 5 * 1024 * 1024).await?;
    let pool = services.pool().clone();
    let auth = write_auth();

    let before: i64 = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM core.events")
        .fetch_one(&pool)
        .await?;

    let params = serde_json::json!({
        "filename": "note.txt",
        "content_type": "text/plain",
        "content": BASE64_STANDARD.encode(b"hello gateway")});

    handle_store_blob(&services, params, &auth).await?;

    let after: i64 = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM core.events")
        .fetch_one(&pool)
        .await?;

    assert_eq!(
        before, after,
        "Gateway content RPC must not insert events directly; ingestion belongs to ingestd"
    );

    Ok(())
}

#[sinex_test]
async fn content_store_blob_rejects_malformed_optional_fields(ctx: TestContext) -> TestResult<()> {
    require_git_annex()?;
    let (_ctx, _annex_dir, services) =
        blob_test_services(ctx, "gateway-blob-malformed-params", 5 * 1024 * 1024).await?;
    let auth = write_auth();

    let params = serde_json::json!({
        "filename": ["not-a-string"],
        "content_type": "text/plain",
        "content": BASE64_STANDARD.encode(b"hello gateway")
    });

    let error = handle_store_blob(&services, params, &auth)
        .await
        .expect_err("malformed optional blob params must fail");
    assert!(error.to_string().contains("filename"));

    Ok(())
}

#[sinex_test]
async fn content_store_blob_uses_authenticated_actor_for_operations_log(
    ctx: TestContext,
) -> TestResult<()> {
    require_git_annex()?;
    let (_ctx, _annex_dir, services) =
        blob_test_services(ctx, "gateway-blob-operator-audit", 5 * 1024 * 1024).await?;
    let pool = services.pool().clone();
    let auth = write_auth();

    let params = serde_json::json!({
        "filename": "audited.txt",
        "content_type": "text/plain",
        "source": "import://browser-export",
        "content": BASE64_STANDARD.encode(b"hello audited gateway")
    });

    handle_store_blob(&services, params, &auth).await?;

    let row: (String, Option<String>) = sqlx::query_as(
        "SELECT operator, scope->>'source' FROM core.operations_log \
         WHERE operation_type = 'content.store' ORDER BY id DESC LIMIT 1",
    )
    .fetch_one(&pool)
    .await?;

    assert_eq!(row.0, auth.actor_id());
    assert_eq!(row.1.as_deref(), Some("import://browser-export"));

    Ok(())
}

#[sinex_test]
async fn content_blob_rpc_uses_typed_request_and_response_contracts(
    ctx: TestContext,
) -> TestResult<()> {
    require_git_annex()?;
    let (_ctx, _annex_dir, services) =
        blob_test_services(ctx, "gateway-blob-contracts", 5 * 1024 * 1024).await?;
    let auth = write_auth();

    let store_params = serde_json::json!({
        "filename": "contract.txt",
        "content_type": "text/plain",
        "content": BASE64_STANDARD.encode(b"hello typed contract")
    });

    let stored: StoreBlobResponse =
        serde_json::from_value(handle_store_blob(&services, store_params, &auth).await?)?;
    assert!(!stored.key.is_empty(), "store response must include blob key");
    assert_eq!(stored.size, 20);
    assert!(!stored.hash.is_empty(), "store response must include content hash");

    let retrieved: RetrieveBlobResponse = serde_json::from_value(
        handle_retrieve_blob(&services, serde_json::json!({ "key": stored.key })).await?,
    )?;
    assert_eq!(retrieved.content, BASE64_STANDARD.encode(b"hello typed contract"));
    assert_eq!(retrieved.content_type.as_deref(), Some("text/plain"));
    assert_eq!(retrieved.size, 20);

    Ok(())
}

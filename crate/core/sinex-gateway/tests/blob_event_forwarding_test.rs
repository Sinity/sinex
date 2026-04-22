use camino::Utf8PathBuf;
use color_eyre::eyre::WrapErr;
use sinex_gateway::ServiceContainer;
use sinex_node_sdk::content_store::MaterialContentStore;
use sinex_primitives::SinexError;
use tempfile::TempDir;
use which::which;
use xtask::sandbox::timing::WaitHelpers;
use xtask::sandbox::{EnvGuard, TestResult, sinex_test};

fn require_git_annex() -> TestResult<()> {
    which("git-annex")
        .wrap_err("git-annex binary is required for gateway blob forwarding tests")
        .map(|_| ())
}

#[sinex_test]
async fn blob_routes_do_not_persist_events(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    require_git_annex()?;
    let mut env_guard = EnvGuard::new();
    env_guard.set("SINEX_NATS_URL", ctx.nats_handle()?.client_url());
    let temp_dir = TempDir::new()?;
    let repo_path = Utf8PathBuf::from_path_buf(temp_dir.path().to_path_buf())
        .map_err(|_| color_eyre::eyre::eyre!("content-store path is not valid UTF-8"))?;
    MaterialContentStore::init(&repo_path, Some("gateway-blob-forwarding")).await?;
    env_guard.set("SINEX_CONTENT_STORE_PATH", repo_path.as_str());
    let _env_guard = env_guard;

    let initial_count: i64 = sqlx::query_scalar!(
        r#"SELECT COUNT(*) FROM core.events WHERE event_type = 'blob.ingested'"#
    )
    .fetch_one(&ctx.pool)
    .await?
    .unwrap_or(0);

    let container = ServiceContainer::from_database_url(ctx.database_url()).await?;

    container
        .content
        .store_content(
            b"blob payload",
            "fixture.bin",
            "application/octet-stream",
            "test",
            "test",
        )
        .await?;

    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            async move {
                let count: i64 = sqlx::query_scalar!(
                    r#"SELECT COUNT(*) FROM core.events WHERE event_type = 'blob.ingested'"#
                )
                .fetch_one(&pool)
                .await?
                .unwrap_or(0);
                Ok::<bool, SinexError>(count == initial_count)
            }
        },
        5,
    )
    .await?;

    let after_count: i64 = sqlx::query_scalar!(
        r#"SELECT COUNT(*) FROM core.events WHERE event_type = 'blob.ingested'"#
    )
    .fetch_one(&ctx.pool)
    .await?
    .unwrap_or(0);

    assert_eq!(
        after_count, initial_count,
        "Gateway helpers must never write blob.ingested events; ingestd is the single writer"
    );

    Ok(())
}

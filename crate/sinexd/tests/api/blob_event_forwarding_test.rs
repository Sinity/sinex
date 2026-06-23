use camino::Utf8PathBuf;
use sinex_primitives::SinexError;
use sinexd::api::ServiceContainer;
use sinexd::runtime::content_store::MaterialContentStore;
use tempfile::TempDir;
use xtask::sandbox::timing::WaitHelpers;
use xtask::sandbox::{EnvGuard, sinex_test};

#[sinex_test]
async fn blob_routes_do_not_persist_events(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let mut env_guard = EnvGuard::new();
    env_guard.set("SINEX_NATS_URL", ctx.nats_handle()?.client_url());
    let temp_dir = TempDir::new()?;
    let repo_path = Utf8PathBuf::from_path_buf(temp_dir.path().to_path_buf())
        .map_err(|_| color_eyre::eyre::eyre!("content-store path is not valid UTF-8"))?;
    MaterialContentStore::init_with_config(&repo_path, Some("gateway-blob-forwarding"), false)
        .await?;
    env_guard.set("SINEX_CONTENT_STORE_PATH", repo_path.as_str());
    let _env_guard = env_guard;

    let initial_count: i64 = sqlx::query_scalar!(
        r#"SELECT COUNT(*) FROM core.events WHERE event_type = 'blob.ingested'"#
    )
    .fetch_one(&ctx.pool)
    .await?
    .expect("COUNT(*) should always return one row");

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
                .expect("COUNT(*) should always return one row");
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
    .expect("COUNT(*) should always return one row");

    assert_eq!(
        after_count, initial_count,
        "Gateway helpers must never write blob.ingested events; event_engine is the single writer"
    );

    Ok(())
}

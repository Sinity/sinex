use serde_json::json;
use sinex_satellite_sdk::stage_as_you_go::StageAsYouGoContext;
use sinex_test_utils::{satellite_runtime::TestRuntimeBuilder, sinex_test, TestContext};
use sqlx::postgres::PgPoolOptions;

#[sinex_test]
async fn satellite_stage_as_you_go_requires_database_pool(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
    let context = StageAsYouGoContext::from_sender(ctx.pool.clone(), sender, true);

    let result = context
        .register_in_flight("test", None, serde_json::json!({}))
        .await;

    assert!(
        result.is_ok(),
        "Stage-as-You-Go contexts should operate without requiring a direct Postgres connection"
    );

    Ok(())
}

#[sinex_test]
async fn jetstream_material_ingest_conflicts_with_satellite_inserts(
    ctx: TestContext,
) -> TestResult<()> {
    let runtime = TestRuntimeBuilder::new(&ctx, "stage-as-you-go-duplication")
        .with_dry_run(false)
        .build()
        .await?;

    let context = StageAsYouGoContext::from_runtime(&runtime.runtime);
    let material_id = context
        .register_in_flight("log_file", Some("/tmp/stage-dup.txt"), json!({}))
        .await?;

    let content = b"stage-as-you-go still writes temporal ledger rows";
    context
        .finalize_source_material(material_id, content, Some("text/plain"), Some("utf-8"))
        .await?;

    let duplicate_insert = sqlx::query!(
        r#"
        INSERT INTO raw.temporal_ledger
            (source_material_id, offset_start, offset_end, offset_kind, ts_capture, precision, clock, source_type)
        VALUES
            ($1::uuid::ulid, $2, $3, 'byte', now(), 'exact', 'wall', 'realtime_capture')
        "#,
        material_id.to_uuid(),
        0_i64,
        content.len() as i64,
    )
    .execute(ctx.pool())
    .await;

    assert!(
        duplicate_insert.is_ok(),
        "Ingestd should be the lone writer for temporal ledger rows; satellites still inserting rows cause duplicate-key violations ({:?})",
        duplicate_insert.err()
    );

    Ok(())
}

#[sinex_test]
async fn stage_as_you_go_context_should_not_require_live_database() -> TestResult<()> {
    let lazy_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy("postgresql://nobody@127.0.0.1:59999/jetstream_only")
        .expect("lazy pool should build even if database is unreachable");

    let (event_tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let ctx = StageAsYouGoContext::from_sender(lazy_pool, event_tx, true);

    let result = ctx
        .register_in_flight(
            "log_file",
            Some("/tmp/stage-as-you-go-without-db"),
            serde_json::json!({"note": "should operate via JetStream"}),
        )
        .await;

    assert!(
        result.is_ok(),
        "Stage-as-You-Go should operate without a live Postgres pool once satellites publish via JetStream"
    );

    Ok(())
}

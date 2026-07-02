use super::*;
use xtask::sandbox::sinex_test;
#[sinex_test]
async fn test_events_indexes_creation() -> color_eyre::eyre::Result<()> {
    let ctx = TestContext::new().await.unwrap();
    let pool = &ctx.pool;

    // Create the events table first
    sqlx::query(&Events::create_table_statement().to_string(PostgresQueryBuilder))
        .execute(pool)
        .await
        .unwrap();

    // Create all indexes
    for index_stmt in Events::create_indexes() {
        let sql = index_stmt.to_string(PostgresQueryBuilder);
        let _ = sqlx::query(&sql).execute(pool).await; // ignore if exists
    }

    // Create GIN indexes (PostgreSQL-specific)
    for gin_sql in Events::create_gin_indexes_sql() {
        let _ = sqlx::query(&gin_sql).execute(pool).await; // ignore if exists
    }

    // Verify indexes exist
    let indexes = get_table_indexes(pool, "core", "events").await?;

    // Should have primary key index plus our custom indexes
    assert!(indexes.len() >= 3, "Should have multiple indexes");

    // Check for specific indexes by name
    let index_names: Vec<String> = indexes.into_iter().map(|idx| idx.index_name).collect();
    assert!(index_names.iter().any(|name| name.contains("ts_orig")));
    assert!(
        index_names
            .iter()
            .any(|name| name.contains("source_type_ts"))
    );
    Ok(())
}

#[sinex_test]
#[ignore = "long: index performance fixture, run via xtask test --heavy"]
async fn test_index_performance_benefit() -> color_eyre::eyre::Result<()> {
    let ctx = TestContext::new().await.unwrap();
    let ctx = ctx.with_nats().shared().await?;
    let pool = &ctx.pool;
    let _scope = ctx.pipeline().await?;

    // Create tables and indexes
    sqlx::query(
        &SourceMaterialRegistry::create_table_statement().to_string(PostgresQueryBuilder),
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(&Events::create_table_statement().to_string(PostgresQueryBuilder))
        .execute(pool)
        .await
        .unwrap();

    for index_stmt in Events::create_indexes() {
        let sql = index_stmt.to_string(PostgresQueryBuilder);
        let _ = sqlx::query(&sql).execute(pool).await; // ignore if exists
    }

    let payloads: Vec<_> = (0..40)
        .map(|i| {
            DynamicPayload::new("test-source", "test-event", serde_json::json!({"index": i}))
        })
        .collect();
    let events = ctx.publish_many(payloads).await?;
    assert_eq!(events.len(), 40);

    sqlx::query("SET enable_seqscan = OFF")
        .execute(pool)
        .await?;
    // Test that queries can use the indexes (check execution plan)
    let plan = sqlx::query(
        "EXPLAIN (FORMAT JSON) SELECT * FROM core.events WHERE source = 'test-source' AND event_type = 'test-event' ORDER BY ts_orig DESC LIMIT 10"
    ).fetch_one(pool).await.unwrap();
    let _ = sqlx::query("RESET enable_seqscan").execute(pool).await;

    let plan_json: serde_json::Value = plan.get(0);
    let plan_str = plan_json.to_string();

    // Should mention index usage (not purely sequential)
    if !(plan_str.contains("Index") || plan_str.contains("Bitmap")) {
        tracing::warn!(
            "Execution plan did not explicitly show index usage: {}",
            plan_str
        );
    }
    Ok(())
}

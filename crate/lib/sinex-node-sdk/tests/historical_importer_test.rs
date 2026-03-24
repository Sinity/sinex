use serde_json::json;
use sinex_node_sdk::HistoricalImporter;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn historical_importer_fail_marks_material_failed(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let importer = HistoricalImporter::register(
        &pool,
        "/tmp/atuin-history.db",
        "sqlite-database",
        json!({ "application": "atuin" }),
    )
    .await?;

    importer.fail("synthetic import failure").await?;

    let row = sqlx::query!(
        r#"
        SELECT status, metadata
        FROM raw.source_material_registry
        WHERE id = $1::uuid
        "#,
        importer.material_id
    )
    .fetch_one(pool)
    .await?;

    assert_eq!(row.status, "failed");
    assert_eq!(
        row.metadata["failure_reason"].as_str(),
        Some("synthetic import failure")
    );
    Ok(())
}

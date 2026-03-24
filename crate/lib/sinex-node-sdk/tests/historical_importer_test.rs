use serde_json::json;
use sinex_db::{DbPoolExt, Id, repositories::StreamBatchRow};
use sinex_node_sdk::HistoricalImporter;
use sinex_primitives::prelude::*;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn historical_importer_fail_marks_material_failed(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let importer = HistoricalImporter::register(
        pool,
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

#[sinex_test]
async fn historical_importer_finalize_marks_partial_when_rows_quarantined(
    ctx: TestContext,
) -> TestResult<()> {
    let pool = ctx.pool();
    let mut importer = HistoricalImporter::register(
        pool,
        "/tmp/atuin-history-partial.db",
        "sqlite-database",
        json!({ "application": "atuin" }),
    )
    .await?;

    importer.quarantine_row(Some(42), "synthetic bad row");
    importer.finalize(None).await?;

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

    assert_eq!(row.status, "recovered_partial");
    assert_eq!(row.metadata["quarantined_rows"].as_u64(), Some(1));
    assert_eq!(
        row.metadata["recovery_info"]["recovery_reason"].as_str(),
        Some("historical_import_quarantined_rows")
    );
    Ok(())
}

#[sinex_test]
async fn historical_importer_finalize_partial_preserves_file_size_metadata(
    ctx: TestContext,
) -> TestResult<()> {
    let pool = ctx.pool();
    let mut importer = HistoricalImporter::register(
        pool,
        "/tmp/atuin-history-partial-sized.db",
        "sqlite-database",
        json!({ "application": "atuin" }),
    )
    .await?;

    importer.quarantine_row(Some(7), "synthetic bad row");
    importer.finalize(Some(4096)).await?;

    let row = sqlx::query!(
        r#"
        SELECT metadata
        FROM raw.source_material_registry
        WHERE id = $1::uuid
        "#,
        importer.material_id
    )
    .fetch_one(pool)
    .await?;

    assert_eq!(row.metadata["file_size_bytes"].as_i64(), Some(4096));
    Ok(())
}

#[sinex_test]
async fn historical_importer_counts_only_inserted_rows_on_conflict(
    ctx: TestContext,
) -> TestResult<()> {
    let pool = ctx.pool();
    let mut importer = HistoricalImporter::register(
        pool,
        "/tmp/atuin-history-duplicates.db",
        "sqlite-database",
        json!({ "application": "atuin" }),
    )
    .await?;

    let event_id = uuid::Uuid::now_v7();
    let row = StreamBatchRow {
        id: event_id,
        source: EventSource::new("historical-importer-test")?,
        event_type: EventType::new("shell.history")?,
        ts_orig: Timestamp::now(),
        host: HostName::new("test-host"),
        payload: json!({ "command": "echo duplicate" }),
        source_material_id: Some(Id::from_uuid(importer.material_id)),
        anchor_byte: Some(0),
        offset_start: Some(0),
        offset_end: Some(14),
        offset_kind: Some("byte".to_string()),
        source_event_ids: None,
        payload_schema_id: None,
        node_run_id: None,
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: Some("duplicate-shell-history".to_string()),
        created_by_operation_id: None,
        node_model: None,
    };

    let inserted = importer.submit_batch(vec![row.clone(), row]).await?;
    assert_eq!(inserted, 1);
    assert_eq!(importer.events_processed(), 1);
    assert_eq!(importer.rows_quarantined(), 0);

    let stored = pool.events().get_by_ids(&[Id::from_uuid(event_id)]).await?;
    assert_eq!(stored.len(), 1);

    Ok(())
}

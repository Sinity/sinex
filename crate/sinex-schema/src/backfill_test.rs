use super::*;
use sqlx::Row;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn parsed_event_count_backfill_registers_status(ctx: TestContext) -> TestResult<()> {
    ensure_backfill_schema(ctx.pool()).await?;

    let runs = list_backfill_runs(ctx.pool()).await?;

    assert!(runs.iter().any(|run| {
        run.backfill_key == PARSED_EVENT_COUNT_BACKFILL_KEY
            && run.version == PARSED_EVENT_COUNT_BACKFILL_VERSION
            && run.status == "registered"
    }));
    Ok(())
}

#[sinex_test]
async fn parsed_event_count_backfill_requires_quiescent_ack(
    ctx: TestContext,
) -> TestResult<()> {
    ensure_backfill_schema(ctx.pool()).await?;

    let error = run_parsed_event_count_backfill(
        ctx.pool(),
        ParsedEventCountBackfillOptions {
            assume_quiescent: false,
            ..Default::default()
        },
    )
    .await
    .expect_err("backfill must refuse without explicit quiescence acknowledgement");

    assert!(error.to_string().contains("quiescent-mode"));
    Ok(())
}

#[sinex_test]
async fn parsed_event_count_backfill_resumes_and_counts_material_events(
    ctx: TestContext,
) -> TestResult<()> {
    ensure_backfill_schema(ctx.pool()).await?;

    let zero_event_material = insert_material(ctx.pool(), "zero-event").await?;
    let one_event_material = insert_material(ctx.pool(), "one-event").await?;
    let multi_event_material = insert_material(ctx.pool(), "multi-event").await?;
    let derived_only_material = insert_material(ctx.pool(), "derived-only").await?;

    let parent_event = insert_material_event(ctx.pool(), one_event_material, 0).await?;
    insert_material_event(ctx.pool(), multi_event_material, 0).await?;
    insert_material_event(ctx.pool(), multi_event_material, 10).await?;
    insert_derived_event(ctx.pool(), parent_event).await?;

    sqlx::query("UPDATE raw.source_material_registry SET parsed_event_count = 0")
        .execute(ctx.pool())
        .await?;

    let interrupted = run_parsed_event_count_backfill(
        ctx.pool(),
        ParsedEventCountBackfillOptions {
            batch_size: 1,
            assume_quiescent: true,
            stop_after_chunks: Some(1),
            ..Default::default()
        },
    )
    .await?;

    assert_eq!(interrupted.status, "running");
    assert_eq!(interrupted.phase, "scanning");
    assert_eq!(interrupted.scanned_events, 1);
    assert!(interrupted.cursor_event_id.is_some());

    let completed = run_parsed_event_count_backfill(
        ctx.pool(),
        ParsedEventCountBackfillOptions {
            batch_size: 1,
            assume_quiescent: true,
            ..Default::default()
        },
    )
    .await?;

    assert_eq!(completed.status, "succeeded");
    assert_eq!(completed.phase, "complete");
    assert_eq!(completed.scanned_events, 3);
    assert_eq!(completed.applied_materials, 2);
    assert_eq!(material_count(ctx.pool(), zero_event_material).await?, 0);
    assert_eq!(material_count(ctx.pool(), one_event_material).await?, 1);
    assert_eq!(material_count(ctx.pool(), multi_event_material).await?, 2);
    assert_eq!(material_count(ctx.pool(), derived_only_material).await?, 0);

    let rerun = run_parsed_event_count_backfill(
        ctx.pool(),
        ParsedEventCountBackfillOptions {
            batch_size: 1,
            assume_quiescent: true,
            ..Default::default()
        },
    )
    .await?;

    assert_eq!(rerun, completed, "successful rerun should no-op");
    Ok(())
}

async fn insert_material(pool: &sqlx::PgPool, label: &str) -> TestResult<Uuid> {
    let id = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO raw.source_material_registry (
            material_kind,
            source_identifier,
            status,
            timing_info_type,
            metadata,
            total_bytes
        )
        VALUES ('local_cas', $1, 'completed', 'staged_at', '{}'::jsonb, 1000)
        RETURNING id
        "#,
    )
    .bind(format!("test.schema-backfill.{label}"))
    .fetch_one(pool)
    .await?;

    Ok(id)
}

async fn insert_material_event(
    pool: &sqlx::PgPool,
    source_material_id: Uuid,
    anchor_byte: i64,
) -> TestResult<Uuid> {
    let id = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO core.events (
            id,
            source,
            event_type,
            host,
            payload,
            ts_orig,
            source_material_id,
            anchor_byte,
            offset_start,
            offset_end,
            offset_kind
        )
        VALUES (
            uuidv7(),
            'test.schema_backfill',
            'test.material_event',
            'test-host',
            '{}'::jsonb,
            now(),
            $1,
            $2,
            $2,
            $2,
            'byte'
        )
        RETURNING id
        "#,
    )
    .bind(source_material_id)
    .bind(anchor_byte)
    .fetch_one(pool)
    .await?;

    Ok(id)
}

async fn insert_derived_event(pool: &sqlx::PgPool, parent_event_id: Uuid) -> TestResult<Uuid> {
    let id = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO core.events (
            id,
            source,
            event_type,
            host,
            payload,
            ts_orig,
            source_event_ids
        )
        VALUES (
            uuidv7(),
            'test.schema_backfill',
            'test.derived_event',
            'test-host',
            '{}'::jsonb,
            now(),
            ARRAY[$1]::uuid[]
        )
        RETURNING id
        "#,
    )
    .bind(parent_event_id)
    .fetch_one(pool)
    .await?;

    Ok(id)
}

async fn material_count(pool: &sqlx::PgPool, source_material_id: Uuid) -> TestResult<i64> {
    let row = sqlx::query(
        r#"
        SELECT parsed_event_count
        FROM raw.source_material_registry
        WHERE id = $1
        "#,
    )
    .bind(source_material_id)
    .fetch_one(pool)
    .await?;

    Ok(row.try_get("parsed_event_count")?)
}

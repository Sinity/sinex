use super::*;
use sqlx::Row;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn parsed_event_count_backfill_registers_status(ctx: TestContext) -> TestResult<()> {
    prepare_backfill(ctx.pool()).await?;

    let runs = list_backfill_runs(ctx.pool()).await?;

    assert!(runs.iter().any(|run| {
        run.backfill_key == PARSED_EVENT_COUNT_BACKFILL_KEY
            && run.version == PARSED_EVENT_COUNT_BACKFILL_VERSION
            && run.status == "registered"
            && run.execution_state == BackfillExecutionState::Registered
    }));
    Ok(())
}

#[sinex_test]
async fn parsed_event_count_backfill_requires_quiescent_ack(ctx: TestContext) -> TestResult<()> {
    prepare_backfill(ctx.pool()).await?;

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
    prepare_backfill(ctx.pool()).await?;

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

    let runs = list_backfill_runs(ctx.pool()).await?;
    let interrupted_status = only_backfill_run(&runs);
    assert_eq!(
        interrupted_status.execution_state,
        BackfillExecutionState::Interrupted
    );

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

#[sinex_test]
async fn parsed_event_count_backfill_reports_active_and_interrupted_runs(
    ctx: TestContext,
) -> TestResult<()> {
    prepare_backfill(ctx.pool()).await?;
    sqlx::query(
        r#"
        UPDATE sinex_schemas.schema_backfill_runs
        SET status = 'running', phase = 'scanning'
        WHERE backfill_key = $1 AND version = $2
        "#,
    )
    .bind(PARSED_EVENT_COUNT_BACKFILL_KEY)
    .bind(PARSED_EVENT_COUNT_BACKFILL_VERSION)
    .execute(ctx.pool())
    .await?;

    let mut lock_conn = ctx.pool().acquire().await?;
    sqlx::query("SELECT pg_advisory_lock(hashtext($1)::bigint)")
        .bind(BACKFILL_LOCK_KEY)
        .execute(&mut *lock_conn)
        .await?;

    let runs = list_backfill_runs(ctx.pool()).await?;
    let active = only_backfill_run(&runs);
    assert_eq!(active.execution_state, BackfillExecutionState::Active);

    sqlx::query("SELECT pg_advisory_unlock(hashtext($1)::bigint)")
        .bind(BACKFILL_LOCK_KEY)
        .execute(&mut *lock_conn)
        .await?;

    let runs = list_backfill_runs(ctx.pool()).await?;
    let interrupted = only_backfill_run(&runs);
    assert_eq!(
        interrupted.execution_state,
        BackfillExecutionState::Interrupted
    );

    sqlx::query(
        r#"
        UPDATE sinex_schemas.schema_backfill_runs
        SET status = 'failed', error_message = 'test failure'
        WHERE backfill_key = $1 AND version = $2
        "#,
    )
    .bind(PARSED_EVENT_COUNT_BACKFILL_KEY)
    .bind(PARSED_EVENT_COUNT_BACKFILL_VERSION)
    .execute(ctx.pool())
    .await?;
    assert_eq!(
        only_backfill_run(&list_backfill_runs(ctx.pool()).await?).execution_state,
        BackfillExecutionState::Failed
    );

    sqlx::query(
        r#"
        UPDATE sinex_schemas.schema_backfill_runs
        SET status = 'succeeded', error_message = NULL
        WHERE backfill_key = $1 AND version = $2
        "#,
    )
    .bind(PARSED_EVENT_COUNT_BACKFILL_KEY)
    .bind(PARSED_EVENT_COUNT_BACKFILL_VERSION)
    .execute(ctx.pool())
    .await?;
    assert_eq!(
        only_backfill_run(&list_backfill_runs(ctx.pool()).await?).execution_state,
        BackfillExecutionState::Succeeded
    );
    Ok(())
}

#[sinex_test]
async fn parsed_event_count_backfill_persists_scan_failure_and_requires_restart(
    ctx: TestContext,
) -> TestResult<()> {
    prepare_backfill(ctx.pool()).await?;
    let material = insert_material(ctx.pool(), "scan-failure").await?;
    insert_material_event(ctx.pool(), material, 0).await?;
    sqlx::query("UPDATE raw.source_material_registry SET parsed_event_count = 0")
        .execute(ctx.pool())
        .await?;

    let error = run_parsed_event_count_backfill(
        ctx.pool(),
        ParsedEventCountBackfillOptions {
            assume_quiescent: true,
            failpoint: Some(BackfillTestFailpoint::DuringScan),
            ..Default::default()
        },
    )
    .await
    .expect_err("the scan failpoint must fail the runner");
    assert!(
        error.to_string().contains("failure during scan"),
        "expected injected scan failure, got: {error}"
    );

    let runs = list_backfill_runs(ctx.pool()).await?;
    let failed = only_backfill_run(&runs);
    assert_eq!(failed.status, "failed");
    assert_eq!(failed.execution_state, BackfillExecutionState::Failed);
    assert!(
        failed
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("failure during scan"))
    );

    let error = run_parsed_event_count_backfill(
        ctx.pool(),
        ParsedEventCountBackfillOptions {
            assume_quiescent: true,
            ..Default::default()
        },
    )
    .await
    .expect_err("failed runs require an explicit restart");
    assert!(error.to_string().contains("--restart"));

    let recovered = run_parsed_event_count_backfill(
        ctx.pool(),
        ParsedEventCountBackfillOptions {
            assume_quiescent: true,
            restart: true,
            ..Default::default()
        },
    )
    .await?;
    assert_eq!(recovered.execution_state, BackfillExecutionState::Succeeded);
    assert_eq!(material_count(ctx.pool(), material).await?, 1);
    Ok(())
}

#[sinex_test]
async fn parsed_event_count_backfill_persists_apply_failure_and_releases_lock(
    ctx: TestContext,
) -> TestResult<()> {
    prepare_backfill(ctx.pool()).await?;
    let material = insert_material(ctx.pool(), "apply-failure").await?;
    insert_material_event(ctx.pool(), material, 0).await?;
    sqlx::query("UPDATE raw.source_material_registry SET parsed_event_count = 0")
        .execute(ctx.pool())
        .await?;

    let error = run_parsed_event_count_backfill(
        ctx.pool(),
        ParsedEventCountBackfillOptions {
            assume_quiescent: true,
            failpoint: Some(BackfillTestFailpoint::DuringApply),
            ..Default::default()
        },
    )
    .await
    .expect_err("the apply failpoint must fail the runner");
    assert!(error.to_string().contains("failure during apply"));
    assert_eq!(material_count(ctx.pool(), material).await?, 0);

    let runs = list_backfill_runs(ctx.pool()).await?;
    let failed = only_backfill_run(&runs);
    assert_eq!(failed.execution_state, BackfillExecutionState::Failed);
    assert!(
        failed
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("failure during apply"))
    );

    let restarted = run_parsed_event_count_backfill(
        ctx.pool(),
        ParsedEventCountBackfillOptions {
            assume_quiescent: true,
            restart: true,
            ..Default::default()
        },
    )
    .await?;
    assert_eq!(restarted.execution_state, BackfillExecutionState::Succeeded);
    Ok(())
}

#[sinex_test]
async fn parsed_event_count_backfill_refuses_an_active_event_writer(
    ctx: TestContext,
) -> TestResult<()> {
    prepare_backfill(ctx.pool()).await?;
    let material = insert_material(ctx.pool(), "concurrent-writer").await?;
    let mut writer = ctx.pool().begin().await?;
    sqlx::query(
        r#"
        INSERT INTO core.events (
            id, source, event_type, host, payload, ts_orig, source_material_id,
            anchor_byte, offset_start, offset_end, offset_kind
        )
        VALUES (
            uuidv7(), 'test.schema_backfill', 'test.concurrent_writer', 'test-host',
            '{}'::jsonb, now(), $1, 0, 0, 0, 'byte'
        )
        "#,
    )
    .bind(material)
    .execute(&mut *writer)
    .await?;

    let error = run_parsed_event_count_backfill(
        ctx.pool(),
        ParsedEventCountBackfillOptions {
            assume_quiescent: true,
            ..Default::default()
        },
    )
    .await
    .expect_err("an active event writer must block the frozen-horizon backfill");
    assert!(error.to_string().contains("core event writer transaction"));

    writer.rollback().await?;
    insert_material_event(ctx.pool(), material, 0).await?;
    let completed = run_parsed_event_count_backfill(
        ctx.pool(),
        ParsedEventCountBackfillOptions {
            assume_quiescent: true,
            ..Default::default()
        },
    )
    .await?;
    assert_eq!(completed.execution_state, BackfillExecutionState::Succeeded);
    Ok(())
}

fn only_backfill_run(runs: &[BackfillRunStatus]) -> &BackfillRunStatus {
    runs.iter()
        .find(|run| {
            run.backfill_key == PARSED_EVENT_COUNT_BACKFILL_KEY
                && run.version == PARSED_EVENT_COUNT_BACKFILL_VERSION
        })
        .expect("parsed-event-count backfill must be registered")
}

async fn prepare_backfill(pool: &sqlx::PgPool) -> TestResult<()> {
    ensure_backfill_schema(pool).await?;
    reset_parsed_event_count_backfill(pool).await?;
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
    const DECLARATION_ID: &str = "test.schema_backfill.derived_event";
    sqlx::query(
        r#"
        INSERT INTO derivation.product_declarations (
            declaration_id, owner, product_class, write_surface,
            output_source, output_event_type, semantics_version,
            input_eligibility, default_claim_support, verification_command
        )
        VALUES (
            $1, 'test-owner', 'canonical_derived_event', 'derived_output',
            'test.schema_backfill', 'test.derived_event', 'v1',
            'default_canonical_input', '{}'::jsonb, 'xtask test -p sinex-schema'
        )
        "#,
    )
    .bind(DECLARATION_ID)
    .execute(pool)
    .await?;

    let id = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO core.events (
            id,
            source,
            event_type,
            host,
            payload,
            ts_orig,
            source_event_ids,
            product_class,
            claim_support,
            derivation_declaration_id
        )
        VALUES (
            uuidv7(),
            'test.schema_backfill',
            'test.derived_event',
            'test-host',
            '{}'::jsonb,
            now(),
            ARRAY[$1]::uuid[],
            'canonical_derived_event',
            '{}'::jsonb,
            $2
        )
        RETURNING id
        "#,
    )
    .bind(parent_event_id)
    .bind(DECLARATION_ID)
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

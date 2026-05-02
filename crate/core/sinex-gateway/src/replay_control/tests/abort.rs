#[allow(unused_imports)]
use super::*;
#[sinex_test]
async fn replay_execute_rejects_zero_event_preview_before_execution(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let client =
        spawn_replay_control(replay.clone(), ctx.nats_client(), Duration::from_secs(30)).await?;

    let operation = replay
        .create_operation(sample_scope(), "test:planner".to_string())
        .await?;
    let now = Timestamp::now();
    replay
        .update_preview(
            operation.operation_id,
            json!({
                "total_events": 0,
                "time_window": {
                    "start": now.format_rfc3339(),
                    "end": (now + time::Duration::seconds(1)).format_rfc3339(),
                }
            }),
        )
        .await?;
    replay
        .approve(operation.operation_id, "admin:approver".to_string())
        .await?;

    let err = client
        .execute(
            operation.operation_id,
            "service:executor-node".into(),
            false,
        )
        .await
        .expect_err("zero-event previews must not enter execution");
    assert!(
        err.to_string().contains("preview matches zero events"),
        "unexpected error: {err}"
    );

    let stored = replay.load_operation(operation.operation_id).await?;
    assert_eq!(stored.state, ReplayState::Failed);
    assert_eq!(
        stored.outcome,
        Some(sinex_primitives::domain::ReplayOutcome::Failed)
    );
    assert_eq!(
        stored.error_details.as_deref(),
        Some(err.to_string().as_str())
    );
    assert!(stored.executor_node.is_none());

    Ok(())
}

#[sinex_test]
async fn replay_preview_rejects_refresh_after_approval(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let client =
        spawn_replay_control(replay.clone(), ctx.nats_client(), Duration::from_secs(30)).await?;

    let planned = client.plan("test:planner".into(), sample_scope()).await?;
    let (previewed, _) = client.preview(planned.operation_id).await?;
    let approved = client
        .approve(previewed.operation_id, "admin:approver".into())
        .await?;

    let err = client
        .preview(approved.operation_id)
        .await
        .expect_err("approved operations must not accept preview refreshes");
    assert!(
        err.to_string().contains("already approved"),
        "unexpected error: {err}"
    );

    let stored = replay.load_operation(approved.operation_id).await?;
    assert_eq!(stored.state, ReplayState::Approved);
    Ok(())
}

#[sinex_test]
async fn replay_execute_dry_run_is_rejected_without_state_changes(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let client =
        spawn_replay_control(replay.clone(), ctx.nats_client(), Duration::from_secs(30)).await?;

    let planned = client.plan("test:planner".into(), sample_scope()).await?;
    let (previewed, _) = client.preview(planned.operation_id).await?;
    let approved = client
        .approve(previewed.operation_id, "admin:approver".into())
        .await?;

    let err = client
        .execute(approved.operation_id, "service:executor-node".into(), true)
        .await
        .expect_err("dry-run execute should redirect callers back to preview");
    assert!(
        err.to_string()
            .contains("does not support dry-run semantics"),
        "unexpected error: {err}"
    );

    let stored = replay.load_operation(approved.operation_id).await?;
    assert_eq!(stored.state, ReplayState::Approved);
    assert!(stored.finished_at.is_none());
    Ok(())
}

#[sinex_test]
async fn replay_execute_fails_when_live_scope_disappears_after_approval(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let material_id = ctx
        .create_source_material(Some("replay-scope-disappeared"))
        .await?;
    let event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-scope-disappeared.txt" }),
    )
    .from_material(material_id)
    .build()?;
    let inserted = ctx.pool.events().insert(event).await?;
    let target_event_id = inserted.id.expect("inserted replay target must have id");
    let target_id = target_event_id.to_uuid();
    let target_ts = target_event_id.timestamp();

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let client =
        spawn_replay_control(replay.clone(), ctx.nats_client(), Duration::from_secs(30)).await?;

    let mut scope = sample_scope();
    scope.time_window = Some((
        target_ts - time::Duration::milliseconds(1),
        target_ts + time::Duration::milliseconds(1),
    ));
    scope.material_filter = Some(vec![*material_id.as_uuid()]);
    scope.filters.insert(
        "event_types".to_string(),
        json!([FileCreatedPayload::EVENT_TYPE.as_static_str()]),
    );

    let planned = client.plan("test:replay-user".into(), scope).await?;
    let (previewed, preview) = client.preview(planned.operation_id).await?;
    assert_eq!(
        preview
            .get("total_events")
            .and_then(serde_json::Value::as_i64),
        Some(1)
    );
    let approved = client
        .approve(previewed.operation_id, "admin:approver".into())
        .await?;

    ctx.pool
        .events()
        .execute_cascade_archive(
            &[target_id],
            "archive replay target before execution",
            &Uuid::now_v7().to_string(),
            "test:archive-before-replay",
        )
        .await?;

    let err = client
        .execute(approved.operation_id, "service:executor-node".into(), false)
        .await
        .expect_err("execution should fail once the approved live scope has vanished");
    assert!(
        err.to_string().contains("matched zero live events"),
        "unexpected error: {err}"
    );

    let failed = replay.load_operation(approved.operation_id).await?;
    assert_eq!(failed.state, ReplayState::Failed);
    assert_eq!(
        failed.outcome,
        Some(sinex_primitives::domain::ReplayOutcome::Failed)
    );

    Ok(())
}

#[sinex_test]
async fn replay_execute_fails_when_live_scope_drifts_after_approval(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let first_material = ctx
        .create_source_material(Some("replay-scope-drift-first"))
        .await?;
    let second_material = ctx
        .create_source_material(Some("replay-scope-drift-second"))
        .await?;

    let first = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-scope-drift-first.txt" }),
    )
    .from_material(first_material)
    .build()?;
    let second = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-scope-drift-second.txt" }),
    )
    .from_material(second_material)
    .build()?;

    let inserted_first = ctx.pool.events().insert(first).await?;
    let inserted_second = ctx.pool.events().insert(second).await?;
    let first_event_id = inserted_first.id.expect("first replay target must have id");
    let second_event_id = inserted_second
        .id
        .expect("second replay target must have id");
    let first_ts = first_event_id.timestamp();
    let second_ts = second_event_id.timestamp();

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let client =
        spawn_replay_control(replay.clone(), ctx.nats_client(), Duration::from_secs(30)).await?;

    let mut scope = sample_scope();
    scope.time_window = Some((
        std::cmp::min(first_ts, second_ts) - time::Duration::milliseconds(1),
        std::cmp::max(first_ts, second_ts) + time::Duration::milliseconds(1),
    ));
    scope.filters.insert(
        "event_types".to_string(),
        json!([FileCreatedPayload::EVENT_TYPE.as_static_str()]),
    );

    let planned = client.plan("test:replay-user".into(), scope).await?;
    let (previewed, preview) = client.preview(planned.operation_id).await?;
    assert_eq!(
        preview
            .get("total_events")
            .and_then(serde_json::Value::as_i64),
        Some(2)
    );
    let approved = client
        .approve(previewed.operation_id, "admin:approver".into())
        .await?;

    ctx.pool
        .events()
        .execute_cascade_archive(
            &[first_event_id.to_uuid()],
            "archive one replay target before execution",
            &Uuid::now_v7().to_string(),
            "test:archive-before-replay",
        )
        .await?;

    let err = client
        .execute(approved.operation_id, "service:executor-node".into(), false)
        .await
        .expect_err("execution should fail once the approved live scope drifts");
    assert!(
        err.to_string().contains("preview is stale"),
        "unexpected error: {err}"
    );
    assert!(
        err.to_string()
            .contains(&second_event_id.to_uuid().to_string())
            || err
                .to_string()
                .contains(&first_event_id.to_uuid().to_string()),
        "drift error should expose the changed root set: {err}"
    );

    let failed = replay.load_operation(approved.operation_id).await?;
    assert_eq!(failed.state, ReplayState::Failed);
    assert_eq!(
        failed.outcome,
        Some(sinex_primitives::domain::ReplayOutcome::Failed)
    );

    Ok(())
}

#[sinex_test]
async fn replay_abort_before_scan_ack_restores_cascade_and_emits_compensating_invalidation(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let engine = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client());

    let material_id = ctx
        .create_source_material(Some("replay-compensating-invalidation"))
        .await?;
    let mut event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-compensating-invalidation.txt" }),
    )
    .from_material(material_id)
    .build()?;
    event.scope_key = Some("scope://fs-test/replay-compensating-invalidation".to_string());
    let inserted = ctx.pool.events().insert(event).await?;
    let event_id = inserted.id.expect("inserted replay target must have id");
    let operation_id = Uuid::now_v7();

    let scope_metadata = engine
        .collect_cascade_scope_metadata(&ctx.pool, &[event_id.to_uuid()])
        .await?;
    assert_eq!(scope_metadata.len(), 1);
    assert_eq!(scope_metadata[0].event_source.as_str(), "fs-test");
    assert_eq!(
        scope_metadata[0].event_type.as_str(),
        FileCreatedPayload::EVENT_TYPE.as_static_str()
    );
    assert!(!scope_metadata[0].has_lineage);
    assert_eq!(scope_metadata[0].event_ids, vec![event_id.to_uuid()]);

    ctx.pool
        .events()
        .execute_cascade_archive(
            &[event_id.to_uuid()],
            "archive before compensating restore test",
            &operation_id.to_string(),
            "test:replay-compensating",
        )
        .await?;

    let mut invalidation_rx = spawn_invalidation_listener_for_test(&ctx.nats_client()).await?;

    let err = engine
        .abort_before_scan_ack(
            &ctx.pool,
            &[event_id.to_uuid()],
            &scope_metadata,
            operation_id,
            eyre!("boom"),
        )
        .await
        .expect_err("abort helper should surface the caller failure");
    assert!(
        err.to_string()
            .contains("published compensating scope invalidations"),
        "unexpected error: {err}"
    );

    let live_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(event_id.to_uuid())
            .fetch_one(&ctx.pool)
            .await?;
    assert_eq!(
        live_count, 1,
        "aborted replay should restore the archived event"
    );

    let payload_bytes = tokio::time::timeout(Duration::from_secs(1), invalidation_rx.recv())
        .await?
        .expect("compensating invalidation should be published");
    let payload = String::from_utf8(payload_bytes)?;
    assert!(payload.contains("scope://fs-test/replay-compensating-invalidation"));
    assert!(payload.contains(&event_id.to_string()));

    Ok(())
}

#[sinex_test]
async fn replay_abort_before_scan_ack_surfaces_compensating_invalidation_failure(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let engine = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client())
        .with_scope_invalidation_publish_failures(Arc::new(AtomicUsize::new(1)));

    let material_id = ctx
        .create_source_material(Some("replay-compensating-invalidation-failure"))
        .await?;
    let mut event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-compensating-invalidation-failure.txt" }),
    )
    .from_material(material_id)
    .build()?;
    event.scope_key = Some("scope://fs-test/replay-compensating-invalidation-failure".to_string());
    let inserted = ctx.pool.events().insert(event).await?;
    let event_id = inserted.id.expect("inserted replay target must have id");
    let operation_id = Uuid::now_v7();

    let scope_metadata = engine
        .collect_cascade_scope_metadata(&ctx.pool, &[event_id.to_uuid()])
        .await?;
    assert_eq!(scope_metadata.len(), 1);

    ctx.pool
        .events()
        .execute_cascade_archive(
            &[event_id.to_uuid()],
            "archive before compensating restore failure test",
            &operation_id.to_string(),
            "test:replay-compensating-failure",
        )
        .await?;

    let err = engine
        .abort_before_scan_ack(
            &ctx.pool,
            &[event_id.to_uuid()],
            &scope_metadata,
            operation_id,
            eyre!("boom"),
        )
        .await
        .expect_err("compensating invalidation publish failure should surface");
    assert!(
        err.to_string()
            .contains("failed to publish compensating scope invalidations"),
        "unexpected error: {err}"
    );

    let live_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(event_id.to_uuid())
            .fetch_one(&ctx.pool)
            .await?;
    let archived_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(event_id.to_uuid())
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(
        live_count, 1,
        "aborted replay should still restore the archived event"
    );
    assert_eq!(
        archived_count, 0,
        "aborted replay should not leave the archived event behind"
    );

    Ok(())
}

#[sinex_test]
async fn replay_execution_returns_cancelled_operation_when_cancelled_midflight(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let nats_url = ctx.nats_handle()?.client_url().to_string();

    let material_id = ctx
        .create_source_material(Some("replay-cancel-midflight"))
        .await?;
    let event = DynamicPayload::new(
        "cancel-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-cancel.txt" }),
    )
    .from_material(material_id)
    .build()?;
    let inserted = ctx.pool.events().insert(event).await?;
    let target_id = inserted
        .id
        .expect("inserted replay target must have id")
        .to_uuid();
    let target_ts = inserted
        .id
        .expect("inserted replay target must have id")
        .timestamp();

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();
    let env = sinex_primitives::environment::environment();
    let (_scan_command_rx, scan_handle) =
        spawn_fake_scan_node_ack_only(nats_client.clone(), env.clone(), "cancel-test").await?;

    let executor = ReplayExecutionEngine::new(replay.clone(), nats_client.clone())
        .with_scan_completion_timeout(Duration::from_secs(5));
    let health = Arc::new(Mutex::new(ReplayControlHealthState::default()));
    ReplayControlServer::new(
        &env,
        nats_client.clone(),
        replay.clone(),
        executor,
        Arc::clone(&health),
    )
    .spawn()
    .await?;

    let execute_client = ReplayControlClient::new(
        &env,
        async_nats::connect(&nats_url).await?,
        Duration::from_secs(30),
        Arc::clone(&health),
    );
    let control_client = ReplayControlClient::new(
        &env,
        async_nats::connect(&nats_url).await?,
        Duration::from_secs(30),
        health,
    );

    let mut scope = sample_scope();
    scope.node_id = "cancel-test".to_string();
    scope.time_window = Some((
        target_ts - time::Duration::milliseconds(1),
        target_ts + time::Duration::milliseconds(1),
    ));

    let planned = control_client
        .plan("test:replay-user".into(), scope)
        .await?;
    let (previewed, _) = control_client.preview(planned.operation_id).await?;
    let approved = control_client
        .approve(previewed.operation_id, "admin:approver".into())
        .await?;

    let operation_id = approved.operation_id;
    let execute_task = tokio::spawn(async move {
        execute_client
            .execute(operation_id, "service:executor-node".into(), false)
            .await
    });

    let mut saw_executing = false;
    for _ in 0..40 {
        let operation = replay.load_operation(operation_id).await?;
        if operation.state == ReplayState::Executing {
            saw_executing = true;
            break;
        }
        sleep(Duration::from_millis(25)).await;
    }
    assert!(
        saw_executing,
        "replay operation should enter Executing before cancellation"
    );

    let cancellation_requested = control_client
        .cancel(
            operation_id,
            "admin:approver".into(),
            Some("operator requested stop".to_string()),
        )
        .await?;
    assert_eq!(cancellation_requested.state, ReplayState::Cancelling);
    assert!(cancellation_requested.outcome.is_none());
    assert_eq!(
        cancellation_requested.error_details.as_deref(),
        Some("operator requested stop")
    );
    assert!(cancellation_requested.finished_at.is_none());

    let executed = execute_task
        .await
        .map_err(|e| eyre!("execute task failed: {e}"))??;
    assert_eq!(executed.state, ReplayState::Cancelled);
    assert_eq!(
        executed.outcome,
        Some(sinex_primitives::domain::ReplayOutcome::Cancelled)
    );
    assert_eq!(
        executed.error_details.as_deref(),
        Some("operator requested stop")
    );

    let live_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(target_id)
            .fetch_one(&ctx.pool)
            .await?;
    let archived_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(target_id)
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(
        live_count, 1,
        "cancelled replay should restore live rows when no replacement events were emitted"
    );
    assert_eq!(
        archived_count, 0,
        "cancelled replay should not leave archived rows behind when execution never emitted replacements"
    );

    scan_handle
        .await
        .map_err(|e| eyre!("fake cancel-test node task failed: {e}"))?;

    Ok(())
}

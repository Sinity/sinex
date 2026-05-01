#[allow(unused_imports)] use super::*;
#[sinex_test]
async fn replay_execution_fails_when_outputs_never_become_query_visible(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let material_id = ctx
        .create_source_material(Some("replay-output-visibility-timeout"))
        .await?;
    let event = DynamicPayload::new(
        "visibility-timeout-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-output-visibility-timeout.txt" }),
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
    let env = environment();
    let (scan_command_rx, scan_handle) = spawn_fake_scan_node_with_progress(
        nats_client.clone(),
        env,
        "visibility-timeout-test",
        1,
        1,
    )
    .await?;

    let mut scope = sample_scope();
    scope.node_id = "visibility-timeout-test".to_string();
    scope.time_window = Some((
        target_ts - time::Duration::milliseconds(1),
        target_ts + time::Duration::milliseconds(1),
    ));

    let planned = replay
        .create_operation(scope.clone(), "test:output-visibility-timeout".into())
        .await?;
    let preview = replay.generate_preview_summary(&scope).await?;
    replay.update_preview(planned.operation_id, preview).await?;
    replay
        .approve(planned.operation_id, "admin:approver".into())
        .await?;

    let executor = ReplayExecutionEngine::new(replay.clone(), nats_client)
        .with_scan_completion_timeout(Duration::from_millis(100));
    let err = executor
        .execute(planned.operation_id, "service:executor-node".into())
        .await
        .expect_err("missing replay outputs must fail before completion");
    assert!(
        err.to_string()
            .contains("Replay outputs were not query-visible after successful scan"),
        "unexpected error: {err}"
    );

    let failed = replay.load_operation(planned.operation_id).await?;
    assert_eq!(failed.state, ReplayState::Failed);
    assert_eq!(
        failed.outcome,
        Some(sinex_primitives::domain::ReplayOutcome::Failed)
    );

    let live_target_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(target_id)
            .fetch_one(&ctx.pool)
            .await?;
    let archived_target_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(target_id)
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(live_target_count, 0);
    assert_eq!(archived_target_count, 1);

    let dispatched_command = scan_command_rx.await.map_err(|_| {
        eyre!("fake visibility-timeout-test node did not receive a scan command")
    })?;
    assert_eq!(dispatched_command.operation_id, planned.operation_id);

    scan_handle
        .await
        .map_err(|e| eyre!("fake visibility-timeout-test node task failed: {e}"))?;

    Ok(())
}

#[sinex_test]
async fn replay_execution_fails_when_node_never_reports_completion(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let material_id = ctx.create_source_material(Some("replay-timeout")).await?;
    let event = DynamicPayload::new(
        "timeout-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-timeout.txt" }),
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
    let (scan_command_rx, scan_handle) =
        spawn_fake_scan_node_ack_only(nats_client.clone(), env.clone(), "timeout-test").await?;

    let executor = ReplayExecutionEngine::new(replay.clone(), nats_client.clone())
        .with_scan_completion_timeout(Duration::from_millis(100));
    ReplayTelemetry::new(replay.clone()).spawn();
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
    let client = ReplayControlClient::new(&env, nats_client, Duration::from_secs(30), health);

    let mut scope = sample_scope();
    scope.node_id = "timeout-test".to_string();
    scope.time_window = Some((
        target_ts - time::Duration::milliseconds(1),
        target_ts + time::Duration::milliseconds(1),
    ));

    let planned = client.plan("test:replay-user".into(), scope).await?;
    let (previewed, _) = client.preview(planned.operation_id).await?;
    let approved = client
        .approve(previewed.operation_id, "admin:approver".into())
        .await?;
    let err = client
        .execute(approved.operation_id, "service:executor-node".into(), false)
        .await
        .expect_err("execute should fail when the node never reports completion");
    assert!(
        err.to_string().contains("archived cascade left untouched"),
        "timeout failure should explain why replay execution failed: {err}"
    );

    let operation = replay.load_operation(approved.operation_id).await?;
    assert_eq!(operation.state, ReplayState::Failed);
    assert_eq!(operation.checkpoint.processed_events, 0);

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
        live_count, 0,
        "timed-out replay should not resurrect archived rows"
    );
    assert_eq!(
        archived_count, 1,
        "timed-out replay should leave the archived cascade untouched"
    );

    let dispatched_command = scan_command_rx
        .await
        .map_err(|_| eyre!("fake timeout-test node did not receive a scan command"))?;
    assert_eq!(dispatched_command.operation_id, approved.operation_id);

    scan_handle
        .await
        .map_err(|e| eyre!("fake timeout-test node task failed: {e}"))?;

    Ok(())
}

#[sinex_test]
async fn replay_execution_fails_fast_when_progress_checkpoint_persist_fails(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let material_id = ctx
        .create_source_material(Some("replay-checkpoint-persist-fail"))
        .await?;
    let event = DynamicPayload::new(
        "checkpoint-fail-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-checkpoint-persist-fail.txt" }),
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
    let env = environment();
    let (_scan_command_rx, scan_handle) = spawn_fake_scan_node_with_progress(
        nats_client.clone(),
        env,
        "checkpoint-fail-test",
        1,
        0,
    )
    .await?;

    let mut scope = sample_scope();
    scope.node_id = "checkpoint-fail-test".to_string();
    scope.time_window = Some((
        target_ts - time::Duration::milliseconds(1),
        target_ts + time::Duration::milliseconds(1),
    ));

    let planned = replay
        .create_operation(scope.clone(), "test:checkpoint-fail".into())
        .await?;
    let preview = replay.generate_preview_summary(&scope).await?;
    replay.update_preview(planned.operation_id, preview).await?;
    replay
        .approve(planned.operation_id, "admin:approver".into())
        .await?;

    let executor = ReplayExecutionEngine::new(replay.clone(), nats_client)
        .with_checkpoint_failures(Arc::new(AtomicUsize::new(1)))
        .with_scan_completion_timeout(Duration::from_secs(5));
    let err = executor
        .execute(planned.operation_id, "service:executor-node".into())
        .await
        .expect_err("checkpoint persistence failure should abort replay execution");
    assert!(
        err.chain().any(|cause| {
            cause
                .to_string()
                .contains("Failed to persist replay progress checkpoint")
        }),
        "unexpected error: {err}"
    );

    let failed = replay.load_operation(planned.operation_id).await?;
    assert_eq!(failed.state, ReplayState::Failed);
    assert_eq!(
        failed.outcome,
        Some(sinex_primitives::domain::ReplayOutcome::Failed)
    );
    assert!(
        failed.error_details.as_deref().is_some_and(
            |details| details.contains("Failed to persist replay progress checkpoint")
        ),
        "failure details should include checkpoint persistence context: {:?}",
        failed.error_details
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
        "checkpoint persistence failure before replacements should restore live rows"
    );
    assert_eq!(
        archived_count, 0,
        "checkpoint persistence failure before replacements should not leave archived rows behind"
    );

    scan_handle
        .await
        .map_err(|e| eyre!("fake checkpoint-fail-test node task failed: {e}"))?;

    Ok(())
}

#[sinex_test]
async fn replay_execution_fails_when_replacement_recording_fails(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let material_id = ctx
        .create_source_material(Some("replay-replacement-record-fail"))
        .await?;
    let mut event = DynamicPayload::new(
        "replacement-record-fail-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-replacement-record-fail.txt" }),
    )
    .from_material(material_id)
    .build()?;
    event.equivalence_key = Some("replacement-record-eq".to_string());
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
    let env = environment();
    let (scan_command_rx, scan_handle) = spawn_fake_scan_node_with_progress(
        nats_client.clone(),
        env,
        "replacement-record-fail-test",
        1,
        1,
    )
    .await?;

    let mut scope = sample_scope();
    scope.node_id = "replacement-record-fail-test".to_string();
    scope.time_window = Some((
        target_ts - time::Duration::milliseconds(1),
        target_ts + time::Duration::milliseconds(1),
    ));

    let planned = replay
        .create_operation(scope.clone(), "test:replacement-record-fail".into())
        .await?;
    let preview = replay.generate_preview_summary(&scope).await?;
    replay.update_preview(planned.operation_id, preview).await?;
    replay
        .approve(planned.operation_id, "admin:approver".into())
        .await?;

    let replay_output_handle = spawn_replay_output_inserter(
        ctx.pool.clone(),
        scan_command_rx,
        "replacement-record-fail-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        "/tmp/replay-replacement-record-fail-output.txt",
        Some("replacement-record-eq"),
    );

    let executor = ReplayExecutionEngine::new(replay.clone(), nats_client)
        .with_replacement_record_failures(Arc::new(AtomicUsize::new(1)))
        .with_scan_completion_timeout(Duration::from_secs(5));
    let err = executor
        .execute(planned.operation_id, "service:executor-node".into())
        .await
        .expect_err("replacement-record failure should abort replay execution");
    assert!(
        err.chain().any(|cause| {
            cause
                .to_string()
                .contains("Failed to record replay replacement relations")
        }),
        "unexpected error: {err}"
    );

    let failed = replay.load_operation(planned.operation_id).await?;
    assert_eq!(failed.state, ReplayState::Failed);
    assert_eq!(
        failed.outcome,
        Some(sinex_primitives::domain::ReplayOutcome::Failed)
    );
    assert!(
        failed.error_details.as_deref().is_some_and(|details| {
            details.contains("Failed to record replay replacement relations")
        }),
        "failure details should include replacement recording context: {:?}",
        failed.error_details
    );

    let replay_command = replay_output_handle
        .await
        .map_err(|e| eyre!("fake replacement-record replay output task failed: {e}"))??;

    let live_target_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(target_id)
            .fetch_one(&ctx.pool)
            .await?;
    let archived_target_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(target_id)
    .fetch_one(&ctx.pool)
    .await?;
    let live_replacement_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM core.events WHERE created_by_operation_id = $1::uuid",
    )
    .bind(replay_command.operation_id)
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(
        live_target_count, 0,
        "replacement-record failure occurs after the original event has already been archived"
    );
    assert_eq!(
        archived_target_count, 1,
        "replacement-record failure must leave the archived target in audit storage"
    );
    assert_eq!(
        live_replacement_count, 1,
        "replacement-record failure must not delete already-emitted replay outputs"
    );

    let replacements = ctx
        .pool
        .events()
        .get_replacements_by_operation(planned.operation_id)
        .await?;
    assert!(
        replacements.is_empty(),
        "failed replacement recording must not partially insert lineage rows"
    );

    scan_handle
        .await
        .map_err(|e| eyre!("fake replacement-record-fail-test node task failed: {e}"))?;

    Ok(())
}

#[sinex_test]
async fn replay_execution_restores_archived_cascade_when_dispatch_fails_before_ack(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let material_id = ctx
        .create_source_material(Some("replay-pre-ack-failure"))
        .await?;
    let event = DynamicPayload::new(
        "pre-ack-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-pre-ack-failure.txt" }),
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

    let executor = ReplayExecutionEngine::new(replay.clone(), nats_client.clone())
        .with_scan_ack_timeout(Duration::from_millis(100));
    ReplayTelemetry::new(replay.clone()).spawn();
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
    let client = ReplayControlClient::new(&env, nats_client, Duration::from_secs(30), health);

    let mut scope = sample_scope();
    scope.node_id = "pre-ack-test".to_string();
    scope.time_window = Some((
        target_ts - time::Duration::milliseconds(1),
        target_ts + time::Duration::milliseconds(1),
    ));

    let planned = client.plan("test:replay-user".into(), scope).await?;
    let (previewed, _) = client.preview(planned.operation_id).await?;
    let approved = client
        .approve(previewed.operation_id, "admin:approver".into())
        .await?;
    let err = client
        .execute(approved.operation_id, "service:executor-node".into(), false)
        .await
        .expect_err("execute should fail before scan ack when no node responder exists");
    assert!(
        err.to_string().contains("restored archived cascade"),
        "pre-ack dispatch failures must explain that the archived cascade was restored: {err}"
    );

    let operation = replay.load_operation(approved.operation_id).await?;
    assert_eq!(operation.state, ReplayState::Failed);
    assert_eq!(operation.checkpoint.processed_events, 0);

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
        "pre-ack dispatch failures must restore the live row"
    );
    assert_eq!(
        archived_count, 0,
        "pre-ack dispatch failures must not leave the archived cascade behind"
    );

    Ok(())
}

#[sinex_test]
async fn replay_execution_fails_before_archive_when_scope_metadata_collection_fails(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let material_id = ctx
        .create_source_material(Some("replay-scope-metadata-failure"))
        .await?;
    let event = DynamicPayload::new(
        "scope-metadata-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-scope-metadata-failure.txt" }),
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
    let mut scope = sample_scope();
    scope.node_id = "scope-metadata-test".to_string();
    scope.time_window = Some((
        target_ts - time::Duration::milliseconds(1),
        target_ts + time::Duration::milliseconds(1),
    ));

    let planned = replay
        .create_operation(scope.clone(), "test:scope-metadata-fail".into())
        .await?;
    let preview = replay.generate_preview_summary(&scope).await?;
    replay.update_preview(planned.operation_id, preview).await?;
    replay
        .approve(planned.operation_id, "admin:approver".into())
        .await?;

    let executor = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client())
        .with_scope_metadata_failures(Arc::new(AtomicUsize::new(1)));
    let err = executor
        .execute(planned.operation_id, "service:executor-node".into())
        .await
        .expect_err("scope metadata collection failure should abort replay execution");
    assert!(
        err.chain().any(|cause| {
            cause
                .to_string()
                .contains("Failed to collect replay cascade scope metadata")
        }),
        "unexpected error: {err}"
    );

    let failed = replay.load_operation(planned.operation_id).await?;
    assert_eq!(failed.state, ReplayState::Failed);
    assert_eq!(
        failed.outcome,
        Some(sinex_primitives::domain::ReplayOutcome::Failed)
    );
    assert!(
        failed.error_details.as_deref().is_some_and(
            |details| details.contains("Failed to collect replay cascade scope metadata")
        ),
        "failure details should include scope metadata context: {:?}",
        failed.error_details
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
        "scope metadata failure must leave the live row untouched"
    );
    assert_eq!(
        archived_count, 0,
        "scope metadata failure must abort before archiving the cascade"
    );

    Ok(())
}

#[sinex_test]
async fn replay_execution_restores_cascade_when_initial_scope_invalidation_publish_fails(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let material_id = ctx
        .create_source_material(Some("replay-scope-invalidation-publish-failure"))
        .await?;
    let mut event = DynamicPayload::new(
        "scope-invalidation-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-scope-invalidation-publish-failure.txt" }),
    )
    .from_material(material_id)
    .build()?;
    event.scope_key = Some("scope://scope-invalidation-test/replay".to_string());
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
    let mut scope = sample_scope();
    scope.node_id = "scope-invalidation-test".to_string();
    scope.time_window = Some((
        target_ts - time::Duration::milliseconds(1),
        target_ts + time::Duration::milliseconds(1),
    ));

    let planned = replay
        .create_operation(scope.clone(), "test:scope-invalidation-fail".into())
        .await?;
    let preview = replay.generate_preview_summary(&scope).await?;
    replay.update_preview(planned.operation_id, preview).await?;
    replay
        .approve(planned.operation_id, "admin:approver".into())
        .await?;

    let mut invalidation_rx =
        spawn_invalidation_listener_for_test(&ctx.nats_client()).await?;

    let executor = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client())
        .with_scope_invalidation_publish_failures(Arc::new(AtomicUsize::new(1)));
    let err = executor
        .execute(planned.operation_id, "service:executor-node".into())
        .await
        .expect_err("scope invalidation publish failure should abort replay execution");
    assert!(
        err.chain().any(|cause| {
            cause
                .to_string()
                .contains("Failed to publish replay scope invalidations before dispatch")
        }),
        "unexpected error: {err}"
    );

    let failed = replay.load_operation(planned.operation_id).await?;
    assert_eq!(failed.state, ReplayState::Failed);
    assert_eq!(
        failed.outcome,
        Some(sinex_primitives::domain::ReplayOutcome::Failed)
    );
    assert!(
        failed.error_details.as_deref().is_some_and(|details| {
            details.contains("Failed to publish replay scope invalidations before dispatch")
        }),
        "failure details should include invalidation publish context: {:?}",
        failed.error_details
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
        "scope invalidation publish failure must restore the live row"
    );
    assert_eq!(
        archived_count, 0,
        "scope invalidation publish failure must not leave archived rows behind"
    );

    let payload_bytes = tokio::time::timeout(Duration::from_secs(1), invalidation_rx.recv())
        .await?
        .expect("compensating invalidation should still publish after restore");
    let payload = String::from_utf8(payload_bytes)?;
    assert!(payload.contains("scope://scope-invalidation-test/replay"));
    assert!(payload.contains(&target_id.to_string()));

    Ok(())
}


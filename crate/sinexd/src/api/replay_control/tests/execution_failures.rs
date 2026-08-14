use super::*;
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
    let target_event_id = inserted.id.expect("inserted replay target must have id");
    let target_id = target_event_id.to_uuid();
    let target_ts = target_event_id.timestamp().expect("test ID must be UUIDv7");

    // The non-cancel failure must recover the entire archived cascade, not
    // merely the material root. A derived child is archived alongside the
    // root before output validation begins.
    let product_class = DerivedProductClass::CanonicalDerivedEvent;
    seed_product_declaration(
        &ctx.pool,
        "sinex.test.replay_output_visibility_timeout",
        product_class,
        "replay-output-visibility-derived",
        "analytics.summary",
    )
    .await?;
    let mut derived = DynamicPayload::new(
        "replay-output-visibility-derived",
        "analytics.summary",
        json!({ "path": "/tmp/replay-output-visibility-timeout-derived.txt" }),
    )
    .from_parents([target_event_id])?
    .build()?;
    derived.product_class = Some(product_class);
    derived.claim_support = Some(sinex_primitives::derivation::ClaimSupport::unknown());
    derived.derivation_declaration_id =
        Some("sinex.test.replay_output_visibility_timeout".to_string());
    let derived_id = ctx
        .pool
        .events()
        .insert(derived)
        .await?
        .id
        .expect("inserted derived cascade member must have id")
        .to_uuid();

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();
    let env = environment();
    let (scan_command_rx, scan_handle) = spawn_fake_scan_source_runtime_with_progress(
        nats_client.clone(),
        env,
        "visibility-timeout-test",
        1,
        1,
    )
    .await?;

    let mut scope = sample_scope();
    scope.source_name = "visibility-timeout-test".to_string();
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
        .execute(planned.operation_id, "service:executor-runtime".into())
        .await
        .expect_err("missing replay outputs must fail before completion");
    assert!(
        err.to_string().contains("Replay outputs did not match"),
        "unexpected error: {err}"
    );

    let failed = replay.load_operation(planned.operation_id).await?;
    assert_eq!(failed.state, ReplayState::Failed);
    assert_eq!(
        failed.outcome,
        Some(sinex_primitives::domain::ReplayOutcome::Failed)
    );

    let cascade_ids = [target_id, derived_id];
    let live_cascade_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = ANY($1::uuid[])")
            .bind(&cascade_ids)
            .fetch_one(&ctx.pool)
            .await?;
    let archived_cascade_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = ANY($1::uuid[])",
    )
    .bind(&cascade_ids)
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(
        live_cascade_count, 2,
        "output-validation timeout must restore every archived cascade member"
    );
    assert_eq!(
        archived_cascade_count, 0,
        "output-validation timeout must not leave root or derived rows orphaned in audit storage"
    );

    let dispatched_command = scan_command_rx.await.map_err(|_| {
        test_error("fake visibility-timeout-test source runtime did not receive a scan command")
    })?;
    assert_eq!(dispatched_command.operation_id, planned.operation_id);

    await_fake_scan_source_runtime(scan_handle, "visibility-timeout-test").await?;

    Ok(())
}

#[sinex_test]
async fn replay_execution_fails_when_source_runtime_never_reports_completion(
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
        .timestamp()
        .expect("test ID must be UUIDv7");

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();
    let env = sinex_primitives::environment::environment();
    let (scan_command_rx, scan_handle) =
        spawn_fake_scan_source_runtime_ack_only(nats_client.clone(), env.clone(), "timeout-test")
            .await?;

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
    scope.source_name = "timeout-test".to_string();
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
        .execute(
            approved.operation_id,
            "service:executor-runtime".into(),
            false,
        )
        .await
        .expect_err("execute should fail when the source runtime never reports completion");
    assert!(
        err.to_string().contains("restored archived cascade"),
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
        live_count, 1,
        "timed-out replay should restore the archived cascade before reporting failure"
    );
    assert_eq!(
        archived_count, 0,
        "timed-out replay should not leave the archived cascade stranded"
    );

    let dispatched_command = scan_command_rx.await.map_err(|_| {
        test_error("fake timeout-test source runtime did not receive a scan command")
    })?;
    assert_eq!(dispatched_command.operation_id, approved.operation_id);

    await_fake_scan_source_runtime(scan_handle, "timeout-test").await?;

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
        .timestamp()
        .expect("test ID must be UUIDv7");

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();
    let env = environment();
    let (_scan_command_rx, scan_handle) = spawn_fake_scan_source_runtime_with_progress(
        nats_client.clone(),
        env,
        "checkpoint-fail-test",
        1,
        1,
    )
    .await?;

    let mut scope = sample_scope();
    scope.source_name = "checkpoint-fail-test".to_string();
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
        .execute(planned.operation_id, "service:executor-runtime".into())
        .await
        .expect_err("checkpoint persistence failure should abort replay execution");
    assert!(
        error_contains(&err, "Failed to persist replay progress checkpoint"),
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
        "checkpoint persistence failure after partial emission should restore untouched live rows"
    );
    assert_eq!(
        archived_count, 0,
        "checkpoint persistence failure after partial emission must not leave archived rows behind"
    );

    await_fake_scan_source_runtime(scan_handle, "checkpoint-fail-test").await?;

    Ok(())
}

#[sinex_test]
async fn replay_execution_fails_when_replacement_recording_fails(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let material_id = ctx
        .create_source_material(Some("replay-replacement-record-fail"))
        .await?;
    let event = DynamicPayload::new(
        "replacement-record-fail-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-replacement-record-fail.txt" }),
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
        .timestamp()
        .expect("test ID must be UUIDv7");

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();
    let env = environment();
    let (scan_command_rx, scan_handle) = spawn_fake_scan_source_runtime_with_progress(
        nats_client.clone(),
        env,
        "replacement-record-fail-test",
        1,
        1,
    )
    .await?;

    let mut scope = sample_scope();
    scope.source_name = "replacement-record-fail-test".to_string();
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
    );

    let executor = ReplayExecutionEngine::new(replay.clone(), nats_client)
        .with_replacement_record_failures(Arc::new(AtomicUsize::new(1)))
        .with_scan_completion_timeout(Duration::from_secs(5));
    let err = executor
        .execute(planned.operation_id, "service:executor-runtime".into())
        .await
        .expect_err("replacement-record failure should abort replay execution");
    assert!(
        error_contains(&err, "Failed to record replay replacement relations"),
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

    let replay_command = replay_output_handle.await.map_err(|e| {
        test_error(format!(
            "fake replacement-record replay output task failed: {e}"
        ))
    })??;

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
        live_target_count, 1,
        "replacement-record failure compensation must restore the original event"
    );
    assert_eq!(
        archived_target_count, 0,
        "replacement-record failure compensation must not strand the original event in audit storage"
    );
    assert_eq!(
        live_replacement_count, 0,
        "replacement-record failure compensation must archive the partial replay output"
    );

    let replacements = ctx
        .pool
        .events()
        .get_replacements_by_operation(planned.operation_id)
        .await?;
    assert_eq!(
        replacements.len(),
        1,
        "replacement-record failure compensation should retry and leave one complete lineage row"
    );

    await_fake_scan_source_runtime(scan_handle, "replacement-record-fail-test").await?;

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
        .timestamp()
        .expect("test ID must be UUIDv7");

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
    scope.source_name = "pre-ack-test".to_string();
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
        .execute(
            approved.operation_id,
            "service:executor-runtime".into(),
            false,
        )
        .await
        .expect_err("execute should fail before scan ack when no source responder exists");
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
        .timestamp()
        .expect("test ID must be UUIDv7");

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let mut scope = sample_scope();
    scope.source_name = "scope-metadata-test".to_string();
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
        .execute(planned.operation_id, "service:executor-runtime".into())
        .await
        .expect_err("scope metadata collection failure should abort replay execution");
    assert!(
        error_contains(&err, "Failed to collect replay cascade scope metadata"),
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
        .timestamp()
        .expect("test ID must be UUIDv7");

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let mut scope = sample_scope();
    scope.source_name = "scope-invalidation-test".to_string();
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

    let mut invalidation_rx = spawn_invalidation_listener_for_test(&ctx.nats_client()).await?;

    let executor = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client())
        .with_scope_invalidation_publish_failures(Arc::new(AtomicUsize::new(1)));
    let err = executor
        .execute(planned.operation_id, "service:executor-runtime".into())
        .await
        .expect_err("scope invalidation publish failure should abort replay execution");
    assert!(
        error_contains(
            &err,
            "Failed to publish replay scope invalidations before dispatch",
        ),
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
        .ok_or_else(|| {
            test_error("compensating invalidation should still publish after restore")
        })??;
    let payload = String::from_utf8(payload_bytes)?;
    assert!(payload.contains("scope://scope-invalidation-test/replay"));
    assert!(payload.contains(&target_id.to_string()));

    Ok(())
}

/// sinex-x47r: a source material that has lost its authoritative CAS/blob
/// backing must stop replay before the archive transaction can remove roots.
#[sinex_test]
async fn replay_rejects_unreadable_authority_before_archive(ctx: TestContext) -> Result<()> {
    use crate::runtime::content_store::{
        ContentStoreConfig, ContentStoreManager, MaterialContentStore,
    };
    use camino::Utf8PathBuf;

    let ctx = ctx.with_nats().dedicated().await?;
    let temp = tempfile::tempdir()?;
    let root = Utf8PathBuf::from_path_buf(temp.path().join("content-store"))
        .map_err(|_| test_error("temporary content-store path must be UTF-8"))?;
    let config = ContentStoreConfig {
        root_path: root,
        ..Default::default()
    };
    let authority = Arc::new(ContentStoreManager::new(
        config.clone(),
        ctx.pool.clone(),
        None,
    )?);
    let blob = authority
        .ingest_from_bytes(
            b"authority-before-archive",
            "authority-before.log",
            "text/plain",
        )
        .await?;
    let material = ctx
        .pool
        .source_materials()
        .register_material(
            sinex_db::repositories::source_materials::SourceMaterial::blob_text(
                "authority-before.log",
            )
            .with_blob_id(blob.id)
            .with_metadata(json!({
                "content_key": blob.content_key(),
                "content_hash": blob.content_hash,
                "size_bytes": blob.size_bytes,
                "storage_backend": blob.storage_backend,
            })),
        )
        .await?;
    let material_id = Id::from_uuid(material.id);
    let backing_store = MaterialContentStore::new(config)?;
    let path = backing_store
        .path_if_local(&blob.content_key())?
        .ok_or_else(|| test_error("test blob must use local CAS"))?;
    tokio::fs::remove_file(path).await?;

    let event = DynamicPayload::new(
        "authority-before-archive",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({"path": "/tmp/authority-before-archive.txt"}),
    )
    .from_material(material_id)
    .build()?;
    let inserted = ctx.pool.events().insert(event).await?;
    let event_id = inserted.id.expect("replay target must have an ID");
    let event_ts = event_id.timestamp().expect("test event ID is UUIDv7");

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let mut scope = sample_scope();
    scope.source_name = "authority-before-archive".to_string();
    scope.time_window = Some((
        event_ts - time::Duration::milliseconds(1),
        event_ts + time::Duration::milliseconds(1),
    ));
    let operation = replay
        .create_operation(scope.clone(), "test:authority-before".into())
        .await?;
    replay
        .update_preview(
            operation.operation_id,
            replay.generate_preview_summary(&scope).await?,
        )
        .await?;
    replay
        .approve(operation.operation_id, "admin:approver".into())
        .await?;

    let error = ReplayExecutionEngine::new(replay, ctx.nats_client())
        .with_material_authority(authority)
        .execute(operation.operation_id, "service:executor-runtime".into())
        .await
        .expect_err("missing authoritative bytes must reject replay before archive");
    assert!(
        error
            .to_string()
            .contains("authority validation failed before archive"),
        "unexpected error: {error}"
    );
    let live: i64 = sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1")
        .bind(event_id.to_uuid())
        .fetch_one(&ctx.pool)
        .await?;
    let archived: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1")
            .bind(event_id.to_uuid())
            .fetch_one(&ctx.pool)
            .await?;
    assert_eq!(
        live, 1,
        "authority preflight must leave the live root intact"
    );
    assert_eq!(
        archived, 0,
        "authority preflight must prevent destructive archive"
    );
    Ok(())
}

/// sinex-x47r: a matching output from another material cannot satisfy the
/// replay contract merely because it has the same source/type/anchor shape.
#[sinex_test]
async fn replay_rejects_output_from_outside_material_scope(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let expected_material = ctx
        .create_source_material(Some("scope-output-expected"))
        .await?;
    let unrelated_material = ctx
        .create_source_material(Some("scope-output-unrelated"))
        .await?;
    let event = DynamicPayload::new(
        "scope-output-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({"path": "/tmp/scope-output-expected.txt"}),
    )
    .from_material(expected_material)
    .build()?;
    let inserted = ctx.pool.events().insert(event).await?;
    let event_id = inserted.id.expect("replay target must have an ID");
    let event_ts = event_id.timestamp().expect("test event ID is UUIDv7");

    let nats = ctx.nats_client();
    let env = environment();
    let (command_rx, scan_handle) =
        spawn_fake_scan_source_runtime(nats.clone(), env, "scope-output-test", 1).await?;
    let output_pool = ctx.pool.clone();
    let output_handle = tokio::spawn(async move {
        let command = command_rx
            .await
            .map_err(|_| test_error("wrong-material fake did not receive scan command"))?;
        let mut output = DynamicPayload::new(
            "scope-output-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            json!({"path": "/tmp/scope-output-unrelated.txt"}),
        )
        .from_material(unrelated_material)
        .build()?;
        output.created_by_operation_id = Some(command.operation_id);
        output_pool.events().insert(output).await?;
        Ok::<(), SinexError>(())
    });

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let mut scope = sample_scope();
    scope.source_name = "scope-output-test".to_string();
    scope.time_window = Some((
        event_ts - time::Duration::milliseconds(1),
        event_ts + time::Duration::milliseconds(1),
    ));
    let operation = replay
        .create_operation(scope.clone(), "test:wrong-material-output".into())
        .await?;
    replay
        .update_preview(
            operation.operation_id,
            replay.generate_preview_summary(&scope).await?,
        )
        .await?;
    replay
        .approve(operation.operation_id, "admin:approver".into())
        .await?;

    let error = ReplayExecutionEngine::new(replay, nats)
        .with_scan_completion_timeout(Duration::from_millis(100))
        .execute(operation.operation_id, "service:executor-runtime".into())
        .await
        .expect_err("cross-material output must not satisfy replay validation");
    assert!(
        error
            .to_string()
            .contains("source-material occurrence scope"),
        "unexpected error: {error}"
    );
    output_handle.await??;
    await_fake_scan_source_runtime(scan_handle, "scope-output-test").await?;
    let live: i64 = sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1")
        .bind(event_id.to_uuid())
        .fetch_one(&ctx.pool)
        .await?;
    assert_eq!(
        live, 1,
        "failed output validation must restore the archived root"
    );
    Ok(())
}

use super::*;
#[sinex_test]
async fn replay_execution_records_outcome(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let (material_id, inserted) = {
        let mut i = 0;
        loop {
            let material_id = ctx.create_source_material(Some("replay-outcome")).await?;
            let event = DynamicPayload::new(
                "fs-test",
                FileCreatedPayload::EVENT_TYPE.as_static_str(),
                json!({ "path": "/tmp/replay.txt" }),
            )
            .from_material_at(material_id, i * 10)
            .build()?;
            let inserted = ctx.pool.events().insert(event).await?;
            if let Some(ts_orig) = inserted.ts_orig
                && ts_orig.inner().nanosecond() > 0
            {
                break (material_id, inserted);
            }
            i += 1;
        }
    };

    let replay_target_event_id = inserted.id.expect("inserted replay target must have id");
    let replay_target_id = replay_target_event_id.to_uuid();
    let target_window_end = replay_target_event_id.timestamp();
    let target_window_start = target_window_end - time::Duration::milliseconds(1);

    let product_class = DerivedProductClass::CanonicalDerivedEvent;
    seed_product_declaration(
        &ctx.pool,
        "sinex.test.replay_execution_records_outcome",
        product_class,
        "analytics-test",
        "analytics.summary",
    )
    .await?;
    let mut cascaded = DynamicPayload::new(
        "analytics-test",
        "analytics.summary",
        json!({ "path": "/tmp/replay-summary.txt" }),
    )
    .from_parents([replay_target_event_id])?
    .build()?;
    cascaded.product_class = Some(product_class);
    cascaded.claim_support = Some(sinex_primitives::derivation::ClaimSupport::unknown());
    cascaded.derivation_declaration_id =
        Some("sinex.test.replay_execution_records_outcome".to_string());
    let cascaded_inserted = ctx.pool.events().insert(cascaded).await?;
    let cascaded_id = cascaded_inserted
        .id
        .expect("inserted cascaded event must have id")
        .to_uuid();

    let nonmatch_material = ctx
        .create_source_material(Some("replay-outcome-nonmatch"))
        .await?;
    let nonmatch_event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-nonmatch.txt" }),
    )
    .from_material(nonmatch_material)
    .build()?;
    let inserted_nonmatch = ctx.pool.events().insert(nonmatch_event).await?;
    let nonmatch_id = inserted_nonmatch
        .id
        .expect("inserted non-matching event must have id")
        .to_uuid();

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();

    // The replay engine should no longer publish raw replay rows itself.
    // Keep a stream around so the test can assert that this count stays zero.
    let env = sinex_primitives::environment::environment();
    let js = async_nats::jetstream::new(nats_client.clone());
    let stream_name = format!("replay-test-{}", Uuid::now_v7().simple());
    js.get_or_create_stream(async_nats::jetstream::stream::Config {
        name: stream_name.clone(),
        subjects: vec![env.nats_subject("events.raw.>")],
        ..Default::default()
    })
    .await?;
    let (scan_command_rx, scan_handle) =
        spawn_fake_scan_source_runtime(nats_client.clone(), env.clone(), "fs-test", 1).await?;
    let replay_output_handle = spawn_replay_output_inserter(
        ctx.pool.clone(),
        scan_command_rx,
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        "/tmp/replay-output.txt",
    );

    let client = spawn_replay_control(replay, nats_client, Duration::from_secs(30)).await?;

    let mut scope = sample_scope();
    scope.time_window = Some((target_window_start, target_window_end));
    scope.material_filter = Some(vec![*material_id.as_uuid()]);
    scope.filters.insert(
        "event_types".to_string(),
        json!([FileCreatedPayload::EVENT_TYPE.as_static_str()]),
    );

    let planned = client
        .plan("test:replay-user".into(), scope.clone())
        .await?;
    assert_eq!(planned.state, ReplayState::Planning);

    let (previewed, preview) = client.preview(planned.operation_id).await?;
    assert_eq!(previewed.state, ReplayState::Previewed);
    assert_eq!(
        preview
            .get("total_events")
            .and_then(serde_json::Value::as_i64),
        Some(1),
        "preview should match only the filtered replay target"
    );
    assert_eq!(
        preview
            .get("replay_semantics")
            .and_then(serde_json::Value::as_str),
        Some("reexecute_material_roots_via_source_scan")
    );
    // Regression: the client-facing preview must never carry the full
    // root_event_ids array -- for a real-scale scope this is hundreds of
    // thousands of UUIDs, producing a reply payload sinexd's own
    // oversized-publish guard silently refuses to send (discovered live
    // while diagnosing sinex-60r's ActivityWatch replay). Execution below
    // still succeeding proves the FULL id list is still stored server-side
    // (state_machine.rs's approve/execute integrity checks would fail
    // loudly otherwise) -- only the wire reply to the client is trimmed.
    assert!(
        preview.get("root_event_ids").is_none(),
        "client-facing preview must not include the full root_event_ids array, got: {preview:?}"
    );
    assert_eq!(
        preview
            .get("root_event_ids_count")
            .and_then(serde_json::Value::as_u64),
        Some(1),
        "client-facing preview should surface the count in place of the full array"
    );

    let approved = client
        .approve(planned.operation_id, "admin:approver".into())
        .await?;
    assert_eq!(approved.state, ReplayState::Approved);

    let executed = client
        .execute(
            planned.operation_id,
            "service:executor-runtime".into(),
            false,
        )
        .await?;
    assert_eq!(executed.state, ReplayState::Completed);
    assert_eq!(executed.checkpoint.processed_events, 1);
    assert_eq!(executed.checkpoint.total_events, 1);
    assert_eq!(
        preview
            .get("total_events")
            .and_then(serde_json::Value::as_u64),
        Some(executed.checkpoint.total_events),
        "execute checkpoint totals must match preview totals"
    );

    assert!(
        executed.outcome.is_some(),
        "Replay execution should record a concrete outcome for automation consumers"
    );

    let dispatched_command = replay_output_handle
        .await
        .map_err(|e| test_error(format!("fake replay output task failed: {e}")))??;
    let replay_context = dispatched_command
        .args
        .replay
        .expect("gateway must populate typed replay context");
    assert_eq!(replay_context.materials.len(), 1);
    assert_eq!(
        replay_context.materials[0].source_material_id,
        *material_id.as_uuid(),
        "replay context must carry resolved source material identity"
    );
    assert_eq!(
        replay_context.replay_scope.material_ids,
        Some(vec![*material_id.as_uuid()]),
        "gateway must preserve normalized material filter in replay scope"
    );
    assert_eq!(
        replay_context.replay_scope.event_types,
        Some(vec![
            FileCreatedPayload::EVENT_TYPE.as_static_str().to_string()
        ]),
        "gateway must preserve normalized event type filter in replay scope"
    );

    use async_nats::jetstream::consumer::{
        AckPolicy, DeliverPolicy, pull::Config as ConsumerConfig,
    };
    let stream = js.get_stream(&stream_name).await?;
    let consumer_name = format!("replay-test-consumer-{}", Uuid::now_v7().simple());
    let consumer = stream
        .get_or_create_consumer(
            &consumer_name,
            ConsumerConfig {
                durable_name: Some(consumer_name.clone()),
                name: Some(consumer_name.clone()),
                deliver_policy: DeliverPolicy::All,
                ack_policy: AckPolicy::Explicit,
                filter_subject: env.nats_subject("events.raw.fs-test.file_created"),
                ..Default::default()
            },
        )
        .await?;

    let mut replay_batch = consumer
        .fetch()
        .max_messages(8)
        .expires(Duration::from_millis(100))
        .messages()
        .await?;
    let mut replay_payloads = Vec::new();
    while let Some(message) = replay_batch.next().await {
        let message = message.map_err(|e| test_error(e.to_string()))?;
        replay_payloads.push(serde_json::from_slice::<serde_json::Value>(
            &message.payload,
        )?);
        message.ack().await.map_err(|e| test_error(e.to_string()))?;
    }
    assert_eq!(
        replay_payloads.len(),
        0,
        "gateway replay must not republish stored raw rows"
    );

    let replay_target_live: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(replay_target_id)
            .fetch_one(&ctx.pool)
            .await?;
    let replay_target_archived: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(replay_target_id)
    .fetch_one(&ctx.pool)
    .await?;
    let cascaded_live: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(cascaded_id)
            .fetch_one(&ctx.pool)
            .await?;
    let cascaded_archived: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(cascaded_id)
    .fetch_one(&ctx.pool)
    .await?;
    let nonmatch_live: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(nonmatch_id)
            .fetch_one(&ctx.pool)
            .await?;
    let nonmatch_archived: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(nonmatch_id)
    .fetch_one(&ctx.pool)
    .await?;

    assert_eq!(replay_target_live, 0);
    assert_eq!(replay_target_archived, 1);
    assert_eq!(cascaded_live, 0);
    assert_eq!(cascaded_archived, 1);
    assert_eq!(nonmatch_live, 1);
    assert_eq!(nonmatch_archived, 0);

    let material_root_id = ctx
        .create_source_material(Some("replay-source-runtime-scan-parity"))
        .await?;
    let root = DynamicPayload::new(
        "reexecution-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/reexecution-root.txt" }),
    )
    .from_material(material_root_id)
    .build()?;
    let root_inserted = ctx.pool.events().insert(root).await?;
    let root_event_id = root_inserted.id.expect("reexecution root must have id");
    let root_id = root_event_id.to_uuid();
    seed_product_declaration(
        &ctx.pool,
        "sinex.test.replay_execution_records_outcome.reexecution",
        product_class,
        "reexecution-test",
        "file.derived",
    )
    .await?;
    let mut reexecution_derived = DynamicPayload::new(
        "reexecution-test",
        "file.derived",
        json!({ "path": "/tmp/reexecution-derived.txt" }),
    )
    .from_parents([root_event_id])?
    .build()?;
    reexecution_derived.product_class = Some(product_class);
    reexecution_derived.claim_support = Some(sinex_primitives::derivation::ClaimSupport::unknown());
    reexecution_derived.derivation_declaration_id =
        Some("sinex.test.replay_execution_records_outcome.reexecution".to_string());
    let derived_inserted = ctx.pool.events().insert(reexecution_derived).await?;
    let derived_id = derived_inserted
        .id
        .expect("reexecution derived must have id")
        .to_uuid();
    let reexecution_root_ts = root_event_id.timestamp();
    let reexecution_scope = ReplayScope {
        source_name: "reexecution-test".to_string(),
        time_window: Some((
            reexecution_root_ts - time::Duration::seconds(1),
            reexecution_root_ts + time::Duration::seconds(1),
        )),
        material_filter: None,
        filters: HashMap::new(),
        ..Default::default()
    };
    let planned_reexecution = client
        .plan("test:replay-user".into(), reexecution_scope)
        .await?;
    let (_, reexecution_preview) = client.preview(planned_reexecution.operation_id).await?;
    assert_eq!(
        reexecution_preview
            .get("total_events")
            .and_then(serde_json::Value::as_i64),
        Some(1),
        "preview must count only material roots for source-runtime scan replay semantics"
    );
    client
        .approve(planned_reexecution.operation_id, "admin:approver".into())
        .await?;
    let (reexecution_command_rx, reexecution_handle) =
        spawn_fake_scan_source_runtime(ctx.nats_client(), env.clone(), "reexecution-test", 1)
            .await?;
    let reexecution_output_handle = spawn_replay_output_inserter(
        ctx.pool.clone(),
        reexecution_command_rx,
        "reexecution-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        "/tmp/reexecution-root.txt",
    );
    let reexecution_executed = client
        .execute(
            planned_reexecution.operation_id,
            "service:executor-runtime".into(),
            false,
        )
        .await?;
    assert_eq!(reexecution_executed.state, ReplayState::Completed);
    assert_eq!(reexecution_executed.checkpoint.total_events, 1);
    assert_eq!(reexecution_executed.checkpoint.processed_events, 1);
    let reexecution_command = reexecution_output_handle
        .await
        .map_err(|e| test_error(format!("fake reexecution replay output task failed: {e}")))??;
    let reexecution_context = reexecution_command
        .args
        .replay
        .expect("reexecution must still carry replay context");
    assert_eq!(reexecution_context.materials.len(), 1);
    assert_eq!(
        reexecution_context.materials[0].source_material_id,
        *material_root_id.as_uuid(),
    );
    assert_eq!(
        reexecution_context.replay_scope.material_ids, None,
        "implicit replay scopes should not invent material filters"
    );
    let root_archived_after_reexecution: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(root_id)
    .fetch_one(&ctx.pool)
    .await?;
    let derived_archived_after_reexecution: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(derived_id)
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(root_archived_after_reexecution, 1);
    assert_eq!(derived_archived_after_reexecution, 1);

    await_fake_scan_source_runtime(scan_handle, "fs-test").await?;
    await_fake_scan_source_runtime(reexecution_handle, "reexecution-test").await?;

    Ok(())
}

#[sinex_test]
async fn replay_dispatch_uses_material_runtime_identity(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let material_id = ctx
        .create_source_material(Some("desktop.activitywatch"))
        .await?;
    let material_uuid = *material_id.as_uuid();
    sqlx::query!(
        r#"
        UPDATE raw.source_material_registry
        SET
            source_identifier = $2,
            metadata = jsonb_build_object('logical_source_identifier', 'desktop.activitywatch')
        WHERE id = $1::uuid
        "#,
        material_uuid,
        format!("desktop.activitywatch#material={material_uuid}"),
    )
    .execute(&ctx.pool)
    .await?;

    let event = DynamicPayload::new(
        "activitywatch",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/activitywatch-runtime-identity.txt" }),
    )
    .from_material(material_id)
    .build()?;
    let inserted = ctx.pool.events().insert(event).await?;
    let event_id = inserted.id.expect("inserted replay target must have an id");
    let execution_window = (
        event_id.timestamp() - time::Duration::milliseconds(1),
        event_id.timestamp() + time::Duration::milliseconds(1),
    );

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let env = sinex_primitives::environment::environment();
    let (command_rx, scan_handle) =
        spawn_fake_scan_source_runtime(ctx.nats_client(), env, "desktop.activitywatch", 1).await?;
    let replay_output_handle = spawn_replay_output_inserter(
        ctx.pool.clone(),
        command_rx,
        "activitywatch",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        "/tmp/activitywatch-runtime-identity.txt",
    );
    let client = spawn_replay_control(replay, ctx.nats_client(), Duration::from_secs(30)).await?;

    let scope = ReplayScope {
        source_name: "activitywatch".to_string(),
        time_window: Some(execution_window),
        material_filter: Some(vec![material_uuid]),
        filters: HashMap::from([(
            "event_types".to_string(),
            json!([FileCreatedPayload::EVENT_TYPE.as_static_str()]),
        )]),
        ..ReplayScope::default()
    };
    let planned = client.plan("test:replay-user".into(), scope).await?;
    client
        .preview(planned.operation_id)
        .await
        .expect("preview should succeed");
    client
        .approve(planned.operation_id, "admin:approver".into())
        .await?;

    let executed = client
        .execute(
            planned.operation_id,
            "service:executor-runtime".into(),
            false,
        )
        .await?;

    assert_eq!(executed.state, ReplayState::Completed);
    let command = replay_output_handle
        .await
        .map_err(|e| test_error(format!("fake replay output task failed: {e}")))??;
    assert_eq!(
        command.args.targets,
        vec!["desktop.activitywatch".to_string()],
        "replay dispatch must target the source runtime identity, not the event source"
    );
    let replay_context = command
        .args
        .replay
        .expect("dispatch command must carry replay context");
    assert_eq!(replay_context.materials.len(), 1);
    assert_eq!(
        replay_context.materials[0].source_material_id,
        material_uuid
    );
    await_fake_scan_source_runtime(scan_handle, "desktop.activitywatch").await?;

    Ok(())
}

#[sinex_test]
async fn replay_replacement_recording_follows_material_occurrence(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let engine = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client());

    let source_material = ctx
        .create_source_material(Some("replay-material-occurrence"))
        .await?;
    let old_event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-material-occurrence.txt" }),
    )
    .from_material(source_material)
    .build()?;
    let old_inserted = ctx.pool.events().insert(old_event).await?;
    let old_id = old_inserted.id.expect("old replay event must have an id");
    let execution_window = (
        old_id.timestamp() - time::Duration::milliseconds(1),
        old_id.timestamp() + time::Duration::milliseconds(1),
    );

    let mut scope = sample_scope();
    scope.time_window = Some(execution_window);

    let operation = replay
        .create_operation(scope.clone(), "test:replacement-recorder".into())
        .await?;
    let operation_id = operation.operation_id;

    ctx.pool
        .events()
        .execute_cascade_archive(
            &[old_id.to_uuid()],
            "archive old replay target",
            &operation_id.to_string(),
            "test:replacement-recorder",
        )
        .await?;

    let replacement_material = ctx
        .create_source_material(Some("replay-material-occurrence-other"))
        .await?;
    let unrelated_event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-material-occurrence-other.txt" }),
    )
    .from_material(replacement_material)
    .build()?;
    ctx.pool.events().insert(unrelated_event).await?;

    let mut replacement_event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-material-occurrence.txt" }),
    )
    .from_material(source_material)
    .build()?;
    replacement_event.created_by_operation_id = Some(operation_id);
    let replacement_inserted = ctx.pool.events().insert(replacement_event).await?;
    let replacement_id = replacement_inserted
        .id
        .expect("replacement replay event must have an id")
        .to_uuid();

    engine
        .record_event_replacements(&ctx.pool, operation_id, &[old_id.to_uuid()])
        .await?;

    let replacements = ctx
        .pool
        .events()
        .get_replacements_by_operation(operation_id)
        .await?;
    assert_eq!(replacements.len(), 1);
    assert_eq!(replacements[0].0, old_id.to_uuid());
    assert_eq!(replacements[0].1, replacement_id);
    assert_eq!(replacements[0].2, "superseded");
    assert_eq!(
        replacements[0].4, None,
        "material replay lineage must not carry derived-output slot keys"
    );

    Ok(())
}

#[sinex_test]
async fn replay_replacement_recording_rejects_cross_material_matches(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let engine = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client());

    let source_material = ctx
        .create_source_material(Some("replay-material-unmatched-old"))
        .await?;
    let old_event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-material-unmatched.txt" }),
    )
    .from_material(source_material)
    .build()?;
    let old_inserted = ctx.pool.events().insert(old_event).await?;
    let old_id = old_inserted.id.expect("old replay event must have an id");
    let execution_window = (
        old_id.timestamp() - time::Duration::milliseconds(1),
        old_id.timestamp() + time::Duration::milliseconds(1),
    );

    let mut scope = sample_scope();
    scope.time_window = Some(execution_window);

    let operation = replay
        .create_operation(scope.clone(), "test:replacement-recorder".into())
        .await?;
    let operation_id = operation.operation_id;

    ctx.pool
        .events()
        .execute_cascade_archive(
            &[old_id.to_uuid()],
            "archive old replay target",
            &operation_id.to_string(),
            "test:replacement-recorder",
        )
        .await?;

    let replacement_material = ctx
        .create_source_material(Some("replay-material-unmatched-new"))
        .await?;
    let mut replacement_event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-material-unmatched.txt" }),
    )
    .from_material(replacement_material)
    .build()?;
    replacement_event.created_by_operation_id = Some(operation_id);
    ctx.pool.events().insert(replacement_event).await?;

    engine
        .record_event_replacements(&ctx.pool, operation_id, &[old_id.to_uuid()])
        .await?;

    let replacements = ctx
        .pool
        .events()
        .get_replacements_by_operation(operation_id)
        .await?;
    assert!(
        replacements.is_empty(),
        "unmatched replay rows must not fabricate replacement lineage"
    );

    Ok(())
}

#[sinex_test]
async fn replay_anchor_payload_hash_mismatch_does_not_block_replacement(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let engine = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client());

    let source_material = ctx
        .create_source_material(Some("replay-hash-mismatch"))
        .await?;

    let hash_a: [u8; 32] = [0xAA; 32];
    let hash_b: [u8; 32] = [0xBB; 32];

    // Old event with hash A
    let old_event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({"path": "/tmp/replay-hash-mismatch.txt"}),
    )
    .from_material(source_material)
    .with_anchor_payload_hash(hash_a)
    .build()?;
    let old_inserted = ctx.pool.events().insert(old_event).await?;
    let old_id = old_inserted.id.expect("old replay event must have an id");
    let execution_window = (
        old_id.timestamp() - time::Duration::milliseconds(1),
        old_id.timestamp() + time::Duration::milliseconds(1),
    );

    let mut scope = sample_scope();
    scope.time_window = Some(execution_window);
    let operation = replay
        .create_operation(scope.clone(), "test:hash-mismatch-recorder".into())
        .await?;
    let operation_id = operation.operation_id;

    // Archive the old event
    ctx.pool
        .events()
        .execute_cascade_archive(
            &[old_id.to_uuid()],
            "archive old replay target with hash A",
            &operation_id.to_string(),
            "test:hash-mismatch-recorder",
        )
        .await?;

    // Replacement event with DIFFERENT hash (hash_b) — triggers mismatch
    let mut replacement_event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({"path": "/tmp/replay-hash-mismatch.txt"}),
    )
    .from_material(source_material)
    .with_anchor_payload_hash(hash_b)
    .build()?;
    replacement_event.created_by_operation_id = Some(operation_id);
    let replacement_inserted = ctx.pool.events().insert(replacement_event).await?;
    let replacement_id = replacement_inserted
        .id
        .expect("replacement replay event must have an id")
        .to_uuid();

    // Hash mismatch should warn but NOT block replacement recording
    engine
        .record_event_replacements(&ctx.pool, operation_id, &[old_id.to_uuid()])
        .await?;

    let replacements = ctx
        .pool
        .events()
        .get_replacements_by_operation(operation_id)
        .await?;
    assert_eq!(
        replacements.len(),
        1,
        "replacement should be recorded even when anchor_payload_hash mismatches"
    );
    assert_eq!(replacements[0].0, old_id.to_uuid());
    assert_eq!(replacements[0].1, replacement_id);
    assert_eq!(replacements[0].2, "superseded");

    Ok(())
}

#[sinex_test]
async fn replay_anchor_payload_hash_null_does_not_false_mismatch(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let engine = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client());

    let source_material = ctx.create_source_material(Some("replay-hash-null")).await?;

    // Old event with NULL hash (legacy or synthesis)
    let old_event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({"path": "/tmp/replay-hash-null.txt"}),
    )
    .from_material(source_material)
    .build()?;
    let old_inserted = ctx.pool.events().insert(old_event).await?;
    let old_id = old_inserted.id.expect("old replay event must have an id");
    let execution_window = (
        old_id.timestamp() - time::Duration::milliseconds(1),
        old_id.timestamp() + time::Duration::milliseconds(1),
    );

    let mut scope = sample_scope();
    scope.time_window = Some(execution_window);
    let operation = replay
        .create_operation(scope.clone(), "test:hash-null-recorder".into())
        .await?;
    let operation_id = operation.operation_id;

    ctx.pool
        .events()
        .execute_cascade_archive(
            &[old_id.to_uuid()],
            "archive old replay target with null hash",
            &operation_id.to_string(),
            "test:hash-null-recorder",
        )
        .await?;

    // Replacement event also with NULL hash
    let mut replacement_event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({"path": "/tmp/replay-hash-null.txt"}),
    )
    .from_material(source_material)
    .build()?;
    replacement_event.created_by_operation_id = Some(operation_id);
    let replacement_inserted = ctx.pool.events().insert(replacement_event).await?;
    let replacement_id = replacement_inserted
        .id
        .expect("replacement replay event must have an id")
        .to_uuid();

    // Both hashes are NULL — no false mismatch
    engine
        .record_event_replacements(&ctx.pool, operation_id, &[old_id.to_uuid()])
        .await?;

    let replacements = ctx
        .pool
        .events()
        .get_replacements_by_operation(operation_id)
        .await?;
    assert_eq!(replacements.len(), 1);
    assert_eq!(replacements[0].0, old_id.to_uuid());
    assert_eq!(replacements[0].1, replacement_id);

    Ok(())
}

#[sinex_test]
async fn replay_anchor_payload_hash_match_is_silent(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let engine = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client());

    let source_material = ctx
        .create_source_material(Some("replay-hash-match"))
        .await?;

    let hash: [u8; 32] = [0xCC; 32];

    // Old event with hash
    let old_event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({"path": "/tmp/replay-hash-match.txt"}),
    )
    .from_material(source_material)
    .with_anchor_payload_hash(hash)
    .build()?;
    let old_inserted = ctx.pool.events().insert(old_event).await?;
    let old_id = old_inserted.id.expect("old replay event must have an id");
    let execution_window = (
        old_id.timestamp() - time::Duration::milliseconds(1),
        old_id.timestamp() + time::Duration::milliseconds(1),
    );

    let mut scope = sample_scope();
    scope.time_window = Some(execution_window);
    let operation = replay
        .create_operation(scope.clone(), "test:hash-match-recorder".into())
        .await?;
    let operation_id = operation.operation_id;

    ctx.pool
        .events()
        .execute_cascade_archive(
            &[old_id.to_uuid()],
            "archive old replay target with hash",
            &operation_id.to_string(),
            "test:hash-match-recorder",
        )
        .await?;

    // Replacement event with SAME hash — no mismatch
    let mut replacement_event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({"path": "/tmp/replay-hash-match.txt"}),
    )
    .from_material(source_material)
    .with_anchor_payload_hash(hash)
    .build()?;
    replacement_event.created_by_operation_id = Some(operation_id);
    let replacement_inserted = ctx.pool.events().insert(replacement_event).await?;
    let replacement_id = replacement_inserted
        .id
        .expect("replacement replay event must have an id")
        .to_uuid();

    // Matching hashes — replacements recorded normally
    engine
        .record_event_replacements(&ctx.pool, operation_id, &[old_id.to_uuid()])
        .await?;

    let replacements = ctx
        .pool
        .events()
        .get_replacements_by_operation(operation_id)
        .await?;
    assert_eq!(replacements.len(), 1);
    assert_eq!(replacements[0].0, old_id.to_uuid());
    assert_eq!(replacements[0].1, replacement_id);

    Ok(())
}

/// Real behavior under test (sinex-68c.4, AC "Replay/archive scope
/// invalidation marks matching projection_registry rows stale"): running a
/// full replay operation through the real client (`plan` -> `preview` ->
/// `approve` -> `execute`, the same path `sinexctl replay execute` drives)
/// archives the target scope's cascade and, via
/// `ReplayExecutionEngine::stale_projection_registry_for_scopes`, stales any
/// `derivation.projection_registry` row keyed to that scope. Removing the
/// `stale_projection_registry_for_scopes` call from `replay_writer.rs`
/// makes this test fail: the seeded `ready` row would stay `ready` even
/// though the events it was built from were just archived out from under
/// it.
#[sinex_test]
async fn projection_registry_replay_invalidation(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let material_id = ctx
        .create_source_material(Some("replay-projection-invalidation"))
        .await?;
    let mut event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-projection-invalidation.txt" }),
    )
    .from_material(material_id)
    .build()?;
    let scope_key = format!("test-projection-scope-{}", Uuid::now_v7());
    event.scope_key = Some(scope_key.clone());
    let inserted = ctx.pool.events().insert(event).await?;
    let replay_target_event_id = inserted.id.expect("inserted replay target must have id");
    let target_window_end = replay_target_event_id.timestamp();
    let target_window_start = target_window_end - time::Duration::milliseconds(1);

    let projection_kind = "test.projection_registry_replay_invalidation";
    let registry = ctx.pool.projection_registry();
    let build_id = registry
        .begin_build(&sinex_db::repositories::ProjectionRegistrationInput {
            projection_kind,
            scope_key: &scope_key,
            semantics_version: "v1",
            input_fingerprint: "fp-test",
            coverage_window: sinex_db::repositories::ProjectionCoverageWindow {
                start: time::OffsetDateTime::now_utc() - time::Duration::hours(1),
                end: None,
            },
            freshness_class: sinex_primitives::derivation::ProjectionFreshnessClass::Hours,
            acceptable_staleness_secs: 3600,
            verification_command: "true",
        })
        .await?;
    registry.mark_ready(build_id, json!({})).await?;

    let before = registry
        .find_latest(projection_kind, &scope_key)
        .await?
        .expect("projection row must exist before replay");
    assert_eq!(before.status, "ready");

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();
    let (scan_command_rx, _scan_handle) =
        spawn_fake_scan_source_runtime(nats_client.clone(), environment(), "fs-test", 1).await?;
    let replay_output_handle = spawn_replay_output_inserter(
        ctx.pool.clone(),
        scan_command_rx,
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        "/tmp/replay-projection-invalidation-out.txt",
    );

    let client = spawn_replay_control(replay, nats_client, Duration::from_secs(30)).await?;

    let mut scope = sample_scope();
    scope.time_window = Some((target_window_start, target_window_end));
    scope.material_filter = Some(vec![*material_id.as_uuid()]);
    scope.filters.insert(
        "event_types".to_string(),
        json!([FileCreatedPayload::EVENT_TYPE.as_static_str()]),
    );

    let planned = client
        .plan("test:replay-projection-invalidation".into(), scope.clone())
        .await?;
    let (_previewed, _preview) = client.preview(planned.operation_id).await?;
    let _approved = client
        .approve(planned.operation_id, "admin:approver".into())
        .await?;
    let executed = client
        .execute(
            planned.operation_id,
            "service:executor-runtime".into(),
            false,
        )
        .await?;
    assert_eq!(executed.state, ReplayState::Completed);

    replay_output_handle
        .await
        .map_err(|e| test_error(format!("fake replay output task failed: {e}")))??;

    let after = registry
        .find_latest(projection_kind, &scope_key)
        .await?
        .expect("projection row must still exist after replay");
    assert_eq!(
        after.status, "stale",
        "replay archiving the scope's events must stale the projection registry row"
    );
    let reason = after.stale_reason.unwrap_or_default();
    assert!(
        reason.contains("replay") && reason.contains(&planned.operation_id.to_string()),
        "stale_reason should reference the replay operation, got: {reason:?}"
    );

    Ok(())
}

/// sinex-x47r: output validation must count returned rows proportionally to
/// the material roots selected for replay. A replay that archives three rows
/// from one logical source and returns one must fail validation.
#[sinex_test]
async fn output_validation_gate_passes_when_most_archived_events_never_return(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let engine = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client());

    let logical_source = "output-gate-trivial-satisfy-test";
    let material_id = ctx.create_source_material(Some(logical_source)).await?;

    // Archive 3 real events from the same logical source.
    let mut archived_ids = Vec::new();
    for i in 0..3 {
        let event = DynamicPayload::new(
            "fs-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            json!({ "path": format!("/tmp/output-gate-{i}.txt") }),
        )
        .from_material(material_id)
        .build()?;
        let inserted = ctx.pool.events().insert(event).await?;
        archived_ids.push(
            inserted
                .id
                .expect("inserted event must have an id")
                .to_uuid(),
        );
    }

    let operation = replay
        .create_operation(sample_scope(), "test:output-gate".into())
        .await?;
    let operation_id = operation.operation_id;
    ctx.pool
        .events()
        .execute_cascade_archive(
            &archived_ids,
            "archive for output-gate trivial-satisfy test",
            &operation_id.to_string(),
            "test:output-gate",
        )
        .await?;

    // The real replay only successfully re-emits ONE of the three archived
    // events (simulating a partially-failed re-scan).
    let mut replacement = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/output-gate-0.txt" }),
    )
    .from_material(material_id)
    .build()?;
    replacement.created_by_operation_id = Some(operation_id);
    ctx.pool.events().insert(replacement).await?;

    let expected = ExpectedReplayOutputs {
        minimum_visible_count: 3,
        sources: vec!["fs-test".to_string()],
        event_types: vec![FileCreatedPayload::EVENT_TYPE.as_static_str().to_string()],
        logical_source_identifiers: vec![logical_source.to_string()],
    };
    let visible = engine
        .count_visible_replay_outputs(&ctx.pool, operation_id, &expected)
        .await?;

    assert!(
        visible < expected.minimum_visible_count as i64,
        "sinex-x47r: the output-validation gate should NOT report success (visible={visible} >= \
         minimum={}) when only 1 of 3 archived events from the same logical source actually came \
         back",
        expected.minimum_visible_count
    );

    Ok(())
}

#[allow(unused_imports)] use super::*;
#[sinex_test]
async fn replay_execution_records_outcome(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let (material_id, inserted) = loop {
        let material_id = ctx.create_source_material(Some("replay-outcome")).await?;
        let event = DynamicPayload::new(
            "fs-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            json!({ "path": "/tmp/replay.txt" }),
        )
        .from_material(material_id)
        .build()?;
        let inserted = ctx.pool.events().insert(event).await?;
        if let Some(ts_orig) = inserted.ts_orig
            && ts_orig.inner().nanosecond() > 0
        {
            break (material_id, inserted);
        }
    };

    let replay_target_event_id = inserted.id.expect("inserted replay target must have id");
    let replay_target_id = replay_target_event_id.to_uuid();
    let target_window_end = replay_target_event_id.timestamp();
    let target_window_start = target_window_end - time::Duration::milliseconds(1);

    let cascaded = DynamicPayload::new(
        "analytics-test",
        "analytics.summary",
        json!({ "path": "/tmp/replay-summary.txt" }),
    )
    .from_parents([replay_target_event_id])?
    .build()?;
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
        spawn_fake_scan_node(nats_client.clone(), env.clone(), "fs-test", 1).await?;
    let replay_output_handle = spawn_replay_output_inserter(
        ctx.pool.clone(),
        scan_command_rx,
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        "/tmp/replay-output.txt",
        None,
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
        Some("reexecute_material_roots_via_node_scan")
    );

    let approved = client
        .approve(planned.operation_id, "admin:approver".into())
        .await?;
    assert_eq!(approved.state, ReplayState::Approved);

    let executed = client
        .execute(planned.operation_id, "service:executor-node".into(), false)
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
        .map_err(|e| eyre!("fake replay output task failed: {e}"))??;
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
        .expires(Duration::from_secs(2))
        .messages()
        .await?;
    let mut replay_payloads = Vec::new();
    while let Some(message) = replay_batch.next().await {
        let message = message.map_err(|e| eyre!(e.to_string()))?;
        replay_payloads.push(serde_json::from_slice::<serde_json::Value>(
            &message.payload,
        )?);
        message.ack().await.map_err(|e| eyre!(e.to_string()))?;
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
        .create_source_material(Some("replay-node-scan-parity"))
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
    let reexecution_derived = DynamicPayload::new(
        "reexecution-test",
        "file.derived",
        json!({ "path": "/tmp/reexecution-derived.txt" }),
    )
    .from_parents([root_event_id])?
    .build()?;
    let derived_inserted = ctx.pool.events().insert(reexecution_derived).await?;
    let derived_id = derived_inserted
        .id
        .expect("reexecution derived must have id")
        .to_uuid();
    let reexecution_root_ts = root_event_id.timestamp();
    let reexecution_scope = ReplayScope {
        node_id: "reexecution-test".to_string(),
        time_window: Some((
            reexecution_root_ts - time::Duration::seconds(1),
            reexecution_root_ts + time::Duration::seconds(1),
        )),
        material_filter: None,
        filters: HashMap::new(),
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
        "preview must count only material roots for node-scan replay semantics"
    );
    client
        .approve(planned_reexecution.operation_id, "admin:approver".into())
        .await?;
    let (reexecution_command_rx, reexecution_handle) =
        spawn_fake_scan_node(ctx.nats_client(), env.clone(), "reexecution-test", 1).await?;
    let reexecution_output_handle = spawn_replay_output_inserter(
        ctx.pool.clone(),
        reexecution_command_rx,
        "reexecution-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        "/tmp/reexecution-root.txt",
        None,
    );
    let reexecution_executed = client
        .execute(
            planned_reexecution.operation_id,
            "service:executor-node".into(),
            false,
        )
        .await?;
    assert_eq!(reexecution_executed.state, ReplayState::Completed);
    assert_eq!(reexecution_executed.checkpoint.total_events, 1);
    assert_eq!(reexecution_executed.checkpoint.processed_events, 1);
    let reexecution_command = reexecution_output_handle
        .await
        .map_err(|e| eyre!("fake reexecution replay output task failed: {e}"))??;
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

    scan_handle
        .await
        .map_err(|e| eyre!("fake fs-test node task failed: {e}"))?;
    reexecution_handle
        .await
        .map_err(|e| eyre!("fake reexecution-test node task failed: {e}"))?;

    Ok(())
}

#[sinex_test]
async fn replay_replacement_recording_follows_operation_outputs(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let engine = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client());

    let source_material = ctx
        .create_source_material(Some("replay-replacement-old"))
        .await?;
    let mut old_event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-replacement-old.txt" }),
    )
    .from_material(source_material)
    .build()?;
    old_event.equivalence_key = Some("replacement-eq".to_string());
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
        .create_source_material(Some("replay-replacement-new"))
        .await?;
    let mut replacement_event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-replacement-new.txt" }),
    )
    .from_material(replacement_material)
    .build()?;
    replacement_event.equivalence_key = Some("replacement-eq".to_string());
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

    Ok(())
}

#[sinex_test]
async fn replay_replacement_recording_skips_unmatched_old_events(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let engine = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client());

    let source_material = ctx
        .create_source_material(Some("replay-replacement-unmatched-old"))
        .await?;
    let mut old_event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-replacement-unmatched-old.txt" }),
    )
    .from_material(source_material)
    .build()?;
    old_event.equivalence_key = Some("old-eq".to_string());
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
        .create_source_material(Some("replay-replacement-unmatched-new"))
        .await?;
    let mut replacement_event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-replacement-unmatched-new.txt" }),
    )
    .from_material(replacement_material)
    .build()?;
    replacement_event.equivalence_key = Some("new-eq".to_string());
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


#[allow(unused_imports)] use super::*;
#[sinex_test]
async fn replay_execution_surfaces_operation_state_corruption_after_failure(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let nats_url = ctx.nats_handle()?.client_url().to_string();

    let material_id = ctx
        .create_source_material(Some("replay-corrupt-failure"))
        .await?;
    let event = DynamicPayload::new(
        "corrupt-failure-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-corrupt-failure.txt" }),
    )
    .from_material(material_id)
    .build()?;
    ctx.pool.events().insert(event).await?;

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();
    let env = sinex_primitives::environment::environment();
    let (_scan_command_rx, scan_handle) =
        spawn_fake_scan_node_ack_only(nats_client.clone(), env.clone(), "corrupt-failure-test")
            .await?;

    let executor = ReplayExecutionEngine::new(replay.clone(), nats_client.clone())
        .with_scan_completion_timeout(Duration::from_millis(200));
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

    let control_client = ReplayControlClient::new(
        &env,
        async_nats::connect(&nats_url).await?,
        Duration::from_secs(30),
        Arc::clone(&health),
    );
    let execute_client = ReplayControlClient::new(
        &env,
        async_nats::connect(&nats_url).await?,
        Duration::from_secs(30),
        health,
    );

    let mut scope = sample_scope();
    scope.node_id = "corrupt-failure-test".to_string();

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

    wait_for_operation_state(&replay, operation_id, ReplayState::Executing).await?;
    corrupt_operation_preview_summary(&ctx.pool, operation_id).await?;

    let err = execute_task
        .await
        .map_err(|e| eyre!("execute task failed: {e}"))?
        .expect_err("corrupt replay metadata should surface as execution failure");
    assert!(
        err.to_string()
            .contains("failed to finalize replay execution bookkeeping"),
        "unexpected error: {err:#}"
    );
    assert!(
        err.to_string()
            .contains("failed to inspect replay operation state after execution"),
        "unexpected error: {err:#}"
    );

    scan_handle
        .await
        .map_err(|e| eyre!("fake corrupt-failure-test node task failed: {e}"))?;

    Ok(())
}

#[sinex_test]
async fn replay_execution_surfaces_cancellation_bookkeeping_corruption(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let nats_url = ctx.nats_handle()?.client_url().to_string();

    let material_id = ctx
        .create_source_material(Some("replay-corrupt-cancel"))
        .await?;
    let event = DynamicPayload::new(
        "corrupt-cancel-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-corrupt-cancel.txt" }),
    )
    .from_material(material_id)
    .build()?;
    let inserted = ctx.pool.events().insert(event).await?;
    let target_ts = inserted
        .id
        .expect("inserted replay target must have id")
        .timestamp();

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();
    let env = sinex_primitives::environment::environment();
    let (_scan_command_rx, scan_handle) =
        spawn_fake_scan_node_ack_only(nats_client.clone(), env.clone(), "corrupt-cancel-test")
            .await?;

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
    scope.node_id = "corrupt-cancel-test".to_string();
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

    wait_for_operation_state(&replay, operation_id, ReplayState::Executing).await?;

    let cancellation_requested = control_client
        .cancel(
            operation_id,
            "admin:approver".into(),
            Some("operator requested stop".to_string()),
        )
        .await?;
    assert_eq!(cancellation_requested.state, ReplayState::Cancelling);

    corrupt_operation_preview_summary(&ctx.pool, operation_id).await?;

    let err = execute_task
        .await
        .map_err(|e| eyre!("execute task failed: {e}"))?
        .expect_err(
            "corrupt replay metadata should surface as cancellation bookkeeping failure",
        );
    assert!(
        err.to_string()
            .contains("failed to finalize replay execution bookkeeping"),
        "unexpected error: {err:#}"
    );
    assert!(
        err.to_string()
            .contains("failed to inspect replay operation state after execution"),
        "unexpected error: {err:#}"
    );

    scan_handle
        .await
        .map_err(|e| eyre!("fake corrupt-cancel-test node task failed: {e}"))?;

    Ok(())
}


#[sinex_test]
async fn replay_list_rejects_missing_operations_payload(_ctx: TestContext) -> Result<()> {
    let err = ReplayControlClient::require_operations(ReplayControlResponse::success(
        None, None, None,
    ))
    .expect_err("list responses without operations must be rejected");
    assert!(
        err.to_string()
            .contains("Replay control response missing operations")
    );
    Ok(())
}

#[sinex_test]
async fn plan_rejects_invalid_actor(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();
    let client = spawn_replay_control(replay, nats_client, Duration::from_secs(30)).await?;

    let scope = sample_scope();
    let result = client.plan("invalid-actor".into(), scope).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Invalid actor"));
    Ok(())
}

#[sinex_test]
async fn plan_rejects_inverted_time_window(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();
    let client = spawn_replay_control(replay, nats_client, Duration::from_secs(30)).await?;

    let end = Timestamp::now();
    let start = end + time::Duration::hours(1);
    let mut scope = sample_scope();
    scope.time_window = Some((start, end));

    let result = client.plan("test:replay-user".into(), scope).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("invalid replay time_window")
    );
    Ok(())
}

//! Runtime-side `NodeRunner<T>` tests: drain bridge under live traffic,
//! signal/watch shutdown channel behaviour, leader-standby coordination,
//! resubscribing listener retries, and shutdown error collapse.

use super::*;

#[cfg(feature = "messaging")]
#[sinex_test]
async fn run_service_drain_finishes_inflight_automaton_batch_and_emits_completion(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let client = ctx.nats_client();
    ensure_default_bridge_streams(&client).await?;

    let transport = EventTransport::Nats(Arc::new(NatsPublisher::new(client.clone())));
    let work_dir = tempdir()?;

    let node = DrainBridgeTestNode::default();
    let processing_started = node.processing_started.clone();
    let release_processing = node.release_processing.clone();
    let processed_event_ids = node.processed_event_ids.clone();

    let mut runner = NodeRunner::new(node);
    runner
        .initialize_with_transport(
            "runtime-drain-automaton-service".to_string(),
            HashMap::new(),
            None,
            transport,
            work_dir.path().to_path_buf(),
            false,
        )
        .await?;

    let runtime = runner
        .runtime_state()
        .ok_or_else(|| color_eyre::eyre::eyre!("runtime state missing after init"))?;
    let control_identity = runtime.control_identity().to_string();
    let drain_controller = runtime.runtime_drain();
    let checkpoint_manager = runtime.checkpoint_manager();
    let drain_complete_subject = sinex_primitives::environment().nats_subject(&format!(
        "sinex.control.nodes.{control_identity}.drain_complete"
    ));
    let mut drain_complete_sub = client.subscribe(drain_complete_subject).await?;

    let run_handle = tokio::spawn(async move { runner.run_service().await });

    let event_id = Uuid::now_v7();
    let event = runtime_test_material_event(
        event_id,
        "runtime-test-source",
        "runtime.test.input",
        serde_json::json!({"value": "drain"}),
    )?;
    publish_confirmed_raw_event(&client, &event).await?;

    tokio::time::timeout(Duration::from_secs(3), processing_started.notified())
        .await
        .map_err(|_| color_eyre::eyre::eyre!("automaton batch did not start"))?;

    request_drain_until_applied(
        &client,
        &control_identity,
        &drain_controller,
        Some("test drain"),
    )
    .await?;

    release_processing.notify_one();

    let drain_complete =
        tokio::time::timeout(Duration::from_secs(3), drain_complete_sub.next())
            .await
            .map_err(|_| color_eyre::eyre::eyre!("automaton drain_complete was not published"))?
            .ok_or_else(|| color_eyre::eyre::eyre!("drain_complete subscription closed"))?;
    let payload: NodeDrainComplete = serde_json::from_slice(&drain_complete.payload)?;

    let run_result = tokio::time::timeout(Duration::from_secs(3), run_handle)
        .await
        .map_err(|_| color_eyre::eyre::eyre!("drained automaton service did not exit"))?;
    run_result??;

    assert_eq!(processed_event_ids.lock().await.as_slice(), &[event_id]);

    let saved = checkpoint_manager.load_checkpoint().await?;
    let expected_checkpoint = Checkpoint::internal(event_id, 1);
    assert_eq!(saved.checkpoint, expected_checkpoint);
    assert_eq!(payload.node_name, control_identity);
    assert_eq!(
        payload.checkpoint.as_deref(),
        Some(expected_checkpoint.description().as_str())
    );
    Ok(())
}

#[sinex_test]
async fn signal_shutdown_channel_reports_dropped_receiver() -> TestResult<()> {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    drop(rx);

    assert!(!NodeRunner::<RuntimeTestNode>::signal_shutdown_channel(
        tx,
        "heartbeat"
    ));
    Ok(())
}

#[sinex_test]
async fn signal_shutdown_channel_delivers_to_receiver() -> TestResult<()> {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    assert!(NodeRunner::<RuntimeTestNode>::signal_shutdown_channel(
        tx,
        "heartbeat"
    ));
    rx.await?;
    Ok(())
}

#[sinex_test]
async fn signal_watch_shutdown_reports_dropped_receiver() -> TestResult<()> {
    let (tx, rx) = tokio::sync::watch::channel(false);
    drop(rx);

    assert!(!NodeRunner::<RuntimeTestNode>::signal_watch_shutdown(
        tx, "listener"
    ));
    Ok(())
}

#[sinex_test]
async fn signal_watch_shutdown_delivers_to_receiver() -> TestResult<()> {
    let (tx, mut rx) = tokio::sync::watch::channel(false);

    assert!(NodeRunner::<RuntimeTestNode>::signal_watch_shutdown(
        tx, "listener"
    ));
    rx.changed().await?;
    assert!(*rx.borrow());
    Ok(())
}

#[cfg(feature = "messaging")]
#[sinex_test]
async fn acquire_leader_standby_waits_for_existing_leader_release(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let transport =
        EventTransport::Nats(Arc::new(crate::NatsPublisher::new(ctx.nats_client())));
    let mut runner = NodeRunner::new(RuntimeTestNode);
    runner
        .initialize_with_transport(
            "runtime-standby-test".to_string(),
            HashMap::new(),
            Some(ctx.pool().clone()),
            transport,
            std::env::temp_dir(),
            false,
        )
        .await?;

    let runtime = runner
        .runtime_state()
        .ok_or_else(|| color_eyre::eyre::eyre!("runtime state missing after init"))?;
    let nats_client = runtime
        .nats_client()
        .ok_or_else(|| color_eyre::eyre::eyre!("nats client missing after init"))?;
    let js = async_nats::jetstream::new(nats_client.clone());
    let kv_client = sinex_primitives::coordination::CoordinationKvClient::new(
        js,
        runtime.service_info().service_name().to_string(),
    );

    kv_client.acquire_leadership("existing-leader").await?;

    let runner = Arc::new(tokio::sync::Mutex::new(runner));
    let acquired = Arc::new(AtomicBool::new(false));
    let runner_task = runner.clone();
    let acquired_task = acquired.clone();

    let wait_handle = tokio::spawn(async move {
        let mut guard = runner_task.lock().await;
        guard.acquire_leader_standby().await?;
        acquired_task.store(true, Ordering::SeqCst);
        Ok::<(), SinexError>(())
    });

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !acquired.load(Ordering::SeqCst),
        "standby runner should wait while another instance holds leadership"
    );

    kv_client.release_leadership("existing-leader").await?;
    let _ = tokio::time::timeout(Duration::from_secs(6), wait_handle).await??;
    assert!(
        acquired.load(Ordering::SeqCst),
        "runner should acquire leadership after the prior leader releases it"
    );

    runner.lock().await.shutdown_leader_state().await?;
    Ok(())
}

#[sinex_test]
async fn shutdown_join_result_rejects_panicked_tasks() -> TestResult<()> {
    let handle = tokio::spawn(async {
        panic!("runtime panic");
    });

    let error =
        NodeRunner::<RuntimeTestNode>::shutdown_join_result("runtime-task", handle.await)
            .expect_err("panicked runtime tasks must fail shutdown honestly");
    let message = format!("{error:#}");
    assert!(message.contains("Task failed during shutdown"));
    assert!(message.contains("runtime-task"));
    Ok(())
}

#[sinex_test]
async fn run_resubscribing_listener_retries_after_subscribe_error() -> TestResult<()> {
    let subscribe_attempts = Arc::new(AtomicU64::new(0));
    let handled_subscriptions = Arc::new(AtomicU64::new(0));
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    run_resubscribing_listener(
        "test listener",
        "sinex.test.subject",
        Duration::from_millis(1),
        shutdown_rx,
        {
            let subscribe_attempts = subscribe_attempts.clone();
            move || {
                let subscribe_attempts = subscribe_attempts.clone();
                async move {
                    let attempt = subscribe_attempts.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        Err(SinexError::processing("subscribe failed".to_string()))
                    } else {
                        Ok("subscription")
                    }
                }
            }
        },
        {
            let handled_subscriptions = handled_subscriptions.clone();
            move |subscription| {
                let handled_subscriptions = handled_subscriptions.clone();
                async move {
                    assert_eq!(subscription, "subscription");
                    handled_subscriptions.fetch_add(1, Ordering::SeqCst);
                    false
                }
            }
        },
    )
    .await;

    assert_eq!(subscribe_attempts.load(Ordering::SeqCst), 2);
    assert_eq!(handled_subscriptions.load(Ordering::SeqCst), 1);
    Ok(())
}

#[sinex_test]
async fn run_resubscribing_listener_retries_after_subscription_exit() -> TestResult<()> {
    let subscribe_attempts = Arc::new(AtomicU64::new(0));
    let handled_subscriptions = Arc::new(AtomicU64::new(0));
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    run_resubscribing_listener(
        "test listener",
        "sinex.test.subject",
        Duration::from_millis(1),
        shutdown_rx,
        {
            let subscribe_attempts = subscribe_attempts.clone();
            move || {
                let subscribe_attempts = subscribe_attempts.clone();
                async move {
                    let attempt = subscribe_attempts.fetch_add(1, Ordering::SeqCst);
                    Ok::<u64, SinexError>(attempt)
                }
            }
        },
        {
            let handled_subscriptions = handled_subscriptions.clone();
            move |_subscription| {
                let handled_subscriptions = handled_subscriptions.clone();
                async move {
                    let handled = handled_subscriptions.fetch_add(1, Ordering::SeqCst);
                    handled == 0
                }
            }
        },
    )
    .await;

    assert_eq!(subscribe_attempts.load(Ordering::SeqCst), 2);
    assert_eq!(handled_subscriptions.load(Ordering::SeqCst), 2);
    Ok(())
}

#[sinex_test]
async fn run_resubscribing_listener_stops_after_shutdown_signal() -> TestResult<()> {
    let subscribe_attempts = Arc::new(AtomicU64::new(0));
    let handled_subscriptions = Arc::new(AtomicU64::new(0));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handler_shutdown_tx = shutdown_tx.clone();

    let listener = tokio::spawn({
        let subscribe_attempts = subscribe_attempts.clone();
        let handled_subscriptions = handled_subscriptions.clone();
        async move {
            run_resubscribing_listener(
                "test listener",
                "sinex.test.subject",
                Duration::from_secs(1),
                shutdown_rx,
                move || {
                    let subscribe_attempts = subscribe_attempts.clone();
                    async move {
                        subscribe_attempts.fetch_add(1, Ordering::SeqCst);
                        Ok::<&'static str, SinexError>("subscription")
                    }
                },
                move |_subscription| {
                    let handled_subscriptions = handled_subscriptions.clone();
                    let mut shutdown_rx = handler_shutdown_tx.subscribe();
                    async move {
                        handled_subscriptions.fetch_add(1, Ordering::SeqCst);
                        shutdown_rx.changed().await.ok();
                        false
                    }
                },
            )
            .await;
        }
    });

    tokio::task::yield_now().await;
    shutdown_tx.send(true)?;
    tokio::time::timeout(Duration::from_secs(1), listener).await??;

    assert_eq!(subscribe_attempts.load(Ordering::SeqCst), 1);
    assert_eq!(handled_subscriptions.load(Ordering::SeqCst), 1);
    Ok(())
}

#[sinex_test]
async fn event_batcher_shutdown_result_rejects_join_panics() -> TestResult<()> {
    let handle = tokio::spawn(async move {
        panic!("batcher panic");
        #[allow(unreachable_code)]
        Ok::<(), SinexError>(())
    });

    let error = NodeRunner::<RuntimeTestNode>::event_batcher_shutdown_result(handle.await)
        .expect_err("panicked batcher tasks must fail shutdown honestly");
    let message = format!("{error:#}");
    assert!(message.contains("Event batcher failed during shutdown"));
    Ok(())
}

#[sinex_test]
async fn shutdown_task_waits_for_watch_signalled_exit() -> TestResult<()> {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let finished = Arc::new(AtomicBool::new(false));
    let finished_clone = finished.clone();
    let task = tokio::spawn(async move {
        shutdown_rx.changed().await.ok();
        finished_clone.store(true, Ordering::SeqCst);
    });

    let mut task = Some(task);
    NodeRunner::<RuntimeTestNode>::shutdown_task(&mut task, Some(shutdown_tx), "listener")
        .await?;

    assert!(finished.load(Ordering::SeqCst));
    assert!(task.is_none());
    Ok(())
}

#[sinex_test]
async fn collapse_shutdown_errors_preserves_additional_failures() -> TestResult<()> {
    let error = NodeRunner::<RuntimeTestNode>::collapse_shutdown_errors(vec![
        (
            "heartbeat".to_string(),
            SinexError::processing("primary shutdown failure"),
        ),
        (
            "event batcher".to_string(),
            SinexError::processing("secondary shutdown failure"),
        ),
    ])
    .expect_err("multiple shutdown failures must stay visible");
    let message = format!("{error:#}");
    assert!(message.contains("primary shutdown failure"));
    assert!(message.contains("event batcher"));
    assert!(message.contains("secondary shutdown failure"));
    Ok(())
}

#[sinex_test]
async fn shutdown_marks_runner_failed_when_cleanup_errors() -> TestResult<()> {
    let mut runner = NodeRunner::new(FailingShutdownNode);
    runner.lifecycle = RunnerLifecycle::Initialized;

    let error = runner
        .shutdown()
        .await
        .expect_err("failing shutdowns must surface as errors");

    assert!(error.to_string().contains("node shutdown failed"));
    assert_eq!(runner.lifecycle(), RunnerLifecycle::ShutdownFailed);
    Ok(())
}

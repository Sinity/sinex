//! Runtime-side `RuntimeRunner` tests: drain bridge under live traffic,
//! signal/watch shutdown channel behaviour,
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

    let module = DrainBridgeTestModule::default();
    let processing_started = module.processing_started.clone();
    let release_processing = module.release_processing.clone();
    let processed_event_ids = module.processed_event_ids.clone();

    let mut runner = RuntimeRunner::new(module);
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
        "sinex.control.sources.{control_identity}.drain_complete"
    ));
    let mut drain_complete_sub = client.subscribe(drain_complete_subject).await?;

    // sinex-li78: the bridge's confirmed-event consumer uses
    // `DeliverPolicy::New`, which only sees messages published after the
    // durable JetStream consumer is created. In production that race is
    // closed by the mandatory historical DB catch-up scan every bridge-backed
    // automaton runs first (a confirmed event is only ever published after
    // its row is already durably persisted, so the scan sees anything the
    // live tail could otherwise miss). `DrainBridgeTestModule::scan()` is a
    // stub with no backing store, so it can't replicate that guarantee here —
    // grab the consumer-ready signal before spawning `run_service()` and wait
    // on it before publishing, instead of racing consumer creation.
    let consumer_ready = runner
        .take_confirmed_consumer_ready()
        .ok_or_else(|| color_eyre::eyre::eyre!("confirmed-consumer-ready receiver already taken"))?;
    let run_handle = tokio::spawn(async move { runner.run_service().await });

    tokio::time::timeout(Duration::from_secs(3), consumer_ready)
        .await
        .map_err(|_| color_eyre::eyre::eyre!("automaton confirmed-event consumer did not become ready"))?
        .map_err(|_| color_eyre::eyre::eyre!("confirmed-consumer-ready sender dropped before signalling"))?;

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

    let drain_complete = tokio::time::timeout(Duration::from_secs(3), drain_complete_sub.next())
        .await
        .map_err(|_| color_eyre::eyre::eyre!("automaton drain_complete was not published"))?
        .ok_or_else(|| color_eyre::eyre::eyre!("drain_complete subscription closed"))?;
    let payload: RuntimeDrainComplete = serde_json::from_slice(&drain_complete.payload)?;

    let run_result = tokio::time::timeout(Duration::from_secs(3), run_handle)
        .await
        .map_err(|_| color_eyre::eyre::eyre!("drained automaton service did not exit"))?;
    run_result??;

    assert_eq!(processed_event_ids.lock().await.as_slice(), &[event_id]);

    let saved = checkpoint_manager.load_checkpoint().await?;
    let expected_checkpoint = Checkpoint::internal(event_id, 1);
    assert_eq!(saved.checkpoint, expected_checkpoint);
    assert_eq!(payload.module_name, control_identity);
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

    assert!(!RuntimeRunner::signal_shutdown_channel(tx, "heartbeat"));
    Ok(())
}

#[sinex_test]
async fn signal_shutdown_channel_delivers_to_receiver() -> TestResult<()> {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    assert!(RuntimeRunner::signal_shutdown_channel(tx, "heartbeat"));
    rx.await?;
    Ok(())
}

#[sinex_test]
async fn signal_watch_shutdown_reports_dropped_receiver() -> TestResult<()> {
    let (tx, rx) = tokio::sync::watch::channel(false);
    drop(rx);

    assert!(!RuntimeRunner::signal_watch_shutdown(tx, "listener"));
    Ok(())
}

#[sinex_test]
async fn signal_watch_shutdown_delivers_to_receiver() -> TestResult<()> {
    let (tx, mut rx) = tokio::sync::watch::channel(false);

    assert!(RuntimeRunner::signal_watch_shutdown(tx, "listener"));
    rx.changed().await?;
    assert!(*rx.borrow());
    Ok(())
}

#[sinex_test]
async fn shutdown_join_result_rejects_panicked_tasks() -> TestResult<()> {
    let handle = tokio::spawn(async {
        panic!("runtime panic");
    });

    let error = RuntimeRunner::shutdown_join_result("runtime-task", handle.await)
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

    tokio::time::timeout(Duration::from_secs(1), async {
        while handled_subscriptions.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| color_eyre::eyre::eyre!("listener did not handle initial subscription"))?;
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

    let error = RuntimeRunner::event_batcher_shutdown_result(handle.await)
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
    RuntimeRunner::shutdown_task(&mut task, Some(shutdown_tx), "listener").await?;

    assert!(finished.load(Ordering::SeqCst));
    assert!(task.is_none());
    Ok(())
}

#[sinex_test]
async fn collapse_shutdown_errors_preserves_additional_failures() -> TestResult<()> {
    let error = RuntimeRunner::collapse_shutdown_errors(vec![
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

/// `sinex-q102`: the schema listener and checkpoint-cleanup background
/// tasks are started BEFORE `module.initialize()` runs inside
/// `initialize_with_transport`. On a failed module init, the function
/// returns early without shutting either down -- dropping the `JoinHandle`
/// values doesn't cancel the detached tokio tasks, so a malformed module
/// config or init panic leaves stale NATS subscriptions/KV cleanup loops
/// running indefinitely.
#[cfg(feature = "messaging")]
#[sinex_test]
#[ignore = "sinex-q102 open: checkpoint-cleanup/schema-listener tasks leak on failed module init -- fails until fixed"]
async fn initialize_with_transport_shuts_down_background_tasks_on_failed_module_init(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let client = ctx.nats_client();
    ensure_default_bridge_streams(&client).await?;

    let mut env_guard = EnvGuard::with_keys(&["SINEX_CHECKPOINT_CLEANUP_ENABLED"]);
    env_guard.set("SINEX_CHECKPOINT_CLEANUP_ENABLED", "true");

    let transport = EventTransport::Nats(Arc::new(NatsPublisher::new(client.clone())));
    let work_dir = tempdir()?;

    let mut runner = RuntimeRunner::new(FailingInitModule);
    let init_result = runner
        .initialize_with_transport(
            "runtime-failing-init-service".to_string(),
            HashMap::new(),
            None,
            transport,
            work_dir.path().to_path_buf(),
            false,
        )
        .await;

    assert!(
        init_result.is_err(),
        "FailingInitModule::initialize must fail for this test to be meaningful"
    );

    let schema_listener_leaked = runner
        .schema_listener_handle
        .as_ref()
        .is_some_and(|handle| !handle.is_finished());
    let checkpoint_cleanup_leaked = runner
        .checkpoint_cleanup_handle
        .as_ref()
        .is_some_and(|handle| !handle.is_finished());

    assert!(
        !schema_listener_leaked,
        "schema listener task must be shut down when module.initialize() fails, not left \
         running detached from the runner that spawned it"
    );
    assert!(
        !checkpoint_cleanup_leaked,
        "checkpoint-cleanup task must be shut down when module.initialize() fails, not left \
         running detached from the runner that spawned it"
    );

    Ok(())
}

#[sinex_test]
async fn shutdown_marks_runner_failed_when_cleanup_errors() -> TestResult<()> {
    let mut runner = RuntimeRunner::new(FailingShutdownModule);
    runner.lifecycle = RunnerLifecycle::Initialized;

    let error = runner
        .shutdown()
        .await
        .expect_err("failing shutdowns must surface as errors");

    assert!(error.to_string().contains("module shutdown failed"));
    assert_eq!(runner.lifecycle(), RunnerLifecycle::ShutdownFailed);
    Ok(())
}

use super::*;
use sinex_primitives::Uuid;
use xtask::sandbox::prelude::*;
use xtask::sandbox::sinex_test;

fn test_service() -> IngestService {
    IngestService {
        config: EventEngineConfig::builder().build(),
        db_pool: None,
        nats_client: None,
        jetstream: None,
        validator: Arc::new(RwLock::new(IngestEventValidator::new(false))),
        observer: Arc::new(SelfObserver::disabled()),
        shutdown_flag: Arc::new(AtomicBool::new(false)),
        shutdown_notify: Arc::new(tokio::sync::Notify::new()),
        runtime_failure_flag: Arc::new(AtomicBool::new(false)),
        task_handles: Arc::new(Mutex::new(Vec::new())),
        heartbeat_counter_handle: None,
    }
}

#[sinex_test]
async fn wait_for_tasks_aborts_hung_tasks_before_shutdown() -> xtask::sandbox::TestResult<()> {
    struct CancelFlag(Arc<AtomicBool>);

    impl Drop for CancelFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let service = test_service();
    let cancelled = Arc::new(AtomicBool::new(false));

    let handle_cancelled = cancelled.clone();
    let handle = tokio::spawn(async move {
        let _guard = CancelFlag(handle_cancelled);
        tokio::time::sleep(Duration::from_secs(5)).await;
    });

    service.task_handles.lock().await.push(handle);

    let error = service
        .wait_for_tasks(Duration::from_millis(10))
        .await
        .expect_err("hung background tasks must fail shutdown honestly");

    assert!(cancelled.load(Ordering::SeqCst));
    assert!(
        error
            .to_string()
            .contains("timed out waiting for 1 background tasks")
    );
    Ok(())
}

#[sinex_test]
async fn log_aborted_task_shutdown_result_accepts_clean_exit() -> xtask::sandbox::TestResult<()>
{
    let handle = tokio::spawn(async {});
    let error = IngestService::log_aborted_task_shutdown_result(0, handle.await);
    assert!(error.is_none());
    Ok(())
}

#[sinex_test]
async fn log_aborted_task_shutdown_result_accepts_cancelled_task()
-> xtask::sandbox::TestResult<()> {
    let handle = tokio::spawn(async {
        tokio::time::sleep(Duration::from_secs(30)).await;
    });
    handle.abort();
    let error = IngestService::log_aborted_task_shutdown_result(1, handle.await);
    assert!(error.is_none());
    Ok(())
}

#[sinex_test]
async fn log_aborted_task_shutdown_result_rejects_panicked_task()
-> xtask::sandbox::TestResult<()> {
    let handle = tokio::spawn(async {
        panic!("event_engine background task panic");
    });
    let error = IngestService::log_aborted_task_shutdown_result(2, handle.await)
        .expect("panicked background task must stay visible");
    assert!(
        error
            .to_string()
            .contains("background task join failed during shutdown")
    );
    Ok(())
}

#[sinex_test]
async fn wait_for_tasks_rejects_panicked_background_task() -> xtask::sandbox::TestResult<()> {
    let service = test_service();
    service.task_handles.lock().await.push(tokio::spawn(async {
        panic!("background task exploded");
    }));

    let error = service
        .wait_for_tasks(Duration::from_secs(1))
        .await
        .expect_err("panicked background task must fail shutdown");

    assert!(
        error
            .to_string()
            .contains("background task join failed during shutdown")
    );
    Ok(())
}

#[sinex_test]
async fn shutdown_surfaces_background_task_failures() -> xtask::sandbox::TestResult<()> {
    let mut service = test_service();
    service.task_handles.lock().await.push(tokio::spawn(async {
        panic!("shutdown background task panic");
    }));

    let error = service
        .shutdown()
        .await
        .expect_err("shutdown must fail when background tasks panic");

    assert!(
        error
            .to_string()
            .contains("background task join failed during shutdown")
    );
    Ok(())
}

#[sinex_test]
async fn task_failure_notifies_shutdown_waiters() -> xtask::sandbox::TestResult<()> {
    let service = test_service();

    let error = IngestService::handle_join_success(
        "JetStream consumer",
        Err(SinexError::service("boom")),
        &service.shutdown_flag,
        &service.shutdown_notify,
        &service.runtime_failure_flag,
    )
    .expect_err("task failure should bubble up");
    assert!(error.to_string().contains("boom"));
    assert!(service.runtime_failure_flag.load(Ordering::Acquire));

    tokio::time::timeout(
        Duration::from_millis(10),
        crate::runtime::wait_for_shutdown_signal_bool(
            &service.shutdown_flag,
            &service.shutdown_notify,
        ),
    )
    .await
    .expect("shutdown waiters should wake immediately");

    Ok(())
}

#[sinex_test]
async fn unexpected_task_exit_notifies_shutdown_waiters() -> xtask::sandbox::TestResult<()> {
    let service = test_service();

    let error = IngestService::handle_join_success(
        "MaterialAssembler",
        Ok(()),
        &service.shutdown_flag,
        &service.shutdown_notify,
        &service.runtime_failure_flag,
    )
    .expect_err("unexpected exit should bubble up");
    assert!(error.to_string().contains("exited unexpectedly"));
    assert!(service.runtime_failure_flag.load(Ordering::Acquire));

    tokio::time::timeout(
        Duration::from_millis(10),
        crate::runtime::wait_for_shutdown_signal_bool(
            &service.shutdown_flag,
            &service.shutdown_notify,
        ),
    )
    .await
    .expect("shutdown waiters should wake immediately");

    Ok(())
}

#[sinex_test]
async fn prior_shutdown_signal_wakes_late_waiters_immediately() -> xtask::sandbox::TestResult<()>
{
    let service = test_service();
    trigger_shutdown(&service.shutdown_flag, &service.shutdown_notify);
    trigger_shutdown(&service.shutdown_flag, &service.shutdown_notify);
    assert!(!service.runtime_failure_flag.load(Ordering::Acquire));

    tokio::time::timeout(
        Duration::from_millis(10),
        crate::runtime::wait_for_shutdown_signal_bool(
            &service.shutdown_flag,
            &service.shutdown_notify,
        ),
    )
    .await
    .expect("late shutdown waiters should observe an already-triggered shutdown");

    Ok(())
}

#[sinex_test]
async fn module_run_shutdown_state_tracks_failure_cause() -> xtask::sandbox::TestResult<()> {
    let service = test_service();
    assert_eq!(
        IngestService::module_run_shutdown_state(&service.runtime_failure_flag),
        ModuleState::Stopped
    );

    trigger_failed_shutdown(
        &service.shutdown_flag,
        &service.shutdown_notify,
        &service.runtime_failure_flag,
    );

    assert_eq!(
        IngestService::module_run_shutdown_state(&service.runtime_failure_flag),
        ModuleState::Failed
    );
    Ok(())
}

#[sinex_test]
async fn await_ready_signal_accepts_ready_component() -> xtask::sandbox::TestResult<()> {
    let (tx, rx) = oneshot::channel();
    tx.send(())
        .expect("sending ready signal should succeed in the test");

    await_ready_signal("JetStream consumer", Duration::from_millis(10), rx).await?;
    Ok(())
}

#[sinex_test]
async fn await_ready_signal_rejects_dropped_sender() -> xtask::sandbox::TestResult<()> {
    let (tx, rx) = oneshot::channel::<()>();
    drop(tx);

    let error = await_ready_signal("MaterialAssembler", Duration::from_millis(10), rx)
        .await
        .expect_err("dropped ready sender must fail honestly");

    let message = error.to_string();
    assert!(message.contains("setup failed"));
    assert!(message.contains("MaterialAssembler"));
    Ok(())
}

#[sinex_test]
async fn await_ready_signal_rejects_timeout() -> xtask::sandbox::TestResult<()> {
    let (_tx, rx) = oneshot::channel::<()>();

    let error = await_ready_signal("JetStream consumer", Duration::from_millis(10), rx)
        .await
        .expect_err("timed out ready signal must fail honestly");

    let message = error.to_string();
    assert!(message.contains("did not signal ready"));
    assert!(message.contains("JetStream consumer"));
    Ok(())
}

#[sinex_test]
async fn handle_material_assembler_result_preserves_errors_during_shutdown()
-> xtask::sandbox::TestResult<()> {
    let shutdown_flag = Arc::new(AtomicBool::new(true));

    let error = IngestService::handle_material_assembler_result(
        Err(SinexError::service("material bootstrap failed")),
        &shutdown_flag,
    )
    .expect_err("material assembler errors must not be masked by shutdown");

    assert!(error.to_string().contains("material bootstrap failed"));
    Ok(())
}

#[sinex_test]
async fn handle_material_assembler_result_allows_clean_shutdown()
-> xtask::sandbox::TestResult<()> {
    let shutdown_flag = Arc::new(AtomicBool::new(true));

    IngestService::handle_material_assembler_result(Ok(()), &shutdown_flag)?;
    Ok(())
}

#[sinex_test]
async fn monitor_runtime_waits_for_remaining_critical_tasks_after_failure()
-> xtask::sandbox::TestResult<()> {
    let service = test_service();
    let sibling_finished = Arc::new(AtomicBool::new(false));

    let failing = tokio::spawn(async { Err(SinexError::service("boom")) });
    let sibling_flag = Arc::clone(&sibling_finished);
    let shutdown_flag = Arc::clone(&service.shutdown_flag);
    let shutdown_notify = Arc::clone(&service.shutdown_notify);
    let sibling = tokio::spawn(async move {
        crate::runtime::wait_for_shutdown_signal_bool(&shutdown_flag, &shutdown_notify).await;
        sibling_flag.store(true, Ordering::SeqCst);
        Ok(())
    });

    let error = service
        .monitor_runtime(Some(failing), Some(sibling))
        .await
        .expect_err("unexpected failure should bubble up");

    assert!(error.to_string().contains("boom"));
    assert!(
        sibling_finished.load(Ordering::SeqCst),
        "monitor_runtime should await the sibling critical task after shutdown"
    );
    Ok(())
}

#[sinex_test]
async fn finish_startup_failure_preserves_cleanup_error_context()
-> xtask::sandbox::TestResult<()> {
    let service = test_service();
    let sibling_finished = Arc::new(AtomicBool::new(false));

    let failing = tokio::spawn(async { Err(SinexError::service("cleanup boom")) });
    let sibling_flag = Arc::clone(&sibling_finished);
    let shutdown_flag = Arc::clone(&service.shutdown_flag);
    let shutdown_notify = Arc::clone(&service.shutdown_notify);
    let sibling = tokio::spawn(async move {
        crate::runtime::wait_for_shutdown_signal_bool(&shutdown_flag, &shutdown_notify).await;
        sibling_flag.store(true, Ordering::SeqCst);
        Ok(())
    });

    let error = service
        .finish_startup_failure(
            SinexError::service("startup failed"),
            Some(failing),
            Some(sibling),
        )
        .await
        .expect_err("startup failure should remain an error");

    assert!(error.to_string().contains("startup failed"));
    let cleanup_context = error
        .context_map()
        .get("shutdown_cleanup_error")
        .expect("cleanup failure should be preserved in startup error context");
    assert!(cleanup_context.contains("JetStream consumer"));
    assert!(cleanup_context.contains("cleanup boom"));
    assert!(
        sibling_finished.load(Ordering::SeqCst),
        "startup cleanup should still await sibling critical tasks"
    );
    Ok(())
}

#[sinex_test]
async fn finish_startup_failure_preserves_background_task_error_context()
-> xtask::sandbox::TestResult<()> {
    let service = test_service();
    service.task_handles.lock().await.push(tokio::spawn(async {
        panic!("startup cleanup background panic");
    }));

    let error = service
        .finish_startup_failure(SinexError::service("startup failed"), None, None)
        .await
        .expect_err("startup failure should remain an error");

    assert!(error.to_string().contains("startup failed"));
    let cleanup_context = error
        .context_map()
        .get("background_shutdown_error")
        .expect("background cleanup failure should stay attached");
    assert!(cleanup_context.contains("background task join failed during shutdown"));
    Ok(())
}

#[sinex_test]
async fn material_ready_set_maintenance_purges_idle_entries() -> xtask::sandbox::TestResult<()>
{
    let mut service = test_service();
    let ready_set =
        MaterialReadySet::with_policy_for_tests(Duration::from_millis(10), u64::MAX);
    let material_id = Uuid::now_v7();
    ready_set.mark_ready(material_id);

    tokio::time::sleep(Duration::from_millis(15)).await;

    let handle = service.start_material_ready_set_maintenance_task(ready_set.clone());
    service.task_handles.lock().await.push(handle);

    WaitHelpers::wait_for_condition(
        || {
            let ready_set = ready_set.clone();
            async move { Ok::<bool, SinexError>(ready_set.is_empty()) }
        },
        Timeouts::SHORT,
    )
    .await?;

    service.shutdown().await?;
    Ok(())
}

#[sinex_test]
async fn material_ready_set_maintenance_stops_promptly_on_shutdown()
-> xtask::sandbox::TestResult<()> {
    let mut service = test_service();
    let ready_set = MaterialReadySet::with_policy_for_tests(Duration::from_mins(1), u64::MAX);
    let handle = service.start_material_ready_set_maintenance_task(ready_set);
    service.task_handles.lock().await.push(handle);

    tokio::time::timeout(Duration::from_millis(200), service.shutdown())
        .await
        .expect("maintenance task should observe shutdown without waiting for its interval")?;
    Ok(())
}

#[sinex_test]
async fn blob_gc_task_stops_promptly_on_shutdown() -> xtask::sandbox::TestResult<()> {
    // This test only verifies the spawned GC task observes the shutdown
    // flag without waiting for its full interval. Use a dummy non-existent
    // path; the first sweep will fail (warn-and-continue) but shutdown
    // still wins.
    let mut service = test_service();
    // Use a long interval so the task is reliably parked in `interval.tick`
    // when shutdown fires, exercising the shutdown branch of the select.
    let interval_duration = Duration::from_mins(1);
    // Drive the path through the spawn helper directly with a placeholder
    // pool — we never actually tick because we shut down immediately.
    // The placeholder pool is constructed via a lazy connect string; we
    // never use it because the first interval tick fires after our long
    // delay. We keep things honest by only exercising the shutdown branch.
    let pool = sqlx::PgPool::connect_lazy("postgres://invalid/invalid")
        .expect("lazy pool construction should not contact the server");
    let handle = service.start_blob_gc_task(pool, interval_duration);
    service.task_handles.lock().await.push(handle);

    tokio::time::timeout(Duration::from_millis(200), service.shutdown())
        .await
        .expect("blob GC task should observe shutdown without waiting for its interval")?;
    Ok(())
}

#[sinex_test]
async fn blob_gc_disabled_by_default() -> xtask::sandbox::TestResult<()> {
    // Default config has `blob_gc_interval_secs = None`. The construction
    // path in `IngestService::run()` only spawns the GC task when the
    // interval is `Some(_)`, so a freshly built test service must have
    // zero registered tasks regardless of any other startup wiring.
    let service = test_service();
    assert_eq!(service.config.blob_gc_interval_secs, None);
    assert_eq!(service.task_handles.lock().await.len(), 0);
    Ok(())
}

#[sinex_test]
async fn store_schemas_in_kv_rejects_invalid_schema_ids(
    ctx: TestContext,
) -> xtask::sandbox::TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let js = ctx.jetstream().await?;
    let entries = vec![SchemaBroadcastEntry {
        name: "test.schema".to_string(),
        version: "1.0.0".to_string(),
        schema_id: "not-a-uuid".to_string(),
    }];

    let error = IngestService::store_schemas_in_kv(&entries, ctx.pool(), &js)
        .await
        .expect_err("invalid schema ids must fail honestly");
    let message = error.to_string();
    assert!(message.contains("invalid schema_id"));
    assert!(message.contains("test.schema"));
    Ok(())
}

#[sinex_test]
async fn store_schemas_in_kv_rejects_missing_repository_rows(
    ctx: TestContext,
) -> xtask::sandbox::TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let js = ctx.jetstream().await?;
    let missing_schema_id = uuid::Uuid::now_v7().to_string();
    let entries = vec![SchemaBroadcastEntry {
        name: "test.schema".to_string(),
        version: "1.0.0".to_string(),
        schema_id: missing_schema_id.clone(),
    }];

    let error = IngestService::store_schemas_in_kv(&entries, ctx.pool(), &js)
        .await
        .expect_err("missing schema rows must fail honestly");
    let message = error.to_string();
    assert!(message.contains("missing from repository"));
    assert!(message.contains(&missing_schema_id));
    Ok(())
}

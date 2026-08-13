//! Regression coverage for staged-source replay (sinex-2vve, sinex-xixl).
//!
//! Before this fix, `dispatch_staged_source_replay` polled for
//! `ReplayState::Completed/Failed/Cancelled` in an unbounded loop, but that
//! transition can only be set by `finalize_operation`, which is called by
//! this very function's own caller *after* it returns -- a structural
//! deadlock on every staged-source replay's success path, and Cancel never
//! worked either (`Cancelling` fell into the loop's catch-all arm). These
//! tests exercise the real production `dispatch_staged_source_replay` code
//! path (via `ReplayExecutionEngine::execute`, the same entry point the
//! gateway RPC layer uses) against a fake source-side parse listener over
//! real NATS, and prove: (1) success reaches `Completed` without hanging,
//! (2) operator Cancel actually reaches `Cancelled` without hanging, (3) a
//! timeout/failure path restores the archived cascade instead of silently
//! orphaning it. Before this fix, none of these three could pass -- (1) and
//! (2) would hang forever (proven bounded only by the test's own outer
//! `tokio::time::timeout`, which would fire and fail the test), and (3) had
//! no compensation logic at all.
use super::*;
use crate::sources::parse_listener::{SourceParseAck, SourceParseCommand};

fn sample_staged_scope(source_id: &str, source_material_id: Uuid) -> ReplayScope {
    ReplayScope {
        source_name: source_id.to_string(),
        source_id: Some(source_id.to_string()),
        source_material_id: Some(source_material_id),
        ..ReplayScope::default()
    }
}

/// Fake source-side parse listener: subscribes to the staged-replay parse
/// control subject, and -- if `insert_output` is `Some` -- synchronously
/// inserts a real derived-material event tied to the operation before
/// acking, mirroring what a real source's parser dispatch does (parse is a
/// single synchronous request/reply; every parsed record is already headed
/// into admission by the time the ack is sent). When `insert_output` is
/// `None`, the ack is still `accepted: true` but no event is ever inserted,
/// simulating a source that accepted the work but never actually produces
/// visible output -- the shape needed to exercise the cancel and timeout
/// paths.
async fn spawn_fake_parse_source_runtime(
    nats: Client,
    env: SinexEnvironment,
    pool: DbPool,
    source_id: &str,
    insert_output: Option<(&'static str, &'static str, &'static str)>,
) -> Result<tokio::task::JoinHandle<Result<SourceParseCommand>>> {
    let source_id = source_id.to_string();
    let subject = env.nats_subject(&format!("sinex.control.sources.{source_id}.parse"));
    let mut sub = nats.subscribe(subject).await.map_err(|e| {
        test_error(format!(
            "failed to subscribe fake parse source runtime dispatcher: {e}"
        ))
    })?;

    Ok(tokio::spawn(async move {
        let Some(msg) = sub.next().await else {
            return Err(test_error(format!(
                "fake {source_id} parse source runtime ended before receiving a parse command"
            )));
        };

        let command: SourceParseCommand = serde_json::from_slice(&msg.payload).map_err(|error| {
            test_error(format!(
                "fake {source_id} parse source runtime received an invalid parse command: {error}"
            ))
        })?;

        let mut event_count = 0usize;
        if let Some((source, event_type, path)) = insert_output {
            let material_id = command.source_material_id.ok_or_else(|| {
                test_error(
                    "fake parse source runtime requires a source_material_id to insert output",
                )
            })?;
            let mut event = DynamicPayload::new(source, event_type, json!({ "path": path }))
                .from_material(Id::from_uuid(material_id))
                .build()?;
            event.created_by_operation_id = Some(command.operation_id);
            pool.events().insert(event).await?;
            event_count = 1;
        }

        if let Some(reply) = msg.reply.clone() {
            let ack = SourceParseAck {
                accepted: true,
                error: None,
                event_count: Some(event_count),
            };
            let payload = serde_json::to_vec(&ack).map_err(|error| {
                test_error(format!(
                    "fake {source_id} parse source runtime could not encode ack: {error}"
                ))
            })?;
            nats.publish(reply, payload.into()).await.map_err(|error| {
                test_error(format!(
                    "fake {source_id} parse source runtime could not publish ack: {error}"
                ))
            })?;
        }

        Ok(command)
    }))
}

/// sinex-2vve: a staged-source replay whose parse produces real, visible
/// output must reach `Completed` -- bounded by the test's own outer
/// timeout, which is the actual regression assertion. Before the fix this
/// hung forever waiting for a `ReplayState` transition only the caller of
/// this very function could ever perform.
#[sinex_test]
async fn staged_replay_reaches_completed_without_deadlock(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let source_id = "staged-replay-success-test";

    let material_id = ctx.create_source_material(Some(source_id)).await?;
    let seed_event = DynamicPayload::new(
        source_id,
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/staged-replay-seed.txt" }),
    )
    .from_material(material_id)
    .build()?;
    let inserted = ctx.pool.events().insert(seed_event).await?;
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

    let parse_handle = spawn_fake_parse_source_runtime(
        nats_client.clone(),
        env.clone(),
        ctx.pool.clone(),
        source_id,
        Some((
            source_id,
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            "/tmp/staged-replay-output.txt",
        )),
    )
    .await?;

    let mut scope = sample_staged_scope(source_id, *material_id.as_uuid());
    scope.time_window = Some((
        target_ts - time::Duration::milliseconds(1),
        target_ts + time::Duration::milliseconds(1),
    ));

    let planned = replay
        .create_operation(scope.clone(), "test:staged-success".into())
        .await?;
    let preview = replay.generate_preview_summary(&scope).await?;
    replay.update_preview(planned.operation_id, preview).await?;
    replay
        .approve(planned.operation_id, "admin:approver".into())
        .await?;

    let executor = ReplayExecutionEngine::new(replay.clone(), nats_client)
        .with_scan_completion_timeout(Duration::from_secs(10));

    // The regression itself: before the fix, this future never resolved.
    // Bound it explicitly so a reintroduced deadlock fails the test instead
    // of hanging the whole suite.
    let outcome = tokio::time::timeout(
        Duration::from_secs(15),
        executor.execute(planned.operation_id, "service:executor-runtime".into()),
    )
    .await
    .map_err(|_| {
        test_error(
            "staged-source replay execute() did not return within 15s -- \
             this is exactly the sinex-2vve deadlock regressing",
        )
    })?;
    outcome.expect("staged-source replay with real visible output should succeed");

    let completed = replay.load_operation(planned.operation_id).await?;
    assert_eq!(completed.state, ReplayState::Completed);

    let archived_target_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(target_id)
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(
        archived_target_count, 1,
        "successful replay should have archived the original occurrence"
    );

    let dispatched = tokio::time::timeout(Duration::from_secs(5), parse_handle)
        .await
        .map_err(|_| test_error("fake parse source runtime task did not finish"))?
        .map_err(|error| {
            test_error(format!("fake parse source runtime task panicked: {error}"))
        })??;
    assert_eq!(dispatched.operation_id, planned.operation_id);
    assert_eq!(dispatched.source_id, source_id);

    Ok(())
}

/// sinex-2vve / sinex-xixl: operator Cancel against a staged-source replay
/// must actually reach `Cancelled` (not hang), and the archived cascade
/// must be restored rather than left orphaned. Before the fix, `Cancelling`
/// fell into the poll loop's catch-all arm and was never observed at all.
#[sinex_test]
async fn staged_replay_cancel_reaches_cancelled_and_restores_cascade(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let source_id = "staged-replay-cancel-test";

    let material_id = ctx.create_source_material(Some(source_id)).await?;
    let seed_event = DynamicPayload::new(
        source_id,
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/staged-replay-cancel-seed.txt" }),
    )
    .from_material(material_id)
    .build()?;
    let inserted = ctx.pool.events().insert(seed_event).await?;
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

    // Accepts the parse but never inserts output -- visibility can never be
    // satisfied, so the only way this test's execute() call returns is via
    // the cancel path below (or the 15s outer timeout catching a
    // regression).
    let _parse_handle = spawn_fake_parse_source_runtime(
        nats_client.clone(),
        env.clone(),
        ctx.pool.clone(),
        source_id,
        None,
    )
    .await?;

    let mut scope = sample_staged_scope(source_id, *material_id.as_uuid());
    scope.time_window = Some((
        target_ts - time::Duration::milliseconds(1),
        target_ts + time::Duration::milliseconds(1),
    ));

    let planned = replay
        .create_operation(scope.clone(), "test:staged-cancel".into())
        .await?;
    let preview = replay.generate_preview_summary(&scope).await?;
    replay.update_preview(planned.operation_id, preview).await?;
    replay
        .approve(planned.operation_id, "admin:approver".into())
        .await?;

    let executor = ReplayExecutionEngine::new(replay.clone(), nats_client)
        .with_scan_completion_timeout(Duration::from_secs(10));

    let operation_id = planned.operation_id;
    let execute_task = tokio::spawn(async move {
        executor
            .execute(operation_id, "service:executor-runtime".into())
            .await
    });

    // Give execute() time to get past the ack and into the visibility wait
    // loop before requesting cancellation.
    sleep(Duration::from_millis(200)).await;
    replay
        .transition(planned.operation_id, ReplayState::Cancelling)
        .await?;

    let outcome = tokio::time::timeout(Duration::from_secs(15), execute_task)
        .await
        .map_err(|_| {
            test_error(
                "staged-source replay execute() did not return within 15s after Cancel -- \
                 this is exactly the sinex-2vve Cancel-never-propagates regression",
            )
        })?
        .map_err(|error| test_error(format!("execute task panicked: {error}")))?;
    // execute_with_overrides() deliberately converts a confirmed-cancelled
    // internal error into Ok(operation) with state=Cancelled (sinex-2vve/
    // xixl) once it verifies the operation genuinely started before being
    // cancelled -- this hands the caller the full ReplayOperation (state,
    // checkpoint, preview) instead of an opaque error that discards it.
    let cancelled = outcome.expect(
        "cancelled staged-source replay must still return Ok with a confirmed Cancelled operation",
    );
    assert_eq!(
        cancelled.state,
        ReplayState::Cancelled,
        "expected the returned operation to report Cancelled, got: {cancelled:?}"
    );

    let live_target_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(target_id)
            .fetch_one(&ctx.pool)
            .await?;
    assert_eq!(
        live_target_count, 1,
        "cascade must be restored to live after cancel, not left stranded in archive"
    );

    Ok(())
}

/// sinex-xixl: a staged-source replay whose outputs never become visible
/// (and is never cancelled) must fail with a proper timeout error and
/// restore the archived cascade -- not silently leave it orphaned. Before
/// the fix this path returned `Err` directly from
/// `dispatch_staged_source_replay` with no compensation at all.
#[sinex_test]
async fn staged_replay_timeout_restores_cascade(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let source_id = "staged-replay-timeout-test";

    let material_id = ctx.create_source_material(Some(source_id)).await?;
    let seed_event = DynamicPayload::new(
        source_id,
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/staged-replay-timeout-seed.txt" }),
    )
    .from_material(material_id)
    .build()?;
    let inserted = ctx.pool.events().insert(seed_event).await?;
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

    let _parse_handle = spawn_fake_parse_source_runtime(
        nats_client.clone(),
        env.clone(),
        ctx.pool.clone(),
        source_id,
        None,
    )
    .await?;

    let mut scope = sample_staged_scope(source_id, *material_id.as_uuid());
    scope.time_window = Some((
        target_ts - time::Duration::milliseconds(1),
        target_ts + time::Duration::milliseconds(1),
    ));

    let planned = replay
        .create_operation(scope.clone(), "test:staged-timeout".into())
        .await?;
    let preview = replay.generate_preview_summary(&scope).await?;
    replay.update_preview(planned.operation_id, preview).await?;
    replay
        .approve(planned.operation_id, "admin:approver".into())
        .await?;

    let executor = ReplayExecutionEngine::new(replay.clone(), nats_client)
        .with_scan_completion_timeout(Duration::from_millis(300));

    let err = tokio::time::timeout(
        Duration::from_secs(10),
        executor.execute(planned.operation_id, "service:executor-runtime".into()),
    )
    .await
    .map_err(|_| {
        test_error(
            "staged-source replay execute() did not return within 10s of its own 300ms timeout",
        )
    })?
    .expect_err("staged-source replay with no visible output must fail");
    assert!(
        error_contains(&err, "query-visible") || error_contains(&err, "waiting for parsed outputs"),
        "expected a visibility-timeout-shaped error, got: {err}"
    );

    let live_target_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(target_id)
            .fetch_one(&ctx.pool)
            .await?;
    assert_eq!(
        live_target_count, 1,
        "cascade must be restored to live after a visibility timeout, not left stranded in archive"
    );

    Ok(())
}

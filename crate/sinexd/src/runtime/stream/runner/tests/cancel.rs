//! Dispatch-level cancellation tests (sinex-audit-replay-cancel-orphan).
//!
//! `execute_dispatched_scan` / `start_command_listener` must actually
//! interrupt an in-flight `worker.scan(..)` future when a `SourceScanCancel`
//! control message arrives for the operation currently running, not merely
//! record the cancellation somewhere else while the scan keeps emitting
//! events to completion in the background. This is the production mechanism
//! `ReplayExecutionEngine::publish_scan_cancel` (replay_writer.rs) relies on;
//! see `replay_execution_cancel_midflight_stops_emission_and_restores_cascade`
//! in `crate::api::replay_control::tests::abort` for the replay-control-level
//! companion covering the archived-cascade-restoration half of the fix.

use super::*;

use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

const SLOW_SOURCE_ITERATIONS: usize = 12;
const SLOW_SOURCE_DELAY: Duration = Duration::from_millis(120);

/// A `RuntimeModule` whose `scan()` actually emits real events, on a delay,
/// across many iterations -- unlike the ack-only/emit-once fakes used
/// elsewhere in the replay-control test suite. This is what lets this test
/// prove cancellation stops emission mid-flight instead of merely reacting
/// to a single terminal report.
#[cfg(feature = "messaging")]
struct SlowEmittingReplaySource {
    emitter: Option<EventEmitter>,
    lifecycle: Option<Arc<ReplayWorkerLifecycle>>,
}

#[cfg(feature = "messaging")]
impl Default for SlowEmittingReplaySource {
    fn default() -> Self {
        Self {
            emitter: None,
            lifecycle: None,
        }
    }
}

#[cfg(feature = "messaging")]
impl SlowEmittingReplaySource {
    fn blocking(lifecycle: Arc<ReplayWorkerLifecycle>) -> Self {
        Self {
            emitter: None,
            lifecycle: Some(lifecycle),
        }
    }
}

#[cfg(feature = "messaging")]
impl Drop for SlowEmittingReplaySource {
    fn drop(&mut self) {
        if let Some(lifecycle) = &self.lifecycle {
            lifecycle.terminated.store(true, Ordering::SeqCst);
        }
    }
}

#[cfg(feature = "messaging")]
impl RuntimeModule for SlowEmittingReplaySource {
    type Config = ();

    async fn initialize(&mut self, init: RuntimeInitContext<Self::Config>) -> RuntimeResult<()> {
        self.emitter = Some(init.handles().emitter().clone());
        if let Some(lifecycle) = &self.lifecycle {
            lifecycle.initialized.notify_one();
        }
        Ok(())
    }

    async fn scan(
        &mut self,
        _from: Checkpoint,
        _until: TimeHorizon,
        _args: ScanArgs,
    ) -> RuntimeResult<ScanReport> {
        if self.lifecycle.is_some() {
            return std::future::pending::<RuntimeResult<ScanReport>>().await;
        }

        let emitter = self
            .emitter
            .clone()
            .ok_or_else(|| SinexError::lifecycle("scan called before initialize"))?;
        for i in 0..SLOW_SOURCE_ITERATIONS {
            tokio::time::sleep(SLOW_SOURCE_DELAY).await;
            let event = runtime_test_material_event(
                Uuid::now_v7(),
                "slow-emitting-test",
                "slow.emitted",
                serde_json::json!({ "i": i }),
            )
            .map_err(|error| SinexError::processing(error.to_string()))?;
            emitter.emit(event).await?;
        }
        Ok(ScanReport {
            events_processed: SLOW_SOURCE_ITERATIONS as u64,
            duration: SLOW_SOURCE_DELAY * SLOW_SOURCE_ITERATIONS as u32,
            final_checkpoint: Checkpoint::None,
            time_range: None,
            runtime_stats: HashMap::new(),
            successful_targets: vec!["slow-emitting-test-source".to_string()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn module_name(&self) -> &'static str {
        "slow-emitting-test-source"
    }

    fn module_kind(&self) -> ModuleKind {
        ModuleKind::Source
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            supports_historical: true,
            ..RuntimeCapabilities::default()
        }
    }

    async fn current_checkpoint(&self) -> RuntimeResult<Checkpoint> {
        Ok(Checkpoint::None)
    }
}

#[cfg(feature = "messaging")]
struct ReplayWorkerLifecycle {
    initialized: Notify,
    terminated: AtomicBool,
}

#[cfg(feature = "messaging")]
impl ReplayWorkerLifecycle {
    fn new() -> Self {
        Self {
            initialized: Notify::new(),
            terminated: AtomicBool::new(false),
        }
    }
}

/// Anti-vacuity: with the un-fixed `execute_dispatched_scan` (no `cancel_rx`,
/// `worker.scan(..)` awaited unconditionally to completion), this test would
/// observe `events_emitted == SLOW_SOURCE_ITERATIONS` and a completion delay
/// of roughly `SLOW_SOURCE_ITERATIONS * SLOW_SOURCE_DELAY` (~1.4s) regardless
/// of the cancel command -- the scan runs to completion in the background
/// exactly as sinex-audit-replay-cancel-orphan describes.
#[cfg(feature = "messaging")]
#[sinex_test]
async fn dispatched_scan_cancel_stops_in_flight_emission(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let client = ctx.nats_client();
    let transport = EventTransport::Nats(Arc::new(NatsPublisher::new(client.clone())));
    let work_dir = tempdir()?;

    let mut runner = RuntimeRunner::new_with_factory(
        SlowEmittingReplaySource::default(),
        Arc::new(SlowEmittingReplaySource::default),
    );
    runner
        .initialize_with_transport(
            "slow-emitting-test-source".to_string(),
            HashMap::new(),
            Some(ctx.pool().clone()),
            transport,
            work_dir.path().to_path_buf(),
            false,
        )
        .await?;
    // Exercise the real dispatch path directly: `start_command_listener` is
    // `pub(super)`, reachable here without paying for the rest of
    // `run_service` (source startup sequence, heartbeat, OS shutdown bridge).
    runner.start_command_listener();

    let env = sinex_primitives::environment::environment();
    let operation_id = Uuid::now_v7();
    let scan_subject =
        env.nats_subject("sinex.control.sources.slow-emitting-test-source.scan");
    let cancel_subject =
        env.nats_subject("sinex.control.sources.slow-emitting-test-source.cancel");
    let progress_subject =
        env.nats_subject(&format!("sinex.control.replay.progress.{operation_id}"));
    let mut progress_sub = client.subscribe(progress_subject).await?;

    let command = SourceScanCommand {
        operation_id,
        from: Checkpoint::None,
        until: TimeHorizon::Historical {
            end_time: Timestamp::now(),
        },
        args: ScanArgs::default(),
    };
    let ack_msg = tokio::time::timeout(
        Duration::from_secs(3),
        client.request(scan_subject, serde_json::to_vec(&command)?.into()),
    )
    .await??;
    let ack: SourceScanAck = serde_json::from_slice(&ack_msg.payload)?;
    assert!(
        ack.accepted,
        "scan command should be accepted: {:?}",
        ack.error
    );

    // Consume the initial (zero-progress) update so the loop below only sees
    // the terminal one.
    let start_msg = tokio::time::timeout(Duration::from_secs(3), progress_sub.next())
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("progress subscription closed early"))?;
    let start_progress: SourceScanProgress = serde_json::from_slice(&start_msg.payload)?;
    assert!(start_progress.final_report.is_none());

    // Let the scan get a couple of iterations in, then cancel it -- well
    // before it would otherwise finish (SLOW_SOURCE_ITERATIONS * SLOW_SOURCE_DELAY).
    tokio::time::sleep(SLOW_SOURCE_DELAY * 2).await;
    let cancel_sent_at = tokio::time::Instant::now();
    client
        .publish(
            cancel_subject,
            serde_json::to_vec(&SourceScanCancel { operation_id })?.into(),
        )
        .await?;
    client.flush().await?;

    let final_msg = tokio::time::timeout(Duration::from_secs(3), progress_sub.next())
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("progress subscription closed before terminal update"))?;
    let stopped_after = cancel_sent_at.elapsed();
    let final_progress: SourceScanProgress = serde_json::from_slice(&final_msg.payload)?;

    assert!(
        final_progress.cancelled,
        "terminal progress should be marked cancelled: {final_progress:?}"
    );
    assert!(
        final_progress.events_emitted < SLOW_SOURCE_ITERATIONS as u64,
        "cancellation should stop the scan before all {SLOW_SOURCE_ITERATIONS} events are \
         emitted, got {} -- the scan kept running to completion",
        final_progress.events_emitted
    );
    assert!(
        stopped_after < SLOW_SOURCE_DELAY * (SLOW_SOURCE_ITERATIONS as u32),
        "the dispatched scan should stop promptly after cancel (took {stopped_after:?}), not \
         run out the full uninterrupted duration"
    );

    Ok(())
}

/// `sinex-q102`: a replay worker is spawned from the command listener, but it
/// remains owned by its parent runner. Shutting down that runner must cancel
/// and join the worker before returning, rather than leaving it to emit after
/// the command listener has stopped.
#[cfg(feature = "messaging")]
#[sinex_test]
async fn runner_shutdown_cancels_and_joins_dispatched_replay_worker(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let client = ctx.nats_client();
    let transport = EventTransport::Nats(Arc::new(NatsPublisher::new(client.clone())));
    let work_dir = tempdir()?;

    let worker_lifecycle = Arc::new(ReplayWorkerLifecycle::new());
    let worker_lifecycle_for_factory = worker_lifecycle.clone();
    let mut runner = RuntimeRunner::new_with_factory(
        SlowEmittingReplaySource::default(),
        Arc::new(move || SlowEmittingReplaySource::blocking(worker_lifecycle_for_factory.clone())),
    );
    runner
        .initialize_with_transport(
            "slow-emitting-test-source".to_string(),
            HashMap::new(),
            Some(ctx.pool().clone()),
            transport,
            work_dir.path().to_path_buf(),
            false,
        )
        .await?;
    runner.start_command_listener();

    let env = sinex_primitives::environment::environment();
    let operation_id = Uuid::now_v7();
    let scan_subject = env.nats_subject("sinex.control.sources.slow-emitting-test-source.scan");
    let progress_subject =
        env.nats_subject(&format!("sinex.control.replay.progress.{operation_id}"));
    let mut progress_sub = client.subscribe(progress_subject).await?;
    let command = SourceScanCommand {
        operation_id,
        from: Checkpoint::None,
        until: TimeHorizon::Historical {
            end_time: Timestamp::now(),
        },
        args: ScanArgs::default(),
    };

    let ack_msg = tokio::time::timeout(
        Duration::from_secs(3),
        client.request(scan_subject, serde_json::to_vec(&command)?.into()),
    )
    .await??;
    let ack: SourceScanAck = serde_json::from_slice(&ack_msg.payload)?;
    assert!(
        ack.accepted,
        "scan command should be accepted: {:?}",
        ack.error
    );

    let start_msg = tokio::time::timeout(Duration::from_secs(3), progress_sub.next())
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("progress subscription closed early"))?;
    let start_progress: SourceScanProgress = serde_json::from_slice(&start_msg.payload)?;
    assert!(start_progress.final_report.is_none());

    tokio::time::timeout(
        Duration::from_secs(3),
        worker_lifecycle.initialized.notified(),
    )
    .await
    .map_err(|_| color_eyre::eyre::eyre!("replay worker did not initialize"))?;

    {
        let handle_guard = runner
            .replay_worker_handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let handle = handle_guard.as_ref().ok_or_else(|| {
            color_eyre::eyre::eyre!(
                "replay worker handle is missing: registration must succeed before shutdown"
            )
        })?;
        assert!(
            !handle.is_finished(),
            "replay worker handle registered in a finished state before shutdown"
        );
    }

    tokio::time::timeout(Duration::from_secs(2), runner.shutdown())
        .await?
        .map_err(|error| color_eyre::eyre::eyre!("runner shutdown failed: {error}"))?;

    assert!(
        worker_lifecycle.terminated.load(Ordering::SeqCst),
        "runner shutdown returned while the registered replay worker remained detached"
    );
    assert!(
        runner
            .replay_worker_handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_none(),
        "runner shutdown must consume the joined replay worker handle"
    );

    let final_msg = tokio::time::timeout(Duration::from_millis(500), progress_sub.next())
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("progress subscription closed before terminal update"))?;
    let final_progress: SourceScanProgress = serde_json::from_slice(&final_msg.payload)?;
    assert!(
        final_progress.cancelled,
        "runner shutdown must cancel the dispatched replay worker: {final_progress:?}"
    );
    assert!(
        final_progress.events_emitted < SLOW_SOURCE_ITERATIONS as u64,
        "runner shutdown must join the worker before it emits every event, got {}",
        final_progress.events_emitted
    );

    Ok(())
}

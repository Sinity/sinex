// Inline because these cover a private shutdown-signaling helper.
use super::{SourceDriverRuntime, SourceDriverState};
use crate::runtime::checkpoint::{CheckpointManager, CheckpointState};
use crate::runtime::exploration::{ExplorationProvider, ExportFormat};
use crate::runtime::shutdown::ShutdownConfig;
use crate::runtime::stream::{
    Checkpoint, ContinuousStart, RuntimeCapabilities, ScanArgs, ScanReport, TimeHorizon,
};
use crate::runtime::{RuntimeResult, SourceDriver};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use sinex_primitives::{SanitizedPath, Timestamp};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::watch;
use xtask::sandbox::prelude::*;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct TestState;

#[derive(Default)]
struct TestSource;

impl SourceDriver for TestSource {
    type Config = ();
    type State = TestState;

    #[allow(clippy::unused_self)]
    fn name(&self) -> &'static str {
        "source-adapter-test"
    }

    #[allow(clippy::unused_self)]
    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities::default()
    }

    async fn initialize(
        &mut self,
        _config: Self::Config,
        _runtime: &crate::runtime::stream::RuntimeContext,
        _state: &mut Self::State,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    async fn scan_snapshot(
        &mut self,
        _state: &mut Self::State,
        _args: ScanArgs,
    ) -> RuntimeResult<ScanReport> {
        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::ZERO,
            final_checkpoint: Checkpoint::None,
            time_range: None,
            runtime_stats: HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    async fn scan_historical(
        &mut self,
        _state: &mut Self::State,
        _from: Checkpoint,
        _until: TimeHorizon,
        _args: ScanArgs,
    ) -> RuntimeResult<ScanReport> {
        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::ZERO,
            final_checkpoint: Checkpoint::None,
            time_range: None,
            runtime_stats: HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    async fn run_continuous(
        &mut self,
        _state: &mut Self::State,
        start: ContinuousStart,
        _shutdown_rx: watch::Receiver<bool>,
    ) -> RuntimeResult<ScanReport> {
        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::ZERO,
            final_checkpoint: start.checkpoint().clone(),
            time_range: None,
            runtime_stats: HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }
}

#[sinex_test]
async fn default_exploration_capabilities_are_explicitly_unavailable() -> TestResult<()> {
    let adapter = SourceDriverRuntime::new(TestSource);
    let export_path = SanitizedPath::from_static("/tmp/source-driver-default-export.json");

    let source_state_error = adapter
        .get_source_state()
        .expect_err("source drivers without exploration state must report unavailable");
    let history_error = adapter
        .get_ingestion_history(10)
        .expect_err("source drivers without history must report unavailable");
    let export_error = adapter
        .export_data(&export_path, ExportFormat::Json)
        .expect_err("source drivers without export support must report unavailable");

    assert!(source_state_error.to_string().contains("source driver"));
    assert!(
        source_state_error
            .to_string()
            .contains("source-adapter-test")
    );
    assert!(
        source_state_error
            .to_string()
            .contains("source-state exploration")
    );
    assert!(history_error.to_string().contains("source-adapter-test"));
    assert!(history_error.to_string().contains("ingestion history"));
    assert!(export_error.to_string().contains("source-adapter-test"));
    assert!(export_error.to_string().contains("data export"));
    Ok(())
}

#[sinex_test]
async fn request_runtime_drain_delivers_to_receiver() -> TestResult<()> {
    crate::runtime::stream::test_support::assert_request_drain_delivers_to_receiver(
        "test-source",
    )
    .await
}

#[sinex_test]
async fn request_runtime_drain_is_idempotent() -> TestResult<()> {
    crate::runtime::stream::test_support::assert_request_drain_is_idempotent("test-source");
    Ok(())
}

#[sinex_test]
async fn load_state_rejects_hot_reload_file_without_state_payload() -> TestResult<()> {
    let temp_dir = tempdir()?;
    let checkpoint_path = temp_dir.path().join("source-empty-state.checkpoint.json");
    CheckpointState {
        checkpoint: Checkpoint::stream("restored", None),
        processed_count: 0,
        last_activity: Timestamp::now(),
        data: None,
        version: 2,
        revision: 0,
    }
    .save_to_file(&checkpoint_path)
    .await?;

    let mut adapter = SourceDriverRuntime::new(TestSource);
    adapter.shutdown_config = ShutdownConfig {
        checkpoint_path: Some(checkpoint_path.clone()),
        ..ShutdownConfig::default()
    };

    let error = adapter
        .load_state()
        .await
        .expect_err("empty hot reload source state must not be treated as absent");
    let message = format!("{error:#}");
    assert!(message.contains("missing state data"));
    assert!(message.contains("source-adapter-test"));
    assert!(message.contains(&checkpoint_path.display().to_string()));
    Ok(())
}

#[sinex_test]
async fn load_state_falls_back_to_kv_when_hot_reload_file_is_corrupt(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let manager = Arc::new(CheckpointManager::new(
        kv,
        "source-adapter-test".to_string(),
        "test-group".to_string(),
        "kv-fallback-consumer".to_string(),
    ));

    let persisted_state = SourceDriverState {
        user_state: TestState,
        last_checkpoint: Timestamp::now(),
        revision: 0,
        checkpoint: Checkpoint::stream("kv-restored", None),
    };
    let revision = manager
        .save_checkpoint(&CheckpointState {
            checkpoint: Checkpoint::stream("kv-restored", None),
            processed_count: 0,
            last_activity: Timestamp::now(),
            data: Some(serde_json::to_value(&persisted_state)?),
            version: 2,
            revision: 0,
        })
        .await?;

    let temp_dir = tempdir()?;
    let checkpoint_path = temp_dir.path().join("corrupt-hot-reload.checkpoint.json");
    tokio::fs::write(&checkpoint_path, "{ definitely not valid json").await?;

    let mut adapter = SourceDriverRuntime::new(TestSource);
    adapter.shutdown_config = ShutdownConfig {
        checkpoint_path: Some(checkpoint_path.clone()),
        ..ShutdownConfig::default()
    };
    adapter.checkpoint_manager = Some(Arc::clone(&manager));

    adapter
        .load_state()
        .await
        .expect("corrupt hot reload file should fall back to healthy KV state");

    assert_eq!(adapter.state.revision, revision);
    assert_eq!(
        adapter.state.checkpoint,
        Checkpoint::stream("kv-restored", None)
    );
    assert!(
        CheckpointState::load_from_file(&checkpoint_path)
            .await?
            .is_none(),
        "corrupt hot reload file should be discarded after successful KV restore"
    );
    Ok(())
}

#[sinex_test]
async fn load_state_rejects_kv_checkpoint_without_state_payload(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let manager = CheckpointManager::new(
        kv.clone(),
        "source-adapter-test".to_string(),
        "test-group".to_string(),
        "test-consumer".to_string(),
    );
    manager.save_checkpoint(&CheckpointState::default()).await?;

    let mut keys = kv.keys().await?;
    let key = keys.try_next().await?.expect("checkpoint key should exist");
    let corrupt = serde_json::to_vec(&CheckpointState {
        checkpoint: Checkpoint::stream("restored", None),
        processed_count: 0,
        last_activity: Timestamp::now(),
        data: None,
        version: 2,
        revision: 0,
    })?;
    kv.put(&key, corrupt.into()).await?;

    let mut adapter = SourceDriverRuntime::new(TestSource);
    adapter.checkpoint_manager = Some(Arc::new(manager));

    let error = adapter
        .load_state()
        .await
        .expect_err("empty source checkpoint KV state must not be treated as fresh");
    let message = format!("{error:#}");
    assert!(message.contains("missing state data"));
    assert!(message.contains("source-adapter-test"));
    Ok(())
}

#[sinex_test]
async fn load_state_accepts_fresh_kv_checkpoint_without_state_payload(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let manager = CheckpointManager::new(
        kv,
        "source-adapter-test".to_string(),
        "test-group".to_string(),
        "fresh-consumer".to_string(),
    );

    let mut adapter = SourceDriverRuntime::new(TestSource);
    adapter.checkpoint_manager = Some(Arc::new(manager));
    adapter
        .load_state()
        .await
        .expect("fresh checkpoint state should be treated as a clean start");

    assert!(matches!(adapter.state.checkpoint, Checkpoint::None));
    assert_eq!(adapter.state.revision, 0);
    Ok(())
}

#[sinex_test]
async fn save_state_keeps_restored_hot_reload_file_until_successful_kv_sync(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let manager = Arc::new(CheckpointManager::new(
        kv,
        "source-adapter-test".to_string(),
        "test-group".to_string(),
        "hot-reload-sync-consumer".to_string(),
    ));

    let persisted_state = SourceDriverState {
        user_state: TestState,
        last_checkpoint: Timestamp::now(),
        revision: 0,
        checkpoint: Checkpoint::stream("file-restored", None),
    };
    let baseline_revision = manager
        .save_checkpoint(&CheckpointState {
            checkpoint: Checkpoint::stream("file-restored", None),
            processed_count: 0,
            last_activity: Timestamp::now(),
            data: Some(serde_json::to_value(&persisted_state)?),
            version: 2,
            revision: 0,
        })
        .await?;

    let temp_dir = tempdir()?;
    let checkpoint_path = temp_dir.path().join("source-hot-reload.checkpoint.json");
    CheckpointState {
        checkpoint: Checkpoint::stream("file-restored", None),
        processed_count: 0,
        last_activity: Timestamp::now(),
        data: Some(serde_json::to_value(&persisted_state)?),
        version: 2,
        revision: baseline_revision,
    }
    .save_to_file(&checkpoint_path)
    .await?;

    let mut adapter = SourceDriverRuntime::new(TestSource);
    adapter.shutdown_config = ShutdownConfig {
        checkpoint_path: Some(checkpoint_path.clone()),
        ..ShutdownConfig::default()
    };
    adapter.checkpoint_manager = Some(Arc::clone(&manager));

    adapter.load_state().await?;
    assert!(
        CheckpointState::load_from_file(&checkpoint_path)
            .await?
            .is_some(),
        "restored hot reload file must remain until the state is durably re-saved"
    );

    adapter.save_state(false).await?;
    assert!(
        CheckpointState::load_from_file(&checkpoint_path)
            .await?
            .is_none(),
        "restored hot reload file should be cleaned up after successful KV sync"
    );
    assert!(
        adapter.state.revision > baseline_revision,
        "follow-up save should update the prior KV checkpoint revision"
    );
    Ok(())
}

#[sinex_test]
async fn save_state_recreates_missing_kv_entry_for_stale_hot_reload_revision(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let manager = Arc::new(CheckpointManager::new(
        kv.clone(),
        "source-adapter-test".to_string(),
        "test-group".to_string(),
        "stale-hot-reload-consumer".to_string(),
    ));

    let persisted_state = SourceDriverState {
        user_state: TestState,
        last_checkpoint: Timestamp::now(),
        revision: 0,
        checkpoint: Checkpoint::stream("file-restored", None),
    };

    let temp_dir = tempdir()?;
    let checkpoint_path = temp_dir.path().join("stale-hot-reload.checkpoint.json");
    CheckpointState {
        checkpoint: Checkpoint::stream("file-restored", None),
        processed_count: 0,
        last_activity: Timestamp::now(),
        data: Some(serde_json::to_value(&persisted_state)?),
        version: 2,
        revision: 7,
    }
    .save_to_file(&checkpoint_path)
    .await?;

    let mut adapter = SourceDriverRuntime::new(TestSource);
    adapter.shutdown_config = ShutdownConfig {
        checkpoint_path: Some(checkpoint_path.clone()),
        ..ShutdownConfig::default()
    };
    adapter.checkpoint_manager = Some(Arc::clone(&manager));

    adapter.load_state().await?;
    assert_eq!(adapter.state.revision, 7);

    adapter.save_state(false).await?;
    assert!(
        adapter.state.revision > 0,
        "successful save should recreate the missing KV entry with a fresh revision"
    );
    assert!(
        CheckpointState::load_from_file(&checkpoint_path)
            .await?
            .is_none(),
        "restored hot reload file should be cleaned up after the recreated KV save"
    );

    let mut keys = kv.keys().await?;
    assert!(
        keys.try_next().await?.is_some(),
        "checkpoint KV entry should be recreated when only a stale hot reload file exists"
    );
    Ok(())
}

#[sinex_test]
async fn save_state_updates_existing_zero_progress_checkpoint(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let manager = Arc::new(CheckpointManager::new(
        kv,
        "source-adapter-test".to_string(),
        "test-group".to_string(),
        "existing-zero-progress-consumer".to_string(),
    ));

    let old_state = SourceDriverState {
        user_state: TestState,
        last_checkpoint: Timestamp::now(),
        revision: 0,
        checkpoint: Checkpoint::stream("old-cursor", None),
    };
    let baseline_revision = manager
        .save_checkpoint(&CheckpointState {
            checkpoint: Checkpoint::stream("old-cursor", None),
            processed_count: 0,
            last_activity: Timestamp::now(),
            data: Some(serde_json::to_value(old_state)?),
            version: 2,
            revision: 0,
        })
        .await?;

    let mut adapter = SourceDriverRuntime::new(TestSource);
    adapter.checkpoint_manager = Some(Arc::clone(&manager));
    adapter.state.checkpoint = Checkpoint::stream("new-cursor", None);

    adapter.save_state(false).await?;

    let saved = manager.load_checkpoint().await?;
    assert!(
        saved.revision > baseline_revision,
        "save should update the existing key instead of attempting create"
    );
    assert_eq!(saved.checkpoint, Checkpoint::stream("new-cursor", None));
    Ok(())
}

#[sinex_test]
async fn save_state_preserves_newer_direct_checkpoint_for_stale_revision(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let manager = Arc::new(CheckpointManager::new(
        kv,
        "source-adapter-test".to_string(),
        "test-group".to_string(),
        "stale-direct-revision-consumer".to_string(),
    ));

    let stale_state = SourceDriverState {
        user_state: TestState,
        last_checkpoint: Timestamp::now(),
        revision: 0,
        checkpoint: Checkpoint::stream("stale-cursor", None),
    };
    let stale_revision = manager
        .save_checkpoint(&CheckpointState {
            checkpoint: Checkpoint::stream("stale-cursor", None),
            processed_count: 0,
            last_activity: Timestamp::now(),
            data: Some(serde_json::to_value(stale_state)?),
            version: 2,
            revision: 0,
        })
        .await?;

    let newer_state = SourceDriverState {
        user_state: TestState,
        last_checkpoint: Timestamp::now(),
        revision: 0,
        checkpoint: Checkpoint::stream("newer-cursor", None),
    };
    let newer_revision = manager
        .save_checkpoint(&CheckpointState {
            checkpoint: Checkpoint::stream("newer-cursor", None),
            processed_count: 70_000,
            last_activity: Timestamp::now(),
            data: Some(serde_json::to_value(newer_state)?),
            version: 2,
            revision: stale_revision,
        })
        .await?;

    let mut adapter = SourceDriverRuntime::new(TestSource);
    adapter.checkpoint_manager = Some(Arc::clone(&manager));
    adapter.state.revision = stale_revision;
    adapter.state.checkpoint = Checkpoint::stream("stale-cursor", None);

    adapter.save_state(false).await?;

    let saved = manager.load_checkpoint().await?;
    assert!(
        saved.revision > newer_revision,
        "save should refresh the stale revision and succeed"
    );
    assert_eq!(saved.processed_count, 70_000);
    assert_eq!(saved.checkpoint, Checkpoint::stream("newer-cursor", None));
    Ok(())
}

#[sinex_test]
async fn load_state_adopts_latest_peer_checkpoint_for_non_concurrent_source(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let peer_manager = CheckpointManager::new(
        kv.clone(),
        "source-adapter-test".to_string(),
        "default".to_string(),
        "host-a-12345".to_string(),
    );
    peer_manager
        .save_checkpoint(&CheckpointState {
            checkpoint: Checkpoint::stream("peer-cursor", None),
            processed_count: 42,
            last_activity: Timestamp::now(),
            data: Some(serde_json::to_value(SourceDriverState {
                user_state: TestState,
                last_checkpoint: Timestamp::now(),
                revision: 0,
                checkpoint: Checkpoint::stream("peer-cursor", None),
            })?),
            version: 2,
            revision: 0,
        })
        .await?;

    let stable_manager = Arc::new(CheckpointManager::new(
        kv,
        "source-adapter-test".to_string(),
        "default".to_string(),
        "source-adapter-test".to_string(),
    ));
    let mut adapter = SourceDriverRuntime::new(TestSource);
    adapter.checkpoint_manager = Some(Arc::clone(&stable_manager));

    adapter.load_state().await?;

    assert_eq!(adapter.state.revision, 0);
    assert_eq!(
        adapter.state.checkpoint,
        Checkpoint::stream("peer-cursor", None)
    );

    adapter.save_state(false).await?;
    let stable_checkpoint = stable_manager.load_checkpoint().await?;
    assert_eq!(
        stable_checkpoint.checkpoint,
        Checkpoint::stream("peer-cursor", None)
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// sinex-hdnv (finding #2): a scan error settled as Settlement::Commit must
// still be recorded on the health reporter, not silently swallowed.
//
// IMPORTANT CONTEXT: as of this commit, `DefaultFailurePolicy::settle` (the
// only `FailurePolicy` implementor in the workspace,
// sinex-primitives/src/settlement.rs) NEVER returns `Settlement::Commit` for
// any `ErrorClass` -- this branch is currently unreachable via the real
// settlement policy. `apply_scan_settlement` was split out from
// `settle_scan_error` specifically so this arm's behavior can be verified
// directly (by constructing `Settlement::Commit` ourselves, since it's a
// plain public unit variant) even though production code can't currently
// reach it. This test verifies the WIRING IS CORRECT IF THE BRANCH IS EVER
// REACHED -- it does not mean sinex-hdnv's original AW-incident hypothesis is
// confirmed live; see the bead's corrected description for the more likely
// real mechanism (a scan returning Ok with zero events, never entering this
// function at all).
// ---------------------------------------------------------------------------

use crate::runtime::health_reporter::{HealthReporter, HealthThresholds};
use crate::runtime::self_observation::SelfObserver;
use sinex_primitives::settlement::Settlement;
use std::sync::atomic::Ordering;

fn test_health_reporter() -> HealthReporter {
    HealthReporter::new(
        "source-driver-test".to_string(),
        Arc::new(SelfObserver::disabled()),
        HealthThresholds {
            error_rate_degraded: 0.05,
            error_rate_failed: 0.20,
            window_seconds: 60,
            emit_stall_seconds: 0,
            refresh_seconds: 10,
        },
    )
}

#[sinex_test]
async fn commit_settlement_records_health_error_and_returns_empty_report() -> TestResult<()> {
    let mut adapter = SourceDriverRuntime::new(TestSource);
    let reporter = Arc::new(test_health_reporter());
    adapter.health_reporter = Some(Arc::clone(&reporter));

    assert_eq!(reporter.metrics().errors.load(Ordering::Relaxed), 0);

    let error = SinexError::processing("synthetic benign scan error for testing");
    let report =
        adapter.apply_scan_settlement(Settlement::Commit, error, "test-phase", &Checkpoint::None)?;

    assert_eq!(report.events_processed, 0);
    assert!(report.failed_targets.is_empty());
    assert_eq!(
        reporter.metrics().errors.load(Ordering::Relaxed),
        1,
        "Settlement::Commit must record the swallowed error on the health reporter"
    );
    Ok(())
}

#[sinex_test]
async fn commit_settlement_without_health_reporter_still_returns_empty_report() -> TestResult<()> {
    // No health_reporter attached (the common case for a driver that hasn't
    // called with_health_monitoring/initialize yet) -- the Commit arm must
    // not panic or error just because there's nowhere to record health.
    let adapter = SourceDriverRuntime::new(TestSource);
    let error = SinexError::processing("synthetic benign scan error for testing");
    let report =
        adapter.apply_scan_settlement(Settlement::Commit, error, "test-phase", &Checkpoint::None)?;
    assert_eq!(report.events_processed, 0);
    Ok(())
}

#[sinex_test]
async fn default_failure_policy_never_produces_commit_settlement() -> TestResult<()> {
    // Reachability canary for the context note above: if this ever starts
    // failing, Settlement::Commit has become reachable in production and the
    // corrected sinex-hdnv bead description (which says it currently isn't)
    // needs updating, and the "real AW incident mechanism" hypothesis in this
    // file's doc comment should be re-examined.
    use sinex_primitives::settlement::{
        DefaultFailurePolicy, FailureContext, FailurePolicy, RuntimeOperation, RuntimePhase,
    };
    let ctx = FailureContext {
        unit_id: "canary".to_string(),
        operation: RuntimeOperation::ProcessBatch,
        phase: RuntimePhase::ProcessInput,
        input_scope: None,
        effect_kind: None,
        delivery_count: None,
        attempts: 0,
    };
    let sample_errors = vec![
        SinexError::processing("sample"),
        SinexError::validation("sample"),
        SinexError::parse("sample"),
        SinexError::network("sample"),
        SinexError::timeout("sample"),
        SinexError::configuration("sample"),
        SinexError::not_found("sample"),
    ];
    for error in sample_errors {
        let settlement = DefaultFailurePolicy.settle(&error, &ctx);
        assert!(
            !matches!(settlement, Settlement::Commit),
            "DefaultFailurePolicy produced Settlement::Commit for {error:?} -- \
             this was believed unreachable; sinex-hdnv's corrected description needs revisiting"
        );
    }
    Ok(())
}

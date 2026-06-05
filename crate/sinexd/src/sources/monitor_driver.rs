//! Lifecycle-hook runner for monitor source contracts.
//!
//! Monitor source contracts are fire-once or periodic emitters that have no adapter
//! input — no file to tail, no socket to read. They emit a small fixed set of
//! events at defined points in the runtime module lifecycle: once at boot, once per
//! interval, or once on clean shutdown.
//!
//! # Design
//!
//! A monitor unit is registered via [`register_source!`], which inserts a
//! [`SourceFactoryEntry`] backed by [`run_monitor_unit_delegated`]. The runner:
//!
//! 1. Opens a synthetic source material via `AcquisitionManager::begin_material`
//!    (satisfies the FK constraint — material content is the JSON of the emitted
//!    events themselves, self-referential but valid).
//! 2. Calls the user's closure with the runtime and the acquired material ID so
//!    the closure can build events with correct material provenance.
//! 3. Appends the serialized events as the material content, finalizes the material.
//! 4. Emits each event through `runtime.emit_event()`.
//! 5. For `PerInterval`, loops with `tokio::time::sleep` until drain is signalled.
//! 6. For `ServiceShutdown`, waits for the drain signal, then fires once.
//!
//! # Provenance
//!
//! All emitted events use **material provenance** anchored to a synthetic
//! material opened per firing. This satisfies the FK constraint on `core.events`.
//! The material's `source_identifier` is the source ID; its content is the
//! serialized JSON of the emitted events.
//!
//! # Relationship to `register_source_contract!`
//!
//! `register_source!` does NOT register the `SourceContract`. Call
//! `register_source_contract!` from `sinex-primitives` separately. The two macros
//! compose — one owns the descriptor inventory, the other owns the factory.

use futures::future::BoxFuture;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::runtime::{
    RuntimeResult, SourceDriver, SourceDriverRuntime,
    acquisition_manager::RotationPolicy,
    runtime_cli::{RuntimeCli, RuntimeCliRunner},
    stream::{
        Checkpoint, ContinuousStart, RuntimeCapabilities, RuntimeContext, ScanArgs, ScanReport,
        TimeHorizon,
    },
};
use sinex_primitives::{
    JsonValue, SinexError,
    events::{Event, SourceMaterial},
    ids::Id,
};

// =============================================================================
// MonitorPhase — when the closure fires
// =============================================================================

/// Determines when a monitor unit's closure fires relative to the runtime module lifecycle.
#[derive(Debug, Clone)]
pub enum MonitorPhase {
    /// Fire once immediately at source boot (inside `run_continuous`).
    ///
    /// The runner fires the closure, emits events, then returns. The module exits
    /// cleanly. Use this for startup-annotation events.
    ServiceStart,

    /// Fire once per `period` for the process lifetime.
    ///
    /// The runner fires, sleeps, fires, sleeps — looping until the drain signal
    /// arrives. Use this for heartbeat or periodic observation events.
    PerInterval { period: Duration },

    /// Fire once when the drain signal arrives (clean shutdown).
    ///
    /// If the process is killed without a drain signal this phase does not fire.
    /// Use `ServiceStart` when missing the shutdown emit is acceptable.
    ServiceShutdown,
}

// =============================================================================
// MonitorEmitFn — the user closure (type-erased)
// =============================================================================

/// A type-erased async function that produces zero or more events.
///
/// The function receives the [`RuntimeContext`] and the [`Id<SourceMaterial>`]
/// of the synthetic material opened for this firing. Every returned event must
/// use `.from_material(material_id)` provenance so the FK constraint is satisfied.
///
/// Using a `fn` pointer (not a boxed closure) allows use inside
/// `inventory::submit!` which requires const-constructible items. Define an
/// `async fn` with this signature and pass it to `register_source!`.
pub type MonitorEmitFn = fn(
    runtime: RuntimeContext,
    material_id: Id<SourceMaterial>,
) -> BoxFuture<'static, RuntimeResult<Vec<Event<JsonValue>>>>;

// =============================================================================
// Stateless checkpoint state
// =============================================================================

/// Stateless checkpoint state — monitors carry no position between firings.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct MonitorState {}

// =============================================================================
// fire_monitor_once — open material, call closure, emit events
// =============================================================================

/// Open a synthetic material, call the closure, finalize, and emit events.
///
/// This is the single atomic unit of monitor work, shared across all phases.
async fn fire_monitor_once(
    source_id: &'static str,
    emit_fn: MonitorEmitFn,
    runtime: &RuntimeContext,
) -> RuntimeResult<()> {
    let acq = runtime.acquisition_manager(RotationPolicy::default(), source_id)?;

    // Open a synthetic material. This registers a row in
    // `raw.source_material_registry` and satisfies the FK on `core.events`.
    let mut mat_handle = acq.begin_material(source_id).await?;
    let material_id: Id<SourceMaterial> = Id::from_uuid(mat_handle.material_id);

    // Call the user async fn. It must return events anchored to `material_id`.
    let events = emit_fn(runtime.clone(), material_id).await?;

    if events.is_empty() {
        debug!(
            source_id,
            "monitor closure returned 0 events — finalizing with empty slice to prevent slice_arrival_timeout"
        );
        // Write an empty slice so the assembler sees at least one slice
        // before FINALIZE. Without this, the periodic timeout check can
        // fire between BEGIN and FINALIZE, routing the material to DLQ
        // as slice_arrival_timeout (#1320).
        acq.append_slice(&mut mat_handle, &[]).await?;
        acq.finalize(mat_handle, "monitor-empty").await?;
        return Ok(());
    }

    // Serialize the events as the material content (self-referential but valid).
    let content =
        serde_json::to_vec(&events).map_err(|e| SinexError::serialization(e.to_string()))?;
    acq.append_slice(&mut mat_handle, &content).await?;
    acq.finalize(mat_handle, "monitor-complete").await?;

    let count = events.len();
    for event in events {
        runtime.emit_event(event).await?;
    }

    info!(source_id, events = count, "monitor unit fired successfully",);
    Ok(())
}

// =============================================================================
// drive_monitor_phase — phase loop
// =============================================================================

async fn drive_monitor_phase(
    source_id: &'static str,
    phase: &MonitorPhase,
    emit_fn: MonitorEmitFn,
    runtime: &RuntimeContext,
    mut shutdown_rx: watch::Receiver<bool>,
) -> RuntimeResult<()> {
    match phase {
        MonitorPhase::ServiceStart => {
            info!(
                source_id,
                "MonitorPhase::ServiceStart — firing once at boot"
            );
            fire_monitor_once(source_id, emit_fn, runtime).await?;
        }

        MonitorPhase::PerInterval { period } => {
            info!(
                source_id,
                interval_secs = period.as_secs_f64(),
                "MonitorPhase::PerInterval — starting loop",
            );
            loop {
                fire_monitor_once(source_id, emit_fn, runtime).await?;

                // Sleep for `period` or exit immediately on drain.
                tokio::select! {
                    biased;
                    result = shutdown_rx.changed() => {
                        if result.is_err() || *shutdown_rx.borrow() {
                            info!(source_id, "drain received — stopping PerInterval loop");
                            break;
                        }
                    }
                    () = tokio::time::sleep(*period) => {}
                }
            }
        }

        MonitorPhase::ServiceShutdown => {
            info!(
                source_id,
                "MonitorPhase::ServiceShutdown — waiting for drain signal",
            );
            loop {
                if shutdown_rx.changed().await.is_err() {
                    break; // Sender dropped — treat as clean exit.
                }
                if *shutdown_rx.borrow() {
                    break;
                }
            }
            info!(source_id, "drain received — firing ServiceShutdown monitor");
            if let Err(e) = fire_monitor_once(source_id, emit_fn, runtime).await {
                warn!(
                    source_id,
                    error = %e,
                    "ServiceShutdown monitor emit failed",
                );
                // Don't propagate — we're already shutting down.
            }
        }
    }

    Ok(())
}

// =============================================================================
// MonitorDriver — SourceDriver bridge
// =============================================================================

/// An `SourceDriver` that bridges the runtime lifecycle into `drive_monitor_phase`.
///
/// `initialize()` captures the `RuntimeContext` into `runtime_snapshot`.
/// `run_continuous()` then drives `drive_monitor_phase` directly, giving the
/// monitor closure full runtime access (NATS, `AcquisitionManager`, etc.).
///
/// This bridges the gap that `SourceDriver::run_continuous` does not receive
/// `RuntimeContext` directly.
#[derive(Default)]
pub struct MonitorDriver {
    source_id: &'static str,
    /// Taken on first call to `run_continuous`. `Option` because the value is
    /// moved out; a second call would find it `None` and return an error.
    phase: Option<MonitorPhase>,
    /// Taken on first call to `run_continuous`.
    emit_fn: Option<MonitorEmitFn>,
    /// Populated during `initialize()`, taken during `run_continuous()`.
    runtime_snapshot: Option<RuntimeContext>,
}

impl MonitorDriver {
    #[must_use]
    pub fn new(source_id: &'static str, phase: MonitorPhase, emit_fn: MonitorEmitFn) -> Self {
        Self {
            source_id,
            phase: Some(phase),
            emit_fn: Some(emit_fn),
            runtime_snapshot: None,
        }
    }
}

impl SourceDriver for MonitorDriver {
    type Config = serde_json::Value;
    type State = MonitorState;

    fn name(&self) -> &str {
        self.source_id
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            supports_snapshot: false,
            supports_historical: false,
            supports_continuous: true,
            supports_interactive: false,
            max_scan_size: None,
            supports_concurrent: false,
            manages_own_continuous_loop: true,
            manages_own_checkpoints: false,
        }
    }

    async fn initialize(
        &mut self,
        _config: Self::Config,
        runtime: &RuntimeContext,
        _state: &mut Self::State,
    ) -> RuntimeResult<()> {
        // Snapshot the runtime so run_continuous() can access it.
        self.runtime_snapshot = Some(runtime.clone());
        info!(source_id = self.source_id, "monitor unit initialized");
        Ok(())
    }

    async fn scan_snapshot(
        &mut self,
        _state: &mut Self::State,
        _args: ScanArgs,
    ) -> RuntimeResult<ScanReport> {
        Ok(ScanReport {
            events_processed: 0,
            duration: Duration::ZERO,
            final_checkpoint: Checkpoint::None,
            time_range: None,
            runtime_stats: HashMap::new(),
            failed_targets: Vec::new(),
            successful_targets: Vec::new(),
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
            duration: Duration::ZERO,
            final_checkpoint: Checkpoint::None,
            time_range: None,
            runtime_stats: HashMap::new(),
            failed_targets: Vec::new(),
            successful_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    async fn run_continuous(
        &mut self,
        _state: &mut Self::State,
        start: ContinuousStart,
        shutdown_rx: watch::Receiver<bool>,
    ) -> RuntimeResult<ScanReport> {
        let started_at = Instant::now();

        let runtime = self.runtime_snapshot.take().ok_or_else(|| {
            SinexError::invalid_state("MonitorDriver: runtime not captured during initialize()")
        })?;

        let phase = self
            .phase
            .take()
            .ok_or_else(|| SinexError::invalid_state("MonitorDriver: phase already consumed"))?;

        let emit_fn = self
            .emit_fn
            .take()
            .ok_or_else(|| SinexError::invalid_state("MonitorDriver: emit_fn already consumed"))?;

        drive_monitor_phase(self.source_id, &phase, emit_fn, &runtime, shutdown_rx).await?;

        Ok(ScanReport {
            events_processed: 0,
            duration: started_at.elapsed(),
            final_checkpoint: start.checkpoint().clone(),
            time_range: None,
            runtime_stats: HashMap::new(),
            failed_targets: Vec::new(),
            successful_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }
}

// =============================================================================
// run_monitor_unit_delegated — called by register_source! factory fn
// =============================================================================

/// Entry point called by [`register_source!`]-generated factory functions.
///
/// Wires up the standard runtime CLI + runner via `SourceDriverRuntime`, which
/// calls `initialize()` (capturing runtime) then `run_continuous()` (driving
/// `drive_monitor_phase`).
///
/// This function is `pub` so the macro can name it; callers should use
/// `register_source!` rather than calling this directly.
pub async fn run_monitor_unit_delegated(
    source_id: &'static str,
    phase: MonitorPhase,
    emit_fn: MonitorEmitFn,
    args: Vec<std::ffi::OsString>,
) -> Result<(), Box<dyn std::error::Error>> {
    use clap::Parser;

    let parsed = RuntimeCli::parse_from(args);
    let node = MonitorDriver::new(source_id, phase, emit_fn);
    let adapter = SourceDriverRuntime::new(node);
    let mut runner = RuntimeCliRunner::new(adapter);
    runner.run(parsed).await.map_err(std::convert::Into::into)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::EventTransport;
    use crate::runtime::stream::{EventEmitter, RuntimeHandles, ServiceInfo};
    use crate::runtime::{CheckpointManager, NatsPublisher};
    use sinex_primitives::domain::HostName;
    use sinex_primitives::events::DynamicPayload;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use xtask::sandbox::prelude::*;

    /// Verify MonitorPhase variants are Debug + Clone.
    #[sinex_test]
    async fn test_monitor_phase_clone_all_variants() -> TestResult<()> {
        let start = MonitorPhase::ServiceStart;
        let interval = MonitorPhase::PerInterval {
            period: Duration::from_mins(1),
        };
        let shutdown = MonitorPhase::ServiceShutdown;

        assert!(matches!(start.clone(), MonitorPhase::ServiceStart));
        assert!(
            matches!(interval.clone(), MonitorPhase::PerInterval { period } if period == Duration::from_mins(1))
        );
        assert!(matches!(shutdown.clone(), MonitorPhase::ServiceShutdown));

        Ok(())
    }

    /// Verify MonitorDriver errors cleanly if `run_continuous` is called
    /// without a prior `initialize()`.
    #[sinex_test]
    async fn test_monitor_driver_node_missing_runtime_errors() -> TestResult<()> {
        fn noop_emit(
            _runtime: RuntimeContext,
            _material_id: Id<SourceMaterial>,
        ) -> futures::future::BoxFuture<'static, RuntimeResult<Vec<Event<JsonValue>>>> {
            Box::pin(async { Ok(vec![]) })
        }

        let mut node = MonitorDriver::new("test.monitor", MonitorPhase::ServiceStart, noop_emit);

        // run_continuous without prior initialize() should return Err.
        let (_tx, rx) = watch::channel(false);
        let mut state = MonitorState::default();
        let start = ContinuousStart::from_checkpoint(Checkpoint::None);
        let result = node.run_continuous(&mut state, start, rx).await;

        assert!(result.is_err(), "expected Err when runtime not captured");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("runtime not captured"),
            "unexpected error message: {err}"
        );

        Ok(())
    }

    /// Verify that a MonitorDriver with a noop emit function reflects the
    /// correct capabilities: continuous only, no snapshot/historical.
    #[sinex_test]
    async fn test_monitor_driver_node_capabilities() -> TestResult<()> {
        fn noop_emit(
            _runtime: RuntimeContext,
            _material_id: Id<SourceMaterial>,
        ) -> futures::future::BoxFuture<'static, RuntimeResult<Vec<Event<JsonValue>>>> {
            Box::pin(async { Ok(vec![]) })
        }

        let node = MonitorDriver::new("test.monitor", MonitorPhase::ServiceStart, noop_emit);
        let caps = node.capabilities();

        assert!(!caps.supports_snapshot, "monitors have no snapshot mode");
        assert!(
            !caps.supports_historical,
            "monitors have no historical mode"
        );
        assert!(caps.supports_continuous, "monitors run in continuous mode");
        assert!(
            caps.manages_own_continuous_loop,
            "monitors manage their own loop"
        );

        Ok(())
    }

    #[sinex_test]
    async fn monitor_fire_once_opens_material_and_emits_event(ctx: TestContext) -> TestResult<()> {
        fn emit_test_monitor(
            _runtime: RuntimeContext,
            material_id: Id<SourceMaterial>,
        ) -> futures::future::BoxFuture<'static, RuntimeResult<Vec<Event<JsonValue>>>> {
            Box::pin(async move {
                let event = DynamicPayload::new(
                    "monitor.test",
                    "monitor.test.started",
                    serde_json::json!({ "ok": true }),
                )
                .from_material(material_id)
                .build()?;
                Ok(vec![event])
            })
        }

        let ctx = ctx.with_nats().shared().await?;
        let (runtime, mut events) = make_monitor_runtime(&ctx).await?;

        fire_monitor_once("test.monitor", emit_test_monitor, &runtime).await?;

        let event = events
            .recv()
            .await
            .ok_or_else(|| SinexError::processing("monitor event channel closed"))?;
        assert_eq!(event.source.as_str(), "monitor.test");
        assert_eq!(event.event_type.as_str(), "monitor.test.started");
        assert!(
            matches!(
                event.provenance,
                sinex_primitives::events::Provenance::Material { .. }
            ),
            "monitor events must use material provenance"
        );
        assert_eq!(event.payload["ok"], true);
        Ok(())
    }

    async fn make_monitor_runtime(
        ctx: &TestContext,
    ) -> TestResult<(RuntimeContext, mpsc::Receiver<Event<JsonValue>>)> {
        let kv = ctx.checkpoint_kv().await?;
        let checkpoint_manager = Arc::new(CheckpointManager::new(
            kv,
            "monitor-fire-once-test".to_string(),
            "test-group".to_string(),
            format!("test-consumer-{}", Uuid::now_v7().simple()),
        ));
        let (event_sender, event_receiver) = mpsc::channel::<Event<JsonValue>>(8);
        let emitter = EventEmitter::new(event_sender, false);
        let publisher = Arc::new(NatsPublisher::new(ctx.nats_client()));
        let handles = RuntimeHandles::new_edge(
            checkpoint_manager,
            emitter,
            EventTransport::Nats(publisher),
            None,
            None,
        );
        let work_dir = tempfile::tempdir()?;
        let work_dir_path = work_dir.keep();
        let work_dir_utf8 =
            camino::Utf8PathBuf::from_path_buf(work_dir_path.clone()).map_err(|path| {
                SinexError::validation("temporary work dir should be utf-8")
                    .with_context("path", path.display().to_string())
            })?;

        Ok((
            RuntimeContext::new(
                ServiceInfo::new_with_runtime_identity(
                    "monitor-fire-once-test".to_string(),
                    "test.monitor".to_string(),
                    Some("test.monitor".to_string()),
                    Some("hosted source binding".to_string()),
                    HostName::from_static("test-host"),
                    work_dir_path,
                    false,
                    format!("instance-{}", Uuid::now_v7().simple()),
                    env!("CARGO_PKG_VERSION").to_string(),
                    None,
                ),
                handles,
                HashMap::new(),
                work_dir_utf8,
            ),
            event_receiver,
        ))
    }
}

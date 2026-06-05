//! Module lifecycle, cancellation, and startup/shutdown ordering.
//!
//! `sinexd` is a single daemon hosting the event engine (admission +
//! persistence + confirmation), the operator API, the enabled
//! automata, and the configured source bindings. Each module starts
//! as a tokio task under the supervisor. The shutdown signal is sourced from
//! `crate::runtime::service_runtime::spawn_shutdown_task` which handles
//! SIGINT/SIGTERM; tasks observe it via a shared `watch` receiver and unwind
//! in reverse start order.

use std::time::Duration;

use crate::runtime::service_runtime;
use crate::runtime::systemd_notify;
use sinex_primitives::error::{Result, SinexError};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::api::config::GatewayConfig;
use crate::api::rpc_server;
use crate::api::service_container::ServiceContainer;
use crate::automata::registry::{self as automata_registry, AutomatonSpec};
use crate::event_engine::{EventEngineConfig, IngestService};
use crate::sources::bindings::{self as source_bindings, SourceBinding};

/// Environment variable selecting which automata `sinexd` hosts.
///
/// Comma-separated list of automaton names, or the literal `all`. Unknown
/// names fail startup. Unset / empty disables every automaton.
const ENV_AUTOMATA_ENABLED: &str = "SINEX_AUTOMATA_ENABLED";

/// Environment variable pointing at the source-bindings manifest JSON.
///
/// Unset / empty means no source bindings are hosted in this `sinexd`
/// instance (used during single-binary local development against an
/// out-of-band source, for example).
const ENV_SOURCE_BINDINGS_PATH: &str = "SINEX_SOURCE_BINDINGS_PATH";

#[derive(Debug)]
pub struct Supervisor {
    pub event_engine_enabled: bool,
    pub api_enabled: bool,
}

impl Default for Supervisor {
    fn default() -> Self {
        Self {
            event_engine_enabled: true,
            api_enabled: true,
        }
    }
}

impl Supervisor {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn run(
        self,
        event_engine_config: EventEngineConfig,
        api_config: GatewayConfig,
    ) -> Result<()> {
        info!("sinexd starting");

        // Set hosted mode BEFORE spawning any subsystem tasks. In-process
        // automata and source bindings must NOT send sd_notify messages
        // to systemd — only this top-level supervisor speaks for the unit.
        // Notably, fire-once monitor bindings emit STOPPING=1 on clean
        // exit; without this latch they would tell systemd the whole sinexd
        // daemon is shutting down.
        //
        // SAFETY: called before any tokio::spawn, so no concurrent access
        // to the environment.
        systemd_notify::enter_hosted_mode();

        let os_shutdown_rx = service_runtime::spawn_shutdown_task("sinexd");

        // Separate in-process escalation channel for event-engine / API
        // task failures. The OS shutdown channel is Receiver-only and shared
        // across all runtime consumers; rather than extending that API (which
        // would propagate through every callsite), we keep a local `watch`
        // whose Sender lives here and whose Receiver is selected alongside
        // the OS receiver in the wait loop. When event-engine or API exits,
        // the wrapper fires escalate_tx — same teardown path as SIGTERM.
        let (escalate_tx, escalate_rx) = watch::channel(false);
        let shutdown_rx = os_shutdown_rx.clone();

        let mut event_engine_handle = if self.event_engine_enabled {
            Some(start_event_engine(
                event_engine_config,
                shutdown_rx.clone(),
                escalate_tx.clone(),
            ))
        } else {
            None
        };

        // If API setup fails after the event-engine task has already been
        // spawned, the engine task would otherwise be orphaned: its
        // JoinHandle would be dropped (detach, not cancel), the supervisor
        // would return before entering the shutdown select! loop, and the
        // engine would keep holding its DB pool + NATS consumer until the
        // process was SIGKILL'd. Signal escalation to drain it, then return.
        let api_handle = if self.api_enabled {
            match start_api(api_config, shutdown_rx.clone(), escalate_tx.clone()).await {
                Ok(handle) => Some(handle),
                Err(error) => {
                    error!(
                        ?error,
                        phase = "api-setup-failed",
                        "API setup failed; tearing down already-started modules"
                    );
                    let _ = escalate_tx.send(true);
                    if let Some(handle) = event_engine_handle {
                        match tokio::time::timeout(Duration::from_secs(5), handle).await {
                            Ok(Ok(())) => {
                                info!("event_engine drained after API setup failure");
                            }
                            Ok(Err(join_error)) => {
                                error!(
                                    ?join_error,
                                    "event_engine task join error during API-failure teardown"
                                );
                            }
                            Err(_elapsed) => {
                                warn!(
                                    "event_engine did not drain within 5s of API setup failure; detaching"
                                );
                            }
                        }
                    }
                    return Err(error);
                }
            }
        } else {
            None
        };

        // Hosted automata. Each runs as an independent supervisor task so a
        // single automaton crash does not take down siblings or the daemon.
        let automaton_handles = start_automata(shutdown_rx.clone())?;

        // Hosted source bindings. Same isolation property: one
        // binding crash is logged and contained, sibling captures continue.
        let source_binding_handles = start_source_bindings(shutdown_rx.clone())?;

        info!(
            automata = automaton_handles.len(),
            source_bindings = source_binding_handles.len(),
            "sinexd running"
        );

        // Use the unhosted variant — `enter_hosted_mode` set the latch so
        // in-process bindings stop calling sd_notify, but the supervisor
        // itself still needs to talk to systemd.
        systemd_notify::notify_ready_unhosted("sinexd");
        let watchdog = systemd_notify::spawn_watchdog_unhosted("sinexd");

        // Monitor the event engine for unexpected exits. If the engine
        // crashes or exits cleanly before the supervisor signals shutdown,
        // log the error AND fire escalate_tx so the wait loop teardown runs.
        // The monitor takes ownership of the JoinHandle; during the
        // shutdown join below we await the monitor instead.
        let ee_monitor = if let Some(handle) = event_engine_handle.take() {
            let escalate_tx_ee = escalate_tx.clone();
            Some(tokio::spawn(async move {
                match handle.await {
                    Ok(()) => {
                        error!("event engine exited unexpectedly before shutdown signal");
                    }
                    Err(error) => {
                        error!(
                            ?error,
                            "event engine task panicked or was cancelled before shutdown signal"
                        );
                    }
                }
                let _ = escalate_tx_ee.send(true);
            }))
        } else {
            None
        };

        let mut os_shutdown_rx = os_shutdown_rx;
        let mut escalate_rx = escalate_rx;
        tokio::select! {
            _ = os_shutdown_rx.changed() => {
                info!("shutdown requested (OS signal)");
            }
            _ = escalate_rx.changed() => {
                error!("shutdown requested (core module crashed — escalated)");
            }
        }
        systemd_notify::notify_stopping_unhosted("sinexd");
        systemd_notify::stop_watchdog(watchdog, "sinexd").await;

        // Unwind in reverse start order: source bindings → automata → api →
        // event_engine. Source bindings publish into the event engine, so
        // they must finish draining before the engine can stop accepting.
        // The event engine handle was consumed by the crash monitor above;
        // awaiting the monitor preserves the reverse-start-order join.
        for (label, handle) in source_binding_handles.into_iter().rev() {
            if let Err(error) = handle.await {
                error!(source_binding = %label, ?error, "source-binding task join error");
            }
        }
        for (name, handle) in automaton_handles.into_iter().rev() {
            if let Err(error) = handle.await {
                error!(automaton = %name, ?error, "automaton task join error");
            }
        }
        if let Some(handle) = api_handle
            && let Err(error) = handle.await
        {
            error!(?error, "api task join error");
        }
        if let Some(monitor) = ee_monitor
            && let Err(error) = monitor.await
        {
            error!(?error, "event engine monitor task join error");
        }

        info!("sinexd stopped");
        Ok(())
    }
}

fn start_event_engine(
    config: EventEngineConfig,
    shutdown_rx: watch::Receiver<bool>,
    escalate_tx: watch::Sender<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut service = match IngestService::new(config).await {
            Ok(s) => s,
            Err(error) => {
                error!(
                    ?error,
                    "IngestService::new failed — escalating to daemon shutdown"
                );
                let _ = escalate_tx.send(true);
                return;
            }
        };
        // Wire the supervisor shutdown signal into the event engine's
        // internal (AtomicBool + Notify) shutdown pair. IngestService
        // dispatches a Clone that shares the same Arcs, so calling
        // shutdown() on the clone sets the flag that run() observes.
        let mut service_for_shutdown = service.clone();
        let mut shutdown_rx_for_signal = shutdown_rx.clone();
        tokio::spawn(async move {
            let _ = shutdown_rx_for_signal.changed().await;
            let _ = service_for_shutdown.shutdown().await;
        });

        let outcome = service.run().await;
        // If the supervisor already triggered teardown, an Ok exit is the
        // intended outcome; don't re-escalate or log noisily. We only fire
        // the escalation channel when the task exits *without* a shutdown
        // having been observed first — that's the failure mode this guard
        // exists to catch.
        let shutdown_now = *shutdown_rx.borrow() || *escalate_tx.borrow();
        match outcome {
            Ok(()) if shutdown_now => {
                info!("IngestService::run exited after shutdown");
            }
            Ok(()) => {
                warn!("IngestService::run returned unexpectedly — escalating to daemon shutdown");
                let _ = escalate_tx.send(true);
            }
            Err(error) => {
                error!(
                    ?error,
                    "IngestService::run failed — escalating to daemon shutdown"
                );
                let _ = escalate_tx.send(true);
            }
        }
    })
}

async fn start_api(
    config: GatewayConfig,
    shutdown_rx: watch::Receiver<bool>,
    escalate_tx: watch::Sender<bool>,
) -> Result<tokio::task::JoinHandle<()>> {
    let services = ServiceContainer::new(&config).await.map_err(|error| {
        SinexError::service("failed to construct ServiceContainer").with_std_error(&error)
    })?;
    Ok(tokio::spawn(async move {
        // For rpc_server we have to hand the receiver in (it observes
        // shutdown internally), so clone for the post-exit check.
        let post_exit_rx = shutdown_rx.clone();
        let outcome = rpc_server::run(&config, services, shutdown_rx).await;
        let shutdown_now = *post_exit_rx.borrow() || *escalate_tx.borrow();
        match outcome {
            Ok(()) if shutdown_now => {
                info!("rpc_server::run exited after shutdown");
            }
            Ok(()) => {
                warn!("rpc_server::run returned unexpectedly — escalating to daemon shutdown");
                let _ = escalate_tx.send(true);
            }
            Err(error) => {
                error!(
                    ?error,
                    "rpc_server::run failed — escalating to daemon shutdown"
                );
                let _ = escalate_tx.send(true);
            }
        }
    }))
}

/// Start each automaton enabled via `SINEX_AUTOMATA_ENABLED`.
///
/// Returns the spawned handles paired with the automaton name for log
/// attribution during the shutdown join. The shared shutdown `watch` is
/// dropped into each task so that automaton-internal lifecycle code can
/// observe it through the runtime if needed (currently the runtime keys
/// off the OS shutdown signal directly, but holding a clone keeps the
/// channel alive for the duration of the task).
fn start_automata(
    shutdown_rx: watch::Receiver<bool>,
) -> Result<Vec<(&'static str, JoinHandle<()>)>> {
    let raw = std::env::var(ENV_AUTOMATA_ENABLED).ok();
    // Default to all automata when unset — the entity/relation/document
    // automata are implemented and should activate by default (#1087).
    // Set SINEX_AUTOMATA_ENABLED= (empty) to explicitly disable.
    let effective = if raw
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .is_none()
    {
        Some("all")
    } else {
        raw.as_deref()
    };
    let selected = automata_registry::parse_enabled(effective)?;

    if selected.is_empty() {
        info!("no automata enabled (SINEX_AUTOMATA_ENABLED set to empty)");
        return Ok(Vec::new());
    }

    info!(
        count = selected.len(),
        automata = ?selected.iter().map(|spec| spec.name).collect::<Vec<_>>(),
        "starting hosted automata"
    );

    let mut handles = Vec::with_capacity(selected.len());
    for spec in selected {
        let handle = spawn_automaton(spec, shutdown_rx.clone());
        handles.push((spec.name, handle));
    }
    Ok(handles)
}

fn spawn_automaton(
    spec: &'static AutomatonSpec,
    shutdown_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Keep the shutdown receiver alive in the task scope so the
        // `watch::Sender` half cannot observe a premature drop.
        let _shutdown_rx = shutdown_rx;
        let future = (spec.run)();
        if let Err(error) = future.await {
            warn!(automaton = %spec.name, ?error, "automaton exited with error");
        } else {
            info!(automaton = %spec.name, "automaton exited");
        }
    })
}

/// Load `SINEX_SOURCE_BINDINGS_PATH` and spawn one supervisor task per
/// enabled binding.
fn start_source_bindings(
    shutdown_rx: watch::Receiver<bool>,
) -> Result<Vec<(String, JoinHandle<()>)>> {
    let Some(manifest) = source_bindings::load_from_env(ENV_SOURCE_BINDINGS_PATH)? else {
        info!("no source bindings configured (SINEX_SOURCE_BINDINGS_PATH unset)");
        return Ok(Vec::new());
    };

    if manifest.bindings.is_empty() {
        info!("source-bindings manifest contains zero bindings");
        return Ok(Vec::new());
    }

    // Validate every binding up front so a misconfigured deployment fails
    // immediately rather than after the first partial spawn.
    source_bindings::validate_bindings(&manifest.bindings)?;

    info!(
        count = manifest.bindings.len(),
        "starting hosted source bindings"
    );

    let mut handles = Vec::with_capacity(manifest.bindings.len());
    for binding in manifest.bindings {
        let label = format!("{}-{}", binding.source_id, binding.instance_idx);
        let handle = spawn_source_binding(binding, shutdown_rx.clone());
        handles.push((label, handle));
    }
    Ok(handles)
}

fn spawn_source_binding(
    binding: SourceBinding,
    shutdown_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    let label = format!("{}-{}", binding.source_id, binding.instance_idx);
    tokio::spawn(async move {
        let _shutdown_rx = shutdown_rx;
        match source_bindings::run_binding(binding).await {
            Ok(()) => info!(source_binding = %label, "source host exited"),
            Err(error) => warn!(
                source_binding = %label,
                ?error,
                "source host exited with error"
            ),
        }
    })
}

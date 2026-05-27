//! Module lifecycle, cancellation, and startup/shutdown ordering.
//!
//! `sinexd` is a single daemon hosting the event engine (admission +
//! persistence + confirmation), the operator API, the enabled derived-node
//! automata, and the configured source-worker bindings. Each module starts
//! as a tokio task under the supervisor. The shutdown signal is sourced from
//! `sinex_node_sdk::service_runtime::spawn_shutdown_task` which handles
//! SIGINT/SIGTERM; tasks observe it via a shared `watch` receiver and unwind
//! in reverse start order.

use sinex_node_sdk::service_runtime;
use sinex_primitives::error::{Result, SinexError};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::api::config::GatewayConfig;
use crate::api::rpc_server;
use crate::api::service_container::ServiceContainer;
use crate::automata::registry::{self as automata_registry, AutomatonSpec};
use crate::event_engine::{IngestService, IngestdConfig};

/// Environment variable selecting which automata `sinexd` hosts.
///
/// Comma-separated list of automaton names, or the literal `all`. Unknown
/// names fail startup. Unset / empty disables every automaton.
const ENV_AUTOMATA_ENABLED: &str = "SINEX_AUTOMATA_ENABLED";

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
        event_engine_config: IngestdConfig,
        api_config: GatewayConfig,
    ) -> Result<()> {
        info!("sinexd starting");

        let shutdown_rx = service_runtime::spawn_shutdown_task("sinexd");

        let event_engine_handle = if self.event_engine_enabled {
            Some(start_event_engine(event_engine_config, shutdown_rx.clone()))
        } else {
            None
        };

        let api_handle = if self.api_enabled {
            Some(start_api(api_config, shutdown_rx.clone()).await?)
        } else {
            None
        };

        // Hosted automata. Each runs as an independent supervisor task so a
        // single automaton crash does not take down siblings or the daemon.
        let automaton_handles = start_automata(shutdown_rx.clone())?;

        info!(
            automata = automaton_handles.len(),
            "sinexd running"
        );

        let mut shutdown_rx = shutdown_rx;
        let _ = shutdown_rx.changed().await;
        info!("shutdown requested");

        // Unwind in reverse start order: automata → api → event_engine.
        for (name, handle) in automaton_handles.into_iter().rev() {
            if let Err(error) = handle.await {
                error!(automaton = %name, ?error, "automaton task join error");
            }
        }
        if let Some(handle) = api_handle {
            if let Err(error) = handle.await {
                error!(?error, "api task join error");
            }
        }
        if let Some(handle) = event_engine_handle {
            if let Err(error) = handle.await {
                error!(?error, "event_engine task join error");
            }
        }

        info!("sinexd stopped");
        Ok(())
    }
}

fn start_event_engine(
    config: IngestdConfig,
    shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut service = match IngestService::new(config).await {
            Ok(s) => s,
            Err(error) => {
                error!(?error, "IngestService::new failed");
                return;
            }
        };
        let _ = shutdown_rx;
        if let Err(error) = service.run().await {
            error!(?error, "IngestService::run failed");
        }
    })
}

async fn start_api(
    config: GatewayConfig,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<tokio::task::JoinHandle<()>> {
    let services = ServiceContainer::new(&config).await.map_err(|error| {
        SinexError::service("failed to construct ServiceContainer").with_std_error(&error)
    })?;
    Ok(tokio::spawn(async move {
        if let Err(error) = rpc_server::run(&config, services, shutdown_rx).await {
            error!(?error, "rpc_server::run failed");
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
    let selected = automata_registry::parse_enabled(raw.as_deref())?;

    if selected.is_empty() {
        info!("no automata enabled (SINEX_AUTOMATA_ENABLED unset or empty)");
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

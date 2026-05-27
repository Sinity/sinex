//! Module lifecycle, cancellation, and startup/shutdown ordering.
//!
//! Modules start in dependency order (event_engine writes first, api serves
//! second) and stop in reverse on shutdown. The shutdown signal is sourced
//! from `sinex_node_sdk::service_runtime::spawn_shutdown_task` which handles
//! SIGINT/SIGTERM.

use sinex_node_sdk::service_runtime;
use sinex_primitives::error::{Result, SinexError};
use tokio::sync::watch;
use tracing::{error, info};

use crate::api::config::GatewayConfig;
use crate::api::rpc_server;
use crate::api::service_container::ServiceContainer;
use crate::event_engine::{IngestService, IngestdConfig};

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

        info!("sinexd running");

        let mut shutdown_rx = shutdown_rx;
        let _ = shutdown_rx.changed().await;
        info!("shutdown requested");

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

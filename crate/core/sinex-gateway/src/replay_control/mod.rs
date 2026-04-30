#![doc = include_str!("../../docs/replay_control.md")]

mod client;
mod execution;
mod protocol;
mod server;
mod telemetry;
mod validation;

#[cfg(test)]
mod tests;

pub use client::ReplayControlClient;
pub use protocol::{
    ReplayControlErrorKind, ReplayControlRequest, ReplayControlResponse, ReplayControlStatus,
};
pub use telemetry::ReplayTelemetrySnapshot;

use execution::ReplayExecutionEngine;
use telemetry::ReplayTelemetry;

use async_nats::Client;
use color_eyre::eyre::Result;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
pub use sinex_db::replay::state_machine::ReplayScope;
use sinex_db::replay::state_machine::ReplayStateMachine;
use sinex_primitives::environment::environment;
use sinex_primitives::Timestamp;
use std::sync::Arc;
use std::time::Duration;


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayControlError {
    pub message: String,
    pub occurred_at: Timestamp,
}

impl ReplayControlError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            occurred_at: Timestamp::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayControlHealth {
    pub connected: bool,
    pub last_error: Option<ReplayControlError>,
}

#[derive(Debug, Default)]
pub(super) struct ReplayControlHealthState {
    pub(super) last_error: Option<ReplayControlError>,
    pub(super) server_subscribed: bool,
}

/// Spawn the replay control bus and return a client handle.
///
/// The replay control system manages distributed replay operations, coordinating
/// event re-processing across the cluster with proper state tracking and locking.
pub async fn spawn_replay_control(
    replay: Arc<ReplayStateMachine>,
    client: Client,
    request_timeout: Duration,
) -> Result<ReplayControlClient> {
    let env = environment().clone();
    let health = Arc::new(Mutex::new(ReplayControlHealthState::default()));

    // Create execution engine with NATS client for node-dispatch replay control
    let executor = ReplayExecutionEngine::new(replay.clone(), client.clone());
    ReplayTelemetry::new(replay.clone()).spawn();

    server::ReplayControlServer::new(&env, client.clone(), replay, executor, Arc::clone(&health))
        .spawn()
        .await?;

    Ok(ReplayControlClient::new(
        &env,
        client,
        request_timeout,
        health,
    ))
}




#![doc = include_str!("../../docs/replay_control.md")]

mod client;
mod engine;
mod expected_outputs;
mod protocol;
mod server;
mod telemetry;
mod validation;

#[cfg(test)]
mod tests;

use color_eyre::eyre::Result;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

use async_nats::Client;
use sinex_db::replay::state_machine::ReplayStateMachine;
use sinex_primitives::Timestamp;
use sinex_primitives::environment::environment;

pub use sinex_db::replay::state_machine::ReplayScope;

pub use client::ReplayControlClient;
pub use protocol::{
    ReplayControlErrorKind, ReplayControlRequest, ReplayControlResponse, ReplayControlStatus,
};
pub use telemetry::ReplayTelemetrySnapshot;

use engine::ReplayExecutionEngine;
use server::ReplayControlServer;
use telemetry::ReplayTelemetry;

// Bring submodule contents into scope for the test module (`use super::*`).
#[cfg(test)]
#[allow(unused_imports)]
use {
    client::*, engine::*, expected_outputs::*, protocol::*, server::*, telemetry::*, validation::*,
};

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
pub(crate) struct ReplayControlHealthState {
    pub(crate) last_error: Option<ReplayControlError>,
    pub(crate) server_subscribed: bool,
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

    ReplayControlServer::new(&env, client.clone(), replay, executor, Arc::clone(&health))
        .spawn()
        .await?;

    Ok(ReplayControlClient::new(
        &env,
        client,
        request_timeout,
        health,
    ))
}

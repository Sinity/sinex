//! Confirmed-event consumption primitives.
//!
//! Automata consume the FINAL post-redaction `Event<JsonValue>` payload directly
//! from the confirmed-events stream (the #2187 confirmed-delivery redesign,
//! "Option C"). This module holds the two small shared types for that path: the
//! processing model and the confirmed-event handler trait.
//!
//! The historical provisional-delivery machinery — a per-automaton raw-events
//! consumer feeding an in-memory provisional buffer, a separate compacted
//! acknowledgement consumer, the timeout/grace sweep, and the Postgres refetch /
//! `#2202` provisional fallback — was deleted with the redesign. Each automaton
//! now opens ONE durable consumer on the confirmed-events stream and receives
//! the authoritative event directly: no buffer, no refetch, no
//! commit/confirmation visibility race.

use crate::runtime::RuntimeResult;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sinex_primitives::JsonValue;
use sinex_primitives::events::Event;

/// Processing model for automata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProcessingModel {
    /// Leader/standby with a single active runtime module.
    /// Uses NATS KV leases for coordination.
    LeaderStandby,
    /// Stateless workers processing confirmed events.
    /// Multiple instances can run in parallel.
    StatelessWorker,
}

/// Handler for confirmed events.
///
/// Called once per event delivered on the confirmed-events stream, after the
/// event has been persisted to the database and redacted by the event engine.
/// The handler receives the full `Event<JsonValue>` exactly as persisted.
#[async_trait]
pub trait ConfirmedEventHandler: Send + Sync {
    /// Process a confirmed (persisted + redacted) event.
    async fn handle_confirmed(&self, event: &Event<JsonValue>) -> RuntimeResult<()>;
}

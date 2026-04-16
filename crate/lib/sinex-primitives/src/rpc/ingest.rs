//! Event ingest RPC types

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ─────────────────────────────────────────────────────────────
// events.ingest
// ─────────────────────────────────────────────────────────────

/// Request: events.ingest
///
/// Publishes a single event directly to the NATS `JetStream` raw event stream.
/// The gateway forwards it to the appropriate subject based on `source` and
/// `event_type`. The event is assigned a new `UUIDv7` ID by the gateway and
/// returned in the response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventIngestRequest {
    /// Event source identifier (e.g. `"fs-watcher"`)
    pub source: String,
    /// Event type string (e.g. `"file.created"`)
    pub event_type: String,
    /// Arbitrary JSON payload
    pub payload: Value,
    /// Explicit RFC 3339 original timestamp.
    pub ts_orig: String,
    /// Optional host override. Defaults to gateway's machine identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
}

/// Response: events.ingest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventIngestResponse {
    /// The `UUIDv7` assigned to the published event
    pub event_id: String,
    /// NATS `JetStream` sequence number the message was stored at
    pub sequence: u64,
}

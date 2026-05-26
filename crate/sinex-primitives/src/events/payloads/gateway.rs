//! Gateway-emitted self-observation payloads.
//!
//! Issue #1172 AC-7: every RPC call (foreground or batch member) becomes a
//! `gateway.rpc.call` event so operators can replay traffic from the event
//! store with the same tooling they use for everything else.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

/// Outcome class for a finished RPC call.
///
/// Mirrors the existing `AccessOutcome` audit values, but lives at the
/// payload layer so it serializes deterministically into the event store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RpcStatus {
    /// Method dispatch returned `Ok(_)`.
    Success,
    /// Token failed authentication.
    Unauthenticated,
    /// Authenticated, but token role rejected (e.g. RBAC).
    Rejected,
    /// Token bucket exhausted.
    RateLimited,
    /// JSON-RPC envelope or schema validation rejected the request.
    InvalidRequest,
    /// Method dispatch returned `Err(_)` for any other reason.
    Failed,
}

/// Audit payload for a single completed RPC dispatch (#1172 AC-7).
///
/// Privacy: only the `token_prefix` (first 8 chars of the bearer token) is
/// recorded — never the full token. The `Role` is safe to record because it
/// describes the request's authorization band, not the bearer.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex.gateway", event_type = "gateway.rpc.call")]
pub struct GatewayRpcCallPayload {
    /// JSON-RPC method name (e.g. `events.query`, `lifecycle.archive`).
    pub method: String,
    /// Authenticated role on the request, lowercased: `readonly`, `write`,
    /// or `admin`. (We use a string here rather than a typed enum because
    /// `Role` lives in `sinex-gateway` and can't be imported by primitives
    /// without inverting the dependency direction.)
    pub role: String,
    /// Wall-clock dispatch latency in milliseconds.
    pub latency_ms: u64,
    /// Outcome classification.
    pub status: RpcStatus,
    /// First 8 characters of the bearer token used for the request, or
    /// `system` for trusted local calls. Never the full token.
    pub token_prefix: String,
}

//! RPC (Remote Procedure Call) event payloads

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "rpc.content", event_type = "rpc.response")]
pub struct RpcContentResponsePayload {
    pub request_id: Option<serde_json::Value>,
    pub response: Option<serde_json::Value>,
    pub error: Option<RpcError>,
    pub service: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "rpc.pkm", event_type = "rpc.response")]
pub struct RpcPkmResponsePayload {
    pub request_id: Option<serde_json::Value>,
    pub response: Option<serde_json::Value>,
    pub error: Option<RpcError>,
    pub service: String,
}

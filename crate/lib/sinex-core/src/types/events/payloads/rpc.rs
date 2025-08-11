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

impl RpcContentResponsePayload {
    /// Builder-style method for request ID
    pub fn with_request_id(mut self, request_id: serde_json::Value) -> Self {
        self.request_id = Some(request_id);
        self
    }

    /// Builder-style method for response
    pub fn with_response(mut self, response: serde_json::Value) -> Self {
        self.response = Some(response);
        self
    }

    /// Builder-style method for error
    pub fn with_error(mut self, code: i32, message: impl Into<String>) -> Self {
        self.error = Some(RpcError {
            code,
            message: message.into(),
        });
        self
    }
}

impl RpcPkmResponsePayload {
    /// Builder-style method for request ID
    pub fn with_request_id(mut self, request_id: serde_json::Value) -> Self {
        self.request_id = Some(request_id);
        self
    }

    /// Builder-style method for response
    pub fn with_response(mut self, response: serde_json::Value) -> Self {
        self.response = Some(response);
        self
    }

    /// Builder-style method for error
    pub fn with_error(mut self, code: i32, message: impl Into<String>) -> Self {
        self.error = Some(RpcError {
            code,
            message: message.into(),
        });
        self
    }
}

use super::*;

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct JsonRpcRequest {
    pub(super) jsonrpc: String,
    pub(super) method: String,
    #[serde(default)]
    pub(super) params: Value,
    pub(super) id: Option<Value>,
}

pub(crate) fn validate_jsonrpc_request(request: &JsonRpcRequest) -> SinexResult<()> {
    if request.jsonrpc != "2.0" {
        return Err(SinexError::validation("jsonrpc must be '2.0'"));
    }
    if request.method.trim().is_empty() {
        return Err(SinexError::validation("method must be a non-empty string"));
    }
    match request.params {
        Value::Object(_) | Value::Array(_) | Value::Null => Ok(()),
        _ => Err(SinexError::validation(
            "params must be an object, array, or null",
        )),
    }
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct JsonRpcResponse {
    pub(super) jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) error: Option<JsonRpcError>,
    pub(super) id: Option<Value>,
}

/// Map `SinexError` variants to JSON-RPC error codes and client-safe messages.
///
/// Code ranges follow JSON-RPC 2.0 conventions:
/// - -32700 to -32600: Protocol errors (parse, invalid request, etc.)
/// - -32099 to -32000: Server errors (reserved)
/// - -32899 to -32800: Application errors (custom)
///
/// Messages are produced via `SinexError::client_message()` — client errors surface
/// their authored primary message; server-internal errors return generic category strings.
/// Context, source chains, and infrastructure details never reach the caller.
pub(super) fn sinex_error_to_rpc_code(
    err: &sinex_primitives::error::SinexError,
) -> (i32, sinex_primitives::error::PublicError) {
    use sinex_primitives::error::SinexError;

    let public = err.public_payload();
    let code = match err {
        // ── Client errors ──
        SinexError::Validation(_) => -32800,
        SinexError::NotFound(_) => -32801,
        SinexError::AlreadyExists(_) => -32802,
        SinexError::InvalidState(_) => -32803,
        SinexError::PermissionDenied(_) => -32804,
        SinexError::Parse(_) => -32805,

        // ── Server-internal errors ──
        SinexError::Database(_) | SinexError::DbPersistenceFailed(_) => -32810,
        SinexError::Network(_) => -32811,
        SinexError::Timeout(_) => -32812,
        SinexError::ResourceExhausted(_) => -32813,

        SinexError::Service(_) => -32820,
        SinexError::Io(_) => -32821,
        SinexError::Configuration(_) => -32822,
        SinexError::Serialization(_) => -32823,

        SinexError::Cancelled(_) => -32830,
        SinexError::MaxRetriesExceeded(_) => -32831,

        SinexError::ChannelSend(_) | SinexError::ChannelReceive(_) => -32840,

        SinexError::Kv(_)
        | SinexError::Automaton(_)
        | SinexError::Checkpoint(_)
        | SinexError::Lifecycle(_)
        | SinexError::Processing(_) => -32850,

        SinexError::BlobStorage(_) => -32860,
        SinexError::Coordination(_) => -32861,

        // NATS-specific variants from sinex-primitives.
        SinexError::Nats(_)
        | SinexError::NatsAckFailed(_)
        | SinexError::NatsPublish(_)
        | SinexError::NatsSubscribe(_) => -32870,

        SinexError::Unknown(_) => -32899,

        // Required by #[non_exhaustive]. If you added a new SinexError variant and
        // reached this arm, add an explicit mapping above with a dedicated error code.
        _ => {
            tracing::warn!(
                variant = err.variant_name(),
                "Unmapped SinexError variant in RPC error code mapping"
            );
            -32603
        }
    };

    (code, public)
}

pub(super) fn rpc_error_data(
    error_id: Uuid,
    public: &sinex_primitives::error::PublicError,
    _err: &sinex_primitives::error::SinexError,
) -> Value {
    #[cfg(feature = "dev-errors")]
    {
        serde_json::json!({
            "error_id": error_id.to_string(),
            "public": public,
            "error": _err,
        })
    }

    #[cfg(not(feature = "dev-errors"))]
    {
        serde_json::json!({
            "error_id": error_id.to_string(),
            "kind": public.kind,
            "kind_name": public.kind_name.as_str(),
            "status_code": public.status_code,
            "context": public.context.clone(),
        })
    }
}

impl JsonRpcResponse {
    pub(super) fn success(id: Option<Value>, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: Some(result),
            error: None,
            id,
        }
    }

    pub(super) fn error(id: Option<Value>, code: i32, message: String) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(JsonRpcError {
                code,
                message,
                data: None,
            }),
            id,
        }
    }

    pub(super) fn error_with_data(
        id: Option<Value>,
        code: i32,
        message: String,
        data: Value,
    ) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(JsonRpcError {
                code,
                message,
                data: Some(data),
            }),
            id,
        }
    }
}

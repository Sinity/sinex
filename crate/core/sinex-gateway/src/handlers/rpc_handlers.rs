//! Shared RPC helpers and replay method handlers.

use crate::replay_control::ReplayControlClient;
use crate::rpc_server::RpcAuthContext;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde_json::Value;
use sinex_db::replay::state_machine::{
    ReplayOperation as DbReplayOperation, ReplayScope, ReplayState,
};
use sinex_primitives::rpc::content::RetrieveBlobResponse;
use sinex_primitives::rpc::replay::{
    ReplayApproveResponse, ReplayCancelResponse, ReplayCreateResponse, ReplayExecuteResponse,
    ReplayListResponse, ReplayOperation, ReplayPreviewResponse, ReplayStatusResponse,
    ReplaySubmitResponse,
};
use sinex_primitives::{Id, Result, SinexError, Uuid, domain::Entity};

pub(crate) struct RpcParams<'a> {
    inner: &'a Value,
}

impl<'a> RpcParams<'a> {
    pub(crate) fn new(inner: &'a Value) -> Self {
        Self { inner }
    }

    pub(crate) fn require_str(&self, key: &str) -> Result<&'a str> {
        self.inner
            .get(key)
            .and_then(|v| v.as_str())
            .ok_or_else(|| missing_or_invalid_param(key, "string"))
    }

    pub(crate) fn optional_str(&self, key: &str) -> Result<Option<&'a str>> {
        match self.inner.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(value) => value
                .as_str()
                .map(Some)
                .ok_or_else(|| invalid_param_type(key, "string")),
        }
    }

    pub(crate) fn optional_bool(&self, key: &str) -> Result<Option<bool>> {
        match self.inner.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(value) => value
                .as_bool()
                .map(Some)
                .ok_or_else(|| invalid_param_type(key, "boolean")),
        }
    }

    pub(crate) fn optional_array(&self, key: &str) -> Result<Option<&'a [Value]>> {
        match self.inner.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(value) => value
                .as_array()
                .map(|items| Some(items.as_slice()))
                .ok_or_else(|| invalid_param_type(key, "array")),
        }
    }

    pub(crate) fn optional_object(
        &self,
        key: &str,
    ) -> Result<Option<&'a serde_json::Map<String, Value>>> {
        match self.inner.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(value) => value
                .as_object()
                .map(Some)
                .ok_or_else(|| invalid_param_type(key, "object")),
        }
    }

    pub(crate) fn optional_i64(&self, key: &str) -> Result<Option<i64>> {
        match self.inner.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(value) => value
                .as_i64()
                .map(Some)
                .ok_or_else(|| invalid_param_type(key, "integer")),
        }
    }

    pub(crate) fn require_value(&self, key: &str) -> Result<&'a Value> {
        self.inner.get(key).ok_or_else(|| {
            SinexError::validation("missing parameter").with_context("parameter", key)
        })
    }

    pub(crate) fn require_uuid(&self, key: &str) -> Result<Uuid> {
        let value = self.require_str(key)?;
        value.parse::<Uuid>().map_err(|error| {
            SinexError::validation("invalid UUIDv7 parameter")
                .with_context("parameter", key)
                .with_context("value", value)
                .with_std_error(&error)
        })
    }
}

fn missing_or_invalid_param(key: &str, expected_type: &str) -> SinexError {
    SinexError::validation("missing or invalid parameter")
        .with_context("parameter", key)
        .with_context("expected_type", expected_type)
}

fn invalid_param_type(key: &str, expected_type: &str) -> SinexError {
    SinexError::validation("invalid parameter type")
        .with_context("parameter", key)
        .with_context("expected_type", expected_type)
}

// Default values for content/blob handling
pub(crate) const DEFAULT_BLOB_FILENAME: &str = "content.txt";
pub(crate) const DEFAULT_BLOB_CONTENT_TYPE: &str = "text/plain";

pub(crate) fn decode_note_content(base64_content: &str) -> Result<String> {
    let decoded_bytes = BASE64_STANDARD.decode(base64_content).map_err(|error| {
        SinexError::serialization("Invalid base64 content").with_std_error(&error)
    })?;

    String::from_utf8(decoded_bytes).map_err(|error| {
        SinexError::serialization("Decoded note content is not valid UTF-8").with_std_error(&error)
    })
}

pub(crate) fn validate_entity_name(name: &str) -> Result<()> {
    if name.trim().is_empty() {
        return Err(SinexError::validation("Entity name cannot be empty"));
    }
    if name.len() > 255 {
        return Err(
            SinexError::validation("Entity name cannot exceed 255 characters")
                .with_context("max_len", 255)
                .with_context("actual_len", name.len()),
        );
    }
    if name.contains(';') || name.contains("--") || name.contains("/*") {
        return Err(SinexError::validation(
            "Entity name contains invalid characters",
        ));
    }
    Ok(())
}

pub(crate) fn validate_entity_link_ids(from: &Id<Entity>, to: &Id<Entity>) -> Result<()> {
    if from == to {
        return Err(SinexError::validation("Cannot link entity to itself"));
    }
    Ok(())
}

/// Decode base64 blob content with size validation
///
/// # Issue 144 (LOW): Base64 Expansion and Body Limits
///
/// Base64 encoding expands data by ~1.33x (4 chars per 3 bytes). When handling
/// blob uploads via RPC, ensure:
///
/// - `SINEX_GATEWAY_MAX_BODY_BYTES` >= `SINEX_GATEWAY_MAX_BLOB_BYTES` * 1.4
///   (1.4 accounts for base64 overhead plus JSON envelope)
///
/// Default configuration:
/// - Body limit: 2MB (`SINEX_GATEWAY_MAX_BODY_BYTES`)
/// - Blob limit: 5MB (`SINEX_GATEWAY_MAX_BLOB_BYTES`)
///
/// This mismatch is intentional: the body limit applies to the raw HTTP request,
/// while the blob limit applies to decoded content. For large blobs, clients should
/// increase `SINEX_GATEWAY_MAX_BODY_BYTES` proportionally.
pub(crate) fn decode_blob_content(content_b64: &str, limit: usize) -> Result<Vec<u8>> {
    let max_encoded = max_base64_length(limit);
    if content_b64.len() > max_encoded {
        return Err(blob_size_error(limit, content_b64.len(), "encoded"));
    }

    let content = BASE64_STANDARD.decode(content_b64).map_err(|error| {
        SinexError::serialization("Invalid base64 content").with_std_error(&error)
    })?;

    if content.len() > limit {
        return Err(blob_size_error(limit, content.len(), "decoded"));
    }

    Ok(content)
}

fn blob_size_error(limit: usize, actual: usize, unit: &'static str) -> SinexError {
    SinexError::validation(format!(
        "Blob content exceeds maximum allowed size of {limit} bytes"
    ))
    .with_context("limit_bytes", limit)
    .with_context("actual_size", actual)
    .with_context("size_unit", unit)
}

// Replay handlers

pub async fn handle_replay_create_operation(
    client: &ReplayControlClient,
    params: Value,
    auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let scope_val = params.require_value("scope")?.clone();
    let scope: ReplayScope = serde_json::from_value(scope_val).map_err(|error| {
        SinexError::serialization("Invalid replay scope payload").with_std_error(&error)
    })?;

    let operation = client
        .plan(auth.replay_actor(), scope)
        .await
        .map_err(|error| {
            SinexError::service("failed to plan replay operation").with_source(error)
        })?;
    serde_json::to_value(ReplayCreateResponse {
        operation: into_replay_operation(operation)?,
    })
    .map_err(|error| {
        SinexError::serialization("failed to serialize replay.create_operation response")
            .with_std_error(&error)
    })
}

pub async fn handle_replay_preview_operation(
    client: &ReplayControlClient,
    params: Value,
    _auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_uuid("operation_id")?;
    let (operation, preview) = client.preview(operation_id).await.map_err(|error| {
        SinexError::service("failed to preview replay operation").with_source(error)
    })?;
    serde_json::to_value(ReplayPreviewResponse {
        operation: into_replay_operation(operation)?,
        preview,
    })
    .map_err(|error| {
        SinexError::serialization("failed to serialize replay.preview_operation response")
            .with_std_error(&error)
    })
}

pub async fn handle_replay_approve_operation(
    client: &ReplayControlClient,
    params: Value,
    auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_uuid("operation_id")?;
    let operation = client
        .approve(operation_id, auth.replay_actor())
        .await
        .map_err(|error| {
            SinexError::service("failed to approve replay operation").with_source(error)
        })?;
    serde_json::to_value(ReplayApproveResponse {
        operation: into_replay_operation(operation)?,
    })
    .map_err(|error| {
        SinexError::serialization("failed to serialize replay.approve_operation response")
            .with_std_error(&error)
    })
}

pub async fn handle_replay_execute_operation(
    client: &ReplayControlClient,
    params: Value,
    auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_uuid("operation_id")?;
    let dry_run = params.optional_bool("dry_run")?.unwrap_or(false);
    let operation = client
        .execute(operation_id, auth.replay_actor(), dry_run)
        .await
        .map_err(|error| {
            SinexError::service("failed to execute replay operation").with_source(error)
        })?;
    serde_json::to_value(ReplayExecuteResponse {
        operation: into_replay_operation(operation)?,
    })
    .map_err(|error| {
        SinexError::serialization("failed to serialize replay.execute_operation response")
            .with_std_error(&error)
    })
}

pub async fn handle_replay_submit_operation(
    client: &ReplayControlClient,
    params: Value,
    auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_uuid("operation_id")?;
    let operation = client
        .submit(operation_id, auth.replay_actor())
        .await
        .map_err(|error| {
            SinexError::service("failed to submit replay operation").with_source(error)
        })?;
    serde_json::to_value(ReplaySubmitResponse {
        operation: into_replay_operation(operation)?,
    })
    .map_err(|error| {
        SinexError::serialization("failed to serialize replay.submit_operation response")
            .with_std_error(&error)
    })
}

pub async fn handle_replay_cancel_operation(
    client: &ReplayControlClient,
    params: Value,
    auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_uuid("operation_id")?;
    let reason = params
        .optional_str("reason")?
        .map(std::string::ToString::to_string);
    let operation = client
        .cancel(operation_id, auth.replay_actor(), reason)
        .await
        .map_err(|error| {
            SinexError::service("failed to cancel replay operation").with_source(error)
        })?;
    serde_json::to_value(ReplayCancelResponse {
        cancelled: true,
        operation: into_replay_operation(operation)?,
    })
    .map_err(|error| {
        SinexError::serialization("failed to serialize replay.cancel_operation response")
            .with_std_error(&error)
    })
}

pub async fn handle_replay_operation_status(
    client: &ReplayControlClient,
    params: Value,
    _auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_uuid("operation_id")?;
    let operation = client.status(operation_id).await.map_err(|error| {
        SinexError::service("failed to fetch replay operation status").with_source(error)
    })?;
    serde_json::to_value(ReplayStatusResponse {
        operation: into_replay_operation(operation)?,
    })
    .map_err(|error| {
        SinexError::serialization("failed to serialize replay.operation_status response")
            .with_std_error(&error)
    })
}

pub async fn handle_replay_list_operations(
    client: &ReplayControlClient,
    params: Value,
    _auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let state = params
        .optional_str("state")?
        .map(parse_replay_state)
        .transpose()?;
    let node = params.optional_str("node")?.map(String::from);
    let limit = params.optional_i64("limit")?;
    let operations = client.list(state, node, limit).await.map_err(|error| {
        SinexError::service("failed to list replay operations").with_source(error)
    })?;
    serde_json::to_value(ReplayListResponse {
        operations: operations
            .into_iter()
            .map(into_replay_operation)
            .collect::<Result<Vec<_>>>()?,
    })
    .map_err(|error| {
        SinexError::serialization("failed to serialize replay.list_operations response")
            .with_std_error(&error)
    })
}

fn into_replay_operation(operation: DbReplayOperation) -> Result<ReplayOperation> {
    serde_json::from_value(serde_json::to_value(operation).map_err(|error| {
        SinexError::serialization("failed to serialize replay operation into wire-compatible form")
            .with_std_error(&error)
    })?)
    .map_err(|error| {
        SinexError::serialization("failed to deserialize replay operation into RPC contract")
            .with_std_error(&error)
    })
}

pub(crate) fn parse_replay_state(value: &str) -> Result<ReplayState> {
    match value.to_lowercase().as_str() {
        "planning" => Ok(ReplayState::Planning),
        "previewed" => Ok(ReplayState::Previewed),
        "approved" => Ok(ReplayState::Approved),
        "executing" => Ok(ReplayState::Executing),
        "cancelling" => Ok(ReplayState::Cancelling),
        "committing" => Ok(ReplayState::Committing),
        "completed" => Ok(ReplayState::Completed),
        "failed" => Ok(ReplayState::Failed),
        "cancelled" => Ok(ReplayState::Cancelled),
        other => Err(SinexError::validation("unknown replay state").with_context("state", other)),
    }
}

pub(crate) fn blob_response_payload(
    content: &[u8],
    metadata: &sinex_node_sdk::content_store::BlobMetadata,
) -> Result<RetrieveBlobResponse> {
    let size = u64::try_from(metadata.size_bytes).map_err(|_| {
        SinexError::validation("blob metadata reported negative size")
            .with_context("size_bytes", metadata.size_bytes)
    })?;
    Ok(RetrieveBlobResponse {
        content: BASE64_STANDARD.encode(content),
        content_type: metadata.mime_type.clone(),
        size,
    })
}
fn max_base64_length(limit_bytes: usize) -> usize {
    // Each 3 bytes become 4 base64 chars. Round up to ensure we account for padding.
    limit_bytes.div_ceil(3) * 4
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sinex_db::models::blob::Blob;
    use sinex_primitives::error::ErrorClass;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn blob_response_payload_encodes_base64() -> TestResult<()> {
        let blob = Blob::builder()
            .storage_backend("SHA256".into())
            .content_hash("deadbeef".into())
            .original_filename("blob.bin".into())
            .size_bytes(2)
            .mime_type("application/octet-stream".into())
            .build();

        let response = blob_response_payload(b"hi", &blob)?;
        assert_eq!(response.content, "aGk=");
        assert_eq!(
            response.content_type.as_deref(),
            Some("application/octet-stream")
        );
        assert_eq!(response.size, 2);
        Ok(())
    }

    #[sinex_test]
    async fn parse_replay_state_accepts_known_variants() -> TestResult<()> {
        let states = [
            ("planning", ReplayState::Planning),
            ("PREVIEWED", ReplayState::Previewed),
            ("Approved", ReplayState::Approved),
            ("cancelling", ReplayState::Cancelling),
        ];
        for (input, expected) in states {
            assert_eq!(parse_replay_state(input).unwrap(), expected);
        }
        assert!(parse_replay_state("unknown").is_err());
        Ok(())
    }

    #[sinex_test]
    async fn rpc_params_uuid_parses_input() -> TestResult<()> {
        let id = Uuid::now_v7();
        let params = json!({"operation_id": id.to_string()});
        let rpc_params = RpcParams::new(&params);
        assert_eq!(rpc_params.require_uuid("operation_id").unwrap(), id);

        let invalid = json!({"operation_id": "not-uuid"});
        let rpc_params = RpcParams::new(&invalid);
        let error = rpc_params.require_uuid("operation_id").unwrap_err();
        assert_eq!(error.error_class(), ErrorClass::DataError);
        Ok(())
    }

    #[sinex_test]
    async fn rpc_params_optional_values_reject_wrong_types() -> TestResult<()> {
        let params = json!({
            "name": ["not-a-string"],
            "enabled": "not-a-bool",
            "limit": "not-an-int"
        });
        let rpc_params = RpcParams::new(&params);

        for error in [
            rpc_params.optional_str("name").unwrap_err(),
            rpc_params.optional_bool("enabled").unwrap_err(),
            rpc_params.optional_i64("limit").unwrap_err(),
        ] {
            assert_eq!(error.error_class(), ErrorClass::DataError);
        }
        Ok(())
    }
}

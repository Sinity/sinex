//! Shared RPC helpers and replay method handlers.

use crate::rpc_server::RpcAuthContext;
use crate::replay_control::ReplayControlClient;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use color_eyre::eyre::{Context, Result, eyre};
use serde_json::{Value, json};
use sinex_db::replay::state_machine::{ReplayScope, ReplayState};
use sinex_primitives::rpc::content::RetrieveBlobResponse;
use sinex_primitives::{Id, Uuid, domain::Entity};

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
            .ok_or_else(|| eyre!("missing string parameter '{}'", key))
    }

    pub(crate) fn optional_str(&self, key: &str) -> Result<Option<&'a str>> {
        match self.inner.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(value) => value
                .as_str()
                .map(Some)
                .ok_or_else(|| eyre!("parameter '{}' must be a string", key)),
        }
    }

    pub(crate) fn optional_bool(&self, key: &str) -> Result<Option<bool>> {
        match self.inner.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(value) => value
                .as_bool()
                .map(Some)
                .ok_or_else(|| eyre!("parameter '{}' must be a boolean", key)),
        }
    }

    pub(crate) fn optional_array(&self, key: &str) -> Result<Option<&'a [Value]>> {
        match self.inner.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(value) => value
                .as_array()
                .map(|items| Some(items.as_slice()))
                .ok_or_else(|| eyre!("parameter '{}' must be an array", key)),
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
                .ok_or_else(|| eyre!("parameter '{}' must be an object", key)),
        }
    }

    pub(crate) fn optional_i64(&self, key: &str) -> Result<Option<i64>> {
        match self.inner.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(value) => value
                .as_i64()
                .map(Some)
                .ok_or_else(|| eyre!("parameter '{}' must be an integer", key)),
        }
    }

    pub(crate) fn require_value(&self, key: &str) -> Result<&'a Value> {
        self.inner
            .get(key)
            .ok_or_else(|| eyre!("missing parameter '{}'", key))
    }

    pub(crate) fn require_uuid(&self, key: &str) -> Result<Uuid> {
        let value = self.require_str(key)?;
        value
            .parse::<Uuid>()
            .map_err(|e| eyre!("invalid UUIDv7 for '{}': {}", key, e))
    }
}

// Default values for content/blob handling
pub(crate) const DEFAULT_BLOB_FILENAME: &str = "content.txt";
pub(crate) const DEFAULT_BLOB_CONTENT_TYPE: &str = "text/plain";

pub(crate) fn decode_note_content(base64_content: &str) -> Result<String> {
    let decoded_bytes = BASE64_STANDARD
        .decode(base64_content)
        .wrap_err("Invalid base64 content")?;

    String::from_utf8(decoded_bytes).wrap_err("Decoded note content is not valid UTF-8")
}

pub(crate) fn validate_entity_name(name: &str) -> Result<()> {
    if name.trim().is_empty() {
        return Err(eyre!("Entity name cannot be empty"));
    }
    if name.len() > 255 {
        return Err(eyre!("Entity name cannot exceed 255 characters"));
    }
    if name.contains(';') || name.contains("--") || name.contains("/*") {
        return Err(eyre!("Entity name contains invalid characters"));
    }
    Ok(())
}

pub(crate) fn validate_entity_link_ids(from: &Id<Entity>, to: &Id<Entity>) -> Result<()> {
    if from == to {
        return Err(eyre!("Cannot link entity to itself"));
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
        return Err(eyre!(
            "Blob content exceeds maximum allowed size of {} bytes",
            limit
        ));
    }

    let content = BASE64_STANDARD
        .decode(content_b64)
        .wrap_err("Invalid base64 content")?;

    if content.len() > limit {
        return Err(eyre!(
            "Blob content exceeds maximum allowed size of {} bytes",
            limit
        ));
    }

    Ok(content)
}

// Replay handlers

pub async fn handle_replay_create_operation(
    client: &ReplayControlClient,
    params: Value,
    auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let scope_val = params.require_value("scope")?.clone();
    let scope: ReplayScope =
        serde_json::from_value(scope_val).wrap_err("Invalid replay scope payload")?;

    let operation = client.plan(auth.replay_actor(), scope).await?;
    Ok(json!({ "operation": operation }))
}

pub async fn handle_replay_preview_operation(
    client: &ReplayControlClient,
    params: Value,
    _auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_uuid("operation_id")?;
    let (operation, preview) = client.preview(operation_id).await?;
    Ok(json!({ "operation": operation, "preview": preview }))
}

pub async fn handle_replay_approve_operation(
    client: &ReplayControlClient,
    params: Value,
    auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_uuid("operation_id")?;
    let operation = client.approve(operation_id, auth.replay_actor()).await?;
    Ok(json!({ "operation": operation }))
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
        .await?;
    Ok(json!({ "operation": operation }))
}

pub async fn handle_replay_cancel_operation(
    client: &ReplayControlClient,
    params: Value,
    auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_uuid("operation_id")?;
    let reason = params
        .optional_str("reason")
        ?
        .map(std::string::ToString::to_string);
    let operation = client.cancel(operation_id, auth.replay_actor(), reason).await?;
    Ok(json!({ "cancelled": true, "operation": operation }))
}

pub async fn handle_replay_operation_status(
    client: &ReplayControlClient,
    params: Value,
    _auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_uuid("operation_id")?;
    let operation = client.status(operation_id).await?;
    Ok(json!({ "operation": operation }))
}

pub async fn handle_replay_list_operations(
    client: &ReplayControlClient,
    params: Value,
    _auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let state = params
        .optional_str("state")
        ?
        .map(parse_replay_state)
        .transpose()?;
    let node = params.optional_str("node")?.map(String::from);
    let limit = params.optional_i64("limit")?;
    let operations = client.list(state, node, limit).await?;
    Ok(json!({ "operations": operations }))
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
        other => Err(eyre!("Unknown replay state '{other}'")),
    }
}

pub(crate) fn blob_response_payload(
    content: &[u8],
    metadata: &sinex_node_sdk::annex::BlobMetadata,
) -> Result<RetrieveBlobResponse> {
    let size = u64::try_from(metadata.size_bytes)
        .map_err(|_| eyre!("blob metadata reported negative size: {}", metadata.size_bytes))?;
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
    use sinex_db::models::blob::Blob;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn blob_response_payload_encodes_base64() -> TestResult<()> {
        let blob = Blob::builder()
            .annex_backend("SHA256".into())
            .content_hash("deadbeef".into())
            .original_filename("blob.bin".into())
            .size_bytes(2)
            .mime_type("application/octet-stream".into())
            .build();

        let response = blob_response_payload(b"hi", &blob)?;
        assert_eq!(response.content, "aGk=");
        assert_eq!(response.content_type.as_deref(), Some("application/octet-stream"));
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
        assert!(rpc_params.require_uuid("operation_id").is_err());
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

        assert!(rpc_params.optional_str("name").is_err());
        assert!(rpc_params.optional_bool("enabled").is_err());
        assert!(rpc_params.optional_i64("limit").is_err());
        Ok(())
    }
}

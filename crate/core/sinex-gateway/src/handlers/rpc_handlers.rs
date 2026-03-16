//! Shared RPC helpers and replay method handlers.

use crate::replay_control::ReplayControlClient;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use color_eyre::eyre::{Context, Result, eyre};
use serde_json::{Value, json};
use sinex_db::replay::state_machine::{ReplayScope, ReplayState};
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

    pub(crate) fn optional_str(&self, key: &str) -> Option<&'a str> {
        self.inner.get(key).and_then(|v| v.as_str())
    }

    pub(crate) fn optional_array(&self, key: &str) -> Option<&'a [Value]> {
        self.inner
            .get(key)
            .and_then(|v| v.as_array())
            .map(Vec::as_slice)
    }

    pub(crate) fn optional_object(&self, key: &str) -> Option<&'a serde_json::Map<String, Value>> {
        self.inner.get(key).and_then(|v| v.as_object())
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

// Default values for created_by fields when not provided by caller
pub(crate) const DEFAULT_CREATOR_HOST: &str = "sinex-host";
pub(crate) const DEFAULT_CREATOR_GATEWAY: &str = "sinex-gateway";
const DEFAULT_REPLAY_ACTOR: &str = "service:sinex-cli";

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
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let actor = params
        .optional_str("actor")
        .unwrap_or(DEFAULT_REPLAY_ACTOR)
        .to_string();

    let scope_val = params.require_value("scope")?.clone();
    let scope: ReplayScope =
        serde_json::from_value(scope_val).wrap_err("Invalid replay scope payload")?;

    let operation = client.plan(actor, scope).await?;
    Ok(json!({ "operation": operation }))
}

pub async fn handle_replay_preview_operation(
    client: &ReplayControlClient,
    params: Value,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_uuid("operation_id")?;
    let (operation, preview) = client.preview(operation_id).await?;
    Ok(json!({ "operation": operation, "preview": preview }))
}

pub async fn handle_replay_approve_operation(
    client: &ReplayControlClient,
    params: Value,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_uuid("operation_id")?;
    let approver = params
        .optional_str("approver")
        .unwrap_or(DEFAULT_REPLAY_ACTOR)
        .to_string();
    let operation = client.approve(operation_id, approver).await?;
    Ok(json!({ "operation": operation }))
}

pub async fn handle_replay_execute_operation(
    client: &ReplayControlClient,
    params: Value,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_uuid("operation_id")?;
    let executor = params
        .optional_str("executor")
        .unwrap_or(DEFAULT_REPLAY_ACTOR)
        .to_string();
    let operation = client.execute(operation_id, executor).await?;
    Ok(json!({ "operation": operation }))
}

pub async fn handle_replay_cancel_operation(
    client: &ReplayControlClient,
    params: Value,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_uuid("operation_id")?;
    let canceller = params
        .optional_str("canceller")
        .unwrap_or(DEFAULT_REPLAY_ACTOR)
        .to_string();
    let reason = params
        .optional_str("reason")
        .map(std::string::ToString::to_string);
    let operation = client.cancel(operation_id, canceller, reason).await?;
    Ok(json!({ "cancelled": true, "operation": operation }))
}

pub async fn handle_replay_operation_status(
    client: &ReplayControlClient,
    params: Value,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_uuid("operation_id")?;
    let operation = client.status(operation_id).await?;
    Ok(json!({ "operation": operation }))
}

pub async fn handle_replay_list_operations(
    client: &ReplayControlClient,
    params: Value,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let state = params
        .optional_str("state")
        .map(parse_replay_state)
        .transpose()?;
    let node = params.optional_str("node").map(String::from);
    let limit = params
        .inner
        .get("limit")
        .and_then(serde_json::Value::as_i64);
    let operations = client.list(state, node, limit).await?;
    Ok(json!({ "operations": operations }))
}

pub(crate) fn parse_replay_state(value: &str) -> Result<ReplayState> {
    match value.to_lowercase().as_str() {
        "planning" => Ok(ReplayState::Planning),
        "previewed" => Ok(ReplayState::Previewed),
        "approved" => Ok(ReplayState::Approved),
        "executing" => Ok(ReplayState::Executing),
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
) -> Value {
    json!({
        "content_base64": BASE64_STANDARD.encode(content),
        "mime_type": metadata.mime_type.clone(),
        "size_bytes": metadata.size_bytes,
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

        let json = blob_response_payload(b"hi", &blob);
        assert_eq!(json["content_base64"], "aGk=");
        assert_eq!(json["mime_type"], "application/octet-stream");
        assert_eq!(json["size_bytes"], 2);
        Ok(())
    }

    #[sinex_test]
    async fn parse_replay_state_accepts_known_variants() -> TestResult<()> {
        let states = [
            ("planning", ReplayState::Planning),
            ("PREVIEWED", ReplayState::Previewed),
            ("Approved", ReplayState::Approved),
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
}

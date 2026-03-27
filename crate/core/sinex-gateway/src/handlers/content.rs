//! Content RPC handlers.

use super::rpc_handlers::{
    DEFAULT_BLOB_CONTENT_TYPE, DEFAULT_BLOB_FILENAME, RpcParams, blob_response_payload,
    decode_blob_content,
};
use crate::rpc_server::RpcAuthContext;
use crate::service_container::ServiceContainer;
use color_eyre::eyre::{Context, Result};
use serde_json::Value;
use sinex_primitives::rpc::content::StoreBlobResponse;

pub async fn handle_store_blob(
    services: &ServiceContainer,
    params: Value,
    auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let content_b64 = params.require_str("content").wrap_err("Missing content")?;

    let limit = services.config().max_blob_bytes;
    let content = decode_blob_content(content_b64, limit)?;

    let filename = params
        .optional_str("filename")
        ?
        .unwrap_or(DEFAULT_BLOB_FILENAME);
    let content_type = params
        .optional_str("content_type")
        ?
        .unwrap_or(DEFAULT_BLOB_CONTENT_TYPE);
    let source = params
        .optional_str("source")
        ?
        .unwrap_or(auth.actor_id());

    let key = services
        .content
        .store_content(&content, filename, content_type, source, auth.actor_id())
        .await?;
    let metadata = services.content.get_content_metadata(&key).await?;
    let size = u64::try_from(metadata.size_bytes).map_err(|_| {
        color_eyre::eyre::eyre!("blob metadata reported negative size: {}", metadata.size_bytes)
    })?;

    Ok(serde_json::to_value(StoreBlobResponse {
        key,
        size,
        hash: metadata.content_hash,
    })?)
}

pub async fn handle_retrieve_blob(services: &ServiceContainer, params: Value) -> Result<Value> {
    let params = RpcParams::new(&params);
    let key = params.require_str("key").wrap_err("Missing key")?;

    let content = services.content.retrieve_content(key).await?;
    let metadata = services.content.get_content_metadata(key).await?;

    Ok(serde_json::to_value(blob_response_payload(&content, &metadata)?)?)
}

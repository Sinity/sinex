//! Content RPC handlers.

use super::rpc_handlers::{
    DEFAULT_BLOB_CONTENT_TYPE, DEFAULT_BLOB_FILENAME, DEFAULT_CREATOR_HOST, RpcParams,
    blob_response_payload, blob_size_limit_bytes, decode_blob_content,
};
use color_eyre::eyre::{Context, Result};
use serde_json::{Value, json};
use sinex_services::ContentService;

pub async fn handle_store_blob(service: &ContentService, params: Value) -> Result<Value> {
    let params = RpcParams::new(&params);
    let content_b64 = params.require_str("content").wrap_err("Missing content")?;

    let limit = blob_size_limit_bytes();
    let content = decode_blob_content(content_b64, limit)?;

    let filename = params
        .optional_str("filename")
        .unwrap_or(DEFAULT_BLOB_FILENAME);
    let content_type = params
        .optional_str("content_type")
        .unwrap_or(DEFAULT_BLOB_CONTENT_TYPE);
    let source = params
        .optional_str("source")
        .unwrap_or(DEFAULT_CREATOR_HOST);

    let annex_key = service
        .store_content(&content, filename, content_type, source)
        .await?;

    Ok(json!({ "annex_key": annex_key }))
}

pub async fn handle_retrieve_blob(service: &ContentService, params: Value) -> Result<Value> {
    let params = RpcParams::new(&params);
    let annex_key = params
        .require_str("annex_key")
        .wrap_err("Missing annex_key")?;

    let content = service.retrieve_content(annex_key).await?;
    let metadata = service.get_content_metadata(annex_key).await?;

    Ok(blob_response_payload(&content, &metadata))
}

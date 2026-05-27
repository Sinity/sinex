//! Content RPC handlers.

use super::rpc_handlers::{
    DEFAULT_BLOB_CONTENT_TYPE, DEFAULT_BLOB_FILENAME, blob_response_payload, decode_blob_content,
};
use crate::api::rpc_server::RpcAuthContext;
use crate::api::service_container::ServiceContainer;
use sinex_primitives::rpc::content::{
    RetrieveBlobRequest, RetrieveBlobResponse, StoreBlobRequest, StoreBlobResponse,
};
use sinex_primitives::{Result, SinexError};

pub async fn handle_store_blob(
    services: &ServiceContainer,
    request: StoreBlobRequest,
    auth: &RpcAuthContext,
) -> Result<StoreBlobResponse> {
    let limit = services.config().max_blob_bytes;
    let content = decode_blob_content(&request.content, limit)?;

    let filename = request.filename.as_deref().unwrap_or(DEFAULT_BLOB_FILENAME);
    let content_type = request
        .content_type
        .as_deref()
        .unwrap_or(DEFAULT_BLOB_CONTENT_TYPE);
    let source = request.source.as_deref().unwrap_or(auth.actor_id());

    let key = services
        .content
        .store_content(&content, filename, content_type, source, auth.actor_id())
        .await?;
    let metadata = services.content.get_content_metadata(&key).await?;
    let size = u64::try_from(metadata.size_bytes).map_err(|_| {
        SinexError::validation("blob metadata reported negative size")
            .with_context("size_bytes", metadata.size_bytes)
    })?;

    Ok(StoreBlobResponse {
        content_key: key,
        size,
        blake3_hash: metadata.content_hash,
    })
}

pub async fn handle_retrieve_blob(
    services: &ServiceContainer,
    request: RetrieveBlobRequest,
) -> Result<RetrieveBlobResponse> {
    let content = services
        .content
        .retrieve_content(&request.content_key)
        .await?;
    let metadata = services
        .content
        .get_content_metadata(&request.content_key)
        .await?;

    blob_response_payload(&content, &metadata)
}

//! Shared RPC parameter and content helpers used across handler modules.
//!
//! Replay handlers used to live here; per #1172 they moved to
//! `handlers/replay.rs` so each domain has its own handler module.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use sinex_primitives::rpc::content::RetrieveBlobResponse;
use sinex_primitives::{Id, Result, SinexError, domain::Entity};

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
/// - `SINEX_API_MAX_BODY_BYTES` >= `SINEX_API_MAX_BLOB_BYTES` * 1.4
///   (1.4 accounts for base64 overhead plus JSON envelope)
///
/// Default configuration:
/// - Body limit: 2MB (`SINEX_API_MAX_BODY_BYTES`)
/// - Blob limit: 5MB (`SINEX_API_MAX_BLOB_BYTES`)
///
/// This mismatch is intentional: the body limit applies to the raw HTTP request,
/// while the blob limit applies to decoded content. For large blobs, clients should
/// increase `SINEX_API_MAX_BODY_BYTES` proportionally.
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
    use sinex_db::models::blob::Blob;
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
}

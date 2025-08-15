//! SQL fragments for source materials repository
//! 
//! This module contains SQL fragments that are reused across multiple queries
//! to handle ULID to UUID type casting consistently.

/// SQL column list for SELECT queries with ULID to UUID casting
pub const SELECT_COLUMNS: &str = r#"
    id::uuid as "id!",
    checksum,
    source_identifier,
    source_type,
    source_path,
    content_type,
    status,
    total_bytes,
    created_at,
    finalized_at,
    staged_at,
    metadata,
    data,
    optional_blob_id::uuid as optional_blob_id,
    material_type,
    content_preview,
    source_uri,
    encoding,
    is_archived,
    retention_policy,
    ingestion_time,
    archive_time,
    updated_at
"#;
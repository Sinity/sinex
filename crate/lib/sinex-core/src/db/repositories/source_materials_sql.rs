//! SQL fragments for source materials repository
//! 
//! This module contains SQL fragments that are reused across multiple queries
//! to handle ULID to UUID type casting consistently.

/// SQL column list for SELECT queries with ULID to UUID casting
pub const SELECT_COLUMNS: &str = r#"
    id::uuid as "id!",
    NULL::text as checksum,
    source_identifier,
    'unknown'::text as source_type,
    NULL::text as source_path,
    NULL::text as content_type,
    'sensing'::text as status,
    NULL::bigint as total_bytes,
    created_at,
    NULL::timestamptz as finalized_at,
    NULL::timestamptz as staged_at,
    metadata,
    NULL::bytea as data,
    optional_blob_id::uuid as optional_blob_id,
    material_type,
    content_preview,
    source_uri,
    encoding,
    false as is_archived,
    NULL::text as retention_policy,
    NULL::timestamptz as ingestion_time,
    NULL::timestamptz as archive_time,
    updated_at
"#;

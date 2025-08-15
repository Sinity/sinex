-- Source Materials Repository SQL Queries with proper ULID handling

-- register_material: INSERT new source material
INSERT INTO raw.source_material_registry (
    id,
    material_type,
    source_uri,
    encoding,
    metadata,
    content_preview,
    retention_policy,
    optional_blob_id,
    source_identifier,
    source_type,
    status
) VALUES ($1::ulid, $2, $3, $4, $5, $6, $7, $8::ulid, $3, 'file', 'sensing')
RETURNING 
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
    updated_at;

-- get_by_id: SELECT by ID with ULID cast
SELECT 
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
FROM raw.source_material_registry
WHERE id = $1::ulid;
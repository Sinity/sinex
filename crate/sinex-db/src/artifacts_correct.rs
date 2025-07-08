use crate::models::{Artifact, CreateArtifactInput};
use crate::query_helpers::{ulid_to_uuid, uuid_to_ulid};
use crate::DbPoolRef;
use anyhow::Result;
use sinex_ulid::Ulid;
use sqlx::types::Uuid;

/// Create a new artifact following the exact same pattern as add_to_work_queue
pub async fn create_artifact(pool: DbPoolRef<'_>, input: CreateArtifactInput) -> Result<Artifact> {
    let metadata = input.metadata.unwrap_or_else(|| serde_json::json!({}));
    let created_from_event_uuid: Option<Uuid> = input.created_from_event_id.map(ulid_to_uuid);
    let blob_uuid: Option<Uuid> = input.blob_id.map(ulid_to_uuid);
    
    let record = sqlx::query!(
        r#"
        INSERT INTO core.artifacts (
            "type", title, source_url, original_path, mime_type, 
            size_bytes, checksum, metadata, created_from_event_id, blob_id
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::uuid, $10::uuid)
        RETURNING 
            id::uuid as "id!",
            "type" as "artifact_type!",
            title as "title!",
            source_url,
            original_path,
            mime_type,
            size_bytes,
            checksum,
            metadata as "metadata!",
            created_at as "created_at!",
            updated_at as "updated_at!",
            deleted_at,
            created_from_event_id::uuid as "created_from_event_id",
            blob_id::uuid as "blob_id"
        "#,
        input.artifact_type,
        input.title,
        input.source_url,
        input.original_path,
        input.mime_type,
        input.size_bytes,
        input.checksum,
        metadata,
        created_from_event_uuid,
        blob_uuid
    )
    .fetch_one(pool)
    .await?;

    Ok(Artifact {
        artifact_id: uuid_to_ulid(record.id),
        artifact_type: record.artifact_type,
        title: record.title,
        source_url: record.source_url,
        original_path: record.original_path,
        mime_type: record.mime_type,
        size_bytes: record.size_bytes,
        checksum: record.checksum,
        metadata: record.metadata,
        created_at: record.created_at,
        updated_at: record.updated_at,
        deleted_at: record.deleted_at,
        created_from_event_id: record.created_from_event_id.map(uuid_to_ulid),
        blob_id: record.blob_id.map(uuid_to_ulid),
    })
}

/// Get an artifact by ID
pub async fn get_artifact_by_id(pool: DbPoolRef<'_>, artifact_id: Ulid) -> Result<Option<Artifact>> {
    let artifact_uuid: Uuid = ulid_to_uuid(artifact_id);
    
    let record = sqlx::query!(
        r#"
        SELECT 
            id::uuid as "id!",
            "type" as "artifact_type!",
            title as "title!",
            source_url,
            original_path,
            mime_type,
            size_bytes,
            checksum,
            metadata as "metadata!",
            created_at as "created_at!",
            updated_at as "updated_at!",
            deleted_at,
            created_from_event_id::uuid as "created_from_event_id",
            blob_id::uuid as "blob_id"
        FROM core.artifacts 
        WHERE id::uuid = $1 AND deleted_at IS NULL
        "#,
        artifact_uuid
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| Artifact {
        artifact_id: uuid_to_ulid(r.id),
        artifact_type: r.artifact_type,
        title: r.title,
        source_url: r.source_url,
        original_path: r.original_path,
        mime_type: r.mime_type,
        size_bytes: r.size_bytes,
        checksum: r.checksum,
        metadata: r.metadata,
        created_at: r.created_at,
        updated_at: r.updated_at,
        deleted_at: r.deleted_at,
        created_from_event_id: r.created_from_event_id.map(uuid_to_ulid),
        blob_id: r.blob_id.map(uuid_to_ulid),
    }))
}

/// Get recent artifacts
pub async fn get_recent_artifacts(pool: DbPoolRef<'_>, limit: i64) -> Result<Vec<Artifact>> {
    let records = sqlx::query!(
        r#"
        SELECT 
            id::uuid as "id!",
            "type" as "artifact_type!",
            title as "title!",
            source_url,
            original_path,
            mime_type,
            size_bytes,
            checksum,
            metadata as "metadata!",
            created_at as "created_at!",
            updated_at as "updated_at!",
            deleted_at,
            created_from_event_id::uuid as "created_from_event_id",
            blob_id::uuid as "blob_id"
        FROM core.artifacts 
        WHERE deleted_at IS NULL
        ORDER BY created_at DESC
        LIMIT $1
        "#,
        limit
    )
    .fetch_all(pool)
    .await?;

    let artifacts = records
        .into_iter()
        .map(|r| Artifact {
            artifact_id: uuid_to_ulid(r.id),
            artifact_type: r.artifact_type,
            title: r.title,
            source_url: r.source_url,
            original_path: r.original_path,
            mime_type: r.mime_type,
            size_bytes: r.size_bytes,
            checksum: r.checksum,
            metadata: r.metadata,
            created_at: r.created_at,
            updated_at: r.updated_at,
            deleted_at: r.deleted_at,
            created_from_event_id: r.created_from_event_id.map(uuid_to_ulid),
            blob_id: r.blob_id.map(uuid_to_ulid),
        })
        .collect();

    Ok(artifacts)
}
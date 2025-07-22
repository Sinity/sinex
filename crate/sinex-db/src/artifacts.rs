use crate::models::{Artifact, CreateArtifactInput};
use crate::queries::ArtifactQueries;
use crate::query_helpers::uuid_to_ulid;
use crate::DbPoolRef;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sinex_ulid::Ulid;
use sqlx::FromRow;

/// Database record structure for artifacts
#[derive(Debug, FromRow)]
pub struct ArtifactRecord {
    pub id: sqlx::types::Uuid,
    pub artifact_type: String,
    pub title: String,
    pub source_url: Option<String>,
    pub original_path: Option<String>,
    pub mime_type: Option<String>,
    pub size_bytes: Option<i64>,
    pub checksum: Option<String>,
    pub metadata: JsonValue,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub created_from_event_id: Option<sqlx::types::Uuid>,
    pub blob_id: Option<sqlx::types::Uuid>,
}

/// Create a new artifact following the exact same pattern as add_to_work_queue
// #[sinex_macros::auto_db_metrics(operation = "create_artifact")]
pub async fn create_artifact(pool: DbPoolRef<'_>, input: CreateArtifactInput) -> Result<Artifact> {
    let metadata = input.metadata.unwrap_or_else(|| serde_json::json!({}));

    let record = ArtifactQueries::insert_artifact_full(
        input.artifact_type,
        input.title,
        input.source_url,
        input.original_path,
        input.mime_type,
        input.size_bytes,
        input.checksum,
        metadata,
        input.created_from_event_id,
        input.blob_id,
    )
    .fetch_one::<ArtifactRecord>(pool)
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create artifact: {}", e))?;

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
pub async fn get_artifact_by_id(
    pool: DbPoolRef<'_>,
    artifact_id: Ulid,
) -> Result<Option<Artifact>> {
    let record = ArtifactQueries::get_by_id(artifact_id)
        .fetch_optional::<ArtifactRecord>(pool)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get artifact by ID: {}", e))?;

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
    let records = ArtifactQueries::get_recent(Some(limit), None)
        .fetch_all::<ArtifactRecord>(pool)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get recent artifacts: {}", e))?;

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

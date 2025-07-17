use crate::models::{CreateAnnotationInput, EventAnnotation};
use crate::queries::AnnotationQueries;
use crate::query_helpers::uuid_to_ulid;
use crate::DbPoolRef;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sinex_ulid::Ulid;
use sqlx::FromRow;

/// Database record structure for annotations
#[derive(Debug, FromRow)]
pub struct AnnotationRecord {
    pub id: sqlx::types::Uuid,
    pub event_id: sqlx::types::Uuid,
    pub annotation_type: String,
    pub content: String,
    pub metadata: JsonValue,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub created_by: String,
}

/// Create a new event annotation following the exact same pattern as add_to_work_queue
#[sinex_macros::auto_db_metrics(operation = "create_annotation")]
pub async fn create_annotation(
    pool: DbPoolRef<'_>,
    input: CreateAnnotationInput,
) -> Result<EventAnnotation> {
    let metadata = input.metadata.unwrap_or_else(|| serde_json::json!({}));

    let record = AnnotationQueries::insert_annotation(
        input.event_id,
        input.annotation_type,
        input.content,
        metadata,
        input.created_by,
    )
    .fetch_one::<AnnotationRecord>(pool)
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create annotation: {}", e))?;

    Ok(EventAnnotation {
        annotation_id: uuid_to_ulid(record.id),
        event_id: uuid_to_ulid(record.event_id),
        annotation_type: record.annotation_type,
        content: record.content,
        metadata: record.metadata,
        created_at: record.created_at,
        updated_at: record.updated_at,
        created_by: record.created_by,
    })
}

/// Get annotations for a specific event
#[sinex_macros::auto_db_metrics(operation = "get_annotations_for_event")]
pub async fn get_annotations_for_event(
    pool: DbPoolRef<'_>,
    event_id: Ulid,
) -> Result<Vec<EventAnnotation>> {
    let records = AnnotationQueries::get_by_event_id(event_id)
        .fetch_all::<AnnotationRecord>(pool)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get annotations for event: {}", e))?;

    let annotations = records
        .into_iter()
        .map(|r| EventAnnotation {
            annotation_id: uuid_to_ulid(r.id),
            event_id: uuid_to_ulid(r.event_id),
            annotation_type: r.annotation_type,
            content: r.content,
            metadata: r.metadata,
            created_at: r.created_at,
            updated_at: r.updated_at,
            created_by: r.created_by,
        })
        .collect();

    Ok(annotations)
}

/// Get annotation by ID
pub async fn get_annotation_by_id(
    pool: DbPoolRef<'_>,
    annotation_id: Ulid,
) -> Result<Option<EventAnnotation>> {
    let record = AnnotationQueries::get_by_id(annotation_id)
        .fetch_optional::<AnnotationRecord>(pool)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get annotation by ID: {}", e))?;

    Ok(record.map(|r| EventAnnotation {
        annotation_id: uuid_to_ulid(r.id),
        event_id: uuid_to_ulid(r.event_id),
        annotation_type: r.annotation_type,
        content: r.content,
        metadata: r.metadata,
        created_at: r.created_at,
        updated_at: r.updated_at,
        created_by: r.created_by,
    }))
}

/// Update annotation content
pub async fn update_annotation_content(
    pool: DbPoolRef<'_>,
    annotation_id: Ulid,
    new_content: &str,
) -> Result<EventAnnotation> {
    let record = AnnotationQueries::update_content(annotation_id, new_content.to_string())
        .fetch_one::<AnnotationRecord>(pool)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to update annotation content: {}", e))?;

    Ok(EventAnnotation {
        annotation_id: uuid_to_ulid(record.id),
        event_id: uuid_to_ulid(record.event_id),
        annotation_type: record.annotation_type,
        content: record.content,
        metadata: record.metadata,
        created_at: record.created_at,
        updated_at: record.updated_at,
        created_by: record.created_by,
    })
}

/// Delete annotation
pub async fn delete_annotation(pool: DbPoolRef<'_>, annotation_id: Ulid) -> Result<bool> {
    let result = AnnotationQueries::delete_by_id(annotation_id)
        .execute(pool)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to delete annotation: {}", e))?;

    Ok(result.rows_affected() > 0)
}

/// Get recent annotations
pub async fn get_recent_annotations(
    pool: DbPoolRef<'_>,
    limit: i64,
) -> Result<Vec<EventAnnotation>> {
    let records = AnnotationQueries::get_recent(limit)
        .fetch_all::<AnnotationRecord>(pool)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get recent annotations: {}", e))?;

    let annotations = records
        .into_iter()
        .map(|r| EventAnnotation {
            annotation_id: uuid_to_ulid(r.id),
            event_id: uuid_to_ulid(r.event_id),
            annotation_type: r.annotation_type,
            content: r.content,
            metadata: r.metadata,
            created_at: r.created_at,
            updated_at: r.updated_at,
            created_by: r.created_by,
        })
        .collect();

    Ok(annotations)
}

use crate::models::{CreateAnnotationInput, EventAnnotation};
use crate::query_helpers::{ulid_to_uuid, uuid_to_ulid};
use crate::DbPoolRef;
use anyhow::Result;
use sinex_ulid::Ulid;
use sqlx::types::Uuid;

/// Create a new event annotation following the exact same pattern as add_to_work_queue
pub async fn create_annotation(
    pool: DbPoolRef<'_>,
    input: CreateAnnotationInput,
) -> Result<EventAnnotation> {
    let metadata = input.metadata.unwrap_or_else(|| serde_json::json!({}));
    let event_uuid: Uuid = ulid_to_uuid(input.event_id);

    let record = sqlx::query!(
        r#"
        INSERT INTO core.event_annotations (
            event_id, annotation_type, content, metadata, created_by
        ) VALUES ($1::uuid, $2, $3, $4, $5)
        RETURNING 
            id::uuid as "id!",
            event_id::uuid as "event_id!",
            annotation_type as "annotation_type!",
            content as "content!",
            metadata as "metadata!",
            created_at as "created_at!",
            updated_at as "updated_at!",
            created_by as "created_by!"
        "#,
        event_uuid,
        input.annotation_type,
        input.content,
        metadata,
        input.created_by
    )
    .fetch_one(pool)
    .await?;

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
pub async fn get_annotations_for_event(
    pool: DbPoolRef<'_>,
    event_id: Ulid,
) -> Result<Vec<EventAnnotation>> {
    let event_uuid: Uuid = ulid_to_uuid(event_id);

    let records = sqlx::query!(
        r#"
        SELECT 
            id::uuid as "id!",
            event_id::uuid as "event_id!",
            annotation_type as "annotation_type!",
            content as "content!",
            metadata as "metadata!",
            created_at as "created_at!",
            updated_at as "updated_at!",
            created_by as "created_by!"
        FROM core.event_annotations 
        WHERE event_id::uuid = $1
        ORDER BY created_at DESC
        "#,
        event_uuid
    )
    .fetch_all(pool)
    .await?;

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
    let annotation_uuid: Uuid = ulid_to_uuid(annotation_id);

    let record = sqlx::query!(
        r#"
        SELECT 
            id::uuid as "id!",
            event_id::uuid as "event_id!",
            annotation_type as "annotation_type!",
            content as "content!",
            metadata as "metadata!",
            created_at as "created_at!",
            updated_at as "updated_at!",
            created_by as "created_by!"
        FROM core.event_annotations 
        WHERE id::uuid = $1
        "#,
        annotation_uuid
    )
    .fetch_optional(pool)
    .await?;

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
    let annotation_uuid: Uuid = ulid_to_uuid(annotation_id);

    let record = sqlx::query!(
        r#"
        UPDATE core.event_annotations 
        SET content = $2, updated_at = NOW()
        WHERE id::uuid = $1
        RETURNING 
            id::uuid as "id!",
            event_id::uuid as "event_id!",
            annotation_type as "annotation_type!",
            content as "content!",
            metadata as "metadata!",
            created_at as "created_at!",
            updated_at as "updated_at!",
            created_by as "created_by!"
        "#,
        annotation_uuid,
        new_content
    )
    .fetch_one(pool)
    .await?;

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
    let annotation_uuid: Uuid = ulid_to_uuid(annotation_id);

    let result = sqlx::query!(
        "DELETE FROM core.event_annotations WHERE id::uuid = $1",
        annotation_uuid
    )
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

/// Get recent annotations
pub async fn get_recent_annotations(
    pool: DbPoolRef<'_>,
    limit: i64,
) -> Result<Vec<EventAnnotation>> {
    let records = sqlx::query!(
        r#"
        SELECT 
            id::uuid as "id!",
            event_id::uuid as "event_id!",
            annotation_type as "annotation_type!",
            content as "content!",
            metadata as "metadata!",
            created_at as "created_at!",
            updated_at as "updated_at!",
            created_by as "created_by!"
        FROM core.event_annotations 
        ORDER BY created_at DESC
        LIMIT $1
        "#,
        limit
    )
    .fetch_all(pool)
    .await?;

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

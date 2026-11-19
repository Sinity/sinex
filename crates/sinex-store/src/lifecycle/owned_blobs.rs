//! Release a single reading's owned bytes without changing the reading.

use super::bytes::{mark_removing, unreferenced};
use crate::{Error, Result};
use serde_json::Value;
use sinex_model::{Event, EventId, OperationId, Origin};
use sqlx::PgConnection;

pub struct OwnedBlobRelease<'a> {
    pub event: EventId,
    pub event_type: &'a str,
    pub payload: &'a Value,
    pub blob: &'a str,
    pub actor: &'a str,
    pub operation_kind: &'a str,
    pub scope: &'a Value,
}

/// The caller has verified the unchanged file on the local host. Run under
/// the writer transaction so the observation, release and byte claim agree.
pub async fn release_owned_blob(
    conn: &mut PgConnection,
    release: OwnedBlobRelease<'_>,
) -> Result<OperationId> {
    let row = sqlx::query!(
        r#"SELECT e.material_id AS "material_id!", e.record_offset AS "record_offset!",
                  e.output_index, e.payload,
                  NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id = e.id) AS "live!"
           FROM core.events e WHERE e.id = $1 AND e.event_type = $2
             AND e.material_id IS NOT NULL AND e.record_offset IS NOT NULL"#,
        release.event.0,
        release.event_type,
    )
    .fetch_optional(&mut *conn)
    .await?
    .ok_or_else(|| Error::Invalid("no recorded copy for that reading".into()))?;
    if &row.payload != release.payload {
        return Err(Error::Invalid(
            "the reading no longer matches the verified copy".into(),
        ));
    }
    if let Some(operation) = sqlx::query_scalar!(
        "SELECT operation_id FROM raw.owned_blob_releases
         WHERE (material_id, record_offset, output_index, event_type, blob) = ($1, $2, $3, $4, $5)",
        row.material_id,
        row.record_offset,
        row.output_index,
        release.event_type,
        release.blob,
    )
    .fetch_optional(&mut *conn)
    .await?
    {
        return Ok(OperationId(operation));
    }
    if !row.live {
        return Err(Error::Invalid(
            "the verified copy is no longer a live reading".into(),
        ));
    }
    let descriptor = sqlx::query_scalar!(
        r#"SELECT e.descriptor FROM core.blobs b JOIN core.blob_extents e ON e.hash = b.hash
             WHERE b.hash = $1 AND b.status = 'present' AND NOT e.deleting
               AND NOT EXISTS (SELECT 1 FROM core.blob_chunks x WHERE x.blob = b.hash OR x.chunk = b.hash)"#,
        release.blob,
    )
    .fetch_optional(&mut *conn)
    .await?
    .ok_or_else(|| Error::Invalid("the copy does not hold a directly stored blob".into()))?;
    let extent = hashed_blobs::Extent::decode(&descriptor)?;
    if crate::backup::covers(conn, &extent.segment, extent.offset).await? {
        return Err(Error::Invalid(
            "a backup hold covers these bytes. Retry after the hold is released".into(),
        ));
    }
    let removed = sqlx::query!(
        "DELETE FROM core.event_blobs WHERE event_id = $1 AND blob = $2",
        release.event.0,
        release.blob,
    )
    .execute(&mut *conn)
    .await?
    .rows_affected();
    if removed != 1 || !unreferenced(conn, release.blob).await? {
        return Err(Error::Invalid(
            "the reading is not the sole owner of these stored bytes".into(),
        ));
    }
    let operation = OperationId::new();
    let seq = crate::admission::position(conn).await?;
    sqlx::query!(
        "INSERT INTO core.operations (id, kind, actor, scope, start_seq, end_seq, result)
         VALUES ($1, $2, $3, $4, $5, $5, 'ok')",
        operation.0,
        release.operation_kind,
        release.actor,
        release.scope,
        seq,
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query!(
        "INSERT INTO raw.owned_blob_releases (material_id, record_offset, output_index, event_type, blob, operation_id)
         VALUES ($1, $2, $3, $4, $5, $6)",
        row.material_id,
        row.record_offset,
        row.output_index,
        release.event_type,
        release.blob,
        operation.0,
    )
    .execute(&mut *conn)
    .await?;
    mark_removing(conn, release.blob).await?;
    Ok(operation)
}

/// The native extent removal is complete. A pending punch is not a release
/// receipt for physical storage.
pub async fn owned_blob_removed(conn: &mut PgConnection, content: &str) -> Result<bool> {
    Ok(sqlx::query_scalar!(
        r#"SELECT status = 'removed' AS "removed!" FROM core.blobs WHERE hash = $1"#,
        content,
    )
    .fetch_one(conn)
    .await?)
}

/// Replaying a released record still yields the same payload, but cannot
/// claim that digest again. A different record remains free to own the bytes.
pub(crate) async fn respect_released_blobs(
    conn: &mut PgConnection,
    kind: &str,
    events: &mut [Event],
) -> Result<()> {
    let (mut indices, mut materials, mut offsets, mut outputs, mut blobs) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for (index, event) in events
        .iter()
        .enumerate()
        .filter(|(_, event)| event.event_type == kind)
    {
        let Origin::Record {
            location,
            output_index,
        } = event.origin
        else {
            continue;
        };
        for blob in &event.owned_blobs {
            indices.push(i64::try_from(index).expect("event index fits bigint"));
            materials.push(location.material.0);
            offsets.push(
                i64::try_from(location.record_offset)
                    .map_err(|_| Error::Invalid("record offset exceeds bigint".into()))?,
            );
            outputs.push(
                i64::try_from(output_index)
                    .map_err(|_| Error::Invalid("record output index exceeds bigint".into()))?,
            );
            blobs.push(blob.blob.clone());
        }
    }
    if indices.is_empty() {
        return Ok(());
    }
    let released = sqlx::query!(
        r#"SELECT offered.idx AS "idx!", released.blob FROM raw.owned_blob_releases released
           JOIN unnest($1::bigint[], $2::uuid[], $3::bigint[], $4::bigint[], $5::text[])
               AS offered(idx, material_id, record_offset, output_index, blob)
             USING (material_id, record_offset, output_index, blob)
           WHERE released.event_type = $6"#,
        &indices,
        &materials,
        &offsets,
        &outputs,
        &blobs,
        kind,
    )
    .fetch_all(conn)
    .await?;
    for row in released {
        events[usize::try_from(row.idx).expect("offered event index")]
            .owned_blobs
            .retain(|blob| blob.blob != row.blob);
    }
    Ok(())
}

//! Durable holds and their immutable operation boundary. Call mutations through the writer.
use crate::{Error, Result};
use sinex_model::backup::{Hold, ProducerCustody, Segment};
use sinex_model::{OperationId, Timestamp};
use sqlx::{Connection, PgConnection};

fn timestamp(value: time::OffsetDateTime) -> Result<Timestamp> {
    Timestamp::from_unix_ns(value.unix_timestamp_nanos()).map_err(|e| Error::Invalid(e.to_string()))
}

pub async fn get(conn: &mut PgConnection, id: OperationId) -> Result<Option<Hold>> {
    let row = sqlx::query!("SELECT o.actor,o.at,h.commit_position,h.blob_changes,h.schema_version,h.released_at
        FROM core.backup_holds h JOIN core.operations o ON o.id=h.operation_id WHERE h.operation_id=$1",id.0)
        .fetch_optional(conn).await?;
    row.map(|row| Ok(Hold { id, actor: row.actor, created_at: timestamp(row.at)?,
        released_at: row.released_at.map(timestamp).transpose()?, commit_position: row.commit_position,
        blob_changes: row.blob_changes, schema_version: row.schema_version,
        producer_custody: ProducerCustody::Incomplete })).transpose()
}

/// The caller holds the existing one-writer claim. `false` means an exact retry.
/// Snapshot staging must not attach a new snapshot to that older boundary.
pub async fn hold(conn: &mut PgConnection, id: OperationId, actor: &str) -> Result<(Hold, bool)> {
    if let Some(existing)=get(conn,id).await? {
        if existing.actor != actor { return Err(Error::Invalid("backup operation belongs to a different actor".into())); }
        return Ok((existing,false));
    }
    let mut tx=conn.begin().await?;
    let position=crate::commit_log::allocate_position(&mut tx).await?;
    let scope=serde_json::json!({"scope":"core","producer_custody":"incomplete"});
    sqlx::query!("INSERT INTO core.operations(id,kind,actor,scope) VALUES($1,'backup_hold',$2,$3)",id.0,actor,scope).execute(&mut *tx).await?;
    let blobs=sqlx::query_scalar!("SELECT value FROM core.counters WHERE name='blob_changes'").fetch_one(&mut *tx).await?;
    sqlx::query!("INSERT INTO core.backup_holds(operation_id,commit_position,blob_changes,schema_version) VALUES($1,$2,$3,$4)",id.0,position,blobs,sinex_schema::VERSION).execute(&mut *tx).await?;
    // Whole prefixes are protected, including existing holes and deleting extents.
    sqlx::query!("INSERT INTO core.backup_segments(hold_id,segment,committed_end) SELECT $1,segment,committed_end FROM core.blob_segments",id.0).execute(&mut *tx).await?;
    sqlx::query!("INSERT INTO core.backup_extents(hold_id,hash,descriptor) SELECT $1,hash,descriptor FROM core.blob_extents",id.0).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok((get(conn,id).await?.expect("hold just committed"),true))
}

/// Release is an operation, not expiry. Every other open hold still protects its prefix.
pub async fn release(conn: &mut PgConnection, id: OperationId, operation: OperationId, actor: &str) -> Result<Hold> {
    let mut tx=conn.begin().await?;
    let released=sqlx::query_scalar!("SELECT released_at FROM core.backup_holds WHERE operation_id=$1 FOR UPDATE",id.0)
        .fetch_optional(&mut *tx).await?.ok_or_else(||Error::Invalid(format!("no backup hold {id}")))?;
    if released.is_none() {
        crate::commit_log::allocate_position(&mut tx).await?;
        let scope=serde_json::json!({"hold":id});
        sqlx::query!("INSERT INTO core.operations(id,kind,actor,scope) VALUES($1,'backup_release',$2,$3)",operation.0,actor,scope).execute(&mut *tx).await?;
        sqlx::query!("UPDATE core.backup_holds SET released_operation=$2,released_at=now() WHERE operation_id=$1",id.0,operation.0).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(get(conn,id).await?.expect("released hold remains recorded"))
}

pub async fn segments(conn: &mut PgConnection, id: OperationId) -> Result<Vec<Segment>> {
    let rows=sqlx::query!("SELECT segment,committed_end FROM core.backup_segments WHERE hold_id=$1 ORDER BY segment",id.0).fetch_all(conn).await?;
    rows.into_iter().map(|row| Ok(Segment { name:row.segment,
        committed_end:u64::try_from(row.committed_end).map_err(|_|Error::Invalid("negative backup prefix".into()))? })).collect()
}

/// A physical mutation is covered by any open prefix intersecting its start.
/// Append-only allocation guarantees an extent never starts before an older prefix and ends beyond it.
pub async fn covers(conn: &mut PgConnection, segment: &str, offset: u64) -> Result<bool> {
    let offset=i64::try_from(offset).map_err(|_|Error::Invalid("backup offset exceeds PostgreSQL range".into()))?;
    Ok(sqlx::query_scalar!(r#"SELECT core.backup_covers($1,$2) AS "covered!""#,segment,offset).fetch_one(conn).await?)
}

/// Open holds are paged without expiry or a total-result cap.
pub async fn list(conn: &mut PgConnection, after: Option<OperationId>, page: i64) -> Result<Vec<Hold>> {
    let after=after.map(|id|id.0);
    let ids=sqlx::query_scalar!("SELECT operation_id FROM core.backup_holds WHERE released_at IS NULL AND ($1::uuid IS NULL OR operation_id>$1) ORDER BY operation_id LIMIT $2",after,page).fetch_all(&mut *conn).await?;
    let mut out=Vec::with_capacity(ids.len());
    for id in ids { out.push(get(conn,OperationId(id)).await?.expect("hold rows are retained")); }
    Ok(out)
}

#[cfg(test)]
mod tests;

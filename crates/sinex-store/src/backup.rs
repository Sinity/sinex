//! Durable holds and their immutable operation boundary. Call mutations through the writer.
use crate::{Error, Result};
use sinex_model::backup::{Hold, Segment};
use sinex_model::{OperationId, Timestamp};
use sqlx::{Connection, PgConnection};

fn timestamp(value: time::OffsetDateTime) -> Result<Timestamp> {
    Timestamp::from_unix_ns(value.unix_timestamp_nanos()).map_err(|e| Error::Invalid(e.to_string()))
}

/// Count open holds and report the oldest committed boundary for status.
pub async fn open_summary(conn: &mut PgConnection) -> Result<(i64, Option<Timestamp>)> {
    let row = sqlx::query!(
        r#"SELECT count(*) AS "open!", min(o.at) AS oldest
           FROM core.backup_holds h JOIN core.operations o ON o.id=h.operation_id
           WHERE h.released_at IS NULL"#
    )
    .fetch_one(conn)
    .await?;
    Ok((row.open, row.oldest.map(timestamp).transpose()?))
}

pub async fn get(conn: &mut PgConnection, id: OperationId) -> Result<Option<Hold>> {
    let row = sqlx::query!("SELECT o.actor,o.at,h.commit_position,h.blob_changes,h.schema_version,h.released_at
        FROM core.backup_holds h JOIN core.operations o ON o.id=h.operation_id WHERE h.operation_id=$1",id.0)
        .fetch_optional(conn).await?;
    row.map(|row| {
        Ok(Hold {
            id,
            actor: row.actor,
            created_at: timestamp(row.at)?,
            released_at: row.released_at.map(timestamp).transpose()?,
            commit_position: row.commit_position,
            blob_changes: row.blob_changes,
            schema_version: row.schema_version,
        })
    })
    .transpose()
}

/// The caller holds the existing one-writer claim. `false` means an exact retry.
/// Snapshot staging must not attach a new snapshot to that older boundary.
pub async fn hold(conn: &mut PgConnection, id: OperationId, actor: &str) -> Result<(Hold, bool)> {
    if let Some(existing) = get(conn, id).await? {
        if existing.actor != actor {
            return Err(Error::Invalid(
                "backup operation belongs to a different actor".into(),
            ));
        }
        return Ok((existing, false));
    }
    let mut tx = conn.begin().await?;
    let position = crate::commit_log::allocate_position(&mut tx).await?;
    let scope = serde_json::json!({"scope":"core"});
    sqlx::query!("INSERT INTO core.operations(id,kind,actor,scope,start_seq,end_seq,result) VALUES($1,'backup_hold',$2,$3,$4,$4,'ok')",id.0,actor,scope,position).execute(&mut *tx).await?;
    let blobs = sqlx::query_scalar!("SELECT value FROM core.counters WHERE name='blob_changes'")
        .fetch_one(&mut *tx)
        .await?;
    sqlx::query!("INSERT INTO core.backup_holds(operation_id,commit_position,blob_changes,schema_version) VALUES($1,$2,$3,$4)",id.0,position,blobs,sinex_schema::VERSION).execute(&mut *tx).await?;
    // Whole prefixes are protected, including existing holes and deleting extents.
    sqlx::query!("INSERT INTO core.backup_segments(hold_id,segment,committed_end) SELECT $1,segment,committed_end FROM core.blob_segments",id.0).execute(&mut *tx).await?;

    tx.commit().await?;
    Ok((get(conn, id).await?.expect("hold just committed"), true))
}

/// Release is an operation, not expiry. Every other open hold still protects its prefix.
pub async fn release(
    conn: &mut PgConnection,
    id: OperationId,
    operation: OperationId,
    actor: &str,
) -> Result<Hold> {
    let mut tx = conn.begin().await?;
    let released = sqlx::query_scalar!(
        "SELECT released_at FROM core.backup_holds WHERE operation_id=$1 FOR UPDATE",
        id.0
    )
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| Error::Invalid(format!("no backup hold {id}")))?;
    if released.is_none() {
        let position = crate::commit_log::allocate_position(&mut tx).await?;
        let scope = serde_json::json!({"hold":id});
        sqlx::query!("INSERT INTO core.operations(id,kind,actor,scope,start_seq,end_seq,result) VALUES($1,'backup_release',$2,$3,$4,$4,'ok')",operation.0,actor,scope,position).execute(&mut *tx).await?;
        sqlx::query!("UPDATE core.backup_holds SET released_operation=$2,released_at=now() WHERE operation_id=$1",id.0,operation.0).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(get(conn, id)
        .await?
        .expect("released hold remains recorded"))
}

pub async fn segments(conn: &mut PgConnection, id: OperationId) -> Result<Vec<Segment>> {
    let rows = sqlx::query!(
        "SELECT segment,committed_end FROM core.backup_segments WHERE hold_id=$1 ORDER BY segment",
        id.0
    )
    .fetch_all(conn)
    .await?;
    rows.into_iter()
        .map(|row| {
            Ok(Segment {
                name: row.segment,
                committed_end: u64::try_from(row.committed_end)
                    .map_err(|_| Error::Invalid("negative backup prefix".into()))?,
            })
        })
        .collect()
}

/// A physical mutation is covered by any open prefix intersecting its start.
/// Append-only allocation guarantees an extent never starts before an older prefix and ends beyond it.
pub async fn covers(conn: &mut PgConnection, segment: &str, offset: u64) -> Result<bool> {
    let offset = i64::try_from(offset)
        .map_err(|_| Error::Invalid("backup offset exceeds PostgreSQL range".into()))?;
    Ok(sqlx::query_scalar!(
        r#"SELECT core.backup_covers($1,$2) AS "covered!""#,
        segment,
        offset
    )
    .fetch_one(conn)
    .await?)
}

/// Union of open prefix holds, loaded once for startup's deferred punches.
pub(crate) async fn open_prefixes(
    conn: &mut PgConnection,
) -> Result<std::collections::BTreeMap<String, u64>> {
    let rows = sqlx::query!(
        r#"SELECT s.segment,max(s.committed_end) AS "committed_end!"
        FROM core.backup_segments s JOIN core.backup_holds h ON h.operation_id=s.hold_id
        WHERE h.released_at IS NULL GROUP BY s.segment"#
    )
    .fetch_all(conn)
    .await?;
    rows.into_iter()
        .map(|row| {
            Ok((
                row.segment,
                u64::try_from(row.committed_end)
                    .map_err(|_| Error::Invalid("negative backup prefix".into()))?,
            ))
        })
        .collect()
}

/// Open holds are paged without expiry or a total-result cap.
pub async fn list(
    conn: &mut PgConnection,
    after: Option<OperationId>,
    page: i64,
) -> Result<Vec<Hold>> {
    let after = after.map(|id| id.0);
    let ids=sqlx::query_scalar!("SELECT operation_id FROM core.backup_holds WHERE released_at IS NULL AND ($1::uuid IS NULL OR operation_id>$1) ORDER BY operation_id LIMIT $2",after,page).fetch_all(&mut *conn).await?;
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        out.push(
            get(conn, OperationId(id))
                .await?
                .expect("hold rows are retained"),
        );
    }
    Ok(out)
}

#[cfg(test)]
mod tests;

/// One exported snapshot, established while the writer still owns the cut.
/// Dropping its connection invalidates the snapshot; an old hold cannot recreate it.
pub struct Snapshot {
    connection: PgConnection,
    database_url: String,
    pub exported: String,
    pub hold: Hold,
    pub database: sinex_model::backup::Database,
}

impl Snapshot {
    pub async fn export(
        mut connection: PgConnection,
        hold: Hold,
        database_url: String,
    ) -> Result<Self> {
        sqlx::query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut connection)
            .await?;
        let exported: String = sqlx::query_scalar("SELECT pg_export_snapshot()")
            .fetch_one(&mut connection)
            .await?;
        let (name, oid): (String, i64) = sqlx::query_as(
            "SELECT datname,oid::bigint FROM pg_database WHERE datname=current_database()",
        )
        .fetch_one(&mut connection)
        .await?;
        // The first snapshot read must include the hold this job just committed.
        let observed = get(&mut connection, hold.id).await?;
        let (position,blobs): (i64,i64) = sqlx::query_as(
            "SELECT (SELECT value FROM core.counters WHERE name='commit_seq'),(SELECT value FROM core.counters WHERE name='blob_changes')")
            .fetch_one(&mut connection).await?;
        if position != hold.commit_position
            || blobs != hold.blob_changes
            || observed.as_ref() != Some(&hold)
            || hold.released_at.is_some()
        {
            return Err(Error::Invalid(
                "the exported snapshot does not contain its open backup hold".into(),
            ));
        }
        Ok(Self {
            connection,
            database_url,
            exported,
            hold,
            database: sinex_model::backup::Database { name, oid },
        })
    }

    /// Keep the original connection URI private. Child adapters must remove its
    /// password before constructing arguments; manifests never contain it.
    pub fn database_url(&self) -> &str {
        &self.database_url
    }

    pub fn connection(&mut self) -> &mut PgConnection {
        &mut self.connection
    }

    pub async fn validate_copy(&mut self, root: &std::path::Path) -> Result<()> {
        validate_copy(&mut self.connection, self.hold.id, root).await
    }

    pub async fn close(mut self) -> Result<()> {
        sqlx::query("ROLLBACK")
            .execute(&mut self.connection)
            .await?;
        self.connection.close().await?;
        Ok(())
    }
}

/// Validate copied live descriptors in bounded pages against their actual encoded bytes.
/// Deleting descriptors are not readable authority and may already be punched after a crash.
async fn validate_copy(
    conn: &mut PgConnection,
    id: OperationId,
    root: &std::path::Path,
) -> Result<()> {
    let prefixes: std::collections::BTreeMap<_, _> = segments(conn, id)
        .await?
        .into_iter()
        .map(|s| (s.name, s.committed_end))
        .collect();
    let mut after = String::new();
    loop {
        let rows = sqlx::query!("SELECT hash,descriptor FROM core.blob_extents WHERE NOT deleting AND hash>$1 ORDER BY hash LIMIT 128",after)
            .fetch_all(&mut *conn).await?;
        if rows.is_empty() {
            return Ok(());
        }
        let mut extents = Vec::with_capacity(rows.len());
        for row in rows {
            let extent = hashed_blobs::Extent::decode(&row.descriptor)?;
            if !std::path::Path::new(&extent.segment)
                .components()
                .all(|part| matches!(part, std::path::Component::Normal(_)))
            {
                return Err(Error::Invalid(
                    "backup descriptor has a non-relative segment path".into(),
                ));
            }
            if prefixes
                .get(&extent.segment)
                .is_none_or(|end| extent.end() > *end)
            {
                return Err(Error::Invalid(
                    "backup extent exceeds its protected committed prefix".into(),
                ));
            }
            if extent.hash != crate::blobs::parse(&row.hash)? {
                return Err(Error::Invalid(format!(
                    "backup descriptor differs from {}",
                    row.hash
                )));
            }
            after = row.hash;
            extents.push(extent);
        }
        let root = root.to_owned();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let reader = hashed_blobs::Reader::new(&root);
            for extent in extents {
                if let Some(mismatch) = reader.scan(&[extent]).first() {
                    return Err(Error::Invalid(format!(
                        "backup extent cannot be read: {}",
                        mismatch.error
                    )));
                }
            }
            Ok(())
        })
        .await
        .map_err(|error| Error::Invalid(error.to_string()))??;
    }
}

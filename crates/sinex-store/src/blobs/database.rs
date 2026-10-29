//! SQL is the durable authority for extents and segment boundaries.
use super::*;
use sqlx::Connection;

/// Groups covered by one database transaction. Acknowledgment follows its commit.
pub struct Commit {
    groups: Vec<Arc<SyncedGroup>>,
}
impl Blobs {
    /// Load read authority without opening a writer or changing segment files.
    pub async fn load(&self, conn: &mut sqlx::PgConnection) -> Result<()> {
        let rows =
            sqlx::query!("SELECT hash,descriptor,deleting FROM core.blob_extents ORDER BY hash")
                .fetch_all(&mut *conn)
                .await?;
        let end: Option<i64> =
            sqlx::query_scalar!("SELECT committed_end FROM core.blob_segments WHERE segment=$1",SEGMENT)
                .fetch_optional(conn)
                .await?;
        let mut state = self.state()?;
        if state.writer.is_some() || !state.groups.is_empty() || !state.prepared.is_empty() {
            return Err(crate::Error::Invalid(
                "cannot reload an active blob writer".into(),
            ));
        }
        let committed_end = u64::try_from(end.unwrap_or(0))
            .map_err(|_| crate::Error::Invalid("negative committed segment end".into()))?;
        let mut extents = BTreeMap::new();
        for row in rows {
            let extent = Extent::decode(&row.descriptor)?;
            let name=row.hash;
            if extent.hash != parse(&name)?
                || extent.segment != SEGMENT
                || extent.end() > committed_end
            {
                return Err(crate::Error::Invalid(format!(
                    "extent for {name} differs from its catalog name or committed segment boundary"
                )));
            }
            if !row.deleting {
                extents.insert(extent.hash, extent);
            }
        }
        state.extents = extents;
        state.initialized = true;
        state.committed_end = committed_end;
        Ok(())
    }
    /// The caller holds the database writer claim. Ordinary startup truncates tails and replays punches.
    pub async fn recover(&self, conn: &mut sqlx::PgConnection) -> Result<()> {
        self.load(conn).await?;
        self.require_converted(conn).await?;
        let descriptors: Vec<Vec<u8>> =
            sqlx::query_scalar!("SELECT descriptor FROM core.blob_extents WHERE deleting")
                .fetch_all(&mut *conn)
                .await?;
        let punches = descriptors
            .iter()
            .map(|bytes| Extent::decode(bytes))
            .collect::<hashed_blobs::Result<Vec<_>>>()?;
        {
            let mut state = self.state()?;
            if state.writer.is_none() {
                state.writer = Some(SegmentWriter::open(
                    self.root(),
                    SEGMENT,
                    state.committed_end,
                    &punches,
                    self.0.mode,
                )?);
            }
        }
        if !punches.is_empty() {
            sqlx::query!("DELETE FROM core.blob_extents WHERE deleting")
                .execute(conn)
                .await?;
        }
        Ok(())
    }
    /// Persist every synced group in the same transaction as the rows citing its bytes.
    pub async fn commit_extents(&self, conn: &mut sqlx::PgConnection) -> Result<Commit> {
        self.sync_pending()?;
        let groups = self.state()?.groups.clone();
        for group in &groups {
            for extent in &group.extents {
                let name=extent.hash.to_string();
                let descriptor=extent.encode();
                sqlx::query!("INSERT INTO core.blob_extents(hash,descriptor,deleting) VALUES($1,$2,false) ON CONFLICT(hash) DO UPDATE SET descriptor=EXCLUDED.descriptor,deleting=false",name,descriptor).execute(&mut *conn).await?;
            }
            let end=i64::try_from(group.end).map_err(|_|crate::Error::Invalid("segment end exceeds i64".into()))?;
            sqlx::query!("INSERT INTO core.blob_segments(segment,committed_end) VALUES($1,$2) ON CONFLICT(segment) DO UPDATE SET committed_end=greatest(core.blob_segments.committed_end,EXCLUDED.committed_end)",SEGMENT,end).execute(&mut *conn).await?;
        }
        let deleting: bool =
            sqlx::query_scalar!("SELECT EXISTS(SELECT 1 FROM core.blob_extents x JOIN core.blobs b ON b.hash=x.hash WHERE x.deleting IS DISTINCT FROM (b.status='deleting')) AS \"changed!\"")
                .fetch_one(&mut *conn)
                .await?;
        if deleting {
            sqlx::query!("UPDATE core.blob_extents x SET deleting=(b.status='deleting') FROM core.blobs b WHERE b.hash=x.hash AND x.deleting IS DISTINCT FROM (b.status='deleting')").execute(conn).await?;
        }
        Ok(Commit { groups })
    }
    pub fn acknowledge(&self, commit: Commit) -> Result<()> {
        let mut state = self.state()?;
        for group in commit.groups {
            state
                .writer
                .as_mut()
                .ok_or_else(|| {
                    crate::Error::Invalid("no segment writer for committed group".into())
                })?
                .acknowledge(&group)?;
            state.committed_end = group.end;
            if state
                .groups
                .first()
                .is_none_or(|pending| pending.id != group.id)
            {
                return Err(crate::Error::Invalid(
                    "segment groups acknowledged out of order".into(),
                ));
            }
            state.groups.remove(0);
        }
        Ok(())
    }
}
/// The deleting marker commits before punching. A crash resumes it on ordinary open.
pub async fn delete_unreferenced_extents(
    conn: &mut sqlx::PgConnection,
    blobs: &Blobs,
) -> Result<Vec<String>> {
    let mut found = Vec::new();
    // A fresh in-process store may already hold preparations made by a test or import.
    let receipt = {
        let mut tx = conn.begin().await?;
        let receipt = blobs.commit_extents(&mut tx).await?;
        tx.commit().await?;
        receipt
    };
    blobs.acknowledge(receipt)?;
    let names:Vec<String>=sqlx::query_scalar!("SELECT x.hash FROM core.blob_extents x WHERE NOT EXISTS(SELECT 1 FROM core.blobs b WHERE b.hash=x.hash)").fetch_all(&mut *conn).await?;
    if !names.is_empty() {
        sqlx::query!("UPDATE core.blob_extents SET deleting=true WHERE hash=ANY($1)",&names)
            .execute(&mut *conn)
            .await?;
    }
    for name in names {
        blobs.delete(&name)?;
        sqlx::query!("DELETE FROM core.blob_extents WHERE hash=$1 AND deleting",name)
            .execute(&mut *conn)
            .await?;
        found.push(name);
    }
    Ok(found)
}
/// Bytes already committed by admission need no separate filesystem marker ledger.
pub async fn commit_pending_extents(conn: &mut sqlx::PgConnection, blobs: &Blobs) -> Result<()> {
    let mut tx = conn.begin().await?;
    let receipt = blobs.commit_extents(&mut tx).await?;
    tx.commit().await?;
    blobs.acknowledge(receipt)
}

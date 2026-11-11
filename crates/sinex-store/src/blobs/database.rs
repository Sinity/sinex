//! SQL is the durable authority for extents and segment boundaries.
use super::*;
use sqlx::Connection;

/// Groups covered by one database transaction. Acknowledgment follows its commit.
pub struct Commit {
    groups: Vec<Arc<SyncedGroup>>,
    chunk_lists: Vec<Hash>,
}
impl Blobs {
    /// Load read authority without opening a writer or changing segment files.
    pub async fn load(&self, conn: &mut sqlx::PgConnection) -> Result<()> {
        let rows =
            sqlx::query!("SELECT hash,descriptor,deleting FROM core.blob_extents ORDER BY hash")
                .fetch_all(&mut *conn)
                .await?;
        let end: Option<i64> = sqlx::query_scalar!(
            "SELECT committed_end FROM core.blob_segments WHERE segment=$1",
            SEGMENT
        )
        .fetch_optional(&mut *conn)
        .await?;
        let chunk_rows =
            sqlx::query!("SELECT blob, chunk FROM core.blob_chunks ORDER BY blob, ordinal")
                .fetch_all(&mut *conn)
                .await?;
        let mut chunk_lists: BTreeMap<Hash, Vec<Hash>> = BTreeMap::new();
        for row in chunk_rows {
            chunk_lists
                .entry(parse(&row.blob)?)
                .or_default()
                .push(parse(&row.chunk)?);
        }
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
            let name = row.hash;
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
        state.chunk_lists = chunk_lists;
        state.unrecorded_chunk_lists.clear();
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
        let prefixes = crate::backup::open_prefixes(conn).await?;
        let mut punches = Vec::new();
        for descriptor in &descriptors {
            let extent = Extent::decode(descriptor)?;
            if prefixes
                .get(&extent.segment)
                .is_none_or(|end| extent.offset >= *end)
            {
                punches.push(extent);
            }
        }
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
            let hashes: Vec<String> = punches
                .iter()
                .map(|extent| extent.hash.to_string())
                .collect();
            sqlx::query!(
                "DELETE FROM core.blob_extents WHERE deleting AND hash=ANY($1)",
                &hashes
            )
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
                let name = extent.hash.to_string();
                let descriptor = extent.encode();
                sqlx::query!("INSERT INTO core.blob_extents(hash,descriptor,deleting) VALUES($1,$2,COALESCE((SELECT status='deleting' FROM core.blobs WHERE hash=$1),false)) ON CONFLICT(hash) DO UPDATE SET descriptor=EXCLUDED.descriptor,deleting=EXCLUDED.deleting",name,descriptor).execute(&mut *conn).await?;
            }
            let end = i64::try_from(group.end)
                .map_err(|_| crate::Error::Invalid("segment end exceeds i64".into()))?;
            sqlx::query!("INSERT INTO core.blob_segments(segment,committed_end) VALUES($1,$2) ON CONFLICT(segment) DO UPDATE SET committed_end=greatest(core.blob_segments.committed_end,EXCLUDED.committed_end)",SEGMENT,end).execute(&mut *conn).await?;
        }
        // A chunk list is recorded once its chunks' extents are committed:
        // they were appended by now, or already held.
        let chunk_lists: Vec<(Hash, Vec<Hash>)> = {
            let state = self.state()?;
            state
                .unrecorded_chunk_lists
                .iter()
                .filter_map(|blob| Some((*blob, state.chunk_lists.get(blob)?.clone())))
                .collect()
        };
        for (blob, chunks) in &chunk_lists {
            let chunks: Vec<String> = chunks.iter().map(Hash::to_string).collect();
            sqlx::query!(
                "INSERT INTO core.blob_chunks (blob, ordinal, chunk)
                 SELECT $1, (n - 1)::integer, chunk FROM unnest($2::text[]) WITH ORDINALITY AS c(chunk, n)
                 ON CONFLICT DO NOTHING",
                blob.to_string(),
                &chunks
            )
            .execute(&mut *conn)
            .await?;
        }
        Ok(Commit {
            groups,
            chunk_lists: chunk_lists.into_iter().map(|(blob, _)| blob).collect(),
        })
    }
    pub fn acknowledge(&self, commit: Commit) -> Result<()> {
        let mut state = self.state()?;
        for blob in &commit.chunk_lists {
            state.unrecorded_chunk_lists.remove(blob);
        }
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
    // A fresh in-process store may already hold preparations made by a test or import.
    let receipt = {
        let mut tx = conn.begin().await?;
        let receipt = blobs.commit_extents(&mut tx).await?;
        tx.commit().await?;
        receipt
    };
    blobs.acknowledge(receipt)?;
    // Chunk lists of bytes no row cites go with them.
    sqlx::query!("DELETE FROM core.blob_chunks c WHERE NOT EXISTS(SELECT 1 FROM core.blobs b WHERE b.hash=c.blob AND b.status<>'removed')").execute(&mut *conn).await?;
    let names:Vec<String>=sqlx::query_scalar!("SELECT x.hash FROM core.blob_extents x WHERE NOT EXISTS(SELECT 1 FROM core.blobs b WHERE b.hash=x.hash) AND NOT EXISTS(SELECT 1 FROM core.blob_chunks c WHERE c.chunk=x.hash)").fetch_all(&mut *conn).await?;
    if !names.is_empty() {
        sqlx::query!(
            "UPDATE core.blob_extents SET deleting=true WHERE hash=ANY($1)",
            &names
        )
        .execute(&mut *conn)
        .await?;
    }
    finish_deleting_extents(conn, blobs).await
}

/// Only previously marked orphan extents are eligible; do not flush or inspect new preparations.
pub async fn finish_deleting_extents(
    conn: &mut sqlx::PgConnection,
    blobs: &Blobs,
) -> Result<Vec<String>> {
    let mut found = Vec::new();
    let names:Vec<String>=sqlx::query_scalar!("SELECT x.hash FROM core.blob_extents x WHERE x.deleting AND NOT EXISTS(SELECT 1 FROM core.blobs b WHERE b.hash=x.hash) AND NOT EXISTS(SELECT 1 FROM core.blob_chunks c WHERE c.chunk=x.hash) ORDER BY x.hash").fetch_all(&mut *conn).await?;
    for name in names {
        if !blobs.delete(conn, &name).await? {
            continue;
        }
        sqlx::query!(
            "DELETE FROM core.blob_extents WHERE hash=$1 AND deleting",
            name
        )
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

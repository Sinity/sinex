//! SQL decides which extents and segment boundaries exist.
use super::*;
use sqlx::Connection;

/// Groups covered by one database transaction. Acknowledgment follows its commit.
pub struct Commit {
    groups: Vec<Arc<SyncedGroup>>,
    chunk_lists: Vec<Hash>,
}
impl Blobs {
    /// Load only the committed segment boundary. Reads resolve selected catalog rows.
    pub async fn load(&self, conn: &mut sqlx::PgConnection) -> Result<()> {
        let end: Option<i64> = sqlx::query_scalar!(
            "SELECT committed_end FROM core.blob_segments WHERE segment=$1",
            SEGMENT,
        )
        .fetch_optional(conn)
        .await?;
        let mut state = self.state()?;
        if state.writer.is_some() || !state.groups.is_empty() {
            return Err(crate::Error::Invalid(
                "cannot reload an active blob writer".into(),
            ));
        }
        state.committed_end = u64::try_from(end.unwrap_or(0))
            .map_err(|_| crate::Error::Invalid("negative committed segment end".into()))?;
        state.initialized = true;
        Ok(())
    }
    /// The caller holds the database writer claim. Startup truncates tails and replays punches.
    pub async fn recover(&self, conn: &mut sqlx::PgConnection) -> Result<()> {
        self.load(conn).await?;
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
    /// The caller owns the database writer claim. Its serialized admissions
    /// deduplicate and append these names, then publish their durable prefix.
    pub async fn commit_extents(
        &self,
        conn: &mut sqlx::PgConnection,
        names: &[String],
    ) -> Result<Commit> {
        let required: BTreeSet<Hash> = names
            .iter()
            .map(|name| parse(name))
            .collect::<Result<_>>()?;
        // Admission already owns the single database writer. Resolve only this
        // admission's representation before appending, never from a reader snapshot.
        let mut known = super::located::catalog_extents(conn, names).await?;
        let rows = sqlx::query!(
            "SELECT blob,chunk FROM core.blob_chunks WHERE blob=ANY($1) ORDER BY blob,ordinal",
            names,
        )
        .fetch_all(&mut *conn)
        .await?;
        let mut recorded_lists: BTreeMap<Hash, Vec<Hash>> = BTreeMap::new();
        for row in rows {
            recorded_lists
                .entry(parse(&row.blob)?)
                .or_default()
                .push(parse(&row.chunk)?);
        }
        let pending_lists: Vec<(Hash, Vec<Hash>)> = {
            let state = self.state()?;
            state
                .unrecorded_chunk_lists
                .iter()
                .filter(|(hash, _)| required.contains(hash))
                .map(|(hash, chunks)| (*hash, chunks.iter().map(|chunk| chunk.hash).collect()))
                .collect()
        };
        let mut parts = BTreeSet::new();
        for hash in &required {
            if known.contains_key(hash) {
                parts.insert(*hash);
            } else if let Some(chunks) = recorded_lists.get(hash).or_else(|| {
                pending_lists
                    .iter()
                    .find(|(blob, _)| blob == hash)
                    .map(|(_, chunks)| chunks)
            }) {
                parts.extend(chunks.iter().copied());
            } else {
                parts.insert(*hash);
            }
        }
        let missing: Vec<_> = parts
            .iter()
            .filter(|hash| !known.contains_key(hash))
            .map(Hash::to_string)
            .collect();
        known.extend(super::located::catalog_extents(conn, &missing).await?);
        // Pending claims may still own an extent marked for deletion; their
        // lifecycle hold keeps its bytes and row until this admission publishes.
        known.extend(self.state()?.pending_stored(&parts));
        {
            let mut state = self.state()?;
            for hash in &required {
                // A whole-file preparation can repeat bytes already held as
                // chunks. Keep the recorded representation and append nothing.
                if let (Some(prepared), Some(chunks)) =
                    (state.prepared.get(hash), recorded_lists.get(hash))
                {
                    let length = chunks.iter().try_fold(0u64, |length, chunk| {
                        let extent = known
                            .get(chunk)
                            .cloned()
                            .or_else(|| state.pending_extent(chunk))
                            .ok_or_else(|| {
                                crate::Error::Invalid(format!("chunk {chunk} has no stored extent"))
                            })?;
                        length.checked_add(extent.decoded_len).ok_or_else(|| {
                            crate::Error::Invalid("chunk length exceeds address space".into())
                        })
                    })?;
                    if prepared.kind() != &Kind::Plain || prepared.decoded_len() != length {
                        return Err(crate::Error::Invalid(
                            "repeated blob name has a different length or sealing key reference"
                                .into(),
                        ));
                    }
                }
            }
            for hash in recorded_lists.keys() {
                state.prepared.remove(hash);
            }
        }
        if !names.is_empty() {
            let store = self.clone();
            let parts = parts.clone();
            let known = known.clone();
            tokio::task::spawn_blocking(move || store.sync_pending(&parts, &known))
                .await
                .map_err(|error| crate::Error::Invalid(error.to_string()))??;
        }
        let groups = {
            let state = self.state()?;
            for hash in &parts {
                if !known.contains_key(hash) && state.pending_extent(hash).is_none() {
                    return Err(hashed_blobs::Error::Refused(format!(
                        "admitted blob {hash} has no synced extent"
                    ))
                    .into());
                }
            }
            state.groups.clone()
        };
        for group in &groups {
            for extent in &group.extents {
                let name = extent.hash.to_string();
                let descriptor = extent.encode();
                sqlx::query!("INSERT INTO core.blob_extents(hash,descriptor,deleting) VALUES($1,$2,COALESCE((SELECT status='deleting' FROM core.blobs WHERE hash=$1),false) AND NOT EXISTS(SELECT 1 FROM core.blob_chunks c JOIN core.blobs b ON b.hash=c.blob WHERE c.chunk=$1 AND c.blob<>$1 AND b.status<>'removed')) ON CONFLICT(hash) DO UPDATE SET descriptor=EXCLUDED.descriptor,deleting=EXCLUDED.deleting",name,descriptor).execute(&mut *conn).await?;
            }
            let end = i64::try_from(group.end)
                .map_err(|_| crate::Error::Invalid("segment end exceeds i64".into()))?;
            sqlx::query!("INSERT INTO core.blob_segments(segment,committed_end) VALUES($1,$2) ON CONFLICT(segment) DO UPDATE SET committed_end=greatest(core.blob_segments.committed_end,EXCLUDED.committed_end)",SEGMENT,end).execute(&mut *conn).await?;
        }
        // Only newly selected lists are recorded. Previously committed direct
        // bytes or chunks retain their canonical representation.
        let chunk_lists: Vec<_> = pending_lists
            .iter()
            .filter(|(blob, _)| !known.contains_key(blob) && !recorded_lists.contains_key(blob))
            .collect();
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
            // A newly held blob can reuse an extent whose own blob row
            // was already marked for removal.
            sqlx::query!(
                "UPDATE core.blob_extents SET deleting=false WHERE hash=ANY($1) AND deleting",
                &chunks,
            )
            .execute(&mut *conn)
            .await?;
        }
        Ok(Commit {
            groups,
            chunk_lists: pending_lists.into_iter().map(|(blob, _)| blob).collect(),
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
/// The deleting marker commits before punching. A crash resumes it on the next open.
pub async fn delete_unreferenced_extents(
    conn: &mut sqlx::PgConnection,
    blobs: &Blobs,
) -> Result<Vec<String>> {
    // Finish already synced bytes; unadmitted preparations retain their owner.
    let extent_commit = {
        let mut tx = conn.begin().await?;
        let extent_commit = blobs.commit_extents(&mut tx, &[]).await?;
        tx.commit().await?;
        extent_commit
    };
    blobs.acknowledge(extent_commit)?;
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
        if blobs.held_by_pending_chunk(&name)? || !blobs.delete(conn, &name).await? {
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
pub async fn commit_pending_extents(
    conn: &mut sqlx::PgConnection,
    blobs: &Blobs,
    names: &[String],
) -> Result<()> {
    let mut tx = conn.begin().await?;
    let extent_commit = blobs.commit_extents(&mut tx, names).await?;
    tx.commit().await?;
    blobs.acknowledge(extent_commit)
}

//! Immutable handles for the selected bytes, resolved before blocking decoding.
use super::*;

pub enum ChunkBytes {
    Prepared(Prepared),
    Located(Located),
}
pub(super) enum Part {
    Stored(Extent),
    Prepared(Prepared),
}
/// Only this blob's ordered parts. A prepared part owns a duplicate encoded
/// inode handle; publishing or dropping the writer's preparation cannot change it.
pub struct Located {
    pub(super) hash: Hash,
    pub(super) values: Vec<Part>,
}
impl Part {
    fn try_clone(&self) -> Result<Self> {
        match self {
            Self::Stored(extent) => Ok(Self::Stored(extent.clone())),
            Self::Prepared(value) => Ok(Self::Prepared(value.try_clone()?)),
        }
    }
}
impl Located {
    pub fn name(&self) -> String {
        self.hash.to_string()
    }
    pub fn is_sealed(&self) -> bool {
        self.values.iter().any(|part| match part {
            Part::Stored(extent) => matches!(extent.kind, Kind::Sealed { .. }),
            Part::Prepared(value) => matches!(value.kind(), Kind::Sealed { .. }),
        })
    }
    pub(super) fn decoded_len(&self) -> u64 {
        self.values
            .iter()
            .map(|part| match part {
                Part::Stored(extent) => extent.decoded_len,
                Part::Prepared(value) => value.decoded_len(),
            })
            .sum()
    }
    pub(super) fn parts(&self) -> impl Iterator<Item = hashed_blobs::ReadPart<'_>> {
        self.values.iter().map(|part| match part {
            Part::Stored(extent) => hashed_blobs::ReadPart::Stored(extent),
            Part::Prepared(value) => hashed_blobs::ReadPart::Prepared(value),
        })
    }
    pub(super) fn extents(&self) -> Result<Vec<Extent>> {
        self.values
            .iter()
            .map(|part| match part {
                Part::Stored(extent) => Ok(extent.clone()),
                Part::Prepared(_) => Err(crate::Error::Invalid(
                    "prepared bytes have no published segment descriptor".into(),
                )),
            })
            .collect()
    }
    pub(super) fn present(&self, root: &Path) -> Result<bool> {
        for part in &self.values {
            match part {
                Part::Stored(extent) => match std::fs::metadata(root.join(&extent.segment)) {
                    Ok(value) if value.len() >= extent.end() => {}
                    Ok(_) => return Ok(false),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
                    Err(error) => return Err(error.into()),
                },
                Part::Prepared(value) => {
                    // Duplication verifies the encoded inode's exact length.
                    value.try_clone()?;
                }
            }
        }
        Ok(true)
    }
}
impl State {
    fn pending_parts(&self, hashes: &BTreeSet<Hash>) -> Result<BTreeMap<Hash, Part>> {
        let mut found: BTreeMap<_, _> = self
            .pending_stored(hashes)
            .into_iter()
            .map(|(hash, extent)| (hash, Part::Stored(extent)))
            .collect();
        for hash in hashes {
            if !found.contains_key(hash)
                && let Some(value) = self.prepared.get(hash)
            {
                found.insert(*hash, Part::Prepared(value.try_clone()?));
            }
        }
        for located in self
            .unrecorded_chunk_lists
            .values()
            .flatten()
            .filter_map(|chunk| chunk.held.as_ref())
        {
            for part in &located.values {
                if let Part::Prepared(value) = part
                    && hashes.contains(&value.hash())
                    && !found.contains_key(&value.hash())
                {
                    found.insert(value.hash(), Part::Prepared(value.try_clone()?));
                }
            }
        }
        Ok(found)
    }
}

impl Blobs {
    /// Resolve one blob through pending owned bytes or indexed catalog lookups.
    /// This does not open a writer, append bytes, or retain other catalog rows.
    pub async fn locate(&self, conn: &mut sqlx::PgConnection, name: &str) -> Result<Located> {
        self.find(conn, name).await?.ok_or_else(|| {
            crate::Error::Invalid(format!("blob {name} has no committed or pending extent"))
        })
    }
    pub async fn contains(&self, conn: &mut sqlx::PgConnection, name: &str) -> Result<bool> {
        match self.find(conn, name).await? {
            Some(located) => self.available(&located),
            None => Ok(false),
        }
    }
    /// Resolve a selected group in a fixed number of catalog queries. Results
    /// retain the caller's order, including repeated names and absent blobs.
    pub async fn locate_many(
        &self,
        conn: &mut sqlx::PgConnection,
        names: &[String],
    ) -> Result<Vec<Option<Located>>> {
        let hashes = names
            .iter()
            .map(|name| parse(name))
            .collect::<Result<Vec<_>>>()?;
        // Committed reads never acquire the append mutex. A large admission
        // cannot hold an unrelated point read behind its segment copy.
        let mut known = catalog_extents(conn, names).await?;
        let unresolved: Vec<_> = hashes
            .iter()
            .filter(|hash| !known.contains_key(hash))
            .map(Hash::to_string)
            .collect();
        let mut lists: BTreeMap<Hash, Vec<Hash>> = BTreeMap::new();
        if !unresolved.is_empty() {
            let rows = sqlx::query!(
                "SELECT blob,chunk FROM core.blob_chunks WHERE blob=ANY($1) ORDER BY blob,ordinal",
                &unresolved,
            )
            .fetch_all(&mut *conn)
            .await?;
            for row in rows {
                lists
                    .entry(parse(&row.blob)?)
                    .or_default()
                    .push(parse(&row.chunk)?);
            }
        }
        let recorded_parts: BTreeSet<_> = lists.values().flatten().copied().collect();
        let missing: Vec<_> = recorded_parts
            .iter()
            .filter(|hash| !known.contains_key(hash))
            .map(Hash::to_string)
            .collect();
        known.extend(catalog_extents(conn, &missing).await?);
        let unresolved: BTreeSet<_> = hashes
            .iter()
            .filter(|hash| {
                !known.contains_key(hash)
                    && lists
                        .get(hash)
                        .is_none_or(|chunks| chunks.iter().any(|chunk| !known.contains_key(chunk)))
            })
            .copied()
            .collect();
        let (pending, pending_parts) = if unresolved.is_empty() {
            (BTreeMap::new(), BTreeMap::new())
        } else {
            let store = self.clone();
            let recorded_parts = recorded_parts.clone();
            let (pending, parts, pending_lists) =
                tokio::task::spawn_blocking(move || -> Result<_> {
                    let state = store.state()?;
                    let mut selected = unresolved.clone();
                    selected.extend(recorded_parts);
                    let mut lists = BTreeMap::new();
                    for hash in &unresolved {
                        if let Some(chunks) = state.unrecorded_chunk_lists.get(hash) {
                            let order: Vec<_> = chunks.iter().map(|chunk| chunk.hash).collect();
                            selected.extend(order.iter().copied());
                            lists.insert(*hash, order);
                        }
                    }
                    // Snapshot unpublished lists with their immutable handles.
                    // Waiting on the append mutex stays off the async workers.
                    let parts = state.pending_parts(&selected)?;
                    let mut pending = BTreeMap::new();
                    for hash in &unresolved {
                        if let Some(part) = parts.get(hash) {
                            pending.insert(*hash, part.try_clone()?);
                        }
                    }
                    Ok((pending, parts, lists))
                })
                .await
                .map_err(|error| crate::Error::Invalid(error.to_string()))??;
            for (hash, chunks) in pending_lists {
                lists.entry(hash).or_insert(chunks);
            }
            (pending, parts)
        };
        let missing: Vec<_> = lists
            .values()
            .flatten()
            .filter(|hash| !pending_parts.contains_key(hash) && !known.contains_key(hash))
            .map(Hash::to_string)
            .collect();
        known.extend(catalog_extents(conn, &missing).await?);
        hashes
            .into_iter()
            .map(|hash| {
                let values = if let Some(extent) = known.get(&hash) {
                    Some(vec![Part::Stored(extent.clone())])
                } else if let Some(part) = pending.get(&hash) {
                    Some(vec![part.try_clone()?])
                } else if let Some(chunks) = lists.get(&hash) {
                    let mut values = Vec::with_capacity(chunks.len());
                    for chunk in chunks {
                        if let Some(extent) = known.get(chunk) {
                            values.push(Part::Stored(extent.clone()));
                        } else if let Some(part) = pending_parts.get(chunk) {
                            values.push(part.try_clone()?);
                        } else {
                            return Ok(None);
                        }
                    }
                    Some(values)
                } else {
                    None
                };
                Ok(values.map(|values| Located { hash, values }))
            })
            .collect()
    }
    /// Check physical presence for an already resolved handle, without another lookup.
    pub fn available(&self, located: &Located) -> Result<bool> {
        located.present(self.root())
    }
    async fn find(&self, conn: &mut sqlx::PgConnection, name: &str) -> Result<Option<Located>> {
        Ok(self
            .locate_many(conn, &[name.to_owned()])
            .await?
            .pop()
            .expect("one requested blob"))
    }
}
/// The catalog owns committed descriptors. The boundary and hash binding are
/// checked for these selected rows, including after opening a cold reader.
pub(super) async fn catalog_extents(
    conn: &mut sqlx::PgConnection,
    names: &[String],
) -> Result<BTreeMap<Hash, Extent>> {
    if names.is_empty() {
        return Ok(BTreeMap::new());
    }
    let rows = sqlx::query!(
        "SELECT x.hash,x.descriptor,s.committed_end FROM core.blob_extents x LEFT JOIN core.blob_segments s ON s.segment=$2 WHERE x.hash=ANY($1) AND NOT x.deleting",
        names, SEGMENT,
    ).fetch_all(conn).await?;
    let mut extents = BTreeMap::new();
    for row in rows {
        let extent = Extent::decode(&row.descriptor)?;
        let hash = parse(&row.hash)?;
        let end = row.committed_end.and_then(|end| u64::try_from(end).ok());
        if extent.hash != hash
            || extent.segment != SEGMENT
            || end.is_none_or(|end| extent.end() > end)
        {
            return Err(crate::Error::Invalid(format!(
                "extent for {} differs from its catalog name or committed segment boundary",
                row.hash,
            )));
        }
        extents.insert(hash, extent);
    }
    Ok(extents)
}

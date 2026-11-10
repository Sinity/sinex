//! The product's committed hash index over append-only framed segments.
use crate::Result;
use hashed_blobs::{Extent, Hash, Kind, Preparation, Prepared, SegmentWriter, SyncedGroup};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Seek};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
mod database;
pub mod migration;
pub use database::{commit_pending_extents, delete_unreferenced_extents, finish_deleting_extents};
pub use hashed_blobs::DEFAULT_FRAME as BLOB_FRAME_BYTES;

/// Explicit budgets supplied by an unlocked operation.
#[derive(Clone, Copy, Debug)]
pub struct SealedReadLimits {
    pub frame_bytes: usize,
    pub index_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verified {
    Intact,
    CiphertextIntact,
    Corrupt(String),
    Missing,
}

const SEGMENT: &str = "segments/current.hb";
#[derive(Default)]
struct State {
    writer: Option<SegmentWriter>,
    prepared: BTreeMap<Hash, Prepared>,
    extents: BTreeMap<Hash, Extent>,
    /// Blobs stored as content-defined chunks: their chunk names in order.
    chunk_lists: BTreeMap<Hash, Vec<Hash>>,
    /// Chunk lists the next extent commit records.
    unrecorded_chunk_lists: BTreeSet<Hash>,
    groups: Vec<Arc<SyncedGroup>>,
    committed_end: u64,
    initialized: bool,
}
struct Inner {
    root: PathBuf,
    mode: u32,
    state: Mutex<State>,
}
#[derive(Clone)]
pub struct Blobs(Arc<Inner>);
impl Blobs {
    pub fn root(&self) -> &Path {
        &self.0.root
    }
    pub fn open(root: &Path) -> Result<Self> {
        std::fs::create_dir_all(root)?;
        let mode = if std::fs::metadata(root)?.permissions().mode() & 0o7777 == 0o2770 {
            0o640
        } else {
            0o600
        };
        Ok(Self(Arc::new(Inner {
            root: root.to_owned(),
            mode,
            state: Mutex::new(State {
                initialized: !root.join(SEGMENT).exists(),
                ..State::default()
            }),
        })))
    }
    fn state(&self) -> Result<MutexGuard<'_, State>> {
        self.0
            .state
            .lock()
            .map_err(|_| crate::Error::Invalid("blob writer lock poisoned".into()))
    }
    fn preparation(&self) -> Result<Preparation> {
        Ok(Preparation::new(self.root().join("incoming"))?)
    }
    fn prepare(&self, prepared: Prepared) -> Result<String> {
        let hash = prepared.hash();
        let mut state = self.state()?;
        if state.chunk_lists.contains_key(&hash) {
            return Ok(hash.to_string());
        }
        let known = state
            .extents
            .get(&hash)
            .map(|extent| (extent.decoded_len, &extent.kind))
            .or_else(|| {
                state
                    .prepared
                    .get(&hash)
                    .map(|value| (value.decoded_len(), value.kind()))
            });
        if let Some((length, kind)) = known {
            if length != prepared.decoded_len() || kind != prepared.kind() {
                return Err(crate::Error::Invalid(
                    "repeated blob name has a different length or sealing key reference".into(),
                ));
            }
        } else {
            state.prepared.insert(hash, prepared);
        }
        Ok(hash.to_string())
    }
    /// Prepared external encodings have already passed common identity verification.
    /// The segment append checks encoded bytes again before they can commit.
    pub fn put_prepared(&self, prepared: Prepared) -> Result<String> {
        self.prepare(prepared)
    }
    /// Compression and hashing happen outside the writer lock; admission syncs all waiting bytes together.
    pub fn put(&self, bytes: &[u8]) -> Result<String> {
        self.prepare(self.preparation()?.plain(bytes, 3)?)
    }
    pub fn put_seekable(&self, reader: impl Read + Seek) -> Result<String> {
        self.prepare(self.preparation()?.plain(reader, 3)?)
    }
    pub fn put_sealed(&self, bytes: &[u8], key: &sinex_sealing::DataKey) -> Result<String> {
        self.put_sealed_reader(std::io::Cursor::new(bytes), key)
    }
    pub fn put_sealed_reader(
        &self,
        reader: impl Read + Seek,
        key: &sinex_sealing::DataKey,
    ) -> Result<String> {
        self.prepare(self.preparation()?.sealed(reader, 3, key)?)
    }

    /// Record `name` as the concatenation of `chunks`, each either held
    /// already or supplied prepared. The chunks are read through once, so the
    /// list is recorded only when they hash to `name` and add up to `length`;
    /// prepared chunks are appended with the next group, held ones are not
    /// stored again.
    pub fn put_chunks(
        &self,
        name: &str,
        length: u64,
        chunks: Vec<(String, Option<Prepared>)>,
    ) -> Result<String> {
        let hash = parse(name)?;
        if chunks.is_empty() {
            return Err(crate::Error::Invalid(format!(
                "chunked blob {name} names no chunks"
            )));
        }
        let mut whole = blake3::Hasher::new();
        let mut total = 0u64;
        let mut order = Vec::with_capacity(chunks.len());
        for (chunk, prepared) in &chunks {
            let chunk_hash = parse(chunk)?;
            let mut reader = match prepared {
                Some(prepared) => {
                    if prepared.hash() != chunk_hash || prepared.kind() != &Kind::Plain {
                        return Err(crate::Error::Invalid(format!(
                            "prepared chunk is not the plain bytes {chunk}"
                        )));
                    }
                    prepared.reader(None, hashed_blobs::ReadLimits::default())?
                }
                None => self.read_plain(chunk, None, || Ok(()))?,
            };
            total += std::io::copy(&mut reader, &mut whole)?;
            order.push(chunk_hash);
        }
        if Hash(*whole.finalize().as_bytes()) != hash || total != length {
            return Err(crate::Error::Invalid(format!(
                "the chunks of {name} are {total} bytes that are not {name}"
            )));
        }
        for (_, prepared) in chunks {
            if let Some(prepared) = prepared {
                self.prepare(prepared)?;
            }
        }
        let mut state = self.state()?;
        if !state.chunk_lists.contains_key(&hash) && !state.extents.contains_key(&hash) {
            state.chunk_lists.insert(hash, order);
            state.unrecorded_chunk_lists.insert(hash);
        }
        Ok(hash.to_string())
    }

    /// Append all prepared blobs in one group and wait for its dedicated sync thread.
    pub fn sync_pending(&self) -> Result<()> {
        let mut state = self.state()?;
        if state.prepared.is_empty() {
            return Ok(());
        }
        if !state.initialized {
            return Err(crate::Error::Invalid("load the authoritative database extent index before appending to an existing store".into()));
        }
        if state.writer.is_none() {
            state.writer = Some(SegmentWriter::open(
                self.root(),
                SEGMENT,
                state.committed_end,
                &[],
                self.0.mode,
            )?);
        }
        let prepared = std::mem::take(&mut state.prepared).into_values().map(Ok);
        let State {
            writer,
            extents,
            groups,
            ..
        } = &mut *state;
        let group = writer
            .as_mut()
            .expect("writer opened")
            .append_group(prepared, |hash| Ok(extents.get(hash).cloned()))?
            .wait()?;
        for extent in &group.extents {
            extents.insert(extent.hash, extent.clone());
        }
        groups.push(Arc::new(group));
        Ok(())
    }
    /// The extents that read back as `name`: its own, or its chunks' in order.
    fn extents(&self, name: &str) -> Result<Vec<Extent>> {
        let hash = parse(name)?;
        if let Some(extents) = self.state()?.held_extents(&hash) {
            return Ok(extents);
        }
        self.sync_pending()?;
        self.state()?.held_extents(&hash).ok_or_else(|| {
            crate::Error::Invalid(format!("blob {name} has no committed or pending extent"))
        })
    }
    /// Encoded descriptors are the portable read authority carried in producer
    /// acknowledgments: one per extent, in reading order.
    pub fn descriptors(&self, name: &str) -> Result<Vec<Vec<u8>>> {
        Ok(self.extents(name)?.iter().map(Extent::encode).collect())
    }
    pub fn contains(&self, name: &str) -> Result<bool> {
        let hash = parse(name)?;
        let state = self.state()?;
        let parts = match state.chunk_lists.get(&hash) {
            Some(chunks) => chunks.as_slice(),
            None => std::slice::from_ref(&hash),
        };
        for part in parts {
            if state.prepared.contains_key(part) {
                continue;
            }
            let Some(extent) = state.extents.get(part) else {
                return Ok(false);
            };
            match std::fs::metadata(self.root().join(&extent.segment)) {
                Ok(metadata) if metadata.len() >= extent.end() => {}
                Ok(_) => return Ok(false),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
                Err(error) => return Err(error.into()),
            }
        }
        Ok(true)
    }
    pub fn is_sealed(&self, name: &str) -> Result<bool> {
        Ok(self
            .extents(name)?
            .iter()
            .any(|extent| matches!(extent.kind, Kind::Sealed { .. })))
    }
    fn reader(
        &self,
        name: &str,
        expected_len: Option<u64>,
        key: Option<&sinex_sealing::DataKey>,
        limits: Option<SealedReadLimits>,
        check: impl FnMut() -> hashed_blobs::Result<()> + Send + 'static,
    ) -> Result<hashed_blobs::BlobReader> {
        let extents = self.extents(name)?;
        let decoded_len: u64 = extents.iter().map(|extent| extent.decoded_len).sum();
        if expected_len.is_some_and(|length| length != decoded_len) {
            return Err(crate::Error::Invalid(format!(
                "blob {name} has a different decoded length"
            )));
        }
        let limits = limits.unwrap_or(SealedReadLimits {
            frame_bytes: BLOB_FRAME_BYTES,
            index_bytes: usize::try_from(decoded_len.div_ceil(BLOB_FRAME_BYTES as u64).max(1))
                .ok()
                .and_then(|frames| frames.checked_mul(128))
                .and_then(|bytes| bytes.checked_add(65536))
                .ok_or_else(|| {
                    crate::Error::Invalid("blob index budget exceeds address space".into())
                })?,
        });
        Ok(
            hashed_blobs::Reader::new(self.root()).open_checked_with_limits(
                &extents,
                key,
                hashed_blobs::ReadLimits {
                    frame_bytes: limits.frame_bytes,
                    index_bytes: limits.index_bytes,
                },
                check,
            )?,
        )
    }
    pub fn read_plain(
        &self,
        name: &str,
        expected_len: Option<u64>,
        check: impl FnMut() -> hashed_blobs::Result<()> + Send + 'static,
    ) -> Result<hashed_blobs::BlobReader> {
        self.reader(name, expected_len, None, None, check)
    }
    pub fn read_plain_seek(
        &self,
        name: &str,
        length: u64,
        check: impl FnMut() -> hashed_blobs::Result<()> + Send + 'static,
    ) -> Result<hashed_blobs::BlobReader> {
        self.read_plain(name, Some(length), check)
    }
    pub fn read_sealed(
        &self,
        name: &str,
        expected_len: Option<u64>,
        key: &sinex_sealing::DataKey,
        limits: SealedReadLimits,
        check: impl FnMut() -> hashed_blobs::Result<()> + Send + 'static,
    ) -> Result<hashed_blobs::BlobReader> {
        self.reader(name, expected_len, Some(key), Some(limits), check)
    }
    pub fn read_range(&self, name: &str, offset: u64, len: u64) -> Result<Vec<u8>> {
        Ok(hashed_blobs::Reader::new(self.root()).read_range(
            &self.extents(name)?,
            offset,
            len,
            None,
        )?)
    }
    pub fn read(&self, name: &str) -> Result<Vec<u8>> {
        Ok(hashed_blobs::Reader::new(self.root()).read(&self.extents(name)?, None)?)
    }
    pub fn copy_to(&self, name: &str, out: &mut impl std::io::Write) -> Result<u64> {
        self.copy_to_checked(name, out, || Ok(()))
    }
    pub fn copy_to_checked(
        &self,
        name: &str,
        out: &mut impl std::io::Write,
        check: impl FnMut() -> hashed_blobs::Result<()> + Send + 'static,
    ) -> Result<u64> {
        Ok(std::io::copy(
            &mut self.read_plain(name, None, check)?,
            out,
        )?)
    }
    /// The caller has already committed the deleting marker. Recovery repeats this punch.
    pub async fn delete(&self, conn: &mut sqlx::PgConnection, name: &str) -> Result<bool> {
        let hash = parse(name)?;
        let chunked = self.state()?.chunk_lists.contains_key(&hash)
            || sqlx::query_scalar!(
                r#"SELECT EXISTS(SELECT 1 FROM core.blob_chunks WHERE blob=$1) AS "chunked!""#,
                name
            )
            .fetch_one(&mut *conn)
            .await?;
        if chunked {
            return self.delete_chunked(conn, name).await;
        }
        let descriptor = sqlx::query_scalar!(
            "SELECT descriptor FROM core.blob_extents WHERE hash=$1",
            name
        )
        .fetch_optional(&mut *conn)
        .await?;
        let extent = descriptor.as_deref().map(Extent::decode).transpose()?;
        if let Some(extent) = &extent {
            if crate::backup::covers(conn, &extent.segment, extent.offset).await? {
                return Ok(false);
            }
        }
        self.punch(name, extent.as_ref())?;
        Ok(true)
    }
    /// Remove a chunked blob: punch the chunks no held blob shares (neither
    /// held as a blob of their own nor listed by another blob that is not
    /// removed) and forget its chunk list. Shared chunks stay.
    async fn delete_chunked(&self, conn: &mut sqlx::PgConnection, name: &str) -> Result<bool> {
        let hash = parse(name)?;
        let unshared: Vec<String> = sqlx::query_scalar!(
            r#"SELECT DISTINCT c.chunk AS "chunk!" FROM core.blob_chunks c WHERE c.blob=$1
               AND NOT EXISTS (SELECT 1 FROM core.blobs b WHERE b.hash=c.chunk AND b.status<>'removed')
               AND NOT EXISTS (SELECT 1 FROM core.blob_chunks o JOIN core.blobs b ON b.hash=o.blob
                               WHERE o.chunk=c.chunk AND o.blob<>c.blob AND b.status<>'removed')
               ORDER BY 1"#,
            name
        )
        .fetch_all(&mut *conn)
        .await?;
        // Lists not yet recorded belong to bytes admitted after this check.
        let unrecorded: BTreeSet<Hash> = {
            let state = self.state()?;
            state
                .unrecorded_chunk_lists
                .iter()
                .filter(|blob| **blob != hash)
                .filter_map(|blob| state.chunk_lists.get(blob))
                .flatten()
                .copied()
                .collect()
        };
        let mut removed = Vec::new();
        for chunk in unshared {
            if unrecorded.contains(&parse(&chunk)?) {
                continue;
            }
            let descriptor = sqlx::query_scalar!(
                "SELECT descriptor FROM core.blob_extents WHERE hash=$1",
                chunk
            )
            .fetch_optional(&mut *conn)
            .await?;
            let Some(descriptor) = descriptor else {
                continue;
            };
            let extent = Extent::decode(&descriptor)?;
            if crate::backup::covers(conn, &extent.segment, extent.offset).await? {
                return Ok(false);
            }
            removed.push((chunk, extent));
        }
        self.sync_pending()?;
        {
            let mut state = self.state()?;
            if !removed.is_empty() {
                if state.writer.is_none() {
                    state.writer = Some(SegmentWriter::open(
                        self.root(),
                        SEGMENT,
                        state.committed_end,
                        &[],
                        self.0.mode,
                    )?);
                }
                let extents: Vec<Extent> =
                    removed.iter().map(|(_, extent)| extent.clone()).collect();
                state
                    .writer
                    .as_mut()
                    .expect("writer opened")
                    .punch(&extents)?;
                for (_, extent) in &removed {
                    state.extents.remove(&extent.hash);
                }
            }
            state.chunk_lists.remove(&hash);
            state.unrecorded_chunk_lists.remove(&hash);
        }
        let names: Vec<String> = removed.into_iter().map(|(chunk, _)| chunk).collect();
        sqlx::query!("DELETE FROM core.blob_extents WHERE hash=ANY($1)", &names)
            .execute(&mut *conn)
            .await?;
        sqlx::query!("DELETE FROM core.blob_chunks WHERE blob=$1", name)
            .execute(&mut *conn)
            .await?;
        Ok(true)
    }
    fn punch(&self, name: &str, recorded: Option<&Extent>) -> Result<()> {
        let hash = parse(name)?;
        self.sync_pending()?;
        let mut state = self.state()?;
        if let Some(extent) = recorded
            .cloned()
            .or_else(|| state.extents.get(&hash).cloned())
        {
            if state.writer.is_none() {
                state.writer = Some(SegmentWriter::open(
                    self.root(),
                    SEGMENT,
                    state.committed_end,
                    &[],
                    self.0.mode,
                )?);
            }
            state
                .writer
                .as_mut()
                .expect("writer opened")
                .punch(&[extent])?;
            state.extents.remove(&hash);
        }
        state.prepared.remove(&hash);
        Ok(())
    }
    pub fn stored_size(&self, name: &str) -> Result<u64> {
        Ok(self.extents(name)?.iter().map(|extent| extent.len).sum())
    }
    pub fn verify(&self, name: &str) -> Result<Verified> {
        if !self.contains(name)? {
            return Ok(Verified::Missing);
        }
        let extents = self.extents(name)?;
        let mismatches = hashed_blobs::Reader::new(self.root()).scan(&extents);
        Ok(if let Some(mismatch) = mismatches.first() {
            Verified::Corrupt(mismatch.error.clone())
        } else if extents
            .iter()
            .any(|extent| matches!(extent.kind, Kind::Sealed { .. }))
        {
            Verified::CiphertextIntact
        } else {
            Verified::Intact
        })
    }
    pub fn names(&self) -> Result<impl Iterator<Item = hashed_blobs::Result<String>>> {
        let state = self.state()?;
        let names: Vec<_> = state
            .extents
            .keys()
            .chain(state.prepared.keys())
            .chain(state.chunk_lists.keys())
            .map(|hash| Ok(hash.to_string()))
            .collect();
        Ok(names.into_iter())
    }
}

impl State {
    /// The extents of `hash` if every one is appended: its own, or its chunks'.
    fn held_extents(&self, hash: &Hash) -> Option<Vec<Extent>> {
        if let Some(extent) = self.extents.get(hash) {
            return Some(vec![extent.clone()]);
        }
        self.chunk_lists
            .get(hash)?
            .iter()
            .map(|chunk| self.extents.get(chunk).cloned())
            .collect()
    }
}

pub(crate) fn parse(name: &str) -> Result<Hash> {
    let invalid = || crate::Error::Invalid(format!("invalid blob name {name}"));
    let hex = name.strip_prefix("blake3:").ok_or_else(invalid)?;
    if hex.len() != 64 || !hex.is_ascii() {
        return Err(invalid());
    }
    let mut raw = [0; 32];
    for (i, byte) in raw.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).map_err(|_| invalid())?;
    }
    Ok(Hash(raw))
}
#[cfg(test)]
mod tests;

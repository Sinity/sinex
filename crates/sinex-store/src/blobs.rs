//! Pending ownership and selected catalog locations for append-only framed bytes.
use crate::Result;
use hashed_blobs::{Extent, Hash, Kind, Preparation, Prepared, SegmentWriter, SyncedGroup};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
mod database;
mod located;
pub use database::{commit_pending_extents, delete_unreferenced_extents, finish_deleting_extents};
pub use hashed_blobs::DEFAULT_FRAME as BLOB_FRAME_BYTES;
pub use located::{ChunkBytes, Located};

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
struct PendingChunk {
    hash: Hash,
    held: Option<Located>,
}
#[derive(Default)]
struct State {
    writer: Option<SegmentWriter>,
    prepared: BTreeMap<Hash, Prepared>,
    /// Only chunk lists not yet recorded by an admission.
    unrecorded_chunk_lists: BTreeMap<Hash, Vec<PendingChunk>>,
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
        if state.unrecorded_chunk_lists.contains_key(&hash) {
            return Ok(hash.to_string());
        }
        let known = state
            .pending_extent(&hash)
            .map(|extent| (extent.decoded_len, extent.kind))
            .or_else(|| {
                state
                    .prepared
                    .get(&hash)
                    .map(|value| (value.decoded_len(), value.kind().clone()))
            });
        if let Some((length, kind)) = known {
            if length != prepared.decoded_len() || &kind != prepared.kind() {
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
    /// Compression and hashing happen outside the writer lock; admission syncs its named bytes.
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
        chunks: Vec<(String, ChunkBytes)>,
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
                ChunkBytes::Prepared(prepared) => {
                    if prepared.hash() != chunk_hash || prepared.kind() != &Kind::Plain {
                        return Err(crate::Error::Invalid(format!(
                            "prepared chunk is not the plain bytes {chunk}"
                        )));
                    }
                    prepared.reader(None, hashed_blobs::ReadLimits::default())?
                }
                ChunkBytes::Located(located) => {
                    if located.hash != chunk_hash || located.is_sealed() {
                        return Err(crate::Error::Invalid(format!(
                            "located chunk is not the plain bytes {chunk}"
                        )));
                    }
                    self.read_plain(located, None, || Ok(()))?
                }
            };
            total += std::io::copy(&mut reader, &mut whole)?;
            order.push(chunk_hash);
        }
        if Hash(*whole.finalize().as_bytes()) != hash || total != length {
            return Err(crate::Error::Invalid(format!(
                "the chunks of {name} are {total} bytes that are not {name}"
            )));
        }
        let mut pending = Vec::with_capacity(chunks.len());
        for ((_, bytes), hash) in chunks.into_iter().zip(order) {
            let held = match bytes {
                ChunkBytes::Prepared(prepared) => {
                    self.prepare(prepared)?;
                    None
                }
                ChunkBytes::Located(located) => Some(located),
            };
            pending.push(PendingChunk { hash, held });
        }
        let mut state = self.state()?;
        if state.pending_extent(&hash).is_none() && !state.prepared.contains_key(&hash) {
            state.unrecorded_chunk_lists.entry(hash).or_insert(pending);
        }
        Ok(hash.to_string())
    }

    /// Only writer admission appends preparations. Readers retain selected encoded
    /// handles instead, so a point read never changes the segment or its catalog.
    fn sync_pending(&self, parts: &BTreeSet<Hash>, known: &BTreeMap<Hash, Extent>) -> Result<()> {
        let mut state = self.state()?;
        for hash in parts {
            if let (Some(prepared), Some(extent)) = (state.prepared.get(hash), known.get(hash)) {
                if prepared.decoded_len() != extent.decoded_len || prepared.kind() != &extent.kind {
                    return Err(crate::Error::Invalid(
                        "repeated blob name has a different length or sealing key reference".into(),
                    ));
                }
            }
        }
        // SQL already owns these immutable bytes. Keep no duplicate preparation.
        for hash in known.keys() {
            state.prepared.remove(hash);
        }
        if !parts.iter().any(|hash| state.prepared.contains_key(hash)) {
            return Ok(());
        }
        if !state.initialized {
            return Err(crate::Error::Invalid(
                "load the committed segment boundary before appending to an existing store".into(),
            ));
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
        let prepared: Vec<_> = parts
            .iter()
            .filter_map(|hash| state.prepared.remove(hash))
            .map(Ok)
            .collect();
        let group = state
            .writer
            .as_mut()
            .expect("writer opened")
            .append_group(prepared, |hash| Ok(known.get(hash).cloned()))?
            .wait()?;
        state.groups.push(Arc::new(group));
        Ok(())
    }
    /// Encoded descriptors are published only after bytes have entered the segment.
    pub fn descriptors(&self, located: &Located) -> Result<Vec<Vec<u8>>> {
        located
            .extents()
            .map(|extents| extents.iter().map(Extent::encode).collect())
    }
    fn reader(
        &self,
        located: &Located,
        expected_len: Option<u64>,
        key: Option<&sinex_sealing::DataKey>,
        limits: Option<SealedReadLimits>,
        check: impl FnMut() -> hashed_blobs::Result<()> + Send + 'static,
    ) -> Result<hashed_blobs::BlobReader> {
        let decoded_len = located.decoded_len();
        if expected_len.is_some_and(|length| length != decoded_len) {
            return Err(crate::Error::Invalid(format!(
                "blob {} has a different decoded length",
                located.hash,
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
            hashed_blobs::Reader::new(self.root()).open_parts_checked_with_limits(
                located.parts(),
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
        located: &Located,
        expected_len: Option<u64>,
        check: impl FnMut() -> hashed_blobs::Result<()> + Send + 'static,
    ) -> Result<hashed_blobs::BlobReader> {
        self.reader(located, expected_len, None, None, check)
    }
    pub fn read_plain_seek(
        &self,
        located: &Located,
        length: u64,
        check: impl FnMut() -> hashed_blobs::Result<()> + Send + 'static,
    ) -> Result<hashed_blobs::BlobReader> {
        self.read_plain(located, Some(length), check)
    }
    pub fn read_sealed(
        &self,
        located: &Located,
        expected_len: Option<u64>,
        key: &sinex_sealing::DataKey,
        limits: SealedReadLimits,
        check: impl FnMut() -> hashed_blobs::Result<()> + Send + 'static,
    ) -> Result<hashed_blobs::BlobReader> {
        self.reader(located, expected_len, Some(key), Some(limits), check)
    }
    pub fn read_range(&self, located: &Located, offset: u64, len: u64) -> Result<Vec<u8>> {
        let end = offset
            .checked_add(len)
            .filter(|end| *end <= located.decoded_len())
            .ok_or_else(|| crate::Error::Invalid("range lies outside decoded stream".into()))?;
        let mut reader = self.read_plain(located, None, || Ok(()))?;
        reader.seek(SeekFrom::Start(offset))?;
        let mut out = Vec::new();
        let copied = std::io::copy(&mut reader.take(end - offset), &mut out)?;
        if copied != len {
            return Err(crate::Error::Invalid(
                "decoded stream ended inside range".into(),
            ));
        }
        Ok(out)
    }
    pub fn read(&self, located: &Located) -> Result<Vec<u8>> {
        Ok(hashed_blobs::Reader::new(self.root()).read_parts(located.parts(), None)?)
    }
    pub fn copy_to(&self, located: &Located, out: &mut impl std::io::Write) -> Result<u64> {
        self.copy_to_checked(located, out, || Ok(()))
    }
    pub fn copy_to_checked(
        &self,
        located: &Located,
        out: &mut impl std::io::Write,
        check: impl FnMut() -> hashed_blobs::Result<()> + Send + 'static,
    ) -> Result<u64> {
        Ok(std::io::copy(
            &mut self.read_plain(located, None, check)?,
            out,
        )?)
    }
    /// The caller has already committed the deleting marker. Recovery repeats this punch.
    pub async fn delete(&self, conn: &mut sqlx::PgConnection, name: &str) -> Result<bool> {
        let hash = parse(name)?;
        let chunked = self.state()?.unrecorded_chunk_lists.contains_key(&hash)
            || sqlx::query_scalar!(
                r#"SELECT EXISTS(SELECT 1 FROM core.blob_chunks WHERE blob=$1) AS "chunked!""#,
                name
            )
            .fetch_one(&mut *conn)
            .await?;
        if chunked {
            return self.delete_chunked(conn, name).await;
        }
        let shared = sqlx::query_scalar!(
            r#"SELECT EXISTS(SELECT 1 FROM core.blob_chunks c JOIN core.blobs b ON b.hash=c.blob
               WHERE c.chunk=$1 AND c.blob<>$1 AND b.status<>'removed') AS "shared!""#,
            name,
        )
        .fetch_one(&mut *conn)
        .await?;
        if shared || self.held_by_pending_chunk(name)? {
            return Ok(true);
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
    /// A prepared chunk list can reuse an extent before its SQL reference commits.
    pub(crate) fn held_by_pending_chunk(&self, name: &str) -> Result<bool> {
        let hash = parse(name)?;
        let state = self.state()?;
        Ok(state
            .unrecorded_chunk_lists
            .iter()
            .any(|(blob, chunks)| *blob != hash && chunks.iter().any(|chunk| chunk.hash == hash)))
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
                .filter(|(blob, _)| **blob != hash)
                .flat_map(|(_, chunks)| chunks.iter().map(|chunk| chunk.hash))
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
            }
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
        let mut state = self.state()?;
        if let Some(extent) = recorded.cloned().or_else(|| state.pending_extent(&hash)) {
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
        }
        state.prepared.remove(&hash);
        Ok(())
    }
    pub fn verify(&self, located: &Located) -> Result<Verified> {
        if !located.present(self.root())? {
            return Ok(Verified::Missing);
        }
        let mismatches = hashed_blobs::Reader::new(self.root())
            .scan_parts_checked(located.parts(), || Ok(()))?;
        Ok(if let Some(mismatch) = mismatches.first() {
            Verified::Corrupt(mismatch.error.clone())
        } else if located.is_sealed() {
            Verified::CiphertextIntact
        } else {
            Verified::Intact
        })
    }
}
impl State {
    fn pending_extent(&self, hash: &Hash) -> Option<Extent> {
        self.writer
            .as_ref()
            .and_then(|writer| writer.uncommitted(hash))
    }
    fn pending_stored(&self, hashes: &BTreeSet<Hash>) -> BTreeMap<Hash, Extent> {
        let mut found: BTreeMap<_, _> = hashes
            .iter()
            .filter_map(|hash| self.pending_extent(hash).map(|extent| (*hash, extent)))
            .collect();
        for located in self
            .unrecorded_chunk_lists
            .values()
            .flatten()
            .filter_map(|chunk| chunk.held.as_ref())
        {
            for part in &located.values {
                if let located::Part::Stored(extent) = part
                    && hashes.contains(&extent.hash)
                {
                    found.entry(extent.hash).or_insert_with(|| extent.clone());
                }
            }
        }
        found
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

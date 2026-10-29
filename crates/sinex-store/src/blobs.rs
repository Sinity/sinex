//! The product's committed hash index over append-only framed segments.
use crate::Result;
use hashed_blobs::{Extent, Hash, Kind, Preparation, Prepared, SegmentWriter, SyncedGroup};
use std::collections::BTreeMap;
use std::io::{Read, Seek};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
mod database;
pub mod migration;
pub use database::{commit_pending_extents, delete_unreferenced_extents};
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
        let prepared = std::mem::take(&mut state.prepared).into_values().collect();
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
    fn extent(&self, name: &str) -> Result<Extent> {
        self.sync_pending()?;
        self.state()?
            .extents
            .get(&parse(name)?)
            .cloned()
            .ok_or_else(|| {
                crate::Error::Invalid(format!("blob {name} has no committed or pending extent"))
            })
    }
    /// Encoded descriptors are the portable read authority carried in producer acknowledgments.
    pub fn descriptors(&self, name: &str) -> Result<Vec<Vec<u8>>> {
        Ok(vec![self.extent(name)?.encode()])
    }
    pub fn contains(&self, name: &str) -> Result<bool> {
        let hash = parse(name)?;
        let state = self.state()?;
        if state.prepared.contains_key(&hash) {
            return Ok(true);
        }
        let Some(extent) = state.extents.get(&hash) else {
            return Ok(false);
        };
        match std::fs::metadata(self.root().join(&extent.segment)) {
            Ok(metadata) => Ok(metadata.len() >= extent.end()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }
    pub fn is_sealed(&self, name: &str) -> Result<bool> {
        Ok(matches!(self.extent(name)?.kind, Kind::Sealed { .. }))
    }
    fn reader(
        &self,
        name: &str,
        expected_len: Option<u64>,
        key: Option<&sinex_sealing::DataKey>,
        limits: Option<SealedReadLimits>,
        check: impl FnMut() -> hashed_blobs::Result<()> + Send + 'static,
    ) -> Result<hashed_blobs::BlobReader> {
        let extent = self.extent(name)?;
        if expected_len.is_some_and(|length| length != extent.decoded_len) {
            return Err(crate::Error::Invalid(format!(
                "blob {name} has a different decoded length"
            )));
        }
        let limits = limits.unwrap_or(SealedReadLimits {
            frame_bytes: BLOB_FRAME_BYTES,
            index_bytes: usize::try_from(
                extent.decoded_len.div_ceil(BLOB_FRAME_BYTES as u64).max(1),
            )
            .ok()
            .and_then(|frames| frames.checked_mul(128))
            .and_then(|bytes| bytes.checked_add(65536))
            .ok_or_else(|| {
                crate::Error::Invalid("blob index budget exceeds address space".into())
            })?,
        });
        Ok(
            hashed_blobs::Reader::new(self.root()).open_checked_with_limits(
                &[extent],
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
            &[self.extent(name)?],
            offset,
            len,
            None,
        )?)
    }
    pub fn read(&self, name: &str) -> Result<Vec<u8>> {
        Ok(hashed_blobs::Reader::new(self.root()).read(&[self.extent(name)?], None)?)
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
    pub fn delete(&self, name: &str) -> Result<()> {
        let hash = parse(name)?;
        self.sync_pending()?;
        let mut state = self.state()?;
        if let Some(extent) = state.extents.get(&hash).cloned() {
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
        Ok(self.extent(name)?.len)
    }
    pub fn verify(&self, name: &str) -> Result<Verified> {
        if !self.contains(name)? {
            return Ok(Verified::Missing);
        }
        let extent = self.extent(name)?;
        let mismatches = hashed_blobs::Reader::new(self.root()).scan(&[extent.clone()]);
        Ok(if let Some(mismatch) = mismatches.first() {
            Verified::Corrupt(mismatch.error.clone())
        } else if matches!(extent.kind, Kind::Sealed { .. }) {
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
            .map(|hash| Ok(hash.to_string()))
            .collect();
        Ok(names.into_iter())
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

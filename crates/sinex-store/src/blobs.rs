//! The content store adapter over `hashed-blobs`: bytes named by BLAKE3, stored
//! as seekable zstd. Drivers write a material's bytes here before the batch that
//! registers it, so bytes always exist before any row refers to them.
use crate::Result;
pub use hashed_blobs::{DEFAULT_FRAME as BLOB_FRAME_BYTES, SealedReadLimits};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone)]
pub struct Blobs(Arc<hashed_blobs::Store>, PathBuf);

/// Where a blob sent in chunks stands after one chunk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Received {
    /// More bytes are expected; this many have arrived.
    Partial(u64),
    /// Every byte arrived, matched the name and is stored.
    Stored,
}

impl Blobs {
    pub fn open(root: &Path) -> Result<Self> {
        std::fs::create_dir_all(root)
            .map_err(|e| crate::Error::Invalid(format!("{}: {e}", root.display())))?;
        Ok(Self(
            Arc::new(hashed_blobs::Store::open(root, vec![], 0o600)?),
            root.join("incoming"),
        ))
    }

    /// Take one chunk of the bytes named `name`, from a producer that does not
    /// share this content store. Chunks arrive in order; a chunk already
    /// received (a resend) is accepted again without effect. The staged bytes
    /// are checked against `name` before they are stored, and dropped if they
    /// do not match.
    pub fn receive_chunk(
        &self,
        name: &str,
        offset: u64,
        total: u64,
        data: &[u8],
    ) -> Result<Received> {
        let hash = parse(name)?;
        if self.0.contains(&hash) {
            return Ok(Received::Stored);
        }
        let invalid = |m: String| crate::Error::Invalid(m);
        let io = |e: std::io::Error| invalid(format!("staging {name}: {e}"));
        std::fs::create_dir_all(&self.1).map_err(io)?;
        let path = self.1.join(hash.hex());
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .map_err(io)?;
        let have = file.metadata().map_err(io)?.len();
        let end = offset + data.len() as u64;
        if end > total {
            return Err(invalid(format!(
                "chunk of {name} ends at {end}, past its total of {total}"
            )));
        }
        if offset > have {
            return Err(invalid(format!(
                "chunk of {name} starts at {offset} but {have} bytes have arrived; send chunks in order"
            )));
        }
        if end > have {
            file.write_all(&data[usize::try_from(have - offset).unwrap_or(0)..])
                .map_err(io)?;
        }
        let have = have.max(end);
        if have < total {
            return Ok(Received::Partial(have));
        }
        file.sync_all().map_err(io)?;
        file.rewind().map_err(io)?;
        let mut hasher = blake3::Hasher::new();
        std::io::copy(&mut (&file).take(total), &mut hasher).map_err(io)?;
        if hasher.finalize().as_bytes() != &hash.0 {
            drop(file);
            let _ = std::fs::remove_file(&path);
            return Err(invalid(format!(
                "the {total} bytes sent as {name} do not match that name"
            )));
        }
        file.rewind().map_err(io)?;
        let stored = self.put_reader((&file).take(total))?;
        drop(file);
        let _ = std::fs::remove_file(&path);
        if stored != name {
            return Err(invalid(format!("stored {name} as {stored}")));
        }
        Ok(Received::Stored)
    }

    /// Store `bytes` and return their name, `blake3:<hex>`.
    pub fn put(&self, bytes: &[u8]) -> Result<String> {
        let hash = self.0.put(bytes, 3)?;
        Ok(format!("blake3:{}", hash.hex()))
    }

    /// Store bytes sealed under `key` (the sealed class): encrypted frames
    /// named by a keyed hash, so the store never confirms a known plaintext.
    pub fn put_sealed(&self, bytes: &[u8], key: &crate::sealed::DataKey) -> Result<String> {
        let hash = self.0.put_sealed(std::io::Cursor::new(bytes), 3, key)?;
        Ok(format!("blake3:{}", hash.hex()))
    }

    /// Seal a seekable input in bounded frames without staging plaintext on disk.
    pub fn put_sealed_reader(
        &self,
        reader: impl Read + Seek,
        key: &crate::sealed::DataKey,
    ) -> Result<String> {
        let hash = self.0.put_sealed(reader, 3, key)?;
        Ok(format!("blake3:{}", hash.hex()))
    }

    /// Sequential sealed reads under the operation's explicit key and memory budgets.
    pub fn read_sealed(
        &self,
        name: &str,
        expected_len: Option<u64>,
        key: &crate::sealed::DataKey,
        limits: SealedReadLimits,
        check: impl FnMut() -> hashed_blobs::Result<()> + Send + 'static,
    ) -> Result<hashed_blobs::SealedReader> {
        Ok(self
            .0
            .read_sealed(&parse(name)?, expected_len, key, limits, check)?)
    }

    /// Whether the blob named `name` is sealed.
    pub fn is_sealed(&self, name: &str) -> Result<bool> {
        Ok(matches!(
            self.0.kind(&parse(name)?)?,
            hashed_blobs::Kind::Sealed { .. }
        ))
    }

    /// Whether the store holds the bytes named `name` (an outside producer
    /// places a material's bytes before registering it).
    pub fn contains(&self, name: &str) -> Result<bool> {
        Ok(self.0.contains(&parse(name)?))
    }

    /// Store bytes read from `reader` (an export archive of any size) without
    /// holding them in memory, and return their name.
    pub fn put_reader(&self, reader: impl std::io::Read) -> Result<String> {
        let hash = self.0.put(reader, 3)?;
        Ok(format!("blake3:{}", hash.hex()))
    }

    pub fn read_range(&self, name: &str, offset: u64, len: u64) -> Result<Vec<u8>> {
        Ok(self.0.read_range(&parse(name)?, offset, len)?)
    }

    /// Verify and open a plain blob for bounded sequential reads. The same representation
    /// stays open; the callback can cancel verification and subsequent reads. Delta and sealed
    /// encodings keep their separate read/key APIs and are refused by this operation.
    pub fn read_plain(
        &self,
        name: &str,
        expected_len: Option<u64>,
        check: impl FnMut() -> hashed_blobs::Result<()> + Send + 'static,
    ) -> Result<hashed_blobs::PlainReader> {
        Ok(self.0.read_plain(&parse(name)?, expected_len, check)?)
    }

    /// The whole blob, verified against its name as it is read.
    pub fn read(&self, name: &str) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        std::io::Read::read_to_end(&mut self.0.read(&parse(name)?)?, &mut out)
            .map_err(|e| crate::Error::Invalid(format!("read {name}: {e}")))?;
        Ok(out)
    }

    /// Copy verified content into `out`. Plain blobs use bounded sequential reads; the
    /// legacy delta read still materializes the decoded body. Sealed blobs require a key
    /// and are refused here. Returns the number of bytes written.
    pub fn copy_to(&self, name: &str, out: &mut impl std::io::Write) -> Result<u64> {
        self.copy_to_checked(name, out, || Ok(()))
    }

    /// Copy one verified representation with cancellation during plain reads.
    /// Legacy deltas still decode their body in the shared reader; cancellation is
    /// checked before and after that decode. Sealed bytes require the keyed API.
    pub fn copy_to_checked(
        &self,
        name: &str,
        out: &mut impl std::io::Write,
        mut check: impl FnMut() -> hashed_blobs::Result<()> + Send + 'static,
    ) -> Result<u64> {
        let hash = parse(name)?;
        let copied = match self.0.kind(&hash)? {
            hashed_blobs::Kind::Plain => {
                std::io::copy(&mut self.0.read_plain(&hash, None, check)?, out)
            }
            _ => {
                check()?;
                let copied = std::io::copy(&mut self.0.read(&hash)?, out);
                check()?;
                copied
            }
        };
        copied.map_err(|e| crate::Error::Invalid(format!("read {name}: {e}")))
    }

    /// Delete a blob's file once no row refers to it (spec `storage.md` §5:
    /// rows first, the file after commit). A file already gone is fine.
    pub fn delete(&self, name: &str) -> Result<()> {
        match self.0.delete(&parse(name)?) {
            Ok(()) | Err(hashed_blobs::Error::Missing(_)) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub fn stored_size(&self, name: &str) -> Result<u64> {
        Ok(self.0.stored_size(&parse(name)?)?)
    }

    /// Assemble only declared image byte extents; map bytes remain outside this stream.
    pub fn assemble(&self, extents: &[sinex_model::ImageExtent]) -> Result<impl Read + Seek + '_> {
        let extents=extents.iter().map(|e| Ok(hashed_blobs::Extent {
            blob:parse(&e.blob)?, offset:e.offset, len:e.length,
        })).collect::<Result<Vec<_>>>()?;
        Ok(self.0.assemble(&extents)?)
    }

    /// Read a blob back and check it against its name.
    pub fn verify(&self, name: &str) -> Result<hashed_blobs::Verified> {
        Ok(self.0.verify(&parse(name)?)?)
    }
}

fn parse(name: &str) -> Result<hashed_blobs::Hash> {
    let invalid = || crate::Error::Invalid(format!("invalid blob name {name}"));
    let hex = name.strip_prefix("blake3:").ok_or_else(invalid)?;
    if hex.len() != 64 {
        return Err(invalid());
    }
    let mut raw = [0u8; 32];
    for (i, b) in raw.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).map_err(|_| invalid())?;
    }
    Ok(hashed_blobs::Hash(raw))
}

#[cfg(test)]
mod tests;

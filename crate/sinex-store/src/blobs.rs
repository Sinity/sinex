//! The content store adapter over `hashed-blobs`: bytes named by BLAKE3, stored
//! as seekable zstd. Drivers write a material's bytes here before the batch that
//! registers it, so bytes always exist before any row refers to them.
use crate::Result;
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

    /// The whole blob, verified against its name as it is read.
    pub fn read(&self, name: &str) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        std::io::Read::read_to_end(&mut self.0.read(&parse(name)?)?, &mut out)
            .map_err(|e| crate::Error::Invalid(format!("read {name}: {e}")))?;
        Ok(out)
    }

    /// Delete a blob's file once no row refers to it (spec `storage.md` §5:
    /// rows first, the file after commit). A file already gone is fine.
    pub fn delete(&self, name: &str) -> Result<()> {
        match self.0.delete(&parse(name)?) {
            Ok(()) | Err(hashed_blobs::Error::Missing(_)) => Ok(()),
            Err(e) => Err(e.into()),
        }
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
mod tests {
    use super::*;

    #[test]
    fn put_then_read_range() {
        let dir = std::env::temp_dir().join(format!("sinex-blobs-{}", uuid::Uuid::now_v7()));
        let blobs = Blobs::open(&dir).unwrap();
        let name = blobs.put(b"hello world").unwrap();
        assert_eq!(
            name,
            format!("blake3:{}", blake3::hash(b"hello world").to_hex())
        );
        assert_eq!(blobs.read_range(&name, 6, 5).unwrap(), b"world");
        assert!(blobs.contains(&name).unwrap());
        assert!(
            !blobs
                .contains(&format!("blake3:{}", blake3::hash(b"other").to_hex()))
                .unwrap()
        );
        assert!(blobs.contains("sha256:00").is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn chunks_in_order_are_checked_then_stored() {
        let dir = std::env::temp_dir().join(format!("sinex-chunks-{}", uuid::Uuid::now_v7()));
        let blobs = Blobs::open(&dir).unwrap();
        let bytes: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
        let name = format!("blake3:{}", blake3::hash(&bytes).to_hex());
        let total = bytes.len() as u64;
        assert_eq!(
            blobs
                .receive_chunk(&name, 0, total, &bytes[..4_000])
                .unwrap(),
            Received::Partial(4_000)
        );
        // A resent chunk changes nothing; a chunk out of order is refused.
        assert_eq!(
            blobs
                .receive_chunk(&name, 0, total, &bytes[..4_000])
                .unwrap(),
            Received::Partial(4_000)
        );
        assert!(
            blobs
                .receive_chunk(&name, 8_000, total, &bytes[8_000..])
                .is_err()
        );
        assert!(!blobs.contains(&name).unwrap());
        assert_eq!(
            blobs
                .receive_chunk(&name, 4_000, total, &bytes[4_000..])
                .unwrap(),
            Received::Stored
        );
        assert_eq!(blobs.read(&name).unwrap(), bytes);
        assert_eq!(
            blobs.receive_chunk(&name, 0, total, &bytes[..10]).unwrap(),
            Received::Stored,
            "a stored blob takes any chunk as done"
        );

        // Bytes that do not match their name are dropped, not stored.
        let wrong = format!("blake3:{}", blake3::hash(b"something else").to_hex());
        assert!(blobs.receive_chunk(&wrong, 0, 5, b"hello").is_err());
        assert!(!blobs.contains(&wrong).unwrap());
        assert_eq!(
            blobs
                .receive_chunk(&wrong, 0, 14, b"something else")
                .unwrap(),
            Received::Stored,
            "a failed attempt leaves nothing staged"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
}

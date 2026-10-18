//! The content store adapter over `hashed-blobs`: bytes named by BLAKE3, stored
//! as seekable zstd. Drivers write a material's bytes here before the batch that
//! registers it, so bytes always exist before any row refers to them.
use crate::Result;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub struct Blobs(Arc<hashed_blobs::Store>);

impl Blobs {
    pub fn open(root: &Path) -> Result<Self> {
        std::fs::create_dir_all(root)
            .map_err(|e| crate::Error::Invalid(format!("{}: {e}", root.display())))?;
        Ok(Self(Arc::new(hashed_blobs::Store::open(
            root,
            vec![],
            0o600,
        )?)))
    }

    /// Store `bytes` and return their name, `blake3:<hex>`.
    pub fn put(&self, bytes: &[u8]) -> Result<String> {
        let hash = self.0.put(bytes, 3)?;
        Ok(format!("blake3:{}", hash.hex()))
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
}

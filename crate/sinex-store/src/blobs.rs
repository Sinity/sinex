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

    pub fn read_range(&self, name: &str, offset: u64, len: u64) -> Result<Vec<u8>> {
        Ok(self.0.read_range(&parse(name)?, offset, len)?)
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
        std::fs::remove_dir_all(dir).unwrap();
    }
}

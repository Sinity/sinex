//! A poll's material bytes, held in a temporary directory by their hash until
//! they are written to the frame's directory.
use super::*;
use std::io::{Read, Write};

pub(super) struct Snapshots(tempfile::TempDir);
impl Snapshots {
    pub(crate) fn new() -> Result<Self, Error> {
        tempfile::tempdir()
            .map(Self)
            .map_err(|e| Error::Local(e.to_string()))
    }
    pub(super) fn put_reader(&self, mut reader: impl Read) -> Result<String, String> {
        let mut file = tempfile::NamedTempFile::new_in(self.0.path()).map_err(|e| e.to_string())?;
        let mut hash = blake3::Hasher::new();
        let mut buffer = [0; 65536];
        loop {
            let n = reader.read(&mut buffer).map_err(|e| e.to_string())?;
            if n == 0 {
                break;
            }
            file.write_all(&buffer[..n]).map_err(|e| e.to_string())?;
            hash.update(&buffer[..n]);
        }
        let hash = hash.finalize().to_hex().to_string();
        file.persist(self.0.path().join(&hash))
            .map_err(|e| e.to_string())?;
        Ok(format!("blake3:{hash}"))
    }
    pub(crate) fn open(&self, name: &str) -> Result<std::fs::File, Error> {
        let hash = name
            .strip_prefix("blake3:")
            .ok_or_else(|| Error::Local("invalid private snapshot name".into()))?;
        blake3::Hash::from_hex(hash).map_err(|e| Error::Local(e.to_string()))?;
        std::fs::File::open(self.0.path().join(hash)).map_err(|e| Error::Local(e.to_string()))
    }
}
impl MaterialSink for Snapshots {
    fn put(&self, bytes: &[u8]) -> Result<String, String> {
        self.put_reader(bytes)
    }
    fn put_file(&self, path: &Path) -> Result<String, String> {
        self.put_reader(std::fs::File::open(path).map_err(|e| e.to_string())?)
    }
}

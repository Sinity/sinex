//! A poll owns one encoded custody inode per material until the frame takes it.
use super::*;
use hashed_blobs::{EncodedBlob, Preparation};
use std::io::Read;

pub(super) struct Snapshots(tempfile::TempDir);
impl Snapshots {
    pub(crate) fn new() -> Result<Self, Error> {
        tempfile::tempdir()
            .map(Self)
            .map_err(|error| Error::Local(error.to_string()))
    }
    pub(super) fn put_reader(&self, reader: impl Read) -> Result<String, String> {
        let temporary =
            tempfile::NamedTempFile::new_in(self.0.path()).map_err(|error| error.to_string())?;
        let prepared = Preparation::new(self.0.path())
            .map_err(|error| error.to_string())?
            .plain_into(
                temporary.reopen().map_err(|error| error.to_string())?,
                reader,
                3,
            )
            .map_err(|error| error.to_string())?;
        let descriptor = prepared.descriptor();
        drop(prepared);
        let name = descriptor.hash.hex();
        temporary
            .persist(self.0.path().join(&name))
            .map_err(|error| error.to_string())?;
        std::fs::write(
            self.0.path().join(format!("{name}.descriptor")),
            descriptor.encode(),
        )
        .map_err(|error| error.to_string())?;
        Ok(descriptor.hash.to_string())
    }
    fn descriptor(&self, name: &str) -> Result<EncodedBlob, Error> {
        let name = name
            .strip_prefix("blake3:")
            .ok_or_else(|| Error::Local("invalid snapshot name".into()))?;
        blake3::Hash::from_hex(name).map_err(|error| Error::Local(error.to_string()))?;
        let bytes = std::fs::read(self.0.path().join(format!("{name}.descriptor")))
            .map_err(|error| Error::Local(error.to_string()))?;
        EncodedBlob::decode(&bytes).map_err(|error| Error::Local(error.to_string()))
    }
    pub(super) fn handoff(
        &self,
        root: &Path,
        producer: &str,
        frame: &str,
        name: &str,
    ) -> Result<(), Error> {
        let descriptor = self.descriptor(name)?;
        frame_bytes::link_encoded(
            root,
            producer,
            frame,
            &descriptor,
            &self.0.path().join(descriptor.hash.hex()),
        )
        .map_err(|error| Error::Local(error.to_string()))
    }
}
impl MaterialSink for Snapshots {
    fn put(&self, bytes: &[u8]) -> Result<String, String> {
        self.put_reader(bytes)
    }
    fn put_file(&self, path: &Path) -> Result<String, String> {
        self.put_reader(std::fs::File::open(path).map_err(|error| error.to_string())?)
    }
}

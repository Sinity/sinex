//! A stored material's records read through one verified reader. Sealed bodies
//! stay off disk.
use sinex_sources::shared_pass::ReadSeek;
use sinex_sources::{Framer, drivers};
use std::io::{Read, Seek, SeekFrom};
use std::sync::Mutex;
mod material;
pub(crate) use material::{MaterialOnDemand, open, open_checked};
pub(crate) type ReadCheck = std::sync::Arc<dyn Fn() -> Result<(), String> + Send + Sync>;

pub(crate) struct StoredReader {
    reader: Mutex<Box<dyn ReadSeek>>,
    length: u64,
}
impl StoredReader {
    pub(crate) fn new(reader: impl Read + Seek + Send + 'static, length: u64) -> Self {
        Self {
            reader: Mutex::new(Box::new(reader)),
            length,
        }
    }
    pub(crate) fn checked(
        reader: impl Read + Seek + Send + 'static,
        length: u64,
        check: impl FnMut() -> std::io::Result<()> + Send + 'static,
    ) -> Self {
        Self::new(Checked { reader, check }, length)
    }
    pub(crate) fn records(
        &self,
        manifest: &sinex_model::Manifest,
        framer: Framer,
    ) -> Result<Vec<(usize, usize)>, String> {
        let mut reader = self.reader.lock().map_err(|_| "stored reader poisoned")?;
        reader.rewind().map_err(|e| e.to_string())?;
        drivers::stored_records_from(manifest, &mut *reader, framer).map_err(|e| e.to_string())
    }
}
impl sinex_sources::drain::RecordRead for StoredReader {
    fn length(&self) -> u64 {
        self.length
    }
    fn read_exact_at(&self, start: u64, bytes: &mut [u8]) -> std::io::Result<()> {
        let mut reader = self
            .reader
            .lock()
            .map_err(|_| std::io::Error::other("stored reader poisoned"))?;
        reader.seek(SeekFrom::Start(start))?;
        reader.read_exact(bytes)
    }
}

struct Checked<R, C> {
    reader: R,
    check: C,
}
impl<R: Read, C: FnMut() -> std::io::Result<()>> Read for Checked<R, C> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        (self.check)()?;
        self.reader.read(out)
    }
}
impl<R: Seek, C: FnMut() -> std::io::Result<()>> Seek for Checked<R, C> {
    fn seek(&mut self, from: SeekFrom) -> std::io::Result<u64> {
        (self.check)()?;
        self.reader.seek(from)
    }
}

#[cfg(test)]
mod tests;

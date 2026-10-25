//! A seekable reader over one verified blob.
use hashed_blobs::{Hash, Reader, Result};
use std::io::{Read, Seek, SeekFrom};
pub(super) struct PlainSeek {
    reader: Reader,
    hash: Hash,
    length: u64,
    at: u64,
    check: Box<dyn FnMut() -> Result<()> + Send>,
}
impl PlainSeek {
    pub fn new(
        root: &std::path::Path,
        hash: Hash,
        length: u64,
        check: Box<dyn FnMut() -> Result<()> + Send>,
    ) -> Result<Self> {
        let mut value = Self {
            reader: Reader::new(root, vec![]),
            hash,
            length,
            at: 0,
            check,
        };
        // Verify the complete name and length before handing out this reader.
        value.reader.read_range_checked::<hashed_blobs::Error>(
            &value.hash,
            Some(length),
            0,
            0,
            &mut value.check,
        )?;
        Ok(value)
    }
}
impl Read for PlainSeek {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        (self.check)().map_err(std::io::Error::other)?;
        let n = self.length.saturating_sub(self.at).min(out.len() as u64);
        let bytes = self
            .reader
            .read_range_checked::<hashed_blobs::Error>(
                &self.hash,
                Some(self.length),
                self.at,
                n,
                &mut self.check,
            )
            .map_err(std::io::Error::other)?;
        if bytes.len() as u64 != n {
            return Err(std::io::Error::other("stored plain range is short"));
        }
        out[..bytes.len()].copy_from_slice(&bytes);
        self.at += n;
        Ok(bytes.len())
    }
}
impl Seek for PlainSeek {
    fn seek(&mut self, from: SeekFrom) -> std::io::Result<u64> {
        (self.check)().map_err(std::io::Error::other)?;
        let at = match from {
            SeekFrom::Start(n) => i128::from(n),
            SeekFrom::Current(n) => i128::from(self.at) + i128::from(n),
            SeekFrom::End(n) => i128::from(self.length) + i128::from(n),
        };
        self.at =
            u64::try_from(at).map_err(|_| std::io::Error::other("invalid stored plain seek"))?;
        Ok(self.at)
    }
}

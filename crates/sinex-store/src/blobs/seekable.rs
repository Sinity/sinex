//! Record each completed input pass before the common store can install it.
use super::*;
use std::io::{self, SeekFrom};

impl Blobs {
    /// Store a seekable input from its current position. Repeated content is
    /// verified without compression or a temporary blob file.
    pub fn put_seekable(&self, reader: impl Read + Seek) -> Result<String> {
        let reader = RecordedInput {
            reader,
            blobs: self,
            hash: blake3::Hasher::new(),
        };
        let hash = self.0.put_seekable(reader, 3)?;
        Ok(format!("blake3:{}", hash.hex()))
    }
}

struct RecordedInput<'a, R> {
    reader: R,
    blobs: &'a Blobs,
    hash: blake3::Hasher,
}

impl<R: Read> Read for RecordedInput<'_, R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let count = self.reader.read(bytes)?;
        if count == 0 && !bytes.is_empty() {
            // Installation follows EOF. Record the compressed pass too, in
            // case the input changed after the first hash pass.
            self.blobs
                .note_unadmitted(hashed_blobs::Hash(*self.hash.finalize().as_bytes()))
                .map_err(io::Error::other)?;
        } else {
            self.hash.update(&bytes[..count]);
        }
        Ok(count)
    }
}

impl<R: Seek> Seek for RecordedInput<'_, R> {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        let position = self.reader.seek(position)?;
        self.hash.reset();
        Ok(position)
    }

    fn stream_position(&mut self) -> io::Result<u64> {
        self.reader.stream_position()
    }
}

//! Slice and reader framing share the source's declaration.
use crate::drivers::export_archive::Framing;
use std::io::Read;

type SliceFramer = fn(&[u8]) -> (Vec<(usize, usize)>, usize);

#[derive(Clone, Copy, Debug)]
pub struct Framer {
    slice: SliceFramer,
    reader: ReaderFraming,
}

#[derive(Clone, Copy, Debug)]
enum ReaderFraming {
    Incremental,
    Export(Framing),
}

impl Framer {
    /// Append framing whose completed records do not change when later bytes arrive.
    /// Keep its last record undecided until another record or true EOF is seen.
    pub const fn incremental(slice: SliceFramer) -> Self {
        Self { slice, reader: ReaderFraming::Incremental }
    }

    pub const fn export(slice: SliceFramer, framing: Framing) -> Self {
        Self { slice, reader: ReaderFraming::Export(framing) }
    }

    pub fn frame(self, bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
        (self.slice)(bytes)
    }

    /// Exact source byte ranges. Bodies retain only the undecided record and read buffer;
    /// the returned range metadata grows with record count. A whole record can be large.
    pub fn ranges(self, reader: &mut dyn Read) -> std::io::Result<Vec<(usize, usize)>> {
        if let ReaderFraming::Export(framing) = self.reader {
            return crate::drivers::export_archive::records::ranges(reader, framing);
        }
        const CHUNK: usize = 64 << 10;
        let mut pending = Vec::new();
        let mut base = 0usize;
        let mut ranges = Vec::new();
        let mut block = [0; CHUNK];
        loop {
            let n = reader.read(&mut block)?;
            let eof = n == 0;
            pending.extend_from_slice(&block[..n]);
            let (records, _) = self.frame(&pending);
            let count = if eof { records.len() } else { records.len().saturating_sub(1) };
            for &(start, end) in &records[..count] {
                if start > end || end > pending.len() {
                    return Err(std::io::Error::other("invalid source record range"));
                }
                ranges.push((base + start, base + end));
            }
            if eof { return Ok(ranges); }
            if count > 0 {
                let consumed = records[count].0;
                pending.drain(..consumed);
                base += consumed;
            }
        }
    }
}

#[cfg(test)]
mod tests;

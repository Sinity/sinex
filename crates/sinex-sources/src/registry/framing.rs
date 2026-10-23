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
    Detected(fn(&[u8]) -> Framing),
}

impl Framer {
    /// Append framing whose completed records do not change when later bytes arrive.
    /// Keep its last record undecided until another record or true EOF is seen.
    pub const fn incremental(slice: SliceFramer) -> Self {
        Self {
            slice,
            reader: ReaderFraming::Incremental,
        }
    }

    pub const fn export(slice: SliceFramer, framing: Framing) -> Self {
        Self {
            slice,
            reader: ReaderFraming::Export(framing),
        }
    }

    /// Choose the existing export framing from the original stream prefix.
    pub const fn detected(slice: SliceFramer, detect: fn(&[u8]) -> Framing) -> Self {
        Self {
            slice,
            reader: ReaderFraming::Detected(detect),
        }
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
        if let ReaderFraming::Detected(detect) = self.reader {
            let mut prefix = Vec::new();
            let mut block = [0; 65536];
            loop {
                let n = reader.read(&mut block)?;
                prefix.extend_from_slice(&block[..n]);
                if n == 0 || !prefix.trim_ascii_start().is_empty() {
                    break;
                }
            }
            let framing = detect(&prefix);
            return crate::drivers::export_archive::records::ranges(
                &mut std::io::Cursor::new(prefix).chain(reader),
                framing,
            );
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
            let count = if eof {
                records.len()
            } else {
                records.len().saturating_sub(1)
            };
            for &(start, end) in &records[..count] {
                if start > end || end > pending.len() {
                    return Err(std::io::Error::other("invalid source record range"));
                }
                ranges.push((base + start, base + end));
            }
            if eof {
                return Ok(ranges);
            }
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

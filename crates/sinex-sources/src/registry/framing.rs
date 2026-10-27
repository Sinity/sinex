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

    /// Choose the export framing from the first bytes of the stream.
    pub const fn detected(slice: SliceFramer, detect: fn(&[u8]) -> Framing) -> Self {
        Self {
            slice,
            reader: ReaderFraming::Detected(detect),
        }
    }

    pub fn frame(self, bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
        (self.slice)(bytes)
    }

    /// Exact source byte ranges. Bodies hold only the undecided record and read
    /// buffer; the returned range metadata grows with record count. A whole
    /// record can be large.
    pub fn ranges(self, reader: &mut dyn Read) -> std::io::Result<Vec<(usize, usize)>> {
        if let ReaderFraming::Export(framing) = self.reader {
            let records = crate::drivers::export_archive::records::ranges(reader, framing)?;
            return if framing == Framing::Whole && records.is_empty() {
                Ok(self.frame(&[]).0)
            } else {
                Ok(records)
            };
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
        let mut ranges = Vec::new();
        self.visit_incremental(reader, |range| ranges.push(range))?;
        Ok(ranges)
    }

    /// The newest completed ranges, bounded by record count and record bytes.
    /// One record larger than the byte window is kept whole. The boolean says
    /// whether every completed record fit, so a restart knows when to stop.
    pub fn tail_ranges(
        self,
        reader: &mut dyn Read,
        bounds: crate::drivers::append_file::TailBounds,
    ) -> std::io::Result<(Vec<(usize, usize)>, bool)> {
        let mut complete = true;
        let mut tail = std::collections::VecDeque::new();
        let mut bytes = 0usize;
        let mut remember = |(start, end)| {
            tail.push_back((start, end));
            bytes += end - start;
            while tail.len() > bounds.records || (bytes > bounds.bytes && tail.len() > 1) {
                let (start, end) = tail.pop_front().expect("nonempty tail");
                bytes -= end - start;
                complete = false;
            }
        };
        if matches!(self.reader, ReaderFraming::Incremental) {
            self.visit_incremental(reader, &mut remember)?;
        } else {
            for range in self.ranges(reader)? {
                remember(range);
            }
        }
        Ok((tail.into_iter().collect(), complete))
    }

    fn visit_incremental(
        self,
        reader: &mut dyn Read,
        mut visit: impl FnMut((usize, usize)),
    ) -> std::io::Result<()> {
        const CHUNK: usize = 64 << 10;
        let mut pending = Vec::new();
        let mut base = 0usize;
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
                visit((base + start, base + end));
            }
            if eof {
                return Ok(());
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

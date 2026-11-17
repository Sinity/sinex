//! Acquisition stops reading when its submission is abandoned.
use super::MaterialAccess;
use crate::writer::AdmissionCheck;
use sinex_source_core::drivers::MaterialSink;
use std::io::{Read, Seek, SeekFrom};

pub(crate) struct CheckedReader<R> {
    pub(crate) input: R,
    pub(crate) check: AdmissionCheck,
}

impl<R: Read> Read for CheckedReader<R> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        (self.check)().map_err(std::io::Error::other)?;
        self.input.read(bytes)
    }
}

impl<R: Seek> Seek for CheckedReader<R> {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        (self.check)().map_err(std::io::Error::other)?;
        self.input.seek(position)
    }
}

pub(super) struct CheckedSink<'a> {
    pub(super) material: &'a MaterialAccess,
    pub(super) check: AdmissionCheck,
}

impl MaterialSink for CheckedSink<'_> {
    fn put(&self, bytes: &[u8]) -> Result<String, String> {
        self.material
            .store_reader(CheckedReader {
                input: std::io::Cursor::new(bytes),
                check: self.check.clone(),
            })
            .map_err(|error| error.to_string())
    }

    fn put_file(&self, path: &std::path::Path) -> Result<String, String> {
        (self.check)().map_err(|error| error.to_string())?;
        let input = std::fs::File::open(path)
            .map_err(|error| crate::Error::file(path, error).to_string())?;
        self.material
            .store_reader(CheckedReader {
                input: std::io::BufReader::new(input),
                check: self.check.clone(),
            })
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests;
